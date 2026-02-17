/**
 * GenDB Orchestrator v20
 *
 * Two-phase agentic system with true per-query pipeline parallelism:
 *   Phase 1 (Offline Data Storage Optimization):
 *     Workload Analyzer → Storage/Index Designer (design + ingest + build_indexes + per-query guides)
 *   Phase 2 (Online Per-Query Pipeline-Parallel Optimization):
 *     Each query runs independently: CodeGen → Execute → [Optimize → Execute]* → done
 *     LLM calls gated by Semaphore, execution serialized via ExecutionQueue.
 *
 * v20 changes: True per-query pipelining (no batch boundaries), per-query benchmark context
 * (replaces full JSON dump), stall detection with restructure override, knowledge base
 * technique files, anti-pattern scan in optimizer, getBestBaselineTime fix, maxIterations=10.
 *
 * Features: Per-agent LLM model config, per-operation timing, notification-based execution queue,
 * Mutex on run.json, PipelineProgressTracker, Iter0Barrier for storage checkpoint.
 *
 * Usage: node src/gendb/orchestrator.mjs [--schema <path>] [--queries <path>] [--data-dir <path>]
 *        [--gendb-dir <path>] [--sf <N>] [--max-iterations <N>] [--model <name>]
 *        [--model-override <name>] [--optimization-target <target>] [--max-concurrent <N>]
 */

import { spawn } from "child_process";
import { readFile, writeFile, mkdir, cp } from "fs/promises";
import { resolve, dirname } from "path";
import { existsSync, readFileSync, rmSync } from "fs";
import { fileURLToPath } from "url";
import {
  DEFAULT_SCHEMA,
  DEFAULT_QUERIES,
  BENCHMARKS_DIR,
  getDataDir,
  getGendbDir,
} from "./config.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const KNOWLEDGE_BASE_PATH = resolve(__dirname, "knowledge");
const COMPARE_TOOL_PATH = resolve(__dirname, "tools", "compare_results.py");
import { defaults } from "./gendb.config.mjs";
import { config as workloadAnalyzerConfig } from "./agents/workload-analyzer/index.mjs";
import { config as storageDesignerConfig } from "./agents/storage-index-designer/index.mjs";
import { config as queryOptimizerConfig } from "./agents/query-optimizer/index.mjs";
import { config as codeGeneratorConfig } from "./agents/code-generator/index.mjs";
import {
  createRunId,
  getWorkloadName,
  createRunDir,
  updateLatestSymlink,
  createQueryDir,
} from "./utils/paths.mjs";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Parse CLI args, merging over gendb.config defaults. */
function parseArgs(argv) {
  const args = {
    schema: DEFAULT_SCHEMA,
    queries: DEFAULT_QUERIES,
    dataDir: null,
    gendbDir: null,
    scaleFactor: defaults.scaleFactor,
    maxIterations: defaults.maxOptimizationIterations,
    model: defaults.model,
    modelOverride: null,
    optimizationTarget: defaults.optimizationTarget,
    maxConcurrent: defaults.maxConcurrentQueries,
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--schema" && argv[i + 1]) args.schema = resolve(argv[++i]);
    if (argv[i] === "--queries" && argv[i + 1]) args.queries = resolve(argv[++i]);
    if (argv[i] === "--data-dir" && argv[i + 1]) args.dataDir = resolve(argv[++i]);
    if (argv[i] === "--gendb-dir" && argv[i + 1]) args.gendbDir = resolve(argv[++i]);
    if (argv[i] === "--sf" && argv[i + 1]) args.scaleFactor = parseInt(argv[++i], 10);
    if (argv[i] === "--max-iterations" && argv[i + 1]) args.maxIterations = parseInt(argv[++i], 10);
    if (argv[i] === "--model" && argv[i + 1]) args.model = argv[++i];
    if (argv[i] === "--model-override" && argv[i + 1]) args.modelOverride = argv[++i];
    if (argv[i] === "--optimization-target" && argv[i + 1]) args.optimizationTarget = argv[++i];
    if (argv[i] === "--max-concurrent" && argv[i + 1]) args.maxConcurrent = parseInt(argv[++i], 10);
  }
  if (!args.dataDir) {
    args.dataDir = getDataDir(defaults.targetBenchmark, args.scaleFactor);
  }
  if (!args.gendbDir) {
    args.gendbDir = getGendbDir(defaults.targetBenchmark, args.scaleFactor);
  }
  return args;
}

/**
 * Resolve which LLM model to use for a given agent.
 * CLI --model-override forces all agents to one model (for testing).
 * Otherwise use agentModels config, falling back to args.model.
 */
function getAgentModel(agentConfigName, args) {
  if (args.modelOverride) return args.modelOverride;
  return defaults.agentModels[agentConfigName] || args.model;
}

/**
 * Read, update, and write run.json atomically.
 */
async function updateRunMeta(runDir, updater) {
  const runMetaPath = resolve(runDir, "run.json");
  const runMeta = JSON.parse(await readFile(runMetaPath, "utf-8"));
  updater(runMeta);
  await writeFile(runMetaPath, JSON.stringify(runMeta, null, 2));
  return runMeta;
}

// ---------------------------------------------------------------------------
// Model pricing (per million tokens)
// ---------------------------------------------------------------------------

const MODEL_PRICING = {
  sonnet: { input: 3, output: 15, cache_read: 0.30, cache_creation: 3.75 },
  haiku: { input: 0.80, output: 4, cache_read: 0.08, cache_creation: 1 },
  opus: { input: 15, output: 75, cache_read: 1.50, cache_creation: 18.75 },
};

function estimateCost(model, tokens) {
  const pricing = MODEL_PRICING[model] || MODEL_PRICING.sonnet;
  const perM = 1_000_000;
  return (tokens.input * pricing.input) / perM
    + (tokens.output * pricing.output) / perM
    + ((tokens.cache_read || 0) * pricing.cache_read) / perM
    + ((tokens.cache_creation || 0) * pricing.cache_creation) / perM;
}

// ---------------------------------------------------------------------------
// Telemetry tracking
// ---------------------------------------------------------------------------

const telemetryData = {
  total_wall_clock_ms: 0,
  total_tokens: { input: 0, output: 0, cache_read: 0, cache_creation: 0 },
  total_cost_usd: 0,
  phases: {},
};

const runStartTime = Date.now();

function recordAgentTelemetry(phase, agentName, durationMs, tokens, costUsd) {
  if (!telemetryData.phases[phase]) {
    telemetryData.phases[phase] = { total_ms: 0, agents: {} };
  }
  const p = telemetryData.phases[phase];
  p.total_ms += durationMs;
  if (!p.agents[agentName]) {
    p.agents[agentName] = { duration_ms: 0, tokens: { input: 0, output: 0, cache_read: 0, cache_creation: 0 }, cost_usd: 0 };
  }
  const a = p.agents[agentName];
  a.duration_ms += durationMs;
  a.tokens.input += tokens.input;
  a.tokens.output += tokens.output;
  a.tokens.cache_read += tokens.cache_read || 0;
  a.tokens.cache_creation += tokens.cache_creation || 0;
  a.cost_usd += costUsd;

  telemetryData.total_tokens.input += tokens.input;
  telemetryData.total_tokens.output += tokens.output;
  telemetryData.total_tokens.cache_read += tokens.cache_read || 0;
  telemetryData.total_tokens.cache_creation += tokens.cache_creation || 0;
  telemetryData.total_cost_usd += costUsd;
}

function formatDuration(ms) {
  const totalSec = Math.floor(ms / 1000);
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return min > 0 ? `${min}m ${sec}s` : `${sec}s`;
}

/**
 * Invoke a Claude Code subprocess with the given system prompt and user prompt.
 * Returns { result, durationMs, tokens, costUsd }.
 */
function runAgent(name, { systemPrompt, userPrompt, allowedTools, model, cwd }) {
  return new Promise((resolveP, rejectP) => {
    const args = [
      "--print",
      "--system-prompt", systemPrompt,
      "--output-format", "json",
      "--permission-mode", "bypassPermissions",
      "--allowedTools", allowedTools.join(","),
    ];
    if (model) args.push("--model", model);
    args.push(userPrompt);

    console.log(`\n[${"=".repeat(60)}]`);
    console.log(`[Orchestrator] Spawning agent: ${name}`);
    console.log(`[${"=".repeat(60)}]\n`);

    const startTime = Date.now();

    const child = spawn("claude", args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env },
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });

    child.on("close", (code, signal) => {
      const durationMs = Date.now() - startTime;
      if (code !== 0) {
        const exitInfo = signal ? `signal ${signal}` : `exit ${code}`;
        console.error(`\n[Orchestrator] Agent "${name}" exited with ${exitInfo} (${formatDuration(durationMs)})`);
        if (stderr) console.error(`[stderr] ${stderr}`);
        rejectP(new Error(`Agent "${name}" failed (${exitInfo}): ${stderr}`));
      } else {
        let resultText = stdout;
        let tokens = { input: 0, output: 0, cache_read: 0, cache_creation: 0 };
        let costUsd = 0;
        try {
          const parsed = JSON.parse(stdout);
          resultText = parsed.result || "";
          const usage = parsed.usage || {};
          tokens = {
            input: usage.input_tokens || 0,
            output: usage.output_tokens || 0,
            cache_read: usage.cache_read_input_tokens || 0,
            cache_creation: usage.cache_creation_input_tokens || 0,
          };
          costUsd = estimateCost(model || defaults.model, tokens);
        } catch {
          resultText = stdout;
        }

        console.log(`\n[Orchestrator] Agent "${name}" completed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)})`);

        // Log agent output to file for debugging (replaces agent-generated docs)
        if (cwd) {
          const logPath = resolve(cwd, `${name.replace(/[\s/]+/g, "_").toLowerCase()}_output.log`);
          import("fs/promises").then(fs => fs.writeFile(logPath, resultText).catch(() => {}));
        }

        resolveP({ result: resultText, durationMs, tokens, costUsd });
      }
    });

    child.on("error", (err) => {
      rejectP(new Error(`Failed to spawn agent "${name}": ${err.message}`));
    });
  });
}

/** Safely read and parse a JSON file, returning null on failure. */
async function readJSON(path) {
  try {
    return JSON.parse(await readFile(path, "utf-8"));
  } catch {
    return null;
  }
}

/** Copy a directory recursively. */
async function copyDir(src, dest) {
  await cp(src, dest, { recursive: true });
}

/**
 * Extract context relevant to a single query from full storage design and workload analysis.
 * Filters to only the tables referenced in the query SQL.
 */
function extractQueryContext(query, storageDesign, workloadAnalysis) {
  const sqlLower = query.sql.toLowerCase();
  const allTables = Object.keys(storageDesign.tables || {});
  const relevantTables = allTables.filter(t => sqlLower.includes(t.toLowerCase()));

  const filteredStorage = {
    persistent_storage: storageDesign.persistent_storage,
    tables: {},
    type_mappings: storageDesign.type_mappings,
    date_encoding: storageDesign.date_encoding,
    hardware_config: storageDesign.hardware_config,
  };
  for (const t of relevantTables) {
    filteredStorage.tables[t] = storageDesign.tables[t];
  }

  const filteredWorkload = { tables: {} };
  const waTables = workloadAnalysis.tables || {};
  for (const t of relevantTables) {
    if (waTables[t]) filteredWorkload.tables[t] = waTables[t];
  }
  const queries = workloadAnalysis.queries || workloadAnalysis.query_analysis || [];
  const qArr = Array.isArray(queries) ? queries : Object.values(queries);
  const qMatch = qArr.find(q => q.id === query.id || q.query_id === query.id);
  if (qMatch) filteredWorkload.query = qMatch;

  return { filteredStorage, filteredWorkload };
}

/** Format benchmark comparison for a single query — extracts only relevant data. */
function formatPerQueryBenchmarkContext(benchmarkResults, queryId, currentTimeMs) {
  if (!benchmarkResults) return "";
  const rows = [];
  let bestTime = Infinity;
  let bestSystem = "";
  for (const [system, data] of Object.entries(benchmarkResults)) {
    const qData = data?.[queryId] || data?.queries?.[queryId];
    const time = qData?.min_ms || qData?.average_ms || qData?.time_ms;
    if (time != null) {
      rows.push({ system, time });
      if (time < bestTime) { bestTime = time; bestSystem = system; }
    }
  }
  if (rows.length === 0) return "";
  rows.sort((a, b) => a.time - b.time);
  const lines = [
    "",
    `## Performance Comparison for ${queryId}`,
    "| System | Time (ms) |",
    "|--------|-----------|",
    ...rows.map(r => `| ${r.system} | ${Math.round(r.time)} |`),
  ];
  if (currentTimeMs != null) {
    lines.push(`| Current GenDB | ${Math.round(currentTimeMs)} |`);
    const gap = (currentTimeMs / bestTime).toFixed(1);
    if (currentTimeMs > bestTime * 5) {
      lines.push("", `Best engine: ${Math.round(bestTime)}ms (${bestSystem}). Gap: ${gap}x. Fundamental restructuring needed.`);
    } else if (currentTimeMs > bestTime * 1.2) {
      lines.push("", `Best engine: ${Math.round(bestTime)}ms (${bestSystem}). Gap: ${gap}x. Further optimization needed.`);
    } else {
      lines.push("", `Best engine: ${Math.round(bestTime)}ms (${bestSystem}). GenDB is competitive.`);
    }
  }
  return lines.join("\n");
}

/** Parse queries.sql into individual queries with IDs. */
function parseQueryFile(queriesText) {
  const queries = [];
  // Split on query separators: lines starting with "-- Q" or numbered query comments
  const parts = queriesText.split(/(?=--\s*(?:Q|Query)\s*\d)/i);
  let queryNum = 1;
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed || !trimmed.includes("SELECT")) continue;

    // Try to extract query ID from comment
    const idMatch = trimmed.match(/--\s*(?:Q|Query)\s*(\d+)/i);
    const id = idMatch ? `Q${idMatch[1]}` : `Q${queryNum}`;
    queries.push({ id, sql: trimmed });
    queryNum++;
  }

  // Fallback: if no queries found via comments, split by semicolons
  if (queries.length === 0) {
    const stmts = queriesText.split(";").filter(s => s.trim().toUpperCase().includes("SELECT"));
    for (let i = 0; i < stmts.length; i++) {
      queries.push({ id: `Q${i + 1}`, sql: stmts[i].trim() + ";" });
    }
  }

  return queries;
}

// ---------------------------------------------------------------------------
// ExecutionQueue: notification-based serial execution (replaces Semaphore)
// ---------------------------------------------------------------------------

/**
 * Notification-based execution queue. Callers await a single promise;
 * Node.js event loop handles notification when their turn arrives.
 * Zero polling, zero token/resource consumption while waiting.
 */
class ExecutionQueue {
  constructor() {
    this.queue = [];
    this.running = false;
  }

  requestExecution(queryId, executionFn) {
    return new Promise((resolve, reject) => {
      this.queue.push({ queryId, executionFn, resolve, reject });
      if (!this.running) this._processNext();
    });
  }

  async _processNext() {
    if (this.queue.length === 0) {
      this.running = false;
      return;
    }
    this.running = true;
    const { queryId, executionFn, resolve, reject } = this.queue.shift();
    try {
      resolve(await executionFn());
    } catch (err) {
      reject(err);
    } finally {
      this._processNext();
    }
  }
}

/** Simple semaphore for concurrency limiting (used for parallel LLM calls). */
class Semaphore {
  constructor(max) {
    this.max = max;
    this.current = 0;
    this.queue = [];
  }
  acquire() {
    return new Promise((resolve) => {
      if (this.current < this.max) {
        this.current++;
        resolve();
      } else {
        this.queue.push(resolve);
      }
    });
  }
  release() {
    this.current--;
    if (this.queue.length > 0) {
      this.current++;
      this.queue.shift()();
    }
  }
}

// ---------------------------------------------------------------------------
// Programmatic shouldContinue() (replaces Orchestrator Agent LLM)
// ---------------------------------------------------------------------------

function getBestBaselineTime(benchmarkResults, queryId) {
  if (!benchmarkResults) return null;
  let best = Infinity;
  for (const [system, data] of Object.entries(benchmarkResults)) {
    const qData = data?.queries?.[queryId] || data?.[queryId];
    const time = qData?.min_ms || qData?.average_ms || qData?.time_ms || qData?.median_ms;
    if (time && time < best) best = time;
  }
  return best === Infinity ? null : best;
}

function shouldContinue(queryId, history, benchmarkResults, execResults, iteration, maxIter) {
  if (iteration > maxIter) return { action: 'stop', reason: 'Max iterations reached' };

  // Never stop if results are still incorrect
  if (execResults?.validation?.status === 'fail') {
    return { action: 'continue', reason: 'Fix correctness first' };
  }

  const timing = execResults?.timing_ms;
  if (timing && timing < 50) return { action: 'stop', reason: 'Already fast (<50ms)' };

  // Already 1.2x faster than best baseline
  if (benchmarkResults && timing) {
    const best = getBestBaselineTime(benchmarkResults, queryId);
    if (best && timing < best / 1.2) return { action: 'stop', reason: `1.2x faster than baseline (${timing.toFixed(1)}ms vs ${best.toFixed(1)}ms)` };
  }

  return { action: 'continue', reason: 'Optimization potential remains' };
}

// ---------------------------------------------------------------------------
// Execution improvement check (replaces evaluation.json-based check)
// ---------------------------------------------------------------------------

function checkExecutionImprovement(prevExec, newExec) {
  // Reject if correctness regressed
  if (prevExec?.validation?.status === 'pass' && newExec?.validation?.status === 'fail') return false;
  // Accept if correctness improved
  if (prevExec?.validation?.status !== 'pass' && newExec?.validation?.status === 'pass') return true;
  // Compare timing
  if (prevExec?.timing_ms && newExec?.timing_ms) return newExec.timing_ms < prevExec.timing_ms;
  return false;
}

// ---------------------------------------------------------------------------
// Phase 1: Offline Data Storage Optimization
// ---------------------------------------------------------------------------

async function runOfflineStorageOptimization(args, runDir, schema, queries) {
  console.log("\n[Orchestrator] ========== PHASE 1: OFFLINE DATA STORAGE OPTIMIZATION ==========\n");

  await updateRunMeta(runDir, (meta) => {
    meta.dataDir = args.dataDir;
    meta.gendbDir = args.gendbDir;
    meta.scaleFactor = args.scaleFactor;
    meta.maxIterations = args.maxIterations;
    meta.model = args.model;
    meta.optimizationTarget = args.optimizationTarget;
    meta.phase1 = {
      status: "running",
      steps: {
        workload_analysis: { status: "pending" },
        storage_design: { status: "pending" },
        data_ingestion: { status: "pending" },
        index_building: { status: "pending" },
      },
    };
  });

  // Clear gendb storage from previous runs
  if (existsSync(args.gendbDir)) {
    rmSync(args.gendbDir, { recursive: true, force: true });
    console.log(`[Orchestrator] Cleared previous gendb storage at ${args.gendbDir}`);
  }

  // --- Step 1: Workload Analysis ---
  console.log("\n[Orchestrator] === Step 1: Workload Analysis ===");
  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.workload_analysis.status = "running";
    meta.phase1.steps.workload_analysis.startedAt = new Date().toISOString();
  });

  const analyzerSystemPrompt = await readFile(workloadAnalyzerConfig.promptPath, "utf-8");
  const workloadAnalysisPath = resolve(runDir, "workload_analysis.json");

  const analyzerUserPrompt = [
    "Analyze the following TPC-H workload.",
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    "",
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    `Read relevant knowledge files to inform your analysis.`,
    "",
    "## Schema",
    "```sql",
    schema.trim(),
    "```",
    "",
    "## Queries",
    "```sql",
    queries.trim(),
    "```",
    "",
    `## Data Directory (source .tbl files for sampling)`,
    `${args.dataDir}`,
    `Use this to profile actual data: row counts (wc -l), column samples (head -100), selectivity estimates.`,
    "",
    `IMPORTANT: You MUST use the Write tool to write your JSON analysis to EXACTLY this path: ${workloadAnalysisPath}`,
    `Use this EXACT file path — do NOT change the filename or extension. Do NOT just print it.`,
  ].join("\n");

  const waResult = await runAgent(workloadAnalyzerConfig.name, {
    systemPrompt: analyzerSystemPrompt,
    userPrompt: analyzerUserPrompt,
    allowedTools: workloadAnalyzerConfig.allowedTools,
    model: getAgentModel("workload_analyzer", args),
    cwd: runDir,
  });
  recordAgentTelemetry("phase1", "workload_analyzer", waResult.durationMs, waResult.tokens, waResult.costUsd);

  const analysis = await readJSON(workloadAnalysisPath);
  if (!analysis) {
    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.workload_analysis.status = "failed";
      meta.phase1.status = "failed";
      meta.status = "failed";
    });
    throw new Error("Workload analysis failed — could not read/parse workload_analysis.json.");
  }

  console.log("\n[Orchestrator] Workload analysis written successfully.");
  console.log(`[Orchestrator] Tables analyzed: ${Object.keys(analysis.tables || {}).length}`);

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.workload_analysis.status = "completed";
    meta.phase1.steps.workload_analysis.completedAt = new Date().toISOString();
  });

  // --- Step 2: Storage/Index Design + Data Ingestion + Index Building ---
  console.log("\n[Orchestrator] === Step 2: Storage/Index Design + Ingestion + Index Building ===");
  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.storage_design.status = "running";
    meta.phase1.steps.storage_design.startedAt = new Date().toISOString();
  });

  const designerSystemPrompt = await readFile(storageDesignerConfig.promptPath, "utf-8");
  const storageDesignPath = resolve(runDir, "storage_design.json");
  const generatedIngestDir = resolve(runDir, "generated_ingest");
  await mkdir(generatedIngestDir, { recursive: true });

  // Create query_guides directory for per-query storage guides
  const queryGuidesDir = resolve(runDir, "query_guides");
  await mkdir(queryGuidesDir, { recursive: true });

  // Parse queries so we can pass them to the designer
  const parsedQueries = parseQueryFile(queries);

  const designerUserPrompt = [
    "Design the storage layout, generate ingestion + index building code, compile and run both.",
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    "",
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    `Read relevant knowledge files (especially storage/ and indexing/) to inform your design decisions.`,
    "",
    "## Workload Analysis",
    `Read the workload analysis from: ${workloadAnalysisPath}`,
    "",
    "## Schema",
    "```sql",
    schema.trim(),
    "```",
    "",
    `## Queries in Workload`,
    `The following queries will be optimized in Phase 2. Generate a per-query storage guide for each.`,
    ...parsedQueries.map(q => `### ${q.id}\n\`\`\`sql\n${q.sql}\n\`\`\``),
    ``,
    `## Query Guides Output Directory`,
    `Write per-query storage guides to: ${queryGuidesDir}`,
    `Each file: <QUERY_ID>_storage_guide.md`,
    ``,
    `## Data Directory (source .tbl files)`,
    `${args.dataDir}`,
    "",
    `## GenDB Storage Directory (output)`,
    `Write binary columnar data to: ${args.gendbDir}`,
    "",
    `## Output Directory for Generated Code`,
    `Write ingest.cpp, build_indexes.cpp, and Makefile to: ${generatedIngestDir}`,
    "",
    `IMPORTANT: You MUST:`,
    `1. Write your JSON design to EXACTLY this path: ${storageDesignPath}`,
    `2. Generate ingest.cpp, build_indexes.cpp, and Makefile in: ${generatedIngestDir}`,
    `3. Compile: cd ${generatedIngestDir} && make clean && make all`,
    `4. Run ingestion: cd ${generatedIngestDir} && ./ingest ${args.dataDir} ${args.gendbDir}`,
    `5. Run index building: cd ${generatedIngestDir} && ./build_indexes ${args.gendbDir}`,
    `6. Generate per-query storage guides in: ${queryGuidesDir}`,
  ].join("\n");

  const sdResult = await runAgent(storageDesignerConfig.name, {
    systemPrompt: designerSystemPrompt,
    userPrompt: designerUserPrompt,
    allowedTools: storageDesignerConfig.allowedTools,
    model: getAgentModel("storage_designer", args),
    cwd: runDir,
  });
  recordAgentTelemetry("phase1", "storage_designer", sdResult.durationMs, sdResult.tokens, sdResult.costUsd);

  const design = await readJSON(storageDesignPath);
  if (!design) {
    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.storage_design.status = "failed";
      meta.phase1.status = "failed";
      meta.status = "failed";
    });
    throw new Error("Storage design failed — could not read/parse storage_design.json.");
  }

  console.log("\n[Orchestrator] Storage design + ingestion + index building completed.");
  console.log(`[Orchestrator] Tables designed: ${Object.keys(design.tables || {}).length}`);

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.storage_design.status = "completed";
    meta.phase1.steps.storage_design.completedAt = new Date().toISOString();
    meta.phase1.steps.data_ingestion.status = "completed";
    meta.phase1.steps.data_ingestion.completedAt = new Date().toISOString();
    meta.phase1.steps.index_building.status = "completed";
    meta.phase1.steps.index_building.completedAt = new Date().toISOString();
  });

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.status = "completed";
    meta.phase1.completedAt = new Date().toISOString();
  });

  return { workloadAnalysisPath, storageDesignPath };
}

// ---------------------------------------------------------------------------
// Storage Issue Detection & Re-ingestion
// ---------------------------------------------------------------------------

/**
 * Scan iteration 0 directories for storage_issue.json files.
 * Returns array of { queryId, issue, details } or empty array if no issues.
 */
async function detectStorageIssues(parsedQueries, runDir) {
  const issues = [];
  for (const query of parsedQueries) {
    const issueFile = resolve(runDir, "queries", query.id, "iter_0", "storage_issue.json");
    const issue = await readJSON(issueFile);
    if (issue) {
      issues.push({ queryId: query.id, ...issue });
    }
  }
  return issues;
}

/**
 * Re-invoke Storage/Index Designer with error context to fix ingestion bugs.
 */
async function reIngestWithFixes(args, runDir, storageDesignPath, workloadAnalysisPath, schema, detectedIssues, parsedQueries) {
  console.log(`\n[Orchestrator] === RE-INGESTION: Fixing ${detectedIssues.length} storage issue(s) ===`);

  // Read previous design for reference
  const previousDesign = await readJSON(storageDesignPath);

  // Clear corrupted storage
  if (existsSync(args.gendbDir)) {
    rmSync(args.gendbDir, { recursive: true, force: true });
    console.log(`[Orchestrator] Cleared corrupted gendb storage at ${args.gendbDir}`);
  }

  const designerSystemPrompt = await readFile(storageDesignerConfig.promptPath, "utf-8");
  const generatedIngestDir = resolve(runDir, "generated_ingest");
  const queryGuidesDir = resolve(runDir, "query_guides");

  const issuesSummary = detectedIssues.map(i =>
    `- ${i.queryId}: ${i.issue} — ${i.details}`
  ).join("\n");

  const designerUserPrompt = [
    "Re-design and re-ingest: the previous ingestion produced CORRUPTED data. Fix the issues below.",
    "",
    "## DETECTED STORAGE ISSUES (MUST FIX ALL)",
    issuesSummary,
    "",
    "## Previous Design (for reference — do NOT repeat the same bugs)",
    "```json",
    JSON.stringify(previousDesign, null, 2),
    "```",
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    `Read relevant knowledge files (especially storage/ and indexing/) to inform your design decisions.`,
    "",
    "## Workload Analysis",
    `Read the workload analysis from: ${workloadAnalysisPath}`,
    "",
    "## Schema",
    "```sql",
    schema.trim(),
    "```",
    "",
    `## Queries in Workload`,
    `The following queries will be optimized in Phase 2. Generate a per-query storage guide for each.`,
    ...(parsedQueries || []).map(q => `### ${q.id}\n\`\`\`sql\n${q.sql}\n\`\`\``),
    ``,
    `## Query Guides Output Directory`,
    `Write per-query storage guides to: ${queryGuidesDir}`,
    `Each file: <QUERY_ID>_storage_guide.md`,
    ``,
    `## Data Directory (source .tbl files)`,
    `${args.dataDir}`,
    "",
    `## GenDB Storage Directory (output)`,
    `Write binary columnar data to: ${args.gendbDir}`,
    "",
    `## Output Directory for Generated Code`,
    `Write ingest.cpp, build_indexes.cpp, and Makefile to: ${generatedIngestDir}`,
    "",
    `IMPORTANT: You MUST:`,
    `1. Write your JSON design to EXACTLY this path: ${storageDesignPath}`,
    `2. Generate ingest.cpp, build_indexes.cpp, and Makefile in: ${generatedIngestDir}`,
    `3. Compile: cd ${generatedIngestDir} && make clean && make all`,
    `4. Run ingestion: cd ${generatedIngestDir} && ./ingest ${args.dataDir} ${args.gendbDir}`,
    `5. Run index building: cd ${generatedIngestDir} && ./build_indexes ${args.gendbDir}`,
    `6. Generate per-query storage guides in: ${queryGuidesDir}`,
    "",
    `Pay special attention to the detected issues above. Verify that the fixes work by spot-checking the binary output.`,
  ].join("\n");

  const sdResult = await runAgent(storageDesignerConfig.name, {
    systemPrompt: designerSystemPrompt,
    userPrompt: designerUserPrompt,
    allowedTools: storageDesignerConfig.allowedTools,
    model: getAgentModel("storage_designer", args),
    cwd: runDir,
  });
  recordAgentTelemetry("phase2", "storage_designer_reingest", sdResult.durationMs, sdResult.tokens, sdResult.costUsd);

  const design = await readJSON(storageDesignPath);
  if (!design) {
    throw new Error("Re-ingestion failed — could not read/parse storage_design.json after re-ingest.");
  }

  console.log(`[Orchestrator] Re-ingestion completed. Tables: ${Object.keys(design.tables || {}).length}`);
}


// ---------------------------------------------------------------------------
// Phase 2: Online Per-Query Parallel Optimization
// ---------------------------------------------------------------------------

/** Mutex for updateRunMeta — prevents race conditions with concurrent pipeline writes. */
class Mutex {
  constructor() { this._queue = []; this._locked = false; }
  async acquire() {
    if (!this._locked) { this._locked = true; return; }
    await new Promise(resolve => this._queue.push(resolve));
  }
  release() {
    if (this._queue.length > 0) { this._queue.shift()(); }
    else { this._locked = false; }
  }
}

const runMetaMutex = new Mutex();

/** Thread-safe updateRunMeta wrapper. */
async function updateRunMetaSafe(runDir, updater) {
  await runMetaMutex.acquire();
  try {
    return await updateRunMeta(runDir, updater);
  } finally {
    runMetaMutex.release();
  }
}

/** Countdown latch for Iter0 barrier — coordinates storage checkpoint after all iter-0 complete. */
class Iter0Barrier {
  constructor(count) {
    this._remaining = count;
    this._resolve = null;
    this._promise = new Promise(resolve => { this._resolve = resolve; });
    this._results = [];
  }
  report(ctx) {
    this._results.push(ctx);
    this._remaining--;
    if (this._remaining <= 0) this._resolve(this._results);
  }
  async wait() { return this._promise; }
}

/** Pipeline progress tracker — prints real-time per-query stage updates. */
class PipelineProgressTracker {
  constructor(queryIds) {
    this._stages = {};
    for (const qid of queryIds) this._stages[qid] = "pending";
  }
  update(queryId, stage) {
    this._stages[queryId] = stage;
    const summary = Object.entries(this._stages)
      .map(([qid, s]) => `${qid}:${s}`)
      .join("  ");
    console.log(`[Pipeline] ${summary}`);
  }
}

async function runPerQueryParallelOptimization(args, runDir, workloadAnalysisPath, storageDesignPath) {
  const queriesText = await readFile(args.queries, "utf-8");
  const schema = await readFile(args.schema, "utf-8");
  const parsedQueries = parseQueryFile(queriesText);

  if (parsedQueries.length === 0) {
    console.error("[Orchestrator] No queries found in queries file.");
    return;
  }

  console.log(`\n[Orchestrator] ========== PHASE 2: PER-QUERY PARALLEL OPTIMIZATION ==========`);
  console.log(`[Orchestrator] Found ${parsedQueries.length} queries: ${parsedQueries.map(q => q.id).join(", ")}`);
  console.log(`[Orchestrator] Max concurrent: ${args.maxConcurrent}, Max iterations per query: ${args.maxIterations}\n`);

  await updateRunMetaSafe(runDir, (meta) => {
    meta.phase2 = {
      status: "running",
      queries: parsedQueries.map(q => q.id),
      maxConcurrent: args.maxConcurrent,
      pipelines: {},
    };
  });

  // Ground truth directory
  const groundTruthDir = resolve(BENCHMARKS_DIR, defaults.targetBenchmark, "query_results");
  const hasGroundTruth = existsSync(groundTruthDir);

  // Shared resources
  const executionQueue = new ExecutionQueue();
  const semaphore = new Semaphore(args.maxConcurrent);
  const cgSystemPrompt = await readFile(codeGeneratorConfig.promptPath, "utf-8");
  const qoSystemPrompt = await readFile(queryOptimizerConfig.promptPath, "utf-8");
  const iter0Barrier = new Iter0Barrier(parsedQueries.length);
  const progressTracker = new PipelineProgressTracker(parsedQueries.map(q => q.id));

  // Launch all query pipelines in parallel
  const results = await Promise.all(
    parsedQueries.map(async (query) => {
      try {
        return await runQueryFullPipeline(
          query, args, runDir, workloadAnalysisPath, storageDesignPath,
          schema, groundTruthDir, hasGroundTruth,
          executionQueue, semaphore, cgSystemPrompt, qoSystemPrompt,
          iter0Barrier, progressTracker
        );
      } catch (err) {
        console.error(`[Orchestrator] Query pipeline ${query.id} failed: ${err.message}`);
        iter0Barrier.report({ query, error: err.message });
        return { queryId: query.id, status: "failed", error: err.message, bestCppPath: null, iterations: 0 };
      }
    })
  );

  // Update run meta with pipeline results
  await updateRunMetaSafe(runDir, (meta) => {
    for (const r of results) {
      meta.phase2.pipelines[r.queryId] = {
        status: r.status,
        bestCppPath: r.bestCppPath,
        iterations: r.iterations || 0,
      };
    }
    meta.phase2.status = "completed";
    meta.phase2.completedAt = new Date().toISOString();
  });

  // Assemble final build
  await assembleFinalBuild(results, runDir, args);
}

// ---------------------------------------------------------------------------
// Per-Query Full Pipeline (Code Gen → Execute → [Optimize → Execute]*)
// ---------------------------------------------------------------------------

/**
 * Full lifecycle pipeline for a single query.
 * Code generation (LLM, gated by Semaphore)
 * → ExecutionQueue (serial, enters immediately after code gen)
 * → [shouldContinue → Query Optimizer (LLM) → ExecutionQueue]*
 * → done
 *
 * Each query progresses independently. No waiting for other queries between stages.
 */
async function runQueryFullPipeline(
  query, args, runDir, workloadAnalysisPath, storageDesignPath,
  schema, groundTruthDir, hasGroundTruth,
  executionQueue, semaphore, cgSystemPrompt, qoSystemPrompt,
  iter0Barrier, progressTracker
) {
  const queryId = query.id;
  const queryDir = await createQueryDir(runDir, queryId);
  const queryPhase = `query_${queryId}`;
  const cppName = `${queryId.toLowerCase()}.cpp`;

  progressTracker.update(queryId, "codegen");

  // Optimization history for this query
  const historyPath = resolve(queryDir, "optimization_history.json");
  const optimizationHistory = { query_id: queryId, iterations: [] };
  await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

  // Create iter_0 directory
  const iterDir = resolve(queryDir, "iter_0");
  await mkdir(iterDir, { recursive: true });

  const iterCppPath = resolve(iterDir, cppName);
  const iterResultsDir = resolve(iterDir, "results");
  await mkdir(iterResultsDir, { recursive: true });

  // Read hardware info and filtered context
  const storageDesign = await readJSON(storageDesignPath);
  const workloadAnalysis = await readJSON(workloadAnalysisPath);
  const hw = storageDesign?.hardware_config || {};
  const { filteredWorkload } = (storageDesign && workloadAnalysis)
    ? extractQueryContext(query, storageDesign, workloadAnalysis)
    : { filteredWorkload: null };

  // Read per-query storage guide if available
  const guidePath = resolve(runDir, "query_guides", `${queryId}_storage_guide.md`);
  let storageGuide = "";
  try { storageGuide = await readFile(guidePath, "utf-8"); } catch {}

  // === ITERATION 0: CODE GENERATION (gated by semaphore) ===
  await semaphore.acquire();
  try {
    console.log(`\n[Orchestrator] [${queryId}] Iteration 0: Code Generator`);

    const cgUserPrompt = [
      `Generate a correct, self-contained C++ implementation for ${queryId} (iteration 0).`,
      ``,
      `## Optimization Target: ${args.optimizationTarget}`,
      `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
      ``,
      `## Hardware Configuration`,
      `- CPU cores: ${hw.cpu_cores || 'unknown'}`,
      `- Disk type: ${hw.disk_type || 'unknown'}`,
      `- L3 cache: ${hw.l3_cache_mb || 'unknown'} MB`,
      `- Total memory: ${hw.total_memory_gb || 'unknown'} GB`,
      ``,
      storageGuide ? [
        `## Storage & Index Guide (from Storage/Index Designer)`,
        `This guide describes the exact data file formats, index binary layouts, and usage recommendations.`,
        `Use this to load data correctly and leverage pre-built indexes for performance.`,
        ``,
        storageGuide,
      ].join("\n") : "",
      "",
      `## Workload Analysis (${queryId})`,
      "```json",
      JSON.stringify(filteredWorkload, null, 2),
      "```",
      "",
      `- Schema: ${args.schema}`,
      ``,
      formatPerQueryBenchmarkContext(args.benchmarkResults, queryId, null),
      ``,
      `## Query to implement`,
      "```sql",
      query.sql,
      "```",
      ``,
      `## GenDB Storage Directory`,
      `Binary columnar data: ${args.gendbDir}`,
      ``,
      `## Ground Truth (for validation)`,
      hasGroundTruth ? `${groundTruthDir}` : `No ground truth available`,
      ``,
      `## Output`,
      `- Write .cpp file to: ${iterCppPath}`,
      ``,
      `## Critical Reminders`,
      `- [TIMING] instrumentation REQUIRED: wrap each major op in #ifdef GENDB_PROFILE guards (see system prompt pattern)`,
      `- DATE columns = int32_t epoch days (values >3000). Compare as integers. YYYY-MM-DD only for CSV output.`,
      `- DECIMAL columns = int64_t × scale_factor. Compute thresholds yourself. NEVER use double.`,
      `- Dictionary strings: load _dict.txt at runtime to find codes. NEVER hardcode dictionary codes.`,
      `- CSV: comma-delimited, include header row, 2 decimal places for monetary values.`,
      ``,
      `## Validation Loop`,
      `Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o ${queryId.toLowerCase()} ${cppName}`,
      `Run → Validate correctness (up to 2 fix attempts)`,
      hasGroundTruth ? `Compare results: python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${iterResultsDir}` : `No validation available - just compile and run`,
    ].join("\n");

    // Retry loop: up to 2 attempts
    const maxCgAttempts = 2;
    for (let cgAttempt = 1; cgAttempt <= maxCgAttempts; cgAttempt++) {
      const attemptPrompt = cgAttempt === 1
        ? cgUserPrompt
        : `RETRY: Previous attempt failed to produce a .cpp file. You MUST write C++ code to ${iterCppPath} using the Write tool. Do NOT output analysis or planning text only.\n\n${cgUserPrompt}`;

      const cgResult = await runAgent(codeGeneratorConfig.name, {
        systemPrompt: cgSystemPrompt,
        userPrompt: attemptPrompt,
        allowedTools: codeGeneratorConfig.allowedTools,
        model: getAgentModel("code_generator", args),
        cwd: iterDir,
      });
      recordAgentTelemetry(queryPhase, "code_generator", cgResult.durationMs, cgResult.tokens, cgResult.costUsd);

      if (existsSync(iterCppPath)) break;

      if (cgAttempt < maxCgAttempts) {
        console.log(`[Orchestrator] Code Generator attempt ${cgAttempt} failed to produce ${iterCppPath}, retrying...`);
      } else {
        throw new Error(`Code Generator failed to produce ${iterCppPath} after ${maxCgAttempts} attempts`);
      }
    }
  } finally {
    semaphore.release();
  }

  // === ITERATION 0: EXECUTE (enters ExecutionQueue immediately) ===
  progressTracker.update(queryId, "exec-0");
  console.log(`[Orchestrator] [${queryId}] Iteration 0: Executor (compile + run + validate)`);
  await executionQueue.requestExecution(queryId, async () => {
    return await executeQuery(query, iterDir, iterCppPath, args.gendbDir, groundTruthDir, iterResultsDir);
  });

  // Report to Iter0 barrier (for storage checkpoint coordination)
  iter0Barrier.report({ query, queryDir, iterDir, iterCppPath, cppName });

  // === ITERATIONS 1+: OPTIMIZE → EXECUTE loop ===
  let bestIterDir = iterDir;
  let bestCppPath = iterCppPath;
  let bestExecResultsPath = resolve(iterDir, "execution_results.json");

  const { filteredWorkload: qoFilteredWorkload } = (storageDesign && workloadAnalysis)
    ? extractQueryContext(query, storageDesign, workloadAnalysis)
    : { filteredWorkload: null };

  const maxIter = args.maxIterations;
  let previousIterationOutcome = null;

  for (let iteration = 1; iteration <= maxIter; iteration++) {
    // --- Programmatic shouldContinue() check ---
    const bestExecResults = await readJSON(bestExecResultsPath);
    const continueDecision = shouldContinue(queryId, optimizationHistory, args.benchmarkResults, bestExecResults, iteration, maxIter);

    progressTracker.update(queryId, `iter-${iteration}`);
    console.log(`\n[Orchestrator] [${queryId}] --- Iteration ${iteration}/${maxIter} --- shouldContinue: ${continueDecision.action} (${continueDecision.reason})`);

    if (continueDecision.action === 'stop') {
      console.log(`[Orchestrator] [${queryId}] Stopping: ${continueDecision.reason}`);
      progressTracker.update(queryId, "done");
      break;
    }

    // Create iteration directory
    const optIterDir = resolve(queryDir, `iter_${iteration}`);
    await mkdir(optIterDir, { recursive: true });

    const optIterCppPath = resolve(optIterDir, cppName);
    const optIterResultsDir = resolve(optIterDir, "results");
    await mkdir(optIterResultsDir, { recursive: true });
    const optIterExecResultsPath = resolve(optIterDir, "execution_results.json");

    // Copy best code as starting point
    if (bestCppPath && existsSync(bestCppPath)) {
      await writeFile(optIterCppPath, await readFile(bestCppPath, "utf-8"));
    }

    // --- Stall detection ---
    let stallSection = "";
    const recentHistory = optimizationHistory.iterations.slice(-3);
    if (recentHistory.length >= 3 && recentHistory.every(it => !it.improved)) {
      const bestBaseline = getBestBaselineTime(args.benchmarkResults, queryId);
      const currentBest = bestExecResults?.timing_ms;
      if (bestBaseline && currentBest && currentBest > bestBaseline * 5) {
        stallSection = [
          "",
          "## OPTIMIZATION STALL DETECTED",
          `${recentHistory.length} consecutive iterations failed to improve. Current: ${Math.round(currentBest)}ms, best engine: ${Math.round(bestBaseline)}ms (${(currentBest / bestBaseline).toFixed(1)}x gap).`,
          "The current code architecture is fundamentally limited. See optimizer system prompt for stall recovery guidance.",
          "",
        ].join("\n");
        console.log(`[Orchestrator] [${queryId}] STALL DETECTED: ${recentHistory.length} non-improving iterations, ${(currentBest / bestBaseline).toFixed(1)}x gap`);
      }
    }

    // --- Join sampling mandate ---
    let joinSamplingMandate = "";
    const opTimings = bestExecResults?.operation_timings || {};
    const totalTime = parseFloat(opTimings.total || bestExecResults?.timing_ms || 0);
    const joinTime = Object.entries(opTimings)
      .filter(([key]) => key.toLowerCase().includes('join'))
      .reduce((sum, [, val]) => sum + parseFloat(val || 0), 0);
    const joinDominant = totalTime > 0 && (joinTime / totalTime) > 0.4;
    const explicitJoins = (query.sql.match(/\bJOIN\b/gi) || []).length;
    const fromMatch = query.sql.match(/\bFROM\s+([^WHERE;]+)/i);
    const implicitJoins = fromMatch ? (fromMatch[1].split(",").length - 1) : 0;
    const joinCount = explicitJoins + implicitJoins;
    const hadJoinSampling = (optimizationHistory?.iterations || []).some(it =>
      (it.categories || []).includes("join_sampling")
    );
    if (joinCount >= 2 && joinDominant && !hadJoinSampling) {
      joinSamplingMandate = [
        "",
        "## MANDATORY: Data-Driven Join Ordering",
        "This query has a multi-table join that dominates execution time (>40% of total).",
        "You MUST:",
        "1. Generate a sampling program (sampling_join_order.cpp) that tests candidate join orders",
        "2. Compile and run it to empirically determine the best order",
        "3. Implement the query using the empirically-best join order",
        "See joins/join-ordering.md in the knowledge base for the technique.",
        "DO NOT skip this step. The current join order has not been validated.",
      ].join("\n");
    }

    // Build correctness failure section
    let correctnessSection = "";
    if (bestExecResults?.validation?.status === 'fail') {
      const details = bestExecResults.validation.details
        ? JSON.stringify(bestExecResults.validation.details, null, 2)
        : (bestExecResults.validation.output || "unknown mismatch");
      correctnessSection = [
        "",
        "## CORRECTNESS FAILURE — FIX THIS FIRST",
        "The current code produces WRONG results. You MUST fix correctness before any performance optimization.",
        "Mismatch details:",
        "```json",
        details,
        "```",
        "Common causes: wrong filter predicate, encoding/decoding bug, wrong join logic, incorrect aggregation.",
        "",
      ].join("\n");
    }

    // Read storage guide
    const qoGuidePath = resolve(runDir, "query_guides", `${queryId}_storage_guide.md`);
    let qoStorageGuide = "";
    try { qoStorageGuide = await readFile(qoGuidePath, "utf-8"); } catch {}

    // --- Query Optimizer (gated by semaphore) ---
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Query Optimizer`);
    await semaphore.acquire();
    try {
      const qoUserPrompt = [
        `Optimize the existing C++ code for ${queryId}. This is iteration ${iteration}.`,
        stallSection,
        correctnessSection,
        `## Optimization Target: ${args.optimizationTarget}`,
        ``,
        `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
        "",
        qoStorageGuide ? [
          `## Storage & Index Guide (from Storage/Index Designer)`,
          `This guide describes the exact data file formats, index binary layouts, and usage recommendations.`,
          `Use this to leverage pre-built indexes for performance.`,
          ``,
          qoStorageGuide,
        ].join("\n") : "",
        "",
        `## Workload Analysis (${queryId})`,
        "```json",
        JSON.stringify(qoFilteredWorkload, null, 2),
        "```",
        "",
        "## Input Files",
        `- Execution results (timing + validation): ${bestExecResultsPath}`,
        `- Optimization history: ${historyPath}`,
        formatPerQueryBenchmarkContext(args.benchmarkResults, queryId, bestExecResults?.timing_ms),
        "",
        `## Critical Reminders`,
        `- Read optimization_history.json before planning — do not repeat failed approaches`,
        `- After regression, try a DIFFERENT optimization category than what failed`,
        `- Preserve all [TIMING] lines inside #ifdef GENDB_PROFILE guards`,
        `- Do NOT change encoding logic unless fixing a reported bug`,
        `- Comma-delimited CSV output — do not change delimiter`,
        ``,
        `## Hardware Configuration`,
        `- CPU cores: ${hw.cpu_cores || 'unknown'}`,
        `- Disk type: ${hw.disk_type || 'unknown'}`,
        `- L3 cache: ${hw.l3_cache_mb || 'unknown'} MB`,
        `- Total memory: ${hw.total_memory_gb || 'unknown'} GB`,
        joinSamplingMandate,
        "",
        previousIterationOutcome ? `## Previous Iteration Outcome\n${previousIterationOutcome}\n` : "",
        `## GenDB Storage Directory`,
        `${args.gendbDir}`,
        "",
        `## Query Code`,
        `Read and modify: ${optIterCppPath}`,
        "",
        `## Original SQL`,
        "```sql",
        query.sql,
        "```",
        "",
        `## Compilation`,
        `Compile (do NOT run — Executor handles validation):`,
        `  g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o ${resolve(optIterDir, queryId.toLowerCase())} ${optIterCppPath}`,
        `Ensure the code compiles successfully (up to 3 fix attempts).`,
      ].join("\n");

      const qoResult = await runAgent(queryOptimizerConfig.name, {
        systemPrompt: qoSystemPrompt,
        userPrompt: qoUserPrompt,
        allowedTools: queryOptimizerConfig.allowedTools,
        model: getAgentModel("query_optimizer", args),
        cwd: optIterDir,
      });
      recordAgentTelemetry(queryPhase, "query_optimizer", qoResult.durationMs, qoResult.tokens, qoResult.costUsd);
    } finally {
      semaphore.release();
    }

    // --- Execute via ExecutionQueue (enters immediately after QO) ---
    progressTracker.update(queryId, `exec-${iteration}`);
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Executor (compile + run + validate)`);
    await executionQueue.requestExecution(queryId, async () => {
      return await executeQuery(query, optIterDir, optIterCppPath, args.gendbDir, groundTruthDir, optIterResultsDir);
    });

    // --- Improvement check ---
    const prevExecResults = await readJSON(bestExecResultsPath);
    const newExecResults = await readJSON(optIterExecResultsPath);
    const improved = checkExecutionImprovement(prevExecResults, newExecResults);

    const categories = [];
    if (joinSamplingMandate) categories.push("join_sampling");
    if (opTimings.join && (joinTime / totalTime) > 0.3) categories.push("join");
    if (opTimings.scan_filter && (parseFloat(opTimings.scan_filter) / totalTime) > 0.3) categories.push("io_bound");

    optimizationHistory.iterations.push({
      iteration,
      improved,
      categories,
      timing_ms: newExecResults?.timing_ms || null,
      validation: newExecResults?.validation?.status || null,
    });
    await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

    if (improved) {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration} improved. Keeping changes.`);
      bestIterDir = optIterDir;
      bestCppPath = optIterCppPath;
      bestExecResultsPath = optIterExecResultsPath;
      previousIterationOutcome = `Iteration ${iteration} IMPROVED. Changes kept.`;
    } else {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration} did not improve. Rolling back.`);
      previousIterationOutcome = `Iteration ${iteration} REGRESSED — rolled back. Avoid repeating the same approach.`;
    }
  }

  progressTracker.update(queryId, "done");

  return {
    queryId,
    status: "completed",
    bestCppPath,
    iterations: optimizationHistory.iterations.length,
  };
}

// ---------------------------------------------------------------------------
// Executor (non-LLM): compile, run, validate, parse timing
// ---------------------------------------------------------------------------

/**
 * Execute a query: compile → run → validate → parse timing.
 * Returns execution results JSON and writes execution_results.json to iterDir.
 */
async function executeQuery(query, iterDir, cppPath, gendbDir, groundTruthDir, resultsDir) {
  const binaryPath = resolve(iterDir, query.id.toLowerCase());
  const execResultsPath = resolve(iterDir, "execution_results.json");
  const results = {
    compile: { status: "fail", output: "" },
    run: { status: "fail", output: "", stderr: "", duration_ms: 0 },
    validation: { status: "skipped", output: "" },
    operation_timings: {},
    timing_ms: null,
  };

  await mkdir(resultsDir, { recursive: true });

  // Step 1: Compile (with -fopenmp)
  console.log(`[Executor] [${query.id}] Compiling...`);
  try {
    const compileOutput = await runProcess("g++", [
      "-O3", "-march=native", "-std=c++17", "-Wall", "-lpthread", "-fopenmp",
      "-DGENDB_PROFILE",
      "-o", binaryPath, cppPath,
    ], { cwd: iterDir, timeout: 120000 });
    results.compile = { status: "pass", output: compileOutput };
    console.log(`[Executor] [${query.id}] Compilation successful.`);
  } catch (err) {
    results.compile = { status: "fail", output: err.message };
    console.error(`[Executor] [${query.id}] Compilation failed: ${err.message.slice(0, 200)}`);
    await writeFile(execResultsPath, JSON.stringify(results, null, 2));
    return results;
  }

  // Step 2: Run binary
  console.log(`[Executor] [${query.id}] Running binary...`);
  const runStart = Date.now();
  try {
    const runOutput = await runProcess(binaryPath, [gendbDir, resultsDir], {
      cwd: iterDir, timeout: 300000,
    });
    results.run = {
      status: "pass",
      output: runOutput.stdout || runOutput,
      stderr: runOutput.stderr || "",
      duration_ms: Date.now() - runStart,
    };
    console.log(`[Executor] [${query.id}] Run completed in ${Date.now() - runStart}ms.`);
  } catch (err) {
    results.run = {
      status: "fail",
      output: err.stdout || "",
      stderr: err.message,
      duration_ms: Date.now() - runStart,
    };
    console.error(`[Executor] [${query.id}] Run failed: ${err.message.slice(0, 200)}`);
    await writeFile(execResultsPath, JSON.stringify(results, null, 2));
    return results;
  }

  // Step 3: Parse [TIMING] lines from stdout
  const stdout = typeof results.run.output === "string" ? results.run.output : "";
  const timingRegex = /\[TIMING\]\s+(\w+):\s+([\d.]+)\s*ms/g;
  let match;
  while ((match = timingRegex.exec(stdout)) !== null) {
    results.operation_timings[match[1]] = parseFloat(match[2]);
  }

  // Extract timing_ms: use total minus output (excludes file I/O)
  if (results.operation_timings.total != null && results.operation_timings.output != null) {
    results.timing_ms = results.operation_timings.total - results.operation_timings.output;
  } else if (results.operation_timings.total != null) {
    results.timing_ms = results.operation_timings.total;
  } else {
    const totalMatch = stdout.match(/Execution time:\s*([\d.]+)\s*ms/);
    if (totalMatch) {
      results.timing_ms = parseFloat(totalMatch[1]);
    }
  }

  // Step 4: Validate against ground truth
  if (groundTruthDir && existsSync(groundTruthDir)) {
    console.log(`[Executor] [${query.id}] Validating results...`);
    try {
      const valOutput = await runProcess("python3", [
        COMPARE_TOOL_PATH, groundTruthDir, resultsDir, "--tpch",
      ], { cwd: iterDir, timeout: 60000 });
      const valStr = valOutput.stdout || valOutput;
      results.validation = { status: "pass", output: valStr };
      // Parse JSON output from compare_results.py and check the "match" field
      try {
        const valJson = JSON.parse(typeof valStr === "string" ? valStr : "{}");
        if (valJson.match === false) {
          results.validation.status = "fail";
          results.validation.details = valJson;
        }
      } catch {
        // If JSON parse fails, fall back to string matching
        if (typeof valStr === "string" && (valStr.includes('"match": false') || valStr.includes("FAIL"))) {
          results.validation.status = "fail";
        }
      }
      console.log(`[Executor] [${query.id}] Validation: ${results.validation.status}`);
    } catch (err) {
      results.validation = { status: "fail", output: err.message };
      console.error(`[Executor] [${query.id}] Validation failed: ${err.message.slice(0, 200)}`);
    }
  }

  await writeFile(execResultsPath, JSON.stringify(results, null, 2));
  console.log(`[Executor] [${query.id}] Results written to ${execResultsPath}`);
  return results;
}

/**
 * Run a subprocess and return its stdout. Rejects on non-zero exit.
 */
function runProcess(cmd, cmdArgs, opts = {}) {
  return new Promise((resolveP, rejectP) => {
    const child = spawn(cmd, cmdArgs, {
      cwd: opts.cwd,
      stdio: ["ignore", "pipe", "pipe"],
      timeout: opts.timeout || 120000,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    child.on("close", (code) => {
      if (code === 0) {
        resolveP({ stdout, stderr });
      } else {
        const err = new Error(stderr || `Process exited with code ${code}`);
        err.stdout = stdout;
        err.stderr = stderr;
        rejectP(err);
      }
    });
    child.on("error", (err) => rejectP(err));
  });
}

// ---------------------------------------------------------------------------
// Final Assembly
// ---------------------------------------------------------------------------

async function assembleFinalBuild(results, runDir, args) {
  console.log("\n[Orchestrator] === Final Assembly ===");

  const finalDir = resolve(runDir, "generated");
  const queriesDir = resolve(finalDir, "queries");
  await mkdir(queriesDir, { recursive: true });

  // Collect best .cpp files
  const queryEntries = [];
  for (const r of results) {
    if (r.bestCppPath && existsSync(r.bestCppPath)) {
      const destPath = resolve(queriesDir, `${r.queryId.toLowerCase()}.cpp`);
      const content = await readFile(r.bestCppPath, "utf-8");
      // Wrap internal helper types/functions in anonymous namespace to prevent
      // ODR violations when multiple query .cpp files define same-named templates
      const funcName = `run_${r.queryId.toLowerCase()}`;
      const lines = content.split("\n");
      const funcLineIdx = lines.findIndex(l =>
        new RegExp(`^void\\s+${funcName}\\s*\\(`).test(l)
      );
      if (funcLineIdx > 0) {
        let lastIncludeIdx = 0;
        for (let i = 0; i < lines.length; i++) {
          if (lines[i].trimStart().startsWith("#include")) lastIncludeIdx = i;
        }
        const wrapStart = lastIncludeIdx + 1;
        const wrapEnd = funcLineIdx;

        // Find namespace std { ... } blocks that must NOT be inside anonymous namespace
        const stdBlocks = [];
        for (let i = wrapStart; i < wrapEnd; i++) {
          if (/^\s*namespace\s+std\s*\{/.test(lines[i])) {
            let braceCount = 0;
            let endIdx = i;
            for (let j = i; j < wrapEnd; j++) {
              for (const ch of lines[j]) {
                if (ch === '{') braceCount++;
                if (ch === '}') braceCount--;
              }
              if (braceCount === 0) { endIdx = j; break; }
            }
            stdBlocks.push({ start: i, end: endIdx });
          }
        }

        // Build output, closing/reopening anonymous namespace around std blocks
        const result = [];
        for (let i = 0; i <= lastIncludeIdx; i++) result.push(lines[i]);
        result.push("", "namespace {");
        if (stdBlocks.length === 0) {
          for (let i = wrapStart; i < wrapEnd; i++) result.push(lines[i]);
        } else {
          let pos = wrapStart;
          for (const block of stdBlocks) {
            for (let i = pos; i < block.start; i++) result.push(lines[i]);
            result.push("} // end anonymous namespace", "");
            for (let i = block.start; i <= block.end; i++) result.push(lines[i]);
            result.push("", "namespace {");
            pos = block.end + 1;
          }
          for (let i = pos; i < wrapEnd; i++) result.push(lines[i]);
        }
        result.push("} // end anonymous namespace", "");
        for (let i = wrapEnd; i < lines.length; i++) result.push(lines[i]);
        await writeFile(destPath, result.join("\n"));
      } else {
        await writeFile(destPath, content);
      }
      queryEntries.push(r.queryId);
      console.log(`[Orchestrator] Collected ${r.queryId}: ${r.bestCppPath}`);
    } else {
      console.log(`[Orchestrator] Skipping ${r.queryId}: no working code produced.`);
    }
  }

  if (queryEntries.length === 0) {
    console.error("[Orchestrator] No query code produced. Cannot assemble final build.");
    return;
  }

  // Generate queries.h
  const queriesH = [
    "#pragma once",
    '#include <string>',
    "",
    ...queryEntries.map(q => `void run_${q.toLowerCase()}(const std::string& gendb_dir, const std::string& results_dir);`),
    "",
  ].join("\n");
  await writeFile(resolve(queriesDir, "queries.h"), queriesH);

  // Generate main.cpp
  const mainCpp = [
    '#include <iostream>',
    '#include <iomanip>',
    '#include <string>',
    '#include <chrono>',
    '#include "queries/queries.h"',
    "",
    "int main(int argc, char* argv[]) {",
    '    if (argc < 2) { std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl; return 1; }',
    "    std::string gendb_dir = argv[1];",
    '    std::string results_dir = argc > 2 ? argv[2] : "";',
    "",
    "    auto total_start = std::chrono::high_resolution_clock::now();",
    "",
    ...queryEntries.map(q => [
      `    std::cout << "Running ${q}..." << std::endl;`,
      `    run_${q.toLowerCase()}(gendb_dir, results_dir);`,
      "",
    ].join("\n")),
    "    auto total_end = std::chrono::high_resolution_clock::now();",
    '    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();',
    '    std::cout << "\\nTotal execution time: " << std::fixed << std::setprecision(2) << total_ms << " ms" << std::endl;',
    "    return 0;",
    "}",
    "",
  ].join("\n");
  await writeFile(resolve(finalDir, "main.cpp"), mainCpp);

  // Generate Makefile (with -fopenmp)
  const queryTargets = queryEntries.map(q => q.toLowerCase());
  const querySources = queryTargets.map(q => `queries/${q}.cpp`);
  const makefile = [
    "CXX = g++",
    "CXXFLAGS = -O3 -march=native -flto -std=c++17 -Wall -lpthread -fopenmp -DGENDB_LIBRARY",
    "",
    `QUERY_SRCS = ${querySources.join(" ")}`,
    "",
    "all: main",
    "",
    "main: main.cpp $(QUERY_SRCS)",
    "\t$(CXX) $(CXXFLAGS) -o $@ $^",
    "",
    "clean:",
    "\trm -f main",
    "",
    ".PHONY: all clean",
    "",
  ].join("\n");
  await writeFile(resolve(finalDir, "Makefile"), makefile);

  console.log(`[Orchestrator] Final build assembled: ${queryEntries.length} queries`);
  console.log(`[Orchestrator] Files: main.cpp, queries/queries.h, ${querySources.join(", ")}, Makefile`);
  console.log(`[Orchestrator] To build: cd ${finalDir} && make`);

  // Attempt final compilation
  const { spawn: spawnSync } = await import("child_process");
  try {
    const makeResult = await new Promise((res, rej) => {
      const child = spawnSync("make", ["clean", "all"], { cwd: finalDir, stdio: "pipe" });
      let out = "", err = "";
      child.stdout.on("data", d => out += d);
      child.stderr.on("data", d => err += d);
      child.on("close", code => code === 0 ? res(out) : rej(new Error(err)));
    });
    console.log("[Orchestrator] Final build compiled successfully.");
  } catch (err) {
    console.error(`[Orchestrator] Final build compilation failed: ${err.message}`);
    console.error("[Orchestrator] Manual compilation may be needed.");
  }

  // Validate final build
  if (existsSync(resolve(finalDir, "main"))) {
    const finalResultsDir = resolve(runDir, "final_results");
    await mkdir(finalResultsDir, { recursive: true });
    try {
      await new Promise((res, rej) => {
        const child = spawnSync(resolve(finalDir, "main"), [args.gendbDir, finalResultsDir], {
          cwd: finalDir, stdio: "pipe", timeout: 300000,
        });
        let out = "";
        child.stdout.on("data", d => { out += d; process.stdout.write(d); });
        child.stderr.on("data", d => process.stderr.write(d));
        child.on("close", code => code === 0 ? res(out) : rej(new Error(`Exit code ${code}`)));
      });
      console.log("[Orchestrator] Final validation run completed.");
    } catch (err) {
      console.error(`[Orchestrator] Final validation run failed: ${err.message}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Per-Query Summary Table
// ---------------------------------------------------------------------------

async function printPerQuerySummary(runDir, parsedQueries) {
  console.log(`\n[Orchestrator] === Per-Query Results ===\n`);

  // Collect results for each query and iteration
  const queryData = [];
  let maxIter = 0;

  for (const query of parsedQueries) {
    const qDir = resolve(runDir, "queries", query.id);
    const iters = [];
    for (let i = 0; ; i++) {
      const execPath = resolve(qDir, `iter_${i}`, "execution_results.json");
      const exec = await readJSON(execPath);
      if (!exec) break;
      iters.push({
        timing_ms: exec.timing_ms,
        validation: exec.validation?.status || "-",
      });
    }
    if (iters.length > maxIter) maxIter = iters.length;
    queryData.push({ id: query.id, iters });
  }

  if (queryData.length === 0 || maxIter === 0) return;

  // Build header
  const colW = 14;
  const qColW = 9;
  let header = "Query".padEnd(qColW) + "|";
  let sep = "-".repeat(qColW - 1) + "|";
  for (let i = 0; i < maxIter; i++) {
    header += `Iter ${i}`.padStart(colW) + " |";
    sep += "-".repeat(colW) + "-|";
  }
  header += "Best".padStart(colW) + " |";
  sep += "-".repeat(colW) + "-|";

  console.log(header);
  console.log(sep);

  // Print each query
  for (const q of queryData) {
    // Timing row
    let timingRow = q.id.padEnd(qColW) + "|";
    let validRow = " ".repeat(qColW) + "|";
    let bestTiming = null;
    let bestValidation = "-";

    for (let i = 0; i < maxIter; i++) {
      if (i < q.iters.length && q.iters[i].timing_ms != null) {
        const t = q.iters[i].timing_ms;
        const v = q.iters[i].validation;
        timingRow += `${Math.round(t)}ms`.padStart(colW) + " |";
        validRow += v.toUpperCase().padStart(colW) + " |";
        if (v === "pass" && (bestTiming === null || t < bestTiming)) {
          bestTiming = t;
          bestValidation = v;
        }
      } else if (i < q.iters.length) {
        timingRow += "-".padStart(colW) + " |";
        validRow += (q.iters[i].validation || "-").toUpperCase().padStart(colW) + " |";
      } else {
        timingRow += "-".padStart(colW) + " |";
        validRow += "-".padStart(colW) + " |";
      }
    }

    // Best column
    if (bestTiming !== null) {
      timingRow += `${Math.round(bestTiming)}ms`.padStart(colW) + " |";
      validRow += bestValidation.toUpperCase().padStart(colW) + " |";
    } else {
      timingRow += "-".padStart(colW) + " |";
      validRow += "-".padStart(colW) + " |";
    }

    console.log(timingRow);
    console.log(validRow);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs(process.argv);

  const schema = await readFile(args.schema, "utf-8");
  const queries = await readFile(args.queries, "utf-8");

  if (!existsSync(args.dataDir)) {
    console.error(`[Orchestrator] Error: data directory not found: ${args.dataDir}`);
    console.error(`[Orchestrator] Run 'bash benchmarks/tpc-h/setup_data.sh ${args.scaleFactor}' first to generate TPC-H data.`);
    process.exit(1);
  }

  const workload = getWorkloadName(args.schema);
  const runId = createRunId();
  const runDir = await createRunDir(workload, runId);

  // Try to load benchmark comparison results
  const benchmarkResultsPath = resolve(
    BENCHMARKS_DIR, defaults.targetBenchmark,
    "results", `sf${args.scaleFactor}`, "metrics", "benchmark_results.json"
  );
  const benchmarkResults = await readJSON(benchmarkResultsPath);
  args.benchmarkResults = benchmarkResults || null;
  if (benchmarkResults) {
    console.log(`[Orchestrator] Benchmark comparison data loaded from: ${benchmarkResultsPath}`);
  }

  console.log("[Orchestrator] GenDB Pipeline v20");
  console.log(`[Orchestrator] Schema:              ${args.schema}`);
  console.log(`[Orchestrator] Queries:             ${args.queries}`);
  console.log(`[Orchestrator] Data Dir:            ${args.dataDir}`);
  console.log(`[Orchestrator] GenDB Dir:           ${args.gendbDir}`);
  console.log(`[Orchestrator] Scale Factor:        ${args.scaleFactor}`);
  console.log(`[Orchestrator] Max Iterations:      ${args.maxIterations}`);
  console.log(`[Orchestrator] Max Concurrent:      ${args.maxConcurrent}`);
  console.log(`[Orchestrator] Default Model:       ${args.model}`);
  console.log(`[Orchestrator] Model Override:      ${args.modelOverride || "(none)"}`);
  console.log(`[Orchestrator] Agent Models:        ${JSON.stringify(defaults.agentModels)}`);
  console.log(`[Orchestrator] Optimization Target: ${args.optimizationTarget}`);
  console.log(`[Orchestrator] Knowledge Base:      ${KNOWLEDGE_BASE_PATH}`);
  console.log(`[Orchestrator] Workload:            ${workload}`);
  console.log(`[Orchestrator] Run ID:              ${runId}`);
  console.log(`[Orchestrator] Run Dir:             ${runDir}`);

  // Phase 1: Offline Data Storage Optimization
  const { workloadAnalysisPath, storageDesignPath } = await runOfflineStorageOptimization(args, runDir, schema, queries);

  // Phase 2: Online Per-Query Parallel Optimization
  await runPerQueryParallelOptimization(args, runDir, workloadAnalysisPath, storageDesignPath);

  // Finalize
  await updateRunMeta(runDir, (meta) => {
    meta.status = "completed";
    meta.completedAt = new Date().toISOString();
  });

  await updateLatestSymlink(workload, runId);

  // Print per-query summary table
  const parsedQueries = parseQueryFile(queries);
  await printPerQuerySummary(runDir, parsedQueries);

  // Write telemetry
  telemetryData.total_wall_clock_ms = Date.now() - runStartTime;
  const telemetryPath = resolve(runDir, "telemetry.json");
  await writeFile(telemetryPath, JSON.stringify(telemetryData, null, 2));

  // Print cost summary
  console.log(`\n[Orchestrator] === Run Summary ===`);
  console.log(`[Orchestrator] Total time: ${formatDuration(telemetryData.total_wall_clock_ms)}`);
  console.log(`[Orchestrator] Total tokens: ${Math.round(telemetryData.total_tokens.input / 1000)}K input, ${Math.round(telemetryData.total_tokens.output / 1000)}K output`);
  if (telemetryData.total_tokens.cache_read > 0) {
    console.log(`[Orchestrator] Cache tokens: ${Math.round(telemetryData.total_tokens.cache_read / 1000)}K cache_read, ${Math.round(telemetryData.total_tokens.cache_creation / 1000)}K cache_creation`);
  }
  console.log(`[Orchestrator] Estimated cost: $${telemetryData.total_cost_usd.toFixed(2)} (cache-aware pricing)`);

  const phaseSummaries = [];
  for (const [phaseName, phase] of Object.entries(telemetryData.phases)) {
    const phaseCost = Object.values(phase.agents || {}).reduce((s, a) => s + a.cost_usd, 0);
    phaseSummaries.push(`${phaseName}: ${formatDuration(phase.total_ms)} ($${phaseCost.toFixed(2)})`);
  }
  if (phaseSummaries.length > 0) {
    console.log(`[Orchestrator] ${phaseSummaries.join(" | ")}`);
  }

  let maxAgent = { name: "", cost: 0 };
  for (const phase of Object.values(telemetryData.phases)) {
    for (const [agentName, agent] of Object.entries(phase.agents || {})) {
      if (agent.cost_usd > maxAgent.cost) {
        maxAgent = { name: agentName, cost: agent.cost_usd };
      }
    }
  }
  if (maxAgent.cost > 0 && telemetryData.total_cost_usd > 0) {
    const pct = Math.round((maxAgent.cost / telemetryData.total_cost_usd) * 100);
    console.log(`[Orchestrator] Most expensive agent: ${maxAgent.name} (${pct}% of total cost)`);
  }

  console.log("\n[Orchestrator] Pipeline complete.");
  console.log(`[Orchestrator] Run Dir:  ${runDir}`);
  console.log(`[Orchestrator] Latest symlink: output/${workload}/latest → ${runId}`);
}

main().catch((err) => {
  console.error("[Orchestrator] Fatal error:", err.message);
  process.exit(1);
});
