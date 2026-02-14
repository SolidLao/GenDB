/**
 * GenDB Orchestrator v12
 *
 * Two-phase agentic system with pipeline parallelism:
 *   Phase 1 (Offline Data Storage Optimization):
 *     Workload Analyzer → Storage/Index Designer (design + ingest + build_indexes)
 *   Phase 2 (Online Per-Query Pipeline-Parallel Optimization):
 *     Iteration 0: Code Generators (parallel) → Executor (serial, fast) → Learner (parallel LLM)
 *     Iteration 1+: Pipeline-parallel per query:
 *       Orchestrator Agent → Query Optimizer → Executor (serial) → Learner (parallel)
 *     → Final Assembly
 *
 * Features: Per-agent LLM model config, per-operation timing, pipeline parallelism
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
import { config as learnerConfig } from "./agents/learner/index.mjs";
import { config as queryOptimizerConfig } from "./agents/query-optimizer/index.mjs";
import { config as codeGeneratorConfig } from "./agents/code-generator/index.mjs";
import { config as orchestratorAgentConfig } from "./agents/orchestrator/index.mjs";
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

/** Format benchmark comparison results as a prompt section. */
function formatBenchmarkContext(benchmarkResults) {
  if (!benchmarkResults) return "";
  return [
    "",
    "## Benchmark Comparison (reference targets)",
    "Performance of other database systems on the same workload and scale factor.",
    "Use these as optimization targets — the goal is to close the gap with the fastest system.",
    "```json",
    JSON.stringify(benchmarkResults, null, 2),
    "```",
  ].join("\n");
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

/** Simple semaphore for concurrency limiting. */
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
async function reIngestWithFixes(args, runDir, storageDesignPath, workloadAnalysisPath, schema, detectedIssues) {
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

  await updateRunMeta(runDir, (meta) => {
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

  // Execution semaphore (max=1): ensures only one Executor runs at a time
  // to avoid CPU/memory contention from concurrent multi-threaded query execution.
  // Only the Executor (compile+run+validate) needs this — LLM agents run freely in parallel.
  const executionSemaphore = new Semaphore(1);

  // --- Iteration 0: Batch Orchestrator Agent → parallel Code Generators → Storage Checkpoint → serialized Learners ---
  console.log(`\n[Orchestrator] === Iteration 0: Batch Orchestrator Agent for all queries ===`);
  const batchDecisionPath = resolve(runDir, "batch_decision_iter0.json");
  const orchAgentSystemPrompt = await readFile(orchestratorAgentConfig.promptPath, "utf-8");

  const allQueriesSql = parsedQueries.map(q => `### ${q.id}\n\`\`\`sql\n${q.sql}\n\`\`\``).join("\n\n");
  const batchOrchUserPrompt = [
    `Provide strategic guidance for initial code generation of ALL queries (iteration 0, batch mode).`,
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    "",
    "## Input Files",
    `- Workload analysis: ${workloadAnalysisPath}`,
    `- Storage design: ${storageDesignPath}`,
    "",
    `## Queries to implement`,
    allQueriesSql,
    "",
    formatBenchmarkContext(args.benchmarkResults),
    "",
    `This is the FIRST iteration — there is no Learner evaluation yet.`,
    `Use BATCH MODE: provide strategies for ALL ${parsedQueries.length} queries in a single JSON file.`,
    `Analyze the workload and provide per-query strategic guidance for the Query Optimizer to generate high-quality initial code.`,
    `Focus on: parallelism strategy, join approach, filter optimization, data access patterns.`,
    "",
    `## Output`,
    `Write your batch decision to: ${batchDecisionPath}`,
  ].join("\n");

  const batchOaResult = await runAgent(orchestratorAgentConfig.name, {
    systemPrompt: orchAgentSystemPrompt,
    userPrompt: batchOrchUserPrompt,
    allowedTools: orchestratorAgentConfig.allowedTools,
    model: getAgentModel("orchestrator_agent", args),
    cwd: runDir,
  });
  recordAgentTelemetry("phase2", "orchestrator_agent_batch", batchOaResult.durationMs, batchOaResult.tokens, batchOaResult.costUsd);

  // --- Step 2a: Run iteration 0 Code Generator for all queries in parallel ---
  console.log(`\n[Orchestrator] === Iteration 0: Parallel Code Generation for all queries ===`);
  const cgSystemPrompt = await readFile(codeGeneratorConfig.promptPath, "utf-8");
  const semaphore = new Semaphore(args.maxConcurrent);

  // Prepare iter_0 directories and run code generators
  const iter0Contexts = await Promise.all(
    parsedQueries.map(async (query) => {
      await semaphore.acquire();
      try {
        return await runQueryCodeGen(query, args, runDir, workloadAnalysisPath, storageDesignPath, groundTruthDir, hasGroundTruth, batchDecisionPath, cgSystemPrompt);
      } catch (err) {
        console.error(`[Orchestrator] Code Generator for ${query.id} failed: ${err.message}`);
        return { query, queryDir: null, iterDir: null, iterCppPath: null, error: err.message };
      } finally {
        semaphore.release();
      }
    })
  );

  // --- Step 2b: STORAGE CHECKPOINT — detect storage issues ---
  const storageIssues = await detectStorageIssues(parsedQueries, runDir);
  let retriesLeft = 1;

  if (storageIssues.length > 0 && retriesLeft > 0) {
    console.log(`\n[Orchestrator] ⚠ STORAGE CHECKPOINT: ${storageIssues.length} query(s) detected storage issues:`);
    for (const issue of storageIssues) {
      console.log(`  - ${issue.queryId}: ${issue.issue}`);
    }

    // Re-ingest with fixes
    await reIngestWithFixes(args, runDir, storageDesignPath, workloadAnalysisPath, schema, storageIssues);
    retriesLeft--;

    // Clear iter_0 directories and re-run code generation
    console.log(`\n[Orchestrator] === Re-running iteration 0 Code Generation after re-ingestion ===`);
    for (const ctx of iter0Contexts) {
      if (ctx.iterDir && existsSync(ctx.iterDir)) {
        rmSync(ctx.iterDir, { recursive: true, force: true });
      }
    }

    // Re-run code generators
    const retryContexts = await Promise.all(
      parsedQueries.map(async (query) => {
        await semaphore.acquire();
        try {
          return await runQueryCodeGen(query, args, runDir, workloadAnalysisPath, storageDesignPath, groundTruthDir, hasGroundTruth, batchDecisionPath, cgSystemPrompt);
        } catch (err) {
          console.error(`[Orchestrator] Code Generator retry for ${query.id} failed: ${err.message}`);
          return { query, queryDir: null, iterDir: null, iterCppPath: null, error: err.message };
        } finally {
          semaphore.release();
        }
      })
    );

    // Check for storage issues again
    const retryIssues = await detectStorageIssues(parsedQueries, runDir);
    if (retryIssues.length > 0) {
      console.error(`[Orchestrator] Storage issues persist after re-ingestion. Continuing with best effort.`);
    }

    // Use retried contexts
    iter0Contexts.splice(0, iter0Contexts.length, ...retryContexts);
  }

  // --- Step 2c: Pipeline-parallel Executor (serial) + Learner (parallel) for iter_0 ---
  console.log(`\n[Orchestrator] === Iteration 0: Pipeline-Parallel Executor + Learner ===`);
  const learnerPromises = [];
  for (const ctx of iter0Contexts) {
    if (!ctx.iterCppPath || !existsSync(ctx.iterCppPath)) {
      console.log(`[Orchestrator] Skipping Executor for ${ctx.query.id}: no code produced.`);
      continue;
    }
    // Executor: serial (needs execution semaphore), fast (~5-10s)
    await executionSemaphore.acquire();
    let execResults;
    try {
      console.log(`[Orchestrator] [${ctx.query.id}] Iteration 0: Executor (compile + run + validate)`);
      const iterResultsDir = resolve(ctx.iterDir, "results");
      execResults = await executeQuery(ctx.query, ctx.iterDir, ctx.iterCppPath, args.gendbDir, groundTruthDir, iterResultsDir);
    } finally {
      executionSemaphore.release();
    }
    // Learner: parallel (pure LLM analysis, no execution semaphore needed)
    console.log(`[Orchestrator] [${ctx.query.id}] Iteration 0: Learner (parallel LLM analysis)`);
    const iterEvalPath = resolve(ctx.iterDir, "evaluation.json");
    const historyPath = resolve(ctx.queryDir, "optimization_history.json");
    const learnerPromise = runLearner(ctx.query, args, ctx.iterDir, ctx.iterCppPath, iterEvalPath, workloadAnalysisPath, storageDesignPath, historyPath, null, 0)
      .then((learnerResult) => {
        recordAgentTelemetry(`query_${ctx.query.id}`, "learner", learnerResult.durationMs, learnerResult.tokens, learnerResult.costUsd);
      })
      .catch((err) => {
        console.error(`[Orchestrator] Learner for ${ctx.query.id} failed: ${err.message}`);
      });
    learnerPromises.push(learnerPromise);
  }
  // Wait for all Learners to complete
  await Promise.all(learnerPromises);

  // --- Step 2d: Pipeline-parallel iterations 1+ for all queries ---
  const results = await Promise.all(
    iter0Contexts.map(async (ctx) => {
      if (!ctx.iterCppPath || !existsSync(ctx.iterCppPath)) {
        return { queryId: ctx.query.id, status: "failed", error: ctx.error || "no code produced", bestCppPath: null, iterations: 0 };
      }
      try {
        return await runQueryIterationsRemaining(ctx.query, args, runDir, workloadAnalysisPath, storageDesignPath, schema, groundTruthDir, hasGroundTruth, executionSemaphore, ctx);
      } catch (err) {
        console.error(`[Orchestrator] Query pipeline ${ctx.query.id} failed: ${err.message}`);
        return { queryId: ctx.query.id, status: "failed", error: err.message, bestCppPath: ctx.iterCppPath };
      }
    })
  );

  // Update run meta with pipeline results
  await updateRunMeta(runDir, (meta) => {
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
// Per-Query Pipeline (split into Code Gen, Learner iter 0, Iterations 1+)
// ---------------------------------------------------------------------------

/**
 * Iteration 0 Code Generator only (runs in parallel across queries).
 * Returns context object for subsequent steps.
 */
async function runQueryCodeGen(query, args, runDir, workloadAnalysisPath, storageDesignPath, groundTruthDir, hasGroundTruth, batchDecisionPath, cgSystemPrompt) {
  const queryId = query.id;
  const queryDir = await createQueryDir(runDir, queryId);
  const queryPhase = `query_${queryId}`;
  const cppName = `${queryId.toLowerCase()}.cpp`;

  console.log(`\n[Orchestrator] [${queryId}] Iteration 0: Code Generator`);

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
  const iterDecisionPath = resolve(iterDir, "decision.json");

  // Extract this query's strategy from the batch decision
  const batchDecision = await readJSON(batchDecisionPath);
  let queryStrategy;
  if (batchDecision && batchDecision.queries && batchDecision.queries[queryId]) {
    queryStrategy = batchDecision.queries[queryId];
    queryStrategy.action = queryStrategy.action || "optimize";
    queryStrategy.reasoning = batchDecision.reasoning || "";
  } else {
    console.log(`[Orchestrator] [${queryId}] Warning: batch decision missing strategy for ${queryId}, using default`);
    queryStrategy = {
      action: "optimize",
      reasoning: "Batch decision did not include this query. Using default strategy.",
      focus_areas: ["parallelism", "efficient data access"],
      strategy_notes: "Generate optimized code with parallelism and efficient mmap-based data access.",
    };
  }
  await writeFile(iterDecisionPath, JSON.stringify(queryStrategy, null, 2));

  // Run Code Generator
  const cgUserPrompt = [
    `Generate and validate a self-contained C++ file for ${queryId}. This is iteration 0.`,
    ``,
    `## Your Goal`,
    `1. Generate CORRECT code that produces the right results`,
    `2. Add basic optimizations (parallelism, efficient I/O)`,
    `3. Validate correctness locally with up to 2 fix attempts`,
    ``,
    `IMPORTANT: Your job is to ensure CORRECTNESS. The Learner agent will handle profiling and bottleneck analysis after you finish.`,
    ``,
    `## Optimization Target: ${args.optimizationTarget}`,
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    ``,
    `## Input Files`,
    `- Workload analysis: ${workloadAnalysisPath}`,
    `- Storage design: ${storageDesignPath}`,
    `- Schema: ${args.schema}`,
    `- Queries: ${args.queries}`,
    `- Orchestrator strategy: ${iterDecisionPath}`,
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
    `## Validation Loop`,
    `Compile → Run → Validate correctness (up to 2 fix attempts)`,
    hasGroundTruth ? `Compare results: python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${iterResultsDir}` : `No validation available - just compile and run`,
    ``,
    `After validation succeeds, you are done. Do NOT write evaluation.json - the Learner handles that.`,
  ].join("\n");

  const cgResult = await runAgent(codeGeneratorConfig.name, {
    systemPrompt: cgSystemPrompt,
    userPrompt: cgUserPrompt,
    allowedTools: codeGeneratorConfig.allowedTools,
    model: getAgentModel("code_generator", args),
    cwd: iterDir,
  });
  recordAgentTelemetry(queryPhase, "code_generator", cgResult.durationMs, cgResult.tokens, cgResult.costUsd);

  if (!existsSync(iterCppPath)) {
    throw new Error(`Code Generator failed to produce ${iterCppPath}`);
  }

  return { query, queryDir, iterDir, iterCppPath, cppName };
}

/**
 * Iterations 1+ for a single query (Orchestrator Agent → Query Optimizer → Learner).
 * Takes the iter_0 context and continues from there.
 */
async function runQueryIterationsRemaining(query, args, runDir, workloadAnalysisPath, storageDesignPath, schema, groundTruthDir, hasGroundTruth, executionSemaphore, iter0Ctx) {
  const queryId = query.id;
  const queryDir = iter0Ctx.queryDir;
  const queryPhase = `query_${queryId}`;
  const cppName = iter0Ctx.cppName;

  const historyPath = resolve(queryDir, "optimization_history.json");
  const optimizationHistory = await readJSON(historyPath) || { query_id: queryId, iterations: [] };

  const qoSystemPrompt = await readFile(queryOptimizerConfig.promptPath, "utf-8");
  const orchAgentSystemPrompt = await readFile(orchestratorAgentConfig.promptPath, "utf-8");

  // Read hardware info from storage design for hardware-aware prompts
  const storageDesign = await readJSON(storageDesignPath);
  const hw = storageDesign?.hardware_config || {};

  // Iteration 0 results are the baseline
  let bestIterDir = iter0Ctx.iterDir;
  let bestCppPath = iter0Ctx.iterCppPath;
  let bestEvalPath = resolve(iter0Ctx.iterDir, "evaluation.json");

  const maxIter = args.maxIterations;
  let previousIterationOutcome = null;

  for (let iteration = 1; iteration <= maxIter; iteration++) {
    console.log(`\n[Orchestrator] [${queryId}] --- Iteration ${iteration}/${maxIter} ---`);

    const iterDir = resolve(queryDir, `iter_${iteration}`);
    await mkdir(iterDir, { recursive: true });

    const iterCppPath = resolve(iterDir, cppName);
    const iterResultsDir = resolve(iterDir, "results");
    await mkdir(iterResultsDir, { recursive: true });
    const iterEvalPath = resolve(iterDir, "evaluation.json");
    const iterDecisionPath = resolve(iterDir, "decision.json");

    // Copy best code from previous best iteration as starting point
    if (bestCppPath && existsSync(bestCppPath)) {
      await writeFile(iterCppPath, await readFile(bestCppPath, "utf-8"));
    }

    // Step A: Orchestrator Agent decision (no semaphore needed — pure LLM)
    const prevEval = await readJSON(bestEvalPath);
    if (!prevEval) {
      console.log(`[Orchestrator] [${queryId}] Could not read previous evaluation. Stopping.`);
      break;
    }

    const criticalFixes = prevEval.critical_fixes || [];
    const perfOptimizations = prevEval.performance_optimizations || [];

    if (criticalFixes.length === 0 && perfOptimizations.length === 0) {
      console.log(`[Orchestrator] [${queryId}] No optimizations recommended. Stopping.`);
      break;
    }

    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Orchestrator Agent`);

    const orchUserPrompt = [
      `Decide whether to continue optimizing or stop for ${queryId} (iteration ${iteration}/${maxIter}).`,
      "",
      `## Optimization Target: ${args.optimizationTarget}`,
      `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
      "",
      "## Input Files",
      `- Current evaluation + recommendations: ${bestEvalPath}`,
      `- Optimization history: ${historyPath}`,
      formatBenchmarkContext(args.benchmarkResults),
      "",
      `Remaining iterations: ${maxIter - iteration}`,
      previousIterationOutcome ? `\n## Previous Iteration Outcome\n${previousIterationOutcome}` : "",
      criticalFixes.length > 0
        ? `\n## CRITICAL: ${criticalFixes.length} critical fix(es) found — these MUST all be included.`
        : "",
      "",
      `## Output`,
      `Write your decision to: ${iterDecisionPath}`,
    ].join("\n");

    const oaResult = await runAgent(orchestratorAgentConfig.name, {
      systemPrompt: orchAgentSystemPrompt,
      userPrompt: orchUserPrompt,
      allowedTools: orchestratorAgentConfig.allowedTools,
      model: getAgentModel("orchestrator_agent", args),
      cwd: iterDir,
    });
    recordAgentTelemetry(queryPhase, "orchestrator_agent", oaResult.durationMs, oaResult.tokens, oaResult.costUsd);

    const decision = await readJSON(iterDecisionPath);
    if (!decision) {
      console.log(`[Orchestrator] [${queryId}] Orchestrator Agent failed. Stopping.`);
      break;
    }

    if (decision.action === "stop") {
      console.log(`[Orchestrator] [${queryId}] Orchestrator Agent decided to stop: ${decision.reasoning}`);
      break;
    }

    // Step B: Query Optimizer (no semaphore — LLM + compile only)
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Query Optimizer (optimize)`);

    // Check for mandatory join ordering sampling
    let joinSamplingMandate = "";
    const qrResults = prevEval.evaluation?.query_results || prevEval.query_results || {};
    const qResult = qrResults[queryId] || {};
    const opTimings = qResult.operation_timings || prevEval.analysis?.per_query?.[queryId]?.operation_timings || {};
    const totalTime = parseFloat(opTimings.total || qResult.timing_ms || 0);
    const joinTime = parseFloat(opTimings.join || 0);
    const joinDominant = totalTime > 0 && (joinTime / totalTime) > 0.4;
    const joinCount = (query.sql.match(/\bJOIN\b/gi) || []).length;
    const prevHistory = await readJSON(historyPath);
    const hadJoinSampling = (prevHistory?.iterations || []).some(it =>
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

    const qoUserPrompt = [
      `Optimize the existing C++ code for ${queryId}. This is iteration ${iteration} (optimize mode).`,
      "",
      `## Optimization Target: ${args.optimizationTarget}`,
      `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
      "",
      "## Input Files",
      `- Current evaluation + recommendations: ${bestEvalPath}`,
      `- Orchestrator decision: ${iterDecisionPath}`,
      `- Workload analysis: ${workloadAnalysisPath}`,
      `- Storage design: ${storageDesignPath}`,
      `- Optimization history: ${historyPath}`,
      formatBenchmarkContext(args.benchmarkResults),
      "",
      `## Hardware Configuration`,
      `- CPU cores: ${hw.cpu_cores || 'unknown'}`,
      `- Disk type: ${hw.disk_type || 'unknown'}`,
      `- L3 cache: ${hw.l3_cache_mb || 'unknown'} MB`,
      `- Total memory: ${hw.total_memory_gb || 'unknown'} GB`,
      joinSamplingMandate,
      "",
      `## GenDB Storage Directory`,
      `${args.gendbDir}`,
      "",
      `## Query Code`,
      `Read and modify: ${iterCppPath}`,
      "",
      `## Original SQL`,
      "```sql",
      query.sql,
      "```",
      "",
      `## Compilation`,
      `After changes, compile (do NOT run the binary — the Executor handles execution and validation):`,
      `  g++ -O3 -march=native -std=c++17 -Wall -lpthread -o ${resolve(iterDir, queryId.toLowerCase())} ${iterCppPath}`,
      `Ensure the code compiles successfully (up to 3 fix attempts).`,
    ].join("\n");

    const qoResult = await runAgent(queryOptimizerConfig.name, {
      systemPrompt: qoSystemPrompt,
      userPrompt: qoUserPrompt,
      allowedTools: queryOptimizerConfig.allowedTools,
      model: getAgentModel("query_optimizer", args),
      cwd: iterDir,
    });
    recordAgentTelemetry(queryPhase, "query_optimizer", qoResult.durationMs, qoResult.tokens, qoResult.costUsd);

    // Step C: Executor (serial — needs execution semaphore for CPU/memory isolation)
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Executor (compile + run + validate)`);
    await executionSemaphore.acquire();
    try {
      await executeQuery(query, iterDir, iterCppPath, args.gendbDir, groundTruthDir, iterResultsDir);
    } finally {
      executionSemaphore.release();
    }

    // Step D: Learner (parallel — pure LLM analysis, no semaphore needed)
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Learner (LLM analysis)`);
    const learnerResult = await runLearner(query, args, iterDir, iterCppPath, iterEvalPath, workloadAnalysisPath, storageDesignPath, historyPath, previousIterationOutcome, iteration);
    recordAgentTelemetry(queryPhase, "learner", learnerResult.durationMs, learnerResult.tokens, learnerResult.costUsd);

    // ---- Improvement check (keep or rollback) ----
    const iterEval = await readJSON(iterEvalPath);
    const prevBestEval = await readJSON(bestEvalPath);
    const improved = checkImprovement(prevBestEval, iterEval);

    // Collect categories from the decision's selected recommendations
    const prevPerfOpts = prevBestEval?.performance_optimizations || [];
    const iterDecision = await readJSON(iterDecisionPath);
    const selectedIdxs = iterDecision?.selected_recommendations || [];
    const selectedOpts = selectedIdxs.map(idx => prevPerfOpts[idx]).filter(Boolean);
    const allOpts = [...(prevBestEval?.critical_fixes || []), ...selectedOpts];
    const categories = [...new Set(allOpts.map(o => o.bottleneck_category).filter(Boolean))];

    optimizationHistory.iterations.push({
      iteration,
      improved,
      categories,
      evaluationPath: iterEvalPath,
    });
    await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

    if (improved) {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration} improved. Keeping changes.`);
      bestIterDir = iterDir;
      bestCppPath = iterCppPath;
      bestEvalPath = iterEvalPath;
      previousIterationOutcome = `Iteration ${iteration} IMPROVED. Changes kept.`;
    } else {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration} did not improve. Rolling back.`);
      previousIterationOutcome = `Iteration ${iteration} REGRESSED — rolled back. Avoid repeating the same approach.`;
    }
  }

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
 * This is the only step that needs the execution semaphore.
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

  // Step 1: Compile
  console.log(`[Executor] [${query.id}] Compiling...`);
  try {
    const compileOutput = await runProcess("g++", [
      "-O3", "-march=native", "-std=c++17", "-Wall", "-lpthread",
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

  // Extract total timing
  const totalMatch = stdout.match(/(?:\[TIMING\]\s+total:\s+([\d.]+)\s*ms|Execution time:\s*([\d.]+)\s*ms)/);
  if (totalMatch) {
    results.timing_ms = parseFloat(totalMatch[1] || totalMatch[2]);
  }

  // Step 4: Validate against ground truth
  if (groundTruthDir && existsSync(groundTruthDir)) {
    console.log(`[Executor] [${query.id}] Validating results...`);
    try {
      const valOutput = await runProcess("python3", [
        COMPARE_TOOL_PATH, groundTruthDir, resultsDir,
      ], { cwd: iterDir, timeout: 60000 });
      results.validation = { status: "pass", output: valOutput.stdout || valOutput };
      // Check if comparison tool reported failures
      const valStr = typeof results.validation.output === "string" ? results.validation.output : "";
      if (valStr.includes('"status": "fail"') || valStr.includes("FAIL")) {
        results.validation.status = "fail";
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
// Learner Runner (LLM-only analysis, receives execution_results.json)
// ---------------------------------------------------------------------------

async function runLearner(query, args, iterDir, cppPath, evalPath, workloadAnalysisPath, storageDesignPath, historyPath, previousOutcome, iteration) {
  const learnerSystemPrompt = await readFile(learnerConfig.promptPath, "utf-8");
  const execResultsPath = resolve(iterDir, "execution_results.json");

  // Read hardware info from storage design for hardware-aware recommendations
  const storageDesignData = await readJSON(storageDesignPath);
  const hwLearner = storageDesignData?.hardware_config || {};

  const learnerUserPrompt = [
    `Analyze execution results and recommend optimizations for ${query.id} (iteration ${iteration}).`,
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    "",
    `## Hardware Configuration`,
    `- CPU cores: ${hwLearner.cpu_cores || 'unknown'}`,
    `- Disk type: ${hwLearner.disk_type || 'unknown'}`,
    `- L3 cache: ${hwLearner.l3_cache_mb || 'unknown'} MB`,
    `- Total memory: ${hwLearner.total_memory_gb || 'unknown'} GB`,
    "",
    `## Execution Results (from Executor)`,
    `Read execution results from: ${execResultsPath}`,
    `This contains compile status, run output, validation results, and per-operation timing.`,
    "",
    `## Query Code`,
    `${cppPath}`,
    "",
    `## Original SQL`,
    "```sql",
    query.sql,
    "```",
    "",
    previousOutcome ? `## Previous Iteration Outcome\n${previousOutcome}` : "",
    "",
    "## Input Files",
    `- Workload analysis: ${workloadAnalysisPath}`,
    `- Storage design: ${storageDesignPath}`,
    `- Optimization history: ${historyPath}`,
    formatBenchmarkContext(args.benchmarkResults),
    "",
    `## Output`,
    `Write combined evaluation + recommendations to: ${evalPath}`,
    `Set "iteration" to ${iteration}.`,
  ].join("\n");

  return await runAgent(learnerConfig.name, {
    systemPrompt: learnerSystemPrompt,
    userPrompt: learnerUserPrompt,
    allowedTools: learnerConfig.allowedTools,
    model: getAgentModel("learner", args),
    cwd: iterDir,
  });
}

// ---------------------------------------------------------------------------
// Improvement Check
// ---------------------------------------------------------------------------

function checkImprovement(prevEval, newEval) {
  if (!prevEval || !newEval) return false;

  const prevResults = prevEval.evaluation?.query_results || prevEval.query_results || {};
  const newResults = newEval.evaluation?.query_results || newEval.query_results || {};

  // Check overall status
  const newOverall = newEval.evaluation?.overall_status || newEval.overall_status;
  if (newOverall === "fail") return false;

  let prevTotal = 0, newTotal = 0;
  let prevCount = 0, newCount = 0;

  for (const qId of Object.keys({ ...prevResults, ...newResults })) {
    const prevQ = prevResults[qId];
    const newQ = newResults[qId];

    const prevTime = prevQ ? parseFloat(prevQ.timing_ms) : NaN;
    const newTime = newQ ? parseFloat(newQ.timing_ms) : NaN;

    // If prev query worked but new one crashes, reject
    if (prevQ && prevQ.status === "pass" && newQ && newQ.status !== "pass") return false;

    if (!isNaN(prevTime) && prevQ?.status === "pass") { prevTotal += prevTime; prevCount++; }
    if (!isNaN(newTime) && newQ?.status === "pass") { newTotal += newTime; newCount++; }
  }

  // More queries working = improvement
  if (newCount > prevCount) return true;
  // Same queries, check time
  if (newCount === prevCount && newCount > 0) return newTotal < prevTotal;
  return false;
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
      // (e.g. MmapColumn in both q3.cpp and q6.cpp with different layouts).
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

  // Generate Makefile
  const queryTargets = queryEntries.map(q => q.toLowerCase());
  const querySources = queryTargets.map(q => `queries/${q}.cpp`);
  const makefile = [
    "CXX = g++",
    "CXXFLAGS = -O3 -march=native -flto -std=c++17 -Wall -lpthread -DGENDB_LIBRARY",
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

  console.log("[Orchestrator] GenDB Pipeline v12");
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
