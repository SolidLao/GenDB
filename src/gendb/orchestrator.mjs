/**
 * GenDB Orchestrator v8
 *
 * Two-phase agentic system:
 *   Phase 1 (Offline Data Storage Optimization):
 *     Workload Analyzer → Storage/Index Designer (design + ingest + build_indexes)
 *   Phase 2 (Online Per-Query Parallel Optimization):
 *     For each query (in parallel): Code Generator → Learner → [Orchestrator Agent → Optimizer(s) → Learner] × N
 *     → Final Assembly
 *
 * 5 optimization agents (conditionally invoked based on bottleneck category):
 *   - I/O Optimizer (io_bound)
 *   - Execution Optimizer (cpu_bound)
 *   - Join Optimizer (join)
 *   - Index Optimizer (index)
 *   - Query Rewriter (semantic/rewrite)
 *
 * Usage: node src/gendb/orchestrator.mjs [--schema <path>] [--queries <path>] [--data-dir <path>]
 *        [--gendb-dir <path>] [--sf <N>] [--max-iterations <N>] [--model <name>]
 *        [--optimization-target <target>] [--max-concurrent <N>]
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
import { config as codeGeneratorConfig } from "./agents/code-generator/index.mjs";
import { config as learnerConfig } from "./agents/learner/index.mjs";
import { config as queryRewriterConfig } from "./agents/query-rewriter/index.mjs";
import { config as joinOptimizerConfig } from "./agents/join-optimizer/index.mjs";
import { config as executionOptimizerConfig } from "./agents/execution-optimizer/index.mjs";
import { config as ioOptimizerConfig } from "./agents/io-optimizer/index.mjs";
import { config as indexOptimizerConfig } from "./agents/index-optimizer/index.mjs";
import { config as orchestratorAgentConfig } from "./agents/orchestrator/index.mjs";
import {
  createRunId,
  getWorkloadName,
  createRunDir,
  updateLatestSymlink,
  createQueryDir,
} from "./utils/paths.mjs";

// ---------------------------------------------------------------------------
// Optimizer dispatch map
// ---------------------------------------------------------------------------

const OPTIMIZER_MAP = {
  io_bound: ioOptimizerConfig,
  cpu_bound: executionOptimizerConfig,
  join: joinOptimizerConfig,
  index: indexOptimizerConfig,
  semantic: queryRewriterConfig,
  rewrite: queryRewriterConfig,
};

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
    `IMPORTANT: You MUST use the Write tool to write your JSON analysis to EXACTLY this path: ${workloadAnalysisPath}`,
    `Use this EXACT file path — do NOT change the filename or extension. Do NOT just print it.`,
  ].join("\n");

  const waResult = await runAgent(workloadAnalyzerConfig.name, {
    systemPrompt: analyzerSystemPrompt,
    userPrompt: analyzerUserPrompt,
    allowedTools: workloadAnalyzerConfig.allowedTools,
    model: args.model,
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
    model: args.model,
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

  // Run per-query pipelines in parallel with concurrency limit
  const semaphore = new Semaphore(args.maxConcurrent);
  const results = await Promise.all(
    parsedQueries.map(async (query) => {
      await semaphore.acquire();
      try {
        return await runQueryPipeline(query, args, runDir, workloadAnalysisPath, storageDesignPath, schema, groundTruthDir, hasGroundTruth);
      } catch (err) {
        console.error(`[Orchestrator] Query pipeline ${query.id} failed: ${err.message}`);
        return { queryId: query.id, status: "failed", error: err.message, bestCppPath: null };
      } finally {
        semaphore.release();
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
// Per-Query Pipeline
// ---------------------------------------------------------------------------

async function runQueryPipeline(query, args, runDir, workloadAnalysisPath, storageDesignPath, schema, groundTruthDir, hasGroundTruth) {
  const queryId = query.id;
  const queryDir = await createQueryDir(runDir, queryId);
  const queryPhase = `query_${queryId}`;

  console.log(`\n[Orchestrator] === Query Pipeline: ${queryId} ===`);

  // Optimization history for this query
  const historyPath = resolve(queryDir, "optimization_history.json");
  const optimizationHistory = { query_id: queryId, iterations: [] };
  await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

  // --- Step 1: Code Generation ---
  console.log(`[Orchestrator] [${queryId}] Step 1: Code Generation`);

  const codeGenSystemPrompt = await readFile(codeGeneratorConfig.promptPath, "utf-8");
  const cppPath = resolve(queryDir, `${queryId.toLowerCase()}.cpp`);
  const resultsDir = resolve(queryDir, "results");
  await mkdir(resultsDir, { recursive: true });

  const codeGenUserPrompt = [
    `Generate a self-contained C++ file for ${queryId}.`,
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    "",
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    `Consult relevant knowledge files for implementation patterns.`,
    "",
    "## Input Files",
    `- Workload analysis: ${workloadAnalysisPath}`,
    `- Storage design: ${storageDesignPath}`,
    `- Schema: ${args.schema}`,
    `- Queries: ${args.queries}`,
    "",
    `## Query to implement`,
    "```sql",
    query.sql,
    "```",
    "",
    `## GenDB Storage Directory`,
    `Binary columnar data is at: ${args.gendbDir}`,
    `Read columns from this directory via mmap.`,
    "",
    `## Output`,
    `Write the self-contained .cpp file to: ${cppPath}`,
    `After writing, compile and run:`,
    `  g++ -O2 -std=c++17 -Wall -lpthread -o ${resolve(queryDir, queryId.toLowerCase())} ${cppPath}`,
    `  ${resolve(queryDir, queryId.toLowerCase())} ${args.gendbDir} ${resultsDir}`,
    `The program should write results to ${resultsDir}/${queryId}.csv`,
    hasGroundTruth
      ? `Validate: python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${resultsDir}`
      : "",
  ].join("\n");

  const cgResult = await runAgent(codeGeneratorConfig.name, {
    systemPrompt: codeGenSystemPrompt,
    userPrompt: codeGenUserPrompt,
    allowedTools: codeGeneratorConfig.allowedTools,
    model: args.model,
    cwd: queryDir,
  });
  recordAgentTelemetry(queryPhase, "code_generator", cgResult.durationMs, cgResult.tokens, cgResult.costUsd);

  if (!existsSync(cppPath)) {
    throw new Error(`Code Generator failed to produce ${cppPath}`);
  }

  // Track the best .cpp file path
  let bestCppPath = cppPath;
  let bestEvalPath = null;

  // --- Step 2: Initial Learner evaluation ---
  console.log(`[Orchestrator] [${queryId}] Step 2: Initial Learner Evaluation`);

  const evalPath = resolve(queryDir, "iter_0_evaluation.json");
  const learnerResult0 = await runLearner(query, args, queryDir, cppPath, evalPath, workloadAnalysisPath, storageDesignPath, historyPath, null, 0, groundTruthDir, hasGroundTruth);
  recordAgentTelemetry(queryPhase, "learner", learnerResult0.durationMs, learnerResult0.tokens, learnerResult0.costUsd);
  bestEvalPath = evalPath;

  // --- Steps 3..N: Optimization loop ---
  const maxIter = args.maxIterations;
  let previousIterationOutcome = null;

  for (let iteration = 1; iteration <= maxIter; iteration++) {
    console.log(`\n[Orchestrator] [${queryId}] --- Optimization Iteration ${iteration}/${maxIter} ---`);

    const iterCppPath = resolve(queryDir, `${queryId.toLowerCase()}_iter${iteration}.cpp`);
    const iterEvalPath = resolve(queryDir, `iter_${iteration}_evaluation.json`);
    const iterDecisionPath = resolve(queryDir, `iter_${iteration}_decision.json`);

    // Copy best code to iteration file
    await writeFile(iterCppPath, await readFile(bestCppPath, "utf-8"));

    // 3a. Read previous evaluation to get recommendations
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

    // 3b. Run Orchestrator Agent
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Orchestrator Agent`);
    const orchAgentSystemPrompt = await readFile(orchestratorAgentConfig.promptPath, "utf-8");

    const orchAgentUserPrompt = [
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
      userPrompt: orchAgentUserPrompt,
      allowedTools: orchestratorAgentConfig.allowedTools,
      model: args.model,
      cwd: queryDir,
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

    // 3c. Determine which optimizer to invoke
    const selectedIdxs = decision.selected_recommendations || [];
    const selectedOpts = selectedIdxs.map(idx => perfOptimizations[idx]).filter(Boolean);
    const allOpts = [...criticalFixes, ...selectedOpts];

    if (allOpts.length === 0) {
      console.log(`[Orchestrator] [${queryId}] No optimizations selected. Skipping.`);
      continue;
    }

    // Find bottleneck categories
    const categories = new Set(allOpts.map(o => o.bottleneck_category).filter(Boolean));

    // Run optimizer(s) based on bottleneck category
    for (const category of categories) {
      const optimizerConfig = OPTIMIZER_MAP[category];
      if (!optimizerConfig) {
        console.log(`[Orchestrator] [${queryId}] Unknown category "${category}". Skipping.`);
        continue;
      }

      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: ${optimizerConfig.name} (${category})`);
      const optSystemPrompt = await readFile(optimizerConfig.promptPath, "utf-8");

      const optUserPrompt = [
        `Apply ${category} optimizations to ${queryId}.`,
        "",
        `## Optimization Target: ${args.optimizationTarget}`,
        `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
        "",
        "## Input Files",
        `- Evaluation + recommendations: ${bestEvalPath}`,
        `- Orchestrator decision: ${iterDecisionPath}`,
        `- Workload analysis: ${workloadAnalysisPath}`,
        `- Storage design: ${storageDesignPath}`,
        formatBenchmarkContext(args.benchmarkResults),
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
        `## Verification`,
        `After changes, compile and run:`,
        `  g++ -O2 -std=c++17 -Wall -lpthread -o ${resolve(queryDir, queryId.toLowerCase() + "_test")} ${iterCppPath}`,
        `  ${resolve(queryDir, queryId.toLowerCase() + "_test")} ${args.gendbDir} ${resultsDir}`,
        hasGroundTruth ? `  python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${resultsDir}` : "",
        `If broken after 3 attempts, revert to original.`,
      ].join("\n");

      const optResult = await runAgent(optimizerConfig.name, {
        systemPrompt: optSystemPrompt,
        userPrompt: optUserPrompt,
        allowedTools: optimizerConfig.allowedTools,
        model: args.model,
        cwd: queryDir,
      });
      const agentKey = optimizerConfig.name.toLowerCase().replace(/\s+/g, "_");
      recordAgentTelemetry(queryPhase, agentKey, optResult.durationMs, optResult.tokens, optResult.costUsd);
    }

    // 3d. Run Learner to evaluate
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Learner Evaluation`);
    const learnerResultN = await runLearner(query, args, queryDir, iterCppPath, iterEvalPath, workloadAnalysisPath, storageDesignPath, historyPath, previousIterationOutcome, iteration, groundTruthDir, hasGroundTruth);
    recordAgentTelemetry(queryPhase, "learner", learnerResultN.durationMs, learnerResultN.tokens, learnerResultN.costUsd);

    // 3e. Check improvement
    const iterEval = await readJSON(iterEvalPath);
    const prevBestEval = await readJSON(bestEvalPath);
    const improved = checkImprovement(prevBestEval, iterEval);

    // Record in history
    optimizationHistory.iterations.push({
      iteration,
      improved,
      categories: [...categories],
      evaluationPath: iterEvalPath,
    });
    await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

    if (improved) {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration} improved. Keeping changes.`);
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
// Learner Runner
// ---------------------------------------------------------------------------

async function runLearner(query, args, queryDir, cppPath, evalPath, workloadAnalysisPath, storageDesignPath, historyPath, previousOutcome, iteration, groundTruthDir, hasGroundTruth) {
  const learnerSystemPrompt = await readFile(learnerConfig.promptPath, "utf-8");
  const resultsDir = resolve(queryDir, "results");
  await mkdir(resultsDir, { recursive: true });

  const learnerUserPrompt = [
    `Compile, run, validate, and analyze ${query.id} (iteration ${iteration}).`,
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    "",
    `## Query Code`,
    `${cppPath}`,
    "",
    `## Original SQL`,
    "```sql",
    query.sql,
    "```",
    "",
    `## GenDB Storage Directory`,
    `${args.gendbDir}`,
    "",
    `## Compilation & Execution`,
    `  g++ -O2 -std=c++17 -Wall -lpthread -o ${resolve(queryDir, query.id.toLowerCase())} ${cppPath}`,
    `  ${resolve(queryDir, query.id.toLowerCase())} ${args.gendbDir} ${resultsDir}`,
    "",
    `## Validation`,
    hasGroundTruth
      ? `python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${resultsDir}`
      : `No ground truth — check output manually.`,
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
    model: args.model,
    cwd: queryDir,
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
        lines.splice(lastIncludeIdx + 1, 0, "", "namespace {");
        // funcLineIdx shifted by 2 due to the 2 lines inserted above
        lines.splice(funcLineIdx + 2, 0, "} // end anonymous namespace", "");
        await writeFile(destPath, lines.join("\n"));
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
    "CXXFLAGS = -O2 -std=c++17 -Wall -lpthread -DGENDB_LIBRARY",
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

  console.log("[Orchestrator] GenDB Pipeline v8");
  console.log(`[Orchestrator] Schema:              ${args.schema}`);
  console.log(`[Orchestrator] Queries:             ${args.queries}`);
  console.log(`[Orchestrator] Data Dir:            ${args.dataDir}`);
  console.log(`[Orchestrator] GenDB Dir:           ${args.gendbDir}`);
  console.log(`[Orchestrator] Scale Factor:        ${args.scaleFactor}`);
  console.log(`[Orchestrator] Max Iterations:      ${args.maxIterations}`);
  console.log(`[Orchestrator] Max Concurrent:      ${args.maxConcurrent}`);
  console.log(`[Orchestrator] Model:               ${args.model}`);
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
