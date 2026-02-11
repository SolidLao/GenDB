/**
 * GenDB Orchestrator
 *
 * Two-phase agentic system:
 *   Phase 1 (Baseline): Workload Analyzer → Storage/Index Designer → Code Generator → Physical Operator Agent → Evaluator
 *   Phase 2 (Optimization Loop): Learner → Orchestrator Agent → [Conditional Optimization Agent] → Evaluator
 *
 * Optimization agents (conditionally invoked based on bottleneck category):
 *   - Query Rewriter (query_structure): Rewrites SQL queries for better performance
 *   - Join Order Optimizer (join_order): Optimizes physical join order in C++ code
 *   - Execution Optimizer (cpu_bound): Adds thread parallelism and SIMD vectorization
 *   - I/O Optimizer (io_bound): Optimizes storage access (mmap hints, column pruning)
 *   - Physical Operator Agent (algorithm): Changes operator algorithms (hash vs sort)
 *   - Operator Specialist (general/fallback): Handles all optimizations when category unclear
 *
 * Usage: node src/gendb/orchestrator.mjs [--schema <path>] [--queries <path>] [--data-dir <path>]
 *        [--gendb-dir <path>] [--sf <N>] [--max-iterations <N>] [--model <name>] [--optimization-target <target>]
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
import { defaults } from "./gendb.config.mjs";
import { config as workloadAnalyzerConfig } from "./agents/workload-analyzer/index.mjs";
import { config as storageDesignerConfig } from "./agents/storage-index-designer/index.mjs";
import { config as codeGeneratorConfig } from "./agents/code-generator/index.mjs";
import { config as evaluatorConfig } from "./agents/evaluator/index.mjs";
import { config as learnerConfig } from "./agents/learner/index.mjs";
import { config as operatorSpecialistConfig } from "./agents/operator-specialist/index.mjs";
import { config as physicalOperatorConfig } from "./agents/physical-operator-agent/index.mjs";
import { config as queryRewriterConfig } from "./agents/query-rewriter/index.mjs";
import { config as joinOrderOptimizerConfig } from "./agents/join-order-optimizer/index.mjs";
import { config as executionOptimizerConfig } from "./agents/execution-optimizer/index.mjs";
import { config as ioOptimizerConfig } from "./agents/io-optimizer/index.mjs";
import { config as orchestratorAgentConfig } from "./agents/orchestrator/index.mjs";
import {
  createRunId,
  getWorkloadName,
  createRunDir,
  updateLatestSymlink,
  createIterationDir,
} from "./utils/paths.mjs";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Parse CLI args, merging over gendb.config defaults. */
function parseArgs(argv) {
  const args = {
    schema: DEFAULT_SCHEMA,
    queries: DEFAULT_QUERIES,
    dataDir: null, // resolved below from --data-dir or --sf
    gendbDir: null, // resolved below from --gendb-dir or --sf
    scaleFactor: defaults.scaleFactor,
    maxIterations: defaults.maxOptimizationIterations,
    model: defaults.model,
    optimizationTarget: defaults.optimizationTarget,
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
  }
  // If no explicit --data-dir, resolve from scale factor
  if (!args.dataDir) {
    args.dataDir = getDataDir(defaults.targetBenchmark, args.scaleFactor);
  }
  // If no explicit --gendb-dir, resolve from scale factor
  if (!args.gendbDir) {
    args.gendbDir = getGendbDir(defaults.targetBenchmark, args.scaleFactor);
  }
  return args;
}

/**
 * Read, update, and write run.json atomically.
 * `updater` receives the current run meta object and should mutate it.
 */
async function updateRunMeta(runDir, updater) {
  const runMetaPath = resolve(runDir, "run.json");
  const runMeta = JSON.parse(await readFile(runMetaPath, "utf-8"));
  updater(runMeta);
  await writeFile(runMetaPath, JSON.stringify(runMeta, null, 2));
  return runMeta;
}

// ---------------------------------------------------------------------------
// Telemetry tracking
// ---------------------------------------------------------------------------

const telemetryData = {
  total_wall_clock_ms: 0,
  total_tokens: { input: 0, output: 0 },
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
  if (!p.agents) p.agents = {};
  if (!p.agents[agentName]) {
    p.agents[agentName] = { duration_ms: 0, tokens: { input: 0, output: 0 }, cost_usd: 0 };
  }
  const a = p.agents[agentName];
  a.duration_ms += durationMs;
  a.tokens.input += tokens.input;
  a.tokens.output += tokens.output;
  a.cost_usd += costUsd;

  telemetryData.total_tokens.input += tokens.input;
  telemetryData.total_tokens.output += tokens.output;
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

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("close", (code) => {
      const durationMs = Date.now() - startTime;
      if (code !== 0) {
        console.error(`\n[Orchestrator] Agent "${name}" exited with code ${code} (${formatDuration(durationMs)})`);
        if (stderr) console.error(`[stderr] ${stderr}`);
        rejectP(new Error(`Agent "${name}" failed (exit ${code}): ${stderr}`));
      } else {
        // Parse JSON output for telemetry
        let resultText = stdout;
        let tokens = { input: 0, output: 0 };
        let costUsd = 0;
        try {
          const parsed = JSON.parse(stdout);
          resultText = parsed.result || "";
          tokens = {
            input: parsed.usage?.input_tokens || 0,
            output: parsed.usage?.output_tokens || 0,
          };
          costUsd = parsed.total_cost_usd || 0;
        } catch {
          // If JSON parsing fails, treat entire stdout as text
          resultText = stdout;
        }

        console.log(`\n[Orchestrator] Agent "${name}" completed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)})`);
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

/** Format benchmark comparison results as a prompt section (or empty string if unavailable). */
function formatBenchmarkContext(benchmarkResults) {
  if (!benchmarkResults) return "";
  const lines = [
    "",
    "## Benchmark Comparison (reference targets)",
    "Performance of other database systems on the same workload and scale factor.",
    "Use these as optimization targets — the goal is to close the gap with the fastest system.",
    "```json",
    JSON.stringify(benchmarkResults, null, 2),
    "```",
  ];
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Phase 1: Baseline Pipeline
// ---------------------------------------------------------------------------

async function runBaseline(args, runDir, schema, queries) {
  console.log("\n[Orchestrator] ========== PHASE 1: BASELINE ==========\n");

  // Initialize step tracking
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
        code_generation: { status: "pending" },
        operator_library: { status: "pending" },
        evaluation: { status: "pending" },
      },
    };
  });

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
    `IMPORTANT: You MUST use the Write tool to create the file ${workloadAnalysisPath} with your JSON analysis. Do NOT just print the analysis — use the Write tool to write it as a file.`,
  ].join("\n");

  const waResult = await runAgent(workloadAnalyzerConfig.name, {
    systemPrompt: analyzerSystemPrompt,
    userPrompt: analyzerUserPrompt,
    allowedTools: workloadAnalyzerConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
  recordAgentTelemetry("phase1", "workload_analyzer", waResult.durationMs, waResult.tokens, waResult.costUsd);

  // Verify workload analysis output
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
  console.log(`[Orchestrator] Joins found: ${(analysis.joins || []).length}`);
  console.log(`[Orchestrator] Filters found: ${(analysis.filters || []).length}`);

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.workload_analysis.status = "completed";
    meta.phase1.steps.workload_analysis.completedAt = new Date().toISOString();
  });

  // --- Step 2: Storage/Index Design ---
  console.log("\n[Orchestrator] === Step 2: Storage/Index Design ===");

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.storage_design.status = "running";
    meta.phase1.steps.storage_design.startedAt = new Date().toISOString();
  });

  const designerSystemPrompt = await readFile(storageDesignerConfig.promptPath, "utf-8");
  const storageDesignPath = resolve(runDir, "storage_design.json");

  const designerUserPrompt = [
    "Design the storage layout and index structures for this workload.",
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
    `IMPORTANT: You MUST use the Write tool to create the file ${storageDesignPath} with your JSON design. Do NOT just print it — use the Write tool to write it as a file.`,
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

  console.log("\n[Orchestrator] Storage design written successfully.");
  console.log(`[Orchestrator] Tables designed: ${Object.keys(design.tables || {}).length}`);

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.storage_design.status = "completed";
    meta.phase1.steps.storage_design.completedAt = new Date().toISOString();
  });

  // --- Step 3: Code Generation ---
  console.log("\n[Orchestrator] === Step 3: Code Generation ===");

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.code_generation.status = "running";
    meta.phase1.steps.code_generation.startedAt = new Date().toISOString();
  });

  const generatorSystemPrompt = await readFile(codeGeneratorConfig.promptPath, "utf-8");
  const generatedDir = resolve(runDir, "generated");
  await mkdir(generatedDir, { recursive: true });
  await mkdir(resolve(generatedDir, "utils"), { recursive: true });
  await mkdir(resolve(generatedDir, "storage"), { recursive: true });
  await mkdir(resolve(generatedDir, "index"), { recursive: true });
  await mkdir(resolve(generatedDir, "queries"), { recursive: true });

  const generatorUserPrompt = [
    "Generate C++ code for this workload (two-program architecture: ingest + main).",
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    "",
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    `Consult relevant knowledge files for implementation patterns and optimization techniques.`,
    `Read storage/persistent-storage.md for binary columnar storage and mmap patterns.`,
    "",
    "## Input Files",
    `- Workload analysis: ${workloadAnalysisPath}`,
    `- Storage design: ${storageDesignPath}`,
    `- Schema: ${args.schema}`,
    `- Queries: ${args.queries}`,
    "",
    `## Data Directory (source .tbl files)`,
    `.tbl files are located at: ${args.dataDir}`,
    "",
    `## GenDB Storage Directory (persistent binary storage)`,
    `The ingest program should write binary columnar data to: ${args.gendbDir}`,
    `The main program should read from this directory.`,
    "",
    `## Output Directory`,
    `Write all generated files to: ${generatedDir}`,
    `Subdirectories already exist: utils/, storage/, index/, queries/`,
    "",
    `Files to produce (at minimum):`,
    `- ${resolve(generatedDir, "utils/date_utils.h")}`,
    `- ${resolve(generatedDir, "storage/storage.h")}`,
    `- ${resolve(generatedDir, "storage/storage.cpp")}`,
    `- ${resolve(generatedDir, "index/index.h")}`,
    `- ${resolve(generatedDir, "queries/queries.h")} (plus one .cpp per query found in queries.sql)`,
    `- ${resolve(generatedDir, "ingest.cpp")}`,
    `- ${resolve(generatedDir, "main.cpp")}`,
    `- ${resolve(generatedDir, "Makefile")}`,
    "",
    `IMPORTANT: Use the Write tool to create each file. The files must compile with g++ -O2 -std=c++17.`,
    `After writing all files, compile AND run to verify correctness:`,
    `  cd ${generatedDir} && make clean && make all`,
    `  cd ${generatedDir} && ./ingest ${args.dataDir} ${args.gendbDir}`,
    `  cd ${generatedDir} && ./main ${args.gendbDir}`,
    `If it crashes or produces wrong results, fix the code and re-run (up to 2 fix attempts).`,
  ].join("\n");

  const cgResult = await runAgent(codeGeneratorConfig.name, {
    systemPrompt: generatorSystemPrompt,
    userPrompt: generatorUserPrompt,
    allowedTools: codeGeneratorConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
  recordAgentTelemetry("phase1", "code_generator", cgResult.durationMs, cgResult.tokens, cgResult.costUsd);

  // Verify generated files exist
  const requiredFiles = [
    "storage/storage.h",
    "storage/storage.cpp",
    "ingest.cpp",
    "main.cpp",
    "Makefile",
  ];
  const missingFiles = requiredFiles.filter((f) => !existsSync(resolve(generatedDir, f)));

  if (missingFiles.length > 0) {
    console.error(`[Orchestrator] Error: missing generated files: ${missingFiles.join(", ")}`);
    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.code_generation.status = "failed";
      meta.phase1.steps.code_generation.error = `Missing files: ${missingFiles.join(", ")}`;
      meta.phase1.status = "failed";
      meta.status = "failed";
    });
    throw new Error(`Code generation incomplete — missing: ${missingFiles.join(", ")}`);
  }

  console.log("\n[Orchestrator] Code generation completed. All files present.");
  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.code_generation.status = "completed";
    meta.phase1.steps.code_generation.completedAt = new Date().toISOString();
  });

  // --- Step 4: Physical Operator Library Creation ---
  console.log("\n[Orchestrator] === Step 4: Physical Operator Library ===");

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.operator_library.status = "running";
    meta.phase1.steps.operator_library.startedAt = new Date().toISOString();
  });

  const physOpSystemPrompt = await readFile(physicalOperatorConfig.promptPath, "utf-8");
  await mkdir(resolve(generatedDir, "operators"), { recursive: true });

  const physOpUserPrompt = [
    "Create a reusable operator library from the generated query code (Phase 1 - Baseline).",
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    "",
    `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
    `Read relevant knowledge files for operator implementation patterns.`,
    "",
    "## Input Files",
    `- Workload analysis: ${workloadAnalysisPath}`,
    `- Storage design: ${storageDesignPath}`,
    `- Current generated code: ${generatedDir}`,
    "",
    "## Task",
    "Extract common operator patterns (scans, hash joins, hash aggregations) from the query files",
    "and create reusable operator implementations in the operators/ subdirectory.",
    "Update query files to use these operators instead of inline implementations.",
    "",
    `## Output Directory`,
    `Create operator library files in: ${resolve(generatedDir, "operators")}`,
    `Examples: operators/scan.h, operators/hash_join.h, operators/hash_agg.h`,
    `Update query files in: ${resolve(generatedDir, "queries")} to use these operators`,
    "",
    `## Verification`,
    `After creating the operator library, compile and run to verify correctness:`,
    `  cd ${generatedDir} && make clean && make all`,
    `  cd ${generatedDir} && ./main ${args.gendbDir}`,
    `Results must match the original implementation (same rows, same values).`,
  ].join("\n");

  const poResult = await runAgent(physicalOperatorConfig.name, {
    systemPrompt: physOpSystemPrompt,
    userPrompt: physOpUserPrompt,
    allowedTools: physicalOperatorConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
  recordAgentTelemetry("phase1", "physical_operator_agent", poResult.durationMs, poResult.tokens, poResult.costUsd);

  console.log("\n[Orchestrator] Physical operator library creation completed.");
  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.operator_library.status = "completed";
    meta.phase1.steps.operator_library.completedAt = new Date().toISOString();
  });

  // --- Step 5: Evaluation ---
  console.log("\n[Orchestrator] === Step 5: Evaluation ===");

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.evaluation.status = "running";
    meta.phase1.steps.evaluation.startedAt = new Date().toISOString();
  });

  const evaluationPath = resolve(runDir, "evaluation.json");
  // Phase 1 evaluator: run ingest if gendb storage doesn't exist, then main
  const evResult = await runEvaluator(args, runDir, generatedDir, evaluationPath);
  recordAgentTelemetry("phase1", "evaluator", evResult.durationMs, evResult.tokens, evResult.costUsd);

  const evaluation = await readJSON(evaluationPath);
  if (evaluation) {
    console.log("\n[Orchestrator] Evaluation completed.");
    console.log(`[Orchestrator] Overall status: ${evaluation.overall_status}`);
    if (evaluation.summary) console.log(`[Orchestrator] Summary: ${evaluation.summary}`);

    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.evaluation.status = "completed";
      meta.phase1.steps.evaluation.completedAt = new Date().toISOString();
      meta.phase1.steps.evaluation.overall_status = evaluation.overall_status;
    });
  } else {
    console.error("[Orchestrator] Warning: could not read/parse evaluation.json");
    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.evaluation.status = "completed_with_warnings";
      meta.phase1.steps.evaluation.warning = "Could not parse evaluation.json";
    });
  }

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.status = "completed";
    meta.phase1.completedAt = new Date().toISOString();
  });

  return { generatedDir, evaluationPath };
}

// ---------------------------------------------------------------------------
// Shared: Evaluator runner
// ---------------------------------------------------------------------------

async function runEvaluator(args, runDir, generatedDir, evaluationPath, { skipIngest = false } = {}) {
  const evaluatorSystemPrompt = await readFile(evaluatorConfig.promptPath, "utf-8");

  const gendbDirExists = existsSync(args.gendbDir);
  const shouldIngest = !skipIngest && !gendbDirExists;

  const evaluatorUserPrompt = [
    "Evaluate the generated C++ code (two-program architecture: ingest + main).",
    "",
    `## Optimization Target: ${args.optimizationTarget}`,
    "",
    `## Generated code directory`,
    `${generatedDir}`,
    "",
    `## Data directory (source .tbl files)`,
    `${args.dataDir}`,
    "",
    `## GenDB storage directory (persistent binary storage)`,
    `${args.gendbDir}`,
    "",
    `## Evaluation steps`,
    `1. Compile: cd ${generatedDir} && make clean && make all`,
    shouldIngest
      ? `2. Ingest (one-time): cd ${generatedDir} && ./ingest ${args.dataDir} ${args.gendbDir}`
      : `2. Ingest: SKIP — persistent storage already exists at ${args.gendbDir}`,
    `3. Run queries: cd ${generatedDir} && ./main ${args.gendbDir}`,
    "",
    `IMPORTANT: Write the evaluation report to: ${evaluationPath}`,
    `Use the Write tool to create this file. Do NOT write it inside generated/ — write it to the exact path above.`,
    `Report ingestion_time_ms separately from query timing. The primary metric is query execution time from ./main.`,
  ].join("\n");

  const evalResult = await runAgent(evaluatorConfig.name, {
    systemPrompt: evaluatorSystemPrompt,
    userPrompt: evaluatorUserPrompt,
    allowedTools: evaluatorConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
  return evalResult;
}

/**
 * Check if any files in the generated directory that affect storage/ingestion were modified.
 * Used to detect when re-ingestion is needed after Operator Specialist changes.
 */
function checkIngestFilesModified(iterGeneratedDir, bestGeneratedDir) {
  const ingestRelatedFiles = ["ingest.cpp", "storage/storage.h", "storage/storage.cpp"];
  for (const f of ingestRelatedFiles) {
    const iterFile = resolve(iterGeneratedDir, f);
    const bestFile = resolve(bestGeneratedDir, f);
    if (!existsSync(iterFile) || !existsSync(bestFile)) continue;
    try {
      const iterContent = readFileSync(iterFile, "utf-8");
      const bestContent = readFileSync(bestFile, "utf-8");
      if (iterContent !== bestContent) return true;
    } catch {
      // If read fails, assume not modified
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Phase 2: Optimization Loop
// ---------------------------------------------------------------------------

async function runOptimizationLoop(args, runDir, baselineGeneratedDir, baselineEvaluationPath) {
  const maxIter = args.maxIterations;
  if (maxIter <= 0) {
    console.log("\n[Orchestrator] Optimization loop skipped (--max-iterations 0).");
    return;
  }

  console.log(`\n[Orchestrator] ========== PHASE 2: OPTIMIZATION LOOP (max ${maxIter} iterations) ==========\n`);

  await updateRunMeta(runDir, (meta) => {
    meta.phase2 = { status: "running", iterations: [] };
  });

  // Optimization history accumulates across iterations
  const optimizationHistory = {
    iterations: [],
  };
  const historyPath = resolve(runDir, "optimization_history.json");
  await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

  // Track the "best so far" generated directory and its evaluation
  let bestGeneratedDir = baselineGeneratedDir;
  let bestEvaluationPath = baselineEvaluationPath;
  let previousIterationOutcome = null; // Track whether previous iteration improved or regressed

  for (let iteration = 1; iteration <= maxIter; iteration++) {
    console.log(`\n[Orchestrator] --- Optimization Iteration ${iteration}/${maxIter} ---\n`);

    const iterDir = await createIterationDir(runDir, iteration);
    const iterGeneratedDir = resolve(iterDir, "generated");
    const iterEvaluationPath = resolve(iterDir, "evaluation.json");
    const iterRecsPath = resolve(iterDir, "optimization_recommendations.json");
    const iterDecisionPath = resolve(iterDir, "orchestrator_decision.json");

    const iterMeta = {
      iteration,
      status: "running",
      startedAt: new Date().toISOString(),
    };

    await updateRunMeta(runDir, (meta) => {
      meta.phase2.iterations.push(iterMeta);
    });

    // 1. Copy best-so-far code to this iteration
    console.log(`[Orchestrator] Copying best code to iteration ${iteration}...`);
    await copyDir(bestGeneratedDir, iterGeneratedDir);

    // 2. Run Learner
    console.log(`\n[Orchestrator] === Iteration ${iteration} Step 1: Learner ===`);
    const learnerSystemPrompt = await readFile(learnerConfig.promptPath, "utf-8");

    const learnerUserPrompt = [
      `Analyze evaluation results and recommend optimizations for iteration ${iteration}.`,
      "",
      `## Optimization Target: ${args.optimizationTarget}`,
      "",
      `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
      `Read relevant knowledge files to inform your bottleneck analysis and optimization recommendations. You are encouraged to propose techniques beyond what's documented.`,
      `Read storage/persistent-storage.md for I/O optimization patterns.`,
      "",
      previousIterationOutcome
        ? `## Previous Iteration Outcome\n${previousIterationOutcome}`
        : "",
      "",
      `## Storage Architecture`,
      `Data is stored in persistent binary columnar format at: ${args.gendbDir}`,
      `The main program reads from .gendb/ via mmap — no .tbl text parsing.`,
      `If you recommend storage layout changes (re-sorting, new indexes, block size changes), note that re-ingestion is needed.`,
      "",
      "## Input Files",
      `- Evaluation results: ${bestEvaluationPath}`,
      `- Workload analysis: ${resolve(runDir, "workload_analysis.json")}`,
      `- Storage design: ${resolve(runDir, "storage_design.json")}`,
      `- Current C++ code directory: ${iterGeneratedDir}`,
      `- Optimization history: ${historyPath}`,
      formatBenchmarkContext(args.benchmarkResults),
      "",
      `## Output`,
      `IMPORTANT: Write your recommendations to: ${iterRecsPath}`,
      `Use the Write tool to create this file.`,
      `Set "iteration" to ${iteration} in the JSON output.`,
    ].join("\n");

    const iterPhase = `iteration_${iteration}`;
    const lnResult = await runAgent(learnerConfig.name, {
      systemPrompt: learnerSystemPrompt,
      userPrompt: learnerUserPrompt,
      allowedTools: learnerConfig.allowedTools,
      model: args.model,
      cwd: runDir,
    });
    recordAgentTelemetry(iterPhase, "learner", lnResult.durationMs, lnResult.tokens, lnResult.costUsd);

    const recommendations = await readJSON(iterRecsPath);
    if (!recommendations) {
      console.error(`[Orchestrator] Learner failed to produce recommendations for iteration ${iteration}. Stopping loop.`);
      await updateIterationStatus(runDir, iteration, "failed", "Learner failed to produce recommendations");
      break;
    }
    const criticalFixes = recommendations.critical_fixes || [];
    const perfOptimizations = recommendations.performance_optimizations || recommendations.recommendations || [];
    console.log(`[Orchestrator] Learner produced ${criticalFixes.length} critical fixes, ${perfOptimizations.length} performance optimizations.`);

    // 3. Run Orchestrator Agent
    console.log(`\n[Orchestrator] === Iteration ${iteration} Step 2: Orchestrator Agent ===`);
    const orchAgentSystemPrompt = await readFile(orchestratorAgentConfig.promptPath, "utf-8");

    const orchAgentUserPrompt = [
      `Decide whether to continue optimizing or stop (iteration ${iteration}/${maxIter}).`,
      "",
      `## Optimization Target: ${args.optimizationTarget}`,
      "",
      `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
      "",
      "## Input Files",
      `- Current evaluation results: ${bestEvaluationPath}`,
      `- Learner recommendations: ${iterRecsPath}`,
      `- Optimization history: ${historyPath}`,
      formatBenchmarkContext(args.benchmarkResults),
      "",
      `Remaining iterations after this one: ${maxIter - iteration}`,
      "",
      criticalFixes.length > 0
        ? `## CRITICAL: ${criticalFixes.length} critical fix(es) found\nThe Learner identified ${criticalFixes.length} critical fix(es) (crashes/correctness bugs). These MUST all be included in your selected_recommendations. Performance optimizations are secondary until all critical fixes are applied.`
        : "",
      "",
      `## Output`,
      `IMPORTANT: Write your decision to: ${iterDecisionPath}`,
      `Use the Write tool to create this file.`,
      `Note: The Learner's recommendations now use "critical_fixes" (always apply) and "performance_optimizations" (select by priority). Your selected_recommendations should index into performance_optimizations. All critical_fixes are automatically included.`,
    ].join("\n");

    const oaResult = await runAgent(orchestratorAgentConfig.name, {
      systemPrompt: orchAgentSystemPrompt,
      userPrompt: orchAgentUserPrompt,
      allowedTools: orchestratorAgentConfig.allowedTools,
      model: args.model,
      cwd: runDir,
    });
    recordAgentTelemetry(iterPhase, "orchestrator_agent", oaResult.durationMs, oaResult.tokens, oaResult.costUsd);

    const decision = await readJSON(iterDecisionPath);
    if (!decision) {
      console.error(`[Orchestrator] Orchestrator Agent failed to produce decision. Stopping loop.`);
      await updateIterationStatus(runDir, iteration, "failed", "Orchestrator Agent failed to produce decision");
      break;
    }

    console.log(`[Orchestrator] Decision: ${decision.action}`);
    console.log(`[Orchestrator] Reasoning: ${decision.reasoning}`);

    // 4. If decision is "stop", end the loop
    if (decision.action === "stop") {
      console.log(`[Orchestrator] Orchestrator Agent decided to stop. Ending optimization loop.`);
      await updateIterationStatus(runDir, iteration, "stopped_early", decision.reasoning);
      break;
    }

    // 5. Determine which optimization agent to invoke based on bottleneck category
    const selectedOptimizations = (decision.selected_recommendations || []).map(idx => perfOptimizations[idx]);
    const bottleneckCategories = new Set(selectedOptimizations.map(opt => opt.bottleneck_category).filter(Boolean));

    // Add critical fixes (all have implicit bottleneck category)
    const allOptimizations = [...criticalFixes, ...selectedOptimizations];

    if (allOptimizations.length === 0) {
      console.log(`[Orchestrator] No optimizations to apply in iteration ${iteration}. Skipping.`);
      await updateIterationStatus(runDir, iteration, "skipped", "No optimizations selected");
      continue;
    }

    // Select the appropriate optimization agent(s) based on bottleneck categories
    const agentConfigs = [];

    if (bottleneckCategories.has("query_structure")) {
      agentConfigs.push({ config: queryRewriterConfig, category: "query_structure" });
    }
    if (bottleneckCategories.has("join_order")) {
      agentConfigs.push({ config: joinOrderOptimizerConfig, category: "join_order" });
    }
    if (bottleneckCategories.has("cpu_bound")) {
      agentConfigs.push({ config: executionOptimizerConfig, category: "cpu_bound" });
    }
    if (bottleneckCategories.has("io_bound")) {
      agentConfigs.push({ config: ioOptimizerConfig, category: "io_bound" });
    }
    if (bottleneckCategories.has("algorithm")) {
      agentConfigs.push({ config: physicalOperatorConfig, category: "algorithm" });
    }

    // If no specific category or multiple categories, fall back to operator specialist
    if (agentConfigs.length === 0 || criticalFixes.length > 0) {
      agentConfigs.push({ config: operatorSpecialistConfig, category: "general" });
    }

    // Run each selected optimization agent
    for (const { config: agentConfig, category } of agentConfigs) {
      console.log(`\n[Orchestrator] === Iteration ${iteration} Step 3: ${agentConfig.name} (${category}) ===`);
      const agentSystemPrompt = await readFile(agentConfig.promptPath, "utf-8");

      const agentUserPrompt = [
        `Apply selected optimizations to the C++ code for iteration ${iteration}.`,
        "",
        `## Optimization Target: ${args.optimizationTarget}`,
        "",
        `## Knowledge Base: ${KNOWLEDGE_BASE_PATH}`,
        `Read relevant knowledge files for implementation patterns.`,
        "",
        "## Input Files",
        `- Orchestrator decision: ${iterDecisionPath}`,
        `- Optimization recommendations: ${iterRecsPath}`,
        `- Evaluation results: ${bestEvaluationPath}`,
        `- Workload analysis: ${resolve(runDir, "workload_analysis.json")}`,
        `- Storage design: ${resolve(runDir, "storage_design.json")}`,
        formatBenchmarkContext(args.benchmarkResults),
        "",
        `## Data Directory (source .tbl files)`,
        `TPC-H .tbl files: ${args.dataDir}`,
        `Scale factor: ${args.scaleFactor}`,
        "",
        `## GenDB Storage Directory (persistent binary storage)`,
        `Binary columnar data: ${args.gendbDir}`,
        `The main program reads from this directory. If you modify ingest.cpp or storage format, re-ingestion will be needed.`,
        "",
        `## Previous Performance (baseline to beat)`,
        `Read the evaluation at: ${bestEvaluationPath}`,
        `Your optimized code must: (1) not crash, (2) produce correct results, (3) ideally be faster`,
        "",
        "## Code Directory",
        `Read and modify C++ files in: ${iterGeneratedDir}`,
        `This directory contains the current best code. Apply optimizations here.`,
        "",
        `## Test-Refine Budget`,
        `You may compile+run up to 3 times to iteratively fix issues. If your changes break correctness, revert them.`,
        `Run command: cd ${iterGeneratedDir} && make clean && make all && ./main ${args.gendbDir}`,
        `If you modified ingest.cpp or storage format, re-run ingest first: ./ingest ${args.dataDir} ${args.gendbDir}`,
        "",
        `## Important`,
        criticalFixes.length > 0
          ? `- Apply ALL critical_fixes from optimization_recommendations.json FIRST (these fix crashes/correctness bugs)`
          : "",
        `- Apply the performance_optimizations selected in the orchestrator's decision (selected_recommendations field)`,
        `- Focus on optimizations with bottleneck_category: "${category}"`,
        `- After making changes, compile AND run to verify: cd ${iterGeneratedDir} && make clean && make all && ./main ${args.gendbDir}`,
        `- Correctness is paramount — results must remain identical`,
        `- CRITICAL: Do not return code that crashes or produces wrong results`,
      ].join("\n");

      const agentResult = await runAgent(agentConfig.name, {
        systemPrompt: agentSystemPrompt,
        userPrompt: agentUserPrompt,
        allowedTools: agentConfig.allowedTools,
        model: args.model,
        cwd: runDir,
      });

      const agentNameKey = agentConfig.name.toLowerCase().replace(/\s+/g, "_");
      recordAgentTelemetry(iterPhase, agentNameKey, agentResult.durationMs, agentResult.tokens, agentResult.costUsd);
    }

    // 6. Check if storage/ingest files were modified — if so, re-ingest
    const needsReingest = checkIngestFilesModified(iterGeneratedDir, bestGeneratedDir);
    if (needsReingest) {
      console.log(`[Orchestrator] Storage/ingest files modified in iteration ${iteration}. Re-ingestion will be triggered.`);
      // Remove existing gendb storage to force re-ingestion
      if (existsSync(args.gendbDir)) {
        rmSync(args.gendbDir, { recursive: true, force: true });
        console.log(`[Orchestrator] Removed existing gendb storage at ${args.gendbDir} for re-ingestion.`);
      }
    }

    // 7. Run Evaluator
    console.log(`\n[Orchestrator] === Iteration ${iteration} Step 4: Evaluator ===`);
    const iterEvResult = await runEvaluator(args, runDir, iterGeneratedDir, iterEvaluationPath);
    recordAgentTelemetry(iterPhase, "evaluator", iterEvResult.durationMs, iterEvResult.tokens, iterEvResult.costUsd);

    const iterEvaluation = await readJSON(iterEvaluationPath);

    // 7. Record iteration results in optimization_history
    const iterResult = {
      iteration,
      evaluationPath: iterEvaluationPath,
      selectedRecommendations: decision.selected_recommendations || [],
      focusAreas: decision.focus_areas || [],
    };

    if (iterEvaluation) {
      iterResult.overall_status = iterEvaluation.overall_status;
      iterResult.query_results = iterEvaluation.query_results || {};
      console.log(`[Orchestrator] Iteration ${iteration} evaluation: ${iterEvaluation.overall_status}`);
      if (iterEvaluation.summary) console.log(`[Orchestrator] Summary: ${iterEvaluation.summary}`);
    } else {
      iterResult.overall_status = "unknown";
      console.error(`[Orchestrator] Warning: could not parse evaluation for iteration ${iteration}`);
    }

    optimizationHistory.iterations.push(iterResult);
    await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

    // 8. Update "current best" if improved
    const improved = checkImprovement(
      await readJSON(bestEvaluationPath),
      iterEvaluation
    );

    if (improved) {
      console.log(`[Orchestrator] Iteration ${iteration} improved performance. Updating best.`);
      bestGeneratedDir = iterGeneratedDir;
      bestEvaluationPath = iterEvaluationPath;
      previousIterationOutcome = `Iteration ${iteration} IMPROVED performance. Changes were kept.`;
    } else {
      console.log(`[Orchestrator] Iteration ${iteration} did not improve. Keeping previous best (code rolled back).`);
      previousIterationOutcome = `Iteration ${iteration} REGRESSED — code was rolled back to previous best. The changes from iteration ${iteration} were discarded. Avoid repeating the same approach.`;
    }

    await updateIterationStatus(runDir, iteration, "completed", null);
  }

  await updateRunMeta(runDir, (meta) => {
    meta.phase2.status = "completed";
    meta.phase2.completedAt = new Date().toISOString();
    meta.phase2.bestGeneratedDir = bestGeneratedDir;
  });
}

/** Update the status of a specific iteration in run.json. */
async function updateIterationStatus(runDir, iteration, status, note) {
  await updateRunMeta(runDir, (meta) => {
    const iter = meta.phase2.iterations.find((it) => it.iteration === iteration);
    if (iter) {
      iter.status = status;
      iter.completedAt = new Date().toISOString();
      if (note) iter.note = note;
    }
  });
}

/**
 * Check if the new evaluation is better than the previous one.
 * Handles partial results: more queries passing = improvement, even if slower.
 * A previously-passing query that now crashes is a regression (always reject).
 */
function checkImprovement(prevEval, newEval) {
  if (!prevEval || !newEval) return false;
  if (!prevEval.query_results || !newEval.query_results) return false;
  if (newEval.overall_status === "fail") return false;

  let prevTotal = 0, newTotal = 0;
  let prevCount = 0, newCount = 0;

  for (const qId of Object.keys(prevEval.query_results)) {
    const prevQ = prevEval.query_results[qId];
    const newQ = newEval.query_results[qId];
    if (!newQ) continue;

    const prevTime = parseFloat(prevQ.timing_ms);
    const newTime = parseFloat(newQ.timing_ms);

    // If prev query worked but new one crashes, that's a regression — reject
    if (!isNaN(prevTime) && prevQ.status === "pass" && newQ.status !== "pass") return false;

    if (!isNaN(prevTime) && prevQ.status === "pass") { prevTotal += prevTime; prevCount++; }
    if (!isNaN(newTime) && newQ.status === "pass") { newTotal += newTime; newCount++; }
  }

  // More queries working = improvement (even if slower)
  if (newCount > prevCount) return true;
  // Same queries working, check total time
  if (newCount === prevCount && newCount > 0) return newTotal < prevTotal;
  return false;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs(process.argv);

  // Read input files
  const schema = await readFile(args.schema, "utf-8");
  const queries = await readFile(args.queries, "utf-8");

  // Validate data directory exists
  if (!existsSync(args.dataDir)) {
    console.error(`[Orchestrator] Error: data directory not found: ${args.dataDir}`);
    console.error(`[Orchestrator] Run 'bash benchmarks/tpc-h/setup_data.sh ${args.scaleFactor}' first to generate TPC-H data.`);
    process.exit(1);
  }

  // Determine workload and create run directory
  const workload = getWorkloadName(args.schema);
  const runId = createRunId();
  const runDir = await createRunDir(workload, runId);

  // Try to load benchmark comparison results (PostgreSQL, DuckDB, etc.)
  const benchmarkResultsPath = resolve(
    BENCHMARKS_DIR, defaults.targetBenchmark,
    "results", `sf${args.scaleFactor}`, "metrics", "benchmark_results.json"
  );
  const benchmarkResults = await readJSON(benchmarkResultsPath);
  if (benchmarkResults) {
    args.benchmarkResults = benchmarkResults;
    console.log(`[Orchestrator] Benchmark comparison data loaded from: ${benchmarkResultsPath}`);
  } else {
    args.benchmarkResults = null;
    console.log(`[Orchestrator] No benchmark comparison data found at: ${benchmarkResultsPath}`);
  }

  console.log("[Orchestrator] GenDB Pipeline");
  console.log(`[Orchestrator] Schema:              ${args.schema}`);
  console.log(`[Orchestrator] Queries:             ${args.queries}`);
  console.log(`[Orchestrator] Data Dir:            ${args.dataDir}`);
  console.log(`[Orchestrator] GenDB Dir:           ${args.gendbDir}`);
  console.log(`[Orchestrator] Scale Factor:        ${args.scaleFactor}`);
  console.log(`[Orchestrator] Max Iterations:      ${args.maxIterations}`);
  console.log(`[Orchestrator] Model:               ${args.model}`);
  console.log(`[Orchestrator] Optimization Target: ${args.optimizationTarget}`);
  console.log(`[Orchestrator] Knowledge Base:      ${KNOWLEDGE_BASE_PATH}`);
  console.log(`[Orchestrator] Workload:            ${workload}`);
  console.log(`[Orchestrator] Run ID:              ${runId}`);
  console.log(`[Orchestrator] Run Dir:             ${runDir}`);

  // Phase 1: Baseline
  const { generatedDir, evaluationPath } = await runBaseline(args, runDir, schema, queries);

  // Phase 2: Optimization Loop
  await runOptimizationLoop(args, runDir, generatedDir, evaluationPath);

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
  const totalTokens = telemetryData.total_tokens.input + telemetryData.total_tokens.output;
  console.log(`\n[Orchestrator] === Run Summary ===`);
  console.log(`[Orchestrator] Total time: ${formatDuration(telemetryData.total_wall_clock_ms)}`);
  console.log(`[Orchestrator] Total tokens: ${Math.round(telemetryData.total_tokens.input / 1000)}K input, ${Math.round(telemetryData.total_tokens.output / 1000)}K output`);
  console.log(`[Orchestrator] Estimated cost: $${telemetryData.total_cost_usd.toFixed(2)}`);

  // Per-phase summary
  const phaseSummaries = [];
  for (const [phaseName, phase] of Object.entries(telemetryData.phases)) {
    const phaseCost = Object.values(phase.agents || {}).reduce((s, a) => s + a.cost_usd, 0);
    phaseSummaries.push(`${phaseName}: ${formatDuration(phase.total_ms)} ($${phaseCost.toFixed(2)})`);
  }
  console.log(`[Orchestrator] ${phaseSummaries.join(" | ")}`);

  // Most expensive agent
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
