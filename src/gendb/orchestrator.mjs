/**
 * GenDB Orchestrator (v7)
 *
 * Parquet storage + pure C++ code generation + per-query parallel optimization.
 *
 * Phase 1 (Analysis + Data Preparation):
 *   Workload Analyzer → convert_to_parquet.py → generate Makefile + copy parquet_reader.h
 *
 * Phase 2 (Per-Query Parallel Optimization):
 *   For each query Qi (all in parallel via Promise.all):
 *     Iter 0: Query Code Generator → Learner (compile, run, validate)
 *     Iter 1..N: Orchestrator Agent → [Optimizers] → Learner
 *   → Assemble main.cpp + queries.h → Final build + validation
 *
 * Key changes from v6:
 *   - No Arrow compute — pure C++ with parquet_reader.h for I/O only
 *   - No baseline phase — directly generate optimized code
 *   - Per-query parallel optimization pipelines
 *   - Each query independently iterated
 *
 * Usage: node src/gendb/orchestrator.mjs [--sf <N>] [--max-iterations <N>] [--model <name>]
 */

import { spawn, execSync } from "child_process";
import { readFile, writeFile, mkdir, cp, copyFile } from "fs/promises";
import { resolve, dirname } from "path";
import { existsSync } from "fs";
import { fileURLToPath } from "url";
import {
  DEFAULT_SCHEMA,
  DEFAULT_QUERIES,
  BENCHMARKS_DIR,
  getDataDir,
  getParquetDir,
} from "./config.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const COMPARE_TOOL_PATH = resolve(__dirname, "tools", "compare_results.py");
const CONVERT_SCRIPT = resolve(BENCHMARKS_DIR, "tpc-h", "convert_to_parquet.py");
const PARQUET_READER_H = resolve(__dirname, "utils", "parquet_reader.h");
import { defaults } from "./gendb.config.mjs";

// Agent configs
import { config as workloadAnalyzerConfig } from "./agents/workload-analyzer/index.mjs";
import { config as queryGeneratorConfig } from "./agents/query-generator/index.mjs";
import { config as learnerConfig } from "./agents/learner/index.mjs";
import { config as orchestratorAgentConfig } from "./agents/orchestrator/index.mjs";
import { config as queryRewriterConfig } from "./agents/query-rewriter/index.mjs";
import { config as joinOrderOptimizerConfig } from "./agents/join-order-optimizer/index.mjs";
import { config as executionOptimizerConfig } from "./agents/execution-optimizer/index.mjs";
import { config as ioOptimizerConfig } from "./agents/io-optimizer/index.mjs";
import { config as indexOptimizerConfig } from "./agents/index-optimizer/index.mjs";

import {
  createRunId,
  getWorkloadName,
  createRunDir,
  updateLatestSymlink,
} from "./utils/paths.mjs";

const BUILD_INDEXES_SCRIPT = resolve(BENCHMARKS_DIR, "tpc-h", "build_indexes.py");

// Optimizer name → agent config mapping (5 optimizers)
const OPTIMIZER_MAP = {
  execution_optimizer: executionOptimizerConfig,
  io_optimizer: ioOptimizerConfig,
  join_order_optimizer: joinOrderOptimizerConfig,
  query_rewriter: queryRewriterConfig,
  index_optimizer: indexOptimizerConfig,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = {
    schema: DEFAULT_SCHEMA,
    queries: DEFAULT_QUERIES,
    dataDir: null,
    parquetDir: null,
    scaleFactor: defaults.scaleFactor,
    maxIterations: defaults.maxOptimizationIterations,
    model: defaults.model,
    optimizationTarget: defaults.optimizationTarget,
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--schema" && argv[i + 1]) args.schema = resolve(argv[++i]);
    if (argv[i] === "--queries" && argv[i + 1]) args.queries = resolve(argv[++i]);
    if (argv[i] === "--data-dir" && argv[i + 1]) args.dataDir = resolve(argv[++i]);
    if (argv[i] === "--parquet-dir" && argv[i + 1]) args.parquetDir = resolve(argv[++i]);
    if (argv[i] === "--sf" && argv[i + 1]) args.scaleFactor = parseInt(argv[++i], 10);
    if (argv[i] === "--max-iterations" && argv[i + 1]) args.maxIterations = parseInt(argv[++i], 10);
    if (argv[i] === "--model" && argv[i + 1]) args.model = argv[++i];
    if (argv[i] === "--optimization-target" && argv[i + 1]) args.optimizationTarget = argv[++i];
  }
  if (!args.dataDir) args.dataDir = getDataDir(defaults.targetBenchmark, args.scaleFactor);
  if (!args.parquetDir) args.parquetDir = getParquetDir(defaults.targetBenchmark, args.scaleFactor);
  return args;
}

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
// Telemetry
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

// ---------------------------------------------------------------------------
// Agent runner
// ---------------------------------------------------------------------------

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
        resolveP({ result: resultText, durationMs, tokens, costUsd });
      }
    });

    child.on("error", (err) => {
      rejectP(new Error(`Failed to spawn agent "${name}": ${err.message}`));
    });
  });
}

async function readJSON(path) {
  try {
    return JSON.parse(await readFile(path, "utf-8"));
  } catch {
    return null;
  }
}

async function copyDir(src, dest) {
  await cp(src, dest, { recursive: true });
}

function formatBenchmarkText(benchmarkResults) {
  if (!benchmarkResults) return "";
  let text = "\n## Benchmark Comparison (targets to beat)\n";
  for (const [system, data] of Object.entries(benchmarkResults)) {
    if (typeof data === "object" && data.queries) {
      text += `${system}:`;
      for (const [q, ms] of Object.entries(data.queries)) {
        text += ` ${q}=${ms}ms`;
      }
      text += "\n";
    }
  }
  return text;
}

// ---------------------------------------------------------------------------
// Query parsing
// ---------------------------------------------------------------------------

function parseQueries(queriesContent) {
  const queries = [];
  const parts = queriesContent.split(/^-- (Q\d+)/m);
  for (let i = 1; i < parts.length; i += 2) {
    const id = parts[i].trim();
    let body = parts[i + 1];
    body = body.replace(/^[^\n]*\n/, "");
    body = body.replace(/^--[^\n]*\n/gm, "");
    const sql = body.trim();
    const name = id.toLowerCase();
    if (sql) {
      queries.push({ id, name, sql });
    }
  }
  return queries;
}

// ---------------------------------------------------------------------------
// Generate main.cpp, queries.h, and Makefile
// ---------------------------------------------------------------------------

async function generateBuildFiles(generatedDir, queries) {
  // queries.h
  const declarations = queries.map(
    (q) => `void run_${q.name}(const std::string& parquet_dir, const std::string& results_dir);`
  ).join("\n");

  const queriesH = `#pragma once
#include <string>

${declarations}
`;
  await writeFile(resolve(generatedDir, "queries", "queries.h"), queriesH);

  // main.cpp
  const calls = queries.map((q) => {
    return `    run_${q.name}(parquet_dir, results_dir);`;
  }).join("\n");

  const mainCpp = `#include <iostream>
#include <string>
#include <chrono>
#include "queries/queries.h"

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string parquet_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    auto total_start = std::chrono::high_resolution_clock::now();

${calls}

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "Total execution time: " << total_ms << " ms" << std::endl;

    return 0;
}
`;
  await writeFile(resolve(generatedDir, "main.cpp"), mainCpp);

  // Makefile — links Parquet/Arrow for I/O only, parquet_reader.h is header-only
  const makefile = `PYARROW_DIR := $(shell python3 -c "import pyarrow; print(pyarrow.__path__[0])")
ARROW_INCLUDE := -I$(PYARROW_DIR)/include
ARROW_LIBS := -L$(PYARROW_DIR) -larrow -lparquet -Wl,-rpath,$(PYARROW_DIR)

CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall $(ARROW_INCLUDE) -I.
LDFLAGS = $(ARROW_LIBS) -lpthread

QUERY_SRCS := $(wildcard queries/*.cpp)
QUERY_OBJS := $(QUERY_SRCS:.cpp=.o)

.PHONY: all clean setup_symlinks

all: main

setup_symlinks:
\t@cd $(PYARROW_DIR) && for f in *.so.*[0-9][0-9][0-9]; do base=$$(echo $$f | sed 's/\\.so\\..*/\\.so/'); [ ! -e $$base ] && ln -sf $$f $$base || true; done

main: setup_symlinks main.o $(QUERY_OBJS)
\t$(CXX) $(CXXFLAGS) -o main main.o $(QUERY_OBJS) $(LDFLAGS)

main.o: main.cpp queries/queries.h
\t$(CXX) $(CXXFLAGS) -c main.cpp -o main.o

queries/%.o: queries/%.cpp parquet_reader.h
\t$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
\trm -f main *.o queries/*.o
`;
  await writeFile(resolve(generatedDir, "Makefile"), makefile);
}

// ---------------------------------------------------------------------------
// Phase 1: Analysis + Data Preparation
// ---------------------------------------------------------------------------

async function runPhase1(args, runDir, schema, queries) {
  console.log("\n[Orchestrator] ========== PHASE 1: ANALYSIS + DATA PREPARATION ==========\n");

  await updateRunMeta(runDir, (meta) => {
    meta.dataDir = args.dataDir;
    meta.parquetDir = args.parquetDir;
    meta.scaleFactor = args.scaleFactor;
    meta.maxIterations = args.maxIterations;
    meta.model = args.model;
    meta.optimizationTarget = args.optimizationTarget;
    meta.phase1 = { status: "running" };
  });

  // --- Step 1: Workload Analysis ---
  console.log("\n[Orchestrator] === Step 1: Workload Analysis ===");
  const workloadAnalysisPath = resolve(runDir, "workload_analysis.txt");
  const parquetConfigPath = resolve(runDir, "parquet_config.json");

  const waResult = await runAgent(workloadAnalyzerConfig.name, {
    systemPrompt: await readFile(workloadAnalyzerConfig.promptPath, "utf-8"),
    userPrompt: [
      "Analyze the following workload and design Parquet configuration.",
      "",
      `## Optimization Target: ${args.optimizationTarget}`,
      `## Scale Factor: ${args.scaleFactor}`,
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
      `Write your workload analysis to: ${workloadAnalysisPath}`,
      `Write your Parquet configuration JSON to: ${parquetConfigPath}`,
    ].join("\n"),
    allowedTools: workloadAnalyzerConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
  recordAgentTelemetry("phase1", "workload_analyzer", waResult.durationMs, waResult.tokens, waResult.costUsd);

  if (!existsSync(workloadAnalysisPath)) {
    throw new Error("Workload analysis failed — analysis file not created.");
  }
  console.log("[Orchestrator] Workload analysis written.");

  // --- Step 2: Convert .tbl → Parquet ---
  console.log("\n[Orchestrator] === Step 2: Data Preparation (.tbl → Parquet) ===");
  const convertStart = Date.now();

  const convertArgs = [
    "python3", CONVERT_SCRIPT,
    "--data-dir", args.dataDir,
    "--output-dir", args.parquetDir,
  ];
  if (existsSync(parquetConfigPath)) {
    convertArgs.push("--config", parquetConfigPath);
  }

  try {
    const output = execSync(convertArgs.join(" "), {
      encoding: "utf-8",
      timeout: 600_000,
    });
    console.log(output);
  } catch (err) {
    throw new Error(`Parquet conversion failed: ${err.message}`);
  }

  const convertMs = Date.now() - convertStart;
  console.log(`[Orchestrator] Parquet conversion completed (${formatDuration(convertMs)})`);

  // Validate Parquet files exist
  const requiredParquet = ["lineitem.parquet", "orders.parquet", "customer.parquet", "nation.parquet"];
  const missingParquet = requiredParquet.filter((f) => !existsSync(resolve(args.parquetDir, f)));
  if (missingParquet.length > 0) {
    throw new Error(`Parquet conversion incomplete — missing: ${missingParquet.join(", ")}`);
  }

  // --- Step 3: Build indexes ---
  console.log("\n[Orchestrator] === Step 3: Build Indexes ===");
  const indexDir = resolve(args.parquetDir, "indexes");
  if (existsSync(parquetConfigPath)) {
    const indexStart = Date.now();
    try {
      const output = execSync(
        `python3 ${BUILD_INDEXES_SCRIPT} --parquet-dir ${args.parquetDir} --output-dir ${indexDir} --config ${parquetConfigPath}`,
        { encoding: "utf-8", timeout: 300_000 }
      );
      console.log(output);
      const indexMs = Date.now() - indexStart;
      console.log(`[Orchestrator] Index building completed (${formatDuration(indexMs)})`);
    } catch (err) {
      console.warn("[Orchestrator] Warning: index building failed: " + err.message);
    }
  } else {
    console.log("[Orchestrator] No parquet config — skipping index building.");
  }

  // --- Step 4: Prepare generated directory with infrastructure ---
  console.log("\n[Orchestrator] === Step 4: Generate Infrastructure ===");
  const generatedDir = resolve(runDir, "generated");
  await mkdir(generatedDir, { recursive: true });
  await mkdir(resolve(generatedDir, "queries"), { recursive: true });

  // Copy parquet_reader.h to generated directory
  await copyFile(PARQUET_READER_H, resolve(generatedDir, "parquet_reader.h"));
  console.log("[Orchestrator] parquet_reader.h copied.");

  // Setup symlinks for Arrow/Parquet shared libraries
  try {
    // Write a temporary Makefile just for symlinks
    const tempMakefile = `PYARROW_DIR := $(shell python3 -c "import pyarrow; print(pyarrow.__path__[0])")
.PHONY: setup_symlinks
setup_symlinks:
\t@cd $(PYARROW_DIR) && for f in *.so.*[0-9][0-9][0-9]; do base=$$(echo $$f | sed 's/\\.so\\..*/\\.so/'); [ ! -e $$base ] && ln -sf $$f $$base || true; done
`;
    await writeFile(resolve(generatedDir, "Makefile"), tempMakefile);
    execSync("make setup_symlinks", { cwd: generatedDir, encoding: "utf-8", timeout: 30_000 });
  } catch (err) {
    console.warn("[Orchestrator] Warning: symlink setup issue: " + err.message);
  }

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.status = "completed";
    meta.phase1.completedAt = new Date().toISOString();
  });

  return { generatedDir };
}

// ---------------------------------------------------------------------------
// Per-Query Optimization Pipeline
// ---------------------------------------------------------------------------

async function runQueryPipeline(query, args, runDir, templateGeneratedDir, groundTruthDir, parsedQueries) {
  const maxIter = args.maxIterations;
  const queryDir = resolve(runDir, "queries", query.name);
  await mkdir(queryDir, { recursive: true });

  console.log(`\n[Pipeline ${query.id}] Starting optimization pipeline (max ${maxIter} iterations)`);

  // Per-query optimization history
  const historyPath = resolve(queryDir, "optimization_history.txt");
  let historyText = `# Optimization History for ${query.id}\n`;
  await writeFile(historyPath, historyText);

  const hasGroundTruth = existsSync(groundTruthDir);

  // Track best result for this query
  let bestIterDir = null;
  let bestEvalPath = null;
  let bestTimeMs = Infinity;

  // =========================================================================
  // Iteration 0: Initial Code Generation
  // =========================================================================
  console.log(`\n[Pipeline ${query.id}] --- Iteration 0: Code Generation ---`);

  const iter0Dir = resolve(queryDir, "iter_0");
  const iter0GenDir = resolve(iter0Dir, "generated");
  await mkdir(resolve(iter0GenDir, "queries"), { recursive: true });

  // Copy infrastructure (Makefile, parquet_reader.h)
  await copyDir(templateGeneratedDir, iter0GenDir);

  const iter0EvalPath = resolve(iter0Dir, "evaluation.json");
  const iter0RecsPath = resolve(iter0Dir, "recommendations.txt");
  const iter0ResultsDir = resolve(iter0Dir, "query_results");
  await mkdir(iter0ResultsDir, { recursive: true });

  // --- Generate query code ---
  const qgSystemPrompt = await readFile(queryGeneratorConfig.promptPath, "utf-8");
  const qgResult = await runAgent(`${queryGeneratorConfig.name} (${query.id})`, {
    systemPrompt: qgSystemPrompt,
    userPrompt: [
      `Generate a high-performance C++ implementation for query ${query.id}.`,
      "",
      `## SQL Query`,
      "```sql",
      query.sql,
      "```",
      "",
      `## Schema: ${args.schema}`,
      `## Parquet Data Directory: ${args.parquetDir}`,
      `## Index Directory: ${resolve(args.parquetDir, "indexes")} (sorted index files for selective lookups)`,
      `## Workload Analysis: ${resolve(runDir, "workload_analysis.txt")}`,
      `## Output Directory: ${iter0GenDir}`,
      "",
      `Write the query implementation to: ${resolve(iter0GenDir, "queries", query.name + ".cpp")}`,
      `The function MUST be named: run_${query.name}`,
      `The function signature MUST be: void run_${query.name}(const std::string& parquet_dir, const std::string& results_dir)`,
      "",
      `IMPORTANT: The file must #include "parquet_reader.h" (already in the output directory).`,
      `Use comma (,) delimiter for CSV output.`,
      `Use std::fixed << std::setprecision(2) for decimal values.`,
      `Index files (.idx) may be available in the index directory for selective lookups — check what exists.`,
    ].join("\n"),
    allowedTools: queryGeneratorConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
  recordAgentTelemetry(`${query.name}_iter0`, "query_generator", qgResult.durationMs, qgResult.tokens, qgResult.costUsd);

  // Generate build files for this single query
  await generateBuildFiles(iter0GenDir, [query]);

  // --- Evaluate ---
  console.log(`\n[Pipeline ${query.id}] --- Iteration 0: Learner (evaluate + analyze) ---`);

  const evalInputs = [
    `mode: evaluate_and_analyze`,
    "",
    `## Evaluation Steps`,
    `1. Compile: cd ${iter0GenDir} && make clean && make all`,
    `2. Run: cd ${iter0GenDir} && ./main ${args.parquetDir} ${iter0ResultsDir}`,
    hasGroundTruth
      ? `3. Validate: python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${iter0ResultsDir}`
      : "3. Validate: no ground truth available",
    "",
    `## Write evaluation JSON to: ${iter0EvalPath}`,
    "",
    `## For optimization analysis:`,
    `- Optimization Target: ${args.optimizationTarget}`,
    `- Workload analysis: ${resolve(runDir, "workload_analysis.txt")}`,
    `- Current code: ${iter0GenDir}/queries/${query.name}.cpp`,
    `- Parquet data directory: ${args.parquetDir}`,
    `- Optimization history: ${historyPath}`,
    formatBenchmarkText(args.benchmarkResults),
    `Write recommendations to: ${iter0RecsPath}`,
  ];

  const eval0Result = await runAgent(`Learner (${query.id} iter0)`, {
    systemPrompt: await readFile(learnerConfig.promptPath, "utf-8"),
    userPrompt: evalInputs.join("\n"),
    allowedTools: learnerConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
  recordAgentTelemetry(`${query.name}_iter0`, "learner", eval0Result.durationMs, eval0Result.tokens, eval0Result.costUsd);

  const eval0 = await readJSON(iter0EvalPath);
  bestIterDir = iter0GenDir;
  bestEvalPath = iter0EvalPath;

  // Record initial result
  if (eval0?.queries) {
    const qData = eval0.queries[query.id] || eval0.queries[query.name] || Object.values(eval0.queries)[0];
    if (qData) {
      bestTimeMs = parseFloat(qData.time_ms) || Infinity;
      historyText += `\n## Iteration 0 (initial generation)\n`;
      historyText += `  ${query.id}: ${qData.time_ms}ms (${qData.status})\n`;
    }
  }
  await writeFile(historyPath, historyText);

  console.log(`[Pipeline ${query.id}] Iteration 0 complete — ${bestTimeMs}ms`);

  // =========================================================================
  // Iterations 1..N: Optimization Loop
  // =========================================================================
  for (let iteration = 1; iteration < maxIter; iteration++) {
    console.log(`\n[Pipeline ${query.id}] --- Optimization Iteration ${iteration}/${maxIter - 1} ---`);

    const iterDir = resolve(queryDir, `iter_${iteration}`);
    const iterGenDir = resolve(iterDir, "generated");
    const iterEvalPath = resolve(iterDir, "evaluation.json");
    const iterRecsPath = resolve(iterDir, "recommendations.txt");
    const iterDecisionPath = resolve(iterDir, "decision.json");
    const iterResultsDir = resolve(iterDir, "query_results");
    await mkdir(iterResultsDir, { recursive: true });

    // Copy best code to this iteration
    await copyDir(bestIterDir, iterGenDir);

    // Read previous recommendations
    const prevRecsPath = iteration === 1
      ? iter0RecsPath
      : resolve(queryDir, `iter_${iteration - 1}`, "recommendations.txt");
    if (!existsSync(prevRecsPath)) {
      console.log(`[Pipeline ${query.id}] No recommendations from previous iteration. Stopping.`);
      break;
    }

    // --- Orchestrator Agent ---
    console.log(`[Pipeline ${query.id}] Orchestrator Agent deciding...`);
    const oaInputs = [
      `Decide optimization strategy for ${query.id}, iteration ${iteration}/${maxIter - 1}.`,
      "",
      `## Optimization Target: ${args.optimizationTarget}`,
      "",
      "## Input Files",
      `- Learner recommendations: ${prevRecsPath}`,
      `- Optimization history: ${historyPath}`,
      `- Current evaluation: ${bestEvalPath}`,
      formatBenchmarkText(args.benchmarkResults),
      "",
      `Remaining iterations after this: ${maxIter - 1 - iteration}`,
      "",
      `Write decision JSON to: ${iterDecisionPath}`,
    ];

    const oaResult = await runAgent(`Orchestrator Agent (${query.id} iter${iteration})`, {
      systemPrompt: await readFile(orchestratorAgentConfig.promptPath, "utf-8"),
      userPrompt: oaInputs.join("\n"),
      allowedTools: orchestratorAgentConfig.allowedTools,
      model: args.model,
      cwd: runDir,
    });
    recordAgentTelemetry(`${query.name}_iter${iteration}`, "orchestrator_agent", oaResult.durationMs, oaResult.tokens, oaResult.costUsd);

    const decision = await readJSON(iterDecisionPath);
    if (!decision) {
      console.error(`[Pipeline ${query.id}] Orchestrator Agent failed. Stopping.`);
      break;
    }

    console.log(`[Pipeline ${query.id}] Decision: ${decision.action} — ${decision.reasoning}`);

    if (decision.action === "stop") {
      console.log(`[Pipeline ${query.id}] Stopping optimization loop.`);
      break;
    }

    // --- Run Optimizers ---
    if (decision.action === "optimize") {
      const optimizations = decision.optimizations || [];
      if (optimizations.length === 0) {
        console.log(`[Pipeline ${query.id}] No optimizations selected. Skipping.`);
        continue;
      }

      const parallelOps = optimizations.filter((o) => o.can_parallel);
      const sequentialOps = optimizations.filter((o) => !o.can_parallel);

      // Run sequential optimizers first
      for (const op of sequentialOps) {
        console.log(`[Pipeline ${query.id}] Running ${op.optimizer} (sequential)...`);
        const result = await invokeOptimizer(op, args, runDir, iterGenDir, bestEvalPath, query);
        if (result) {
          recordAgentTelemetry(`${query.name}_iter${iteration}`, op.optimizer, result.durationMs, result.tokens, result.costUsd);
        }
      }

      // Run parallel optimizers together
      if (parallelOps.length > 0) {
        console.log(`[Pipeline ${query.id}] Running ${parallelOps.length} optimizers in parallel...`);
        const results = await Promise.all(
          parallelOps.map(async (op) => {
            const result = await invokeOptimizer(op, args, runDir, iterGenDir, bestEvalPath, query);
            return { op, result };
          })
        );
        for (const { op, result } of results) {
          if (result) {
            recordAgentTelemetry(`${query.name}_iter${iteration}`, op.optimizer, result.durationMs, result.tokens, result.costUsd);
          }
        }
      }
    }

    // --- Evaluate ---
    console.log(`[Pipeline ${query.id}] Learner evaluating iteration ${iteration}...`);

    const iterEvalInputs = [
      `mode: evaluate_and_analyze`,
      "",
      `## Evaluation Steps`,
      `1. Compile: cd ${iterGenDir} && make clean && make all`,
      `2. Run: cd ${iterGenDir} && ./main ${args.parquetDir} ${iterResultsDir}`,
      hasGroundTruth
        ? `3. Validate: python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${iterResultsDir}`
        : "3. Validate: no ground truth available",
      "",
      `## Write evaluation JSON to: ${iterEvalPath}`,
      "",
      `## For optimization analysis:`,
      `- Optimization Target: ${args.optimizationTarget}`,
      `- Workload analysis: ${resolve(runDir, "workload_analysis.txt")}`,
      `- Current code: ${iterGenDir}/queries/${query.name}.cpp`,
      `- Parquet data directory: ${args.parquetDir}`,
      `- Optimization history: ${historyPath}`,
      formatBenchmarkText(args.benchmarkResults),
      `Write recommendations to: ${iterRecsPath}`,
    ];

    const iterEvalResult = await runAgent(`Learner (${query.id} iter${iteration})`, {
      systemPrompt: await readFile(learnerConfig.promptPath, "utf-8"),
      userPrompt: iterEvalInputs.join("\n"),
      allowedTools: learnerConfig.allowedTools,
      model: args.model,
      cwd: runDir,
    });
    recordAgentTelemetry(`${query.name}_iter${iteration}`, "learner", iterEvalResult.durationMs, iterEvalResult.tokens, iterEvalResult.costUsd);

    const iterEval = await readJSON(iterEvalPath);

    // Check improvement
    const improved = await checkQueryImprovement(bestEvalPath, iterEvalPath, query);
    const optimizerNames = (decision.optimizations || []).map((o) => o.optimizer).join(", ");

    if (improved.isImproved) {
      console.log(`[Pipeline ${query.id}] Iteration ${iteration} IMPROVED (${improved.newTimeMs}ms vs ${bestTimeMs}ms)`);
      bestIterDir = iterGenDir;
      bestEvalPath = iterEvalPath;
      bestTimeMs = improved.newTimeMs;
      historyText += `\n## Iteration ${iteration}: IMPROVED (${improved.newTimeMs}ms)\n`;
      historyText += `Optimizers: ${optimizerNames}\n`;
    } else {
      console.log(`[Pipeline ${query.id}] Iteration ${iteration} REGRESSED. Rolling back.`);
      historyText += `\n## Iteration ${iteration}: REGRESSED (rolled back)\n`;
      historyText += `Optimizers: ${optimizerNames}\n`;
      historyText += `Avoid repeating this approach.\n`;
    }
    await writeFile(historyPath, historyText);
  }

  console.log(`[Pipeline ${query.id}] Pipeline complete — best: ${bestTimeMs}ms`);
  return { query, bestIterDir, bestTimeMs, bestEvalPath };
}

// ---------------------------------------------------------------------------
// Optimizer invocation
// ---------------------------------------------------------------------------

async function invokeOptimizer(op, args, runDir, generatedDir, evalPath, query) {
  const config = OPTIMIZER_MAP[op.optimizer];
  if (!config) {
    console.error(`[Orchestrator] Unknown optimizer: ${op.optimizer}`);
    return null;
  }

  const systemPrompt = await readFile(config.promptPath, "utf-8");
  const userPrompt = [
    `Apply the following optimization to ${query.id}.`,
    "",
    `## Guidance`,
    op.guidance || "See recommendations file for details.",
    "",
    `## Target Query: ${query.id} (${generatedDir}/queries/${query.name}.cpp)`,
    "",
    `## Code Directory: ${generatedDir}`,
    `## Parquet Data Directory: ${args.parquetDir} (read-only, NEVER modify)`,
    `## Index Directory: ${resolve(args.parquetDir, "indexes")} (sorted index files, if available)`,
    `## Build Indexes Tool: python3 ${BUILD_INDEXES_SCRIPT} (for index_optimizer only)`,
    "",
    `## Current Performance`,
    `Read evaluation: ${evalPath}`,
    `Read workload analysis: ${resolve(runDir, "workload_analysis.txt")}`,
    "",
    `After changes, compile and run to verify:`,
    `  cd ${generatedDir} && make clean && make all && ./main ${args.parquetDir}`,
    `Results must remain correct. Correctness is paramount.`,
  ].join("\n");

  try {
    return await runAgent(`${config.name} (${query.id})`, {
      systemPrompt,
      userPrompt,
      allowedTools: config.allowedTools,
      model: args.model,
      cwd: runDir,
    });
  } catch (err) {
    console.error(`[Orchestrator] Optimizer ${op.optimizer} (${query.id}) failed: ${err.message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Per-query improvement check
// ---------------------------------------------------------------------------

async function checkQueryImprovement(prevEvalPath, newEvalPath, query) {
  try {
    const prevEval = JSON.parse(await readFile(prevEvalPath, "utf-8"));
    const newEval = JSON.parse(await readFile(newEvalPath, "utf-8"));

    const prevQueries = prevEval.queries || {};
    const newQueries = newEval.queries || {};

    const prevQ = prevQueries[query.id] || prevQueries[query.name] || Object.values(prevQueries)[0];
    const newQ = newQueries[query.id] || newQueries[query.name] || Object.values(newQueries)[0];

    if (!prevQ || !newQ) return { isImproved: false, newTimeMs: Infinity };

    const prevTime = parseFloat(prevQ.time_ms) || Infinity;
    const newTime = parseFloat(newQ.time_ms) || Infinity;

    // If previous passed but new fails, it's a regression
    if (prevQ.status === "pass" && newQ.status !== "pass") {
      return { isImproved: false, newTimeMs: newTime };
    }

    // If new passes but previous didn't, it's an improvement
    if (newQ.status === "pass" && prevQ.status !== "pass") {
      return { isImproved: true, newTimeMs: newTime };
    }

    // Both pass (or both fail): compare times
    return { isImproved: newTime < prevTime, newTimeMs: newTime };
  } catch {
    return { isImproved: false, newTimeMs: Infinity };
  }
}

// ---------------------------------------------------------------------------
// Phase 2: Per-Query Parallel Optimization
// ---------------------------------------------------------------------------

async function runPhase2(args, runDir, templateGeneratedDir, queriesContent) {
  console.log(`\n[Orchestrator] ========== PHASE 2: PER-QUERY PARALLEL OPTIMIZATION ==========\n`);

  await updateRunMeta(runDir, (meta) => {
    meta.phase2 = { status: "running" };
  });

  const parsedQueries = parseQueries(queriesContent);
  console.log(`[Orchestrator] Parsed ${parsedQueries.length} queries: ${parsedQueries.map(q => q.id).join(", ")}`);

  const groundTruthDir = resolve(BENCHMARKS_DIR, defaults.targetBenchmark, "query_results");

  // Run ALL query pipelines in parallel
  console.log(`[Orchestrator] Launching ${parsedQueries.length} parallel query pipelines...`);

  const pipelineResults = await Promise.all(
    parsedQueries.map((query) =>
      runQueryPipeline(query, args, runDir, templateGeneratedDir, groundTruthDir, parsedQueries)
        .catch((err) => {
          console.error(`[Pipeline ${query.id}] FAILED: ${err.message}`);
          return { query, bestIterDir: null, bestTimeMs: Infinity, bestEvalPath: null };
        })
    )
  );

  // =========================================================================
  // Assemble final build from best per-query results
  // =========================================================================
  console.log(`\n[Orchestrator] === Assembling Final Build ===`);

  const finalGenDir = resolve(runDir, "generated");
  // Ensure clean final directory
  await mkdir(resolve(finalGenDir, "queries"), { recursive: true });
  await copyFile(PARQUET_READER_H, resolve(finalGenDir, "parquet_reader.h"));

  const successfulQueries = [];

  for (const { query, bestIterDir } of pipelineResults) {
    if (!bestIterDir) {
      console.error(`[Orchestrator] No successful iteration for ${query.id} — skipping.`);
      continue;
    }

    // Copy best query .cpp to final directory
    const srcCpp = resolve(bestIterDir, "queries", `${query.name}.cpp`);
    const dstCpp = resolve(finalGenDir, "queries", `${query.name}.cpp`);
    if (existsSync(srcCpp)) {
      await copyFile(srcCpp, dstCpp);
      successfulQueries.push(query);
      console.log(`[Orchestrator] ${query.id}: copied from best iteration`);
    }
  }

  // Generate final main.cpp, queries.h, Makefile
  await generateBuildFiles(finalGenDir, successfulQueries);

  // Final build + validation
  console.log(`\n[Orchestrator] === Final Build ===`);
  const finalResultsDir = resolve(runDir, "final_results");
  await mkdir(finalResultsDir, { recursive: true });

  try {
    execSync("make clean && make all", {
      cwd: finalGenDir,
      encoding: "utf-8",
      timeout: 120_000,
    });
    console.log("[Orchestrator] Final build: SUCCESS");

    const runOutput = execSync(`./main ${args.parquetDir} ${finalResultsDir}`, {
      cwd: finalGenDir,
      encoding: "utf-8",
      timeout: 300_000,
    });
    console.log(runOutput);

    // Validate against ground truth
    if (existsSync(groundTruthDir)) {
      try {
        const validateOutput = execSync(
          `python3 ${COMPARE_TOOL_PATH} ${groundTruthDir} ${finalResultsDir}`,
          { encoding: "utf-8", timeout: 60_000 }
        );
        console.log(validateOutput);
      } catch (err) {
        console.warn("[Orchestrator] Validation issues: " + err.stdout);
      }
    }
  } catch (err) {
    console.error("[Orchestrator] Final build/run failed: " + err.message);
  }

  // Print per-query summary
  console.log(`\n[Orchestrator] === Per-Query Results ===`);
  for (const { query, bestTimeMs } of pipelineResults) {
    const status = bestTimeMs < Infinity ? `${bestTimeMs.toFixed(1)}ms` : "FAILED";
    console.log(`  ${query.id}: ${status}`);
  }

  await updateRunMeta(runDir, (meta) => {
    meta.phase2.status = "completed";
    meta.phase2.completedAt = new Date().toISOString();
    meta.phase2.queryResults = pipelineResults.map(({ query, bestTimeMs }) => ({
      query: query.id,
      best_time_ms: bestTimeMs < Infinity ? bestTimeMs : null,
    }));
  });
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
    console.error(`[Orchestrator] Run 'bash benchmarks/tpc-h/setup_data.sh ${args.scaleFactor}' first.`);
    process.exit(1);
  }

  const workload = getWorkloadName(args.schema);
  const runId = createRunId();
  const runDir = await createRunDir(workload, runId);

  // Load benchmark comparison data
  const benchmarkResultsPath = resolve(
    BENCHMARKS_DIR, defaults.targetBenchmark,
    "results", `sf${args.scaleFactor}`, "metrics", "benchmark_results.json"
  );
  args.benchmarkResults = await readJSON(benchmarkResultsPath);
  if (args.benchmarkResults) {
    console.log(`[Orchestrator] Benchmark data loaded from: ${benchmarkResultsPath}`);
  }

  console.log("[Orchestrator] GenDB Pipeline (v7 — Parquet storage, pure C++, per-query parallel optimization)");
  console.log(`[Orchestrator] Schema:      ${args.schema}`);
  console.log(`[Orchestrator] Queries:     ${args.queries}`);
  console.log(`[Orchestrator] Data Dir:    ${args.dataDir}`);
  console.log(`[Orchestrator] Parquet Dir: ${args.parquetDir}`);
  console.log(`[Orchestrator] SF:          ${args.scaleFactor}`);
  console.log(`[Orchestrator] Iterations:  ${args.maxIterations}`);
  console.log(`[Orchestrator] Model:       ${args.model}`);
  console.log(`[Orchestrator] Target:      ${args.optimizationTarget}`);
  console.log(`[Orchestrator] Run Dir:     ${runDir}`);

  // Phase 1: Analysis + Parquet conversion + infrastructure
  const { generatedDir } = await runPhase1(args, runDir, schema, queries);

  // Phase 2: Per-query parallel optimization
  await runPhase2(args, runDir, generatedDir, queries);

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

  // Print summary
  console.log(`\n[Orchestrator] === Run Summary ===`);
  console.log(`[Orchestrator] Total time: ${formatDuration(telemetryData.total_wall_clock_ms)}`);
  console.log(`[Orchestrator] Total tokens: ${Math.round(telemetryData.total_tokens.input / 1000)}K input, ${Math.round(telemetryData.total_tokens.output / 1000)}K output`);
  if (telemetryData.total_tokens.cache_read > 0) {
    console.log(`[Orchestrator] Cache: ${Math.round(telemetryData.total_tokens.cache_read / 1000)}K read, ${Math.round(telemetryData.total_tokens.cache_creation / 1000)}K creation`);
  }
  console.log(`[Orchestrator] Cost: $${telemetryData.total_cost_usd.toFixed(2)}`);

  const phaseSummaries = [];
  for (const [name, phase] of Object.entries(telemetryData.phases)) {
    const cost = Object.values(phase.agents || {}).reduce((s, a) => s + a.cost_usd, 0);
    phaseSummaries.push(`${name}: ${formatDuration(phase.total_ms)} ($${cost.toFixed(2)})`);
  }
  if (phaseSummaries.length > 0) {
    console.log(`[Orchestrator] ${phaseSummaries.join(" | ")}`);
  }

  console.log(`\n[Orchestrator] Pipeline complete.`);
  console.log(`[Orchestrator] Run Dir: ${runDir}`);
  console.log(`[Orchestrator] Latest: output/${workload}/latest → ${runId}`);
}

main().catch((err) => {
  console.error("[Orchestrator] Fatal:", err.message);
  process.exit(1);
});
