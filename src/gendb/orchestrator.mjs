/**
 * GenDB Orchestrator
 *
 * Two-phase agentic system:
 *   Phase 1 (Baseline): Workload Analyzer → Storage/Index Designer → Code Generator → Evaluator
 *   Phase 2 (Optimization Loop): Learner → Orchestrator Agent → Operator Specialist → Evaluator
 *
 * Usage: node src/gendb/orchestrator.mjs [--schema <path>] [--queries <path>] [--data-dir <path>]
 *        [--sf <N>] [--max-iterations <N>] [--model <name>]
 */

import { spawn } from "child_process";
import { readFile, writeFile, mkdir, cp } from "fs/promises";
import { resolve } from "path";
import { existsSync } from "fs";
import {
  DEFAULT_SCHEMA,
  DEFAULT_QUERIES,
  BENCHMARKS_DIR,
  getDataDir,
} from "./config.mjs";
import { defaults } from "./gendb.config.mjs";
import { config as workloadAnalyzerConfig } from "./agents/workload-analyzer/index.mjs";
import { config as storageDesignerConfig } from "./agents/storage-index-designer/index.mjs";
import { config as codeGeneratorConfig } from "./agents/code-generator/index.mjs";
import { config as evaluatorConfig } from "./agents/evaluator/index.mjs";
import { config as learnerConfig } from "./agents/learner/index.mjs";
import { config as operatorSpecialistConfig } from "./agents/operator-specialist/index.mjs";
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
    scaleFactor: defaults.scaleFactor,
    maxIterations: defaults.maxOptimizationIterations,
    model: defaults.model,
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--schema" && argv[i + 1]) args.schema = resolve(argv[++i]);
    if (argv[i] === "--queries" && argv[i + 1]) args.queries = resolve(argv[++i]);
    if (argv[i] === "--data-dir" && argv[i + 1]) args.dataDir = resolve(argv[++i]);
    if (argv[i] === "--sf" && argv[i + 1]) args.scaleFactor = parseInt(argv[++i], 10);
    if (argv[i] === "--max-iterations" && argv[i + 1]) args.maxIterations = parseInt(argv[++i], 10);
    if (argv[i] === "--model" && argv[i + 1]) args.model = argv[++i];
  }
  // If no explicit --data-dir, resolve from scale factor
  if (!args.dataDir) {
    args.dataDir = getDataDir(defaults.targetBenchmark, args.scaleFactor);
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

/**
 * Invoke a Claude Code subprocess with the given system prompt and user prompt.
 * Returns the full stdout text.
 */
function runAgent(name, { systemPrompt, userPrompt, allowedTools, model, cwd }) {
  return new Promise((resolveP, rejectP) => {
    const args = [
      "--print",
      "--system-prompt", systemPrompt,
      "--output-format", "text",
      "--permission-mode", "bypassPermissions",
      "--allowedTools", allowedTools.join(","),
    ];
    if (model) args.push("--model", model);
    args.push(userPrompt);

    console.log(`\n[${"=".repeat(60)}]`);
    console.log(`[Orchestrator] Spawning agent: ${name}`);
    console.log(`[${"=".repeat(60)}]\n`);

    const child = spawn("claude", args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env },
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      stdout += text;
      process.stdout.write(text);
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("close", (code) => {
      if (code !== 0) {
        console.error(`\n[Orchestrator] Agent "${name}" exited with code ${code}`);
        if (stderr) console.error(`[stderr] ${stderr}`);
        rejectP(new Error(`Agent "${name}" failed (exit ${code}): ${stderr}`));
      } else {
        console.log(`\n[Orchestrator] Agent "${name}" completed successfully.`);
        resolveP(stdout);
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
    meta.scaleFactor = args.scaleFactor;
    meta.maxIterations = args.maxIterations;
    meta.model = args.model;
    meta.phase1 = {
      status: "running",
      steps: {
        workload_analysis: { status: "pending" },
        storage_design: { status: "pending" },
        code_generation: { status: "pending" },
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

  await runAgent(workloadAnalyzerConfig.name, {
    systemPrompt: analyzerSystemPrompt,
    userPrompt: analyzerUserPrompt,
    allowedTools: workloadAnalyzerConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });

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

  await runAgent(storageDesignerConfig.name, {
    systemPrompt: designerSystemPrompt,
    userPrompt: designerUserPrompt,
    allowedTools: storageDesignerConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });

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
    "Generate C++ code for this workload.",
    "",
    "## Input Files",
    `- Workload analysis: ${workloadAnalysisPath}`,
    `- Storage design: ${storageDesignPath}`,
    `- Schema: ${args.schema}`,
    `- Queries: ${args.queries}`,
    "",
    `## Data Directory`,
    `TPC-H .tbl files are located at: ${args.dataDir}`,
    "",
    `## Output Directory`,
    `Write all generated files to: ${generatedDir}`,
    `Subdirectories already exist: utils/, storage/, index/, queries/`,
    "",
    `Files to produce:`,
    `- ${resolve(generatedDir, "utils/date_utils.h")}`,
    `- ${resolve(generatedDir, "storage/storage.h")}`,
    `- ${resolve(generatedDir, "storage/storage.cpp")}`,
    `- ${resolve(generatedDir, "index/index.h")}`,
    `- ${resolve(generatedDir, "queries/queries.h")}`,
    `- ${resolve(generatedDir, "queries/q1.cpp")}`,
    `- ${resolve(generatedDir, "queries/q3.cpp")}`,
    `- ${resolve(generatedDir, "queries/q6.cpp")}`,
    `- ${resolve(generatedDir, "main.cpp")}`,
    `- ${resolve(generatedDir, "Makefile")}`,
    "",
    `IMPORTANT: Use the Write tool to create each file. The files must compile with g++ -O2 -std=c++17. After writing all files, use Bash to verify compilation: cd ${generatedDir} && make clean && make all`,
  ].join("\n");

  await runAgent(codeGeneratorConfig.name, {
    systemPrompt: generatorSystemPrompt,
    userPrompt: generatorUserPrompt,
    allowedTools: codeGeneratorConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });

  // Verify generated files exist
  const requiredFiles = [
    "utils/date_utils.h",
    "storage/storage.h",
    "storage/storage.cpp",
    "index/index.h",
    "queries/queries.h",
    "queries/q1.cpp",
    "queries/q3.cpp",
    "queries/q6.cpp",
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

  // --- Step 4: Evaluation ---
  console.log("\n[Orchestrator] === Step 4: Evaluation ===");

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.evaluation.status = "running";
    meta.phase1.steps.evaluation.startedAt = new Date().toISOString();
  });

  const evaluationPath = resolve(runDir, "evaluation.json");
  await runEvaluator(args, runDir, generatedDir, evaluationPath);

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

async function runEvaluator(args, runDir, generatedDir, evaluationPath) {
  const evaluatorSystemPrompt = await readFile(evaluatorConfig.promptPath, "utf-8");

  const evaluatorUserPrompt = [
    "Evaluate the generated C++ code.",
    "",
    `## Generated code directory`,
    `${generatedDir}`,
    "",
    `## TPC-H data directory`,
    `${args.dataDir}`,
    "",
    `## Evaluation steps`,
    `1. cd ${generatedDir} && make clean && make all`,
    `2. cd ${generatedDir} && ./main ${args.dataDir}`,
    "",
    `IMPORTANT: Write the evaluation report to: ${evaluationPath}`,
    `Use the Write tool to create this file. Do NOT write it inside generated/ — write it to the exact path above.`,
  ].join("\n");

  await runAgent(evaluatorConfig.name, {
    systemPrompt: evaluatorSystemPrompt,
    userPrompt: evaluatorUserPrompt,
    allowedTools: evaluatorConfig.allowedTools,
    model: args.model,
    cwd: runDir,
  });
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

    await runAgent(learnerConfig.name, {
      systemPrompt: learnerSystemPrompt,
      userPrompt: learnerUserPrompt,
      allowedTools: learnerConfig.allowedTools,
      model: args.model,
      cwd: runDir,
    });

    const recommendations = await readJSON(iterRecsPath);
    if (!recommendations) {
      console.error(`[Orchestrator] Learner failed to produce recommendations for iteration ${iteration}. Stopping loop.`);
      await updateIterationStatus(runDir, iteration, "failed", "Learner failed to produce recommendations");
      break;
    }
    console.log(`[Orchestrator] Learner produced ${(recommendations.recommendations || []).length} recommendations.`);

    // 3. Run Orchestrator Agent
    console.log(`\n[Orchestrator] === Iteration ${iteration} Step 2: Orchestrator Agent ===`);
    const orchAgentSystemPrompt = await readFile(orchestratorAgentConfig.promptPath, "utf-8");

    const orchAgentUserPrompt = [
      `Decide whether to continue optimizing or stop (iteration ${iteration}/${maxIter}).`,
      "",
      "## Input Files",
      `- Current evaluation results: ${bestEvaluationPath}`,
      `- Learner recommendations: ${iterRecsPath}`,
      `- Optimization history: ${historyPath}`,
      formatBenchmarkContext(args.benchmarkResults),
      "",
      `Remaining iterations after this one: ${maxIter - iteration}`,
      "",
      `## Output`,
      `IMPORTANT: Write your decision to: ${iterDecisionPath}`,
      `Use the Write tool to create this file.`,
    ].join("\n");

    await runAgent(orchestratorAgentConfig.name, {
      systemPrompt: orchAgentSystemPrompt,
      userPrompt: orchAgentUserPrompt,
      allowedTools: orchestratorAgentConfig.allowedTools,
      model: args.model,
      cwd: runDir,
    });

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

    // 5. Run Operator Specialist
    console.log(`\n[Orchestrator] === Iteration ${iteration} Step 3: Operator Specialist ===`);
    const opSpecSystemPrompt = await readFile(operatorSpecialistConfig.promptPath, "utf-8");

    const opSpecUserPrompt = [
      `Apply selected optimizations to the C++ code for iteration ${iteration}.`,
      "",
      "## Input Files",
      `- Orchestrator decision: ${iterDecisionPath}`,
      `- Optimization recommendations: ${iterRecsPath}`,
      `- Evaluation results: ${bestEvaluationPath}`,
      `- Workload analysis: ${resolve(runDir, "workload_analysis.json")}`,
      `- Storage design: ${resolve(runDir, "storage_design.json")}`,
      formatBenchmarkContext(args.benchmarkResults),
      "",
      "## Code Directory",
      `Read and modify C++ files in: ${iterGeneratedDir}`,
      `This directory contains the current best code. Apply optimizations here.`,
      "",
      `## Important`,
      `- Only apply the recommendations selected in the orchestrator's decision (selected_recommendations field)`,
      `- After making changes, verify compilation: cd ${iterGeneratedDir} && make clean && make all`,
      `- Correctness is paramount — results must remain identical`,
    ].join("\n");

    await runAgent(operatorSpecialistConfig.name, {
      systemPrompt: opSpecSystemPrompt,
      userPrompt: opSpecUserPrompt,
      allowedTools: operatorSpecialistConfig.allowedTools,
      model: args.model,
      cwd: runDir,
    });

    // 6. Run Evaluator
    console.log(`\n[Orchestrator] === Iteration ${iteration} Step 4: Evaluator ===`);
    await runEvaluator(args, runDir, iterGeneratedDir, iterEvaluationPath);

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
    } else {
      console.log(`[Orchestrator] Iteration ${iteration} did not improve. Keeping previous best.`);
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
 * Compares total query time across all queries.
 * Returns true if new is strictly better.
 */
function checkImprovement(prevEval, newEval) {
  if (!prevEval || !newEval) return false;
  if (!prevEval.query_results || !newEval.query_results) return false;

  // Must at least pass correctness
  if (newEval.overall_status === "fail") return false;

  let prevTotal = 0;
  let newTotal = 0;
  let hasTimings = false;

  for (const qId of Object.keys(prevEval.query_results)) {
    const prevQ = prevEval.query_results[qId];
    const newQ = newEval.query_results[qId];
    if (!newQ) return false; // regression: query missing

    const prevTime = parseFloat(prevQ.timing_ms);
    const newTime = parseFloat(newQ.timing_ms);

    if (!isNaN(prevTime) && !isNaN(newTime)) {
      prevTotal += prevTime;
      newTotal += newTime;
      hasTimings = true;
    }
  }

  if (!hasTimings) return false;
  return newTotal < prevTotal;
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
  console.log(`[Orchestrator] Schema:         ${args.schema}`);
  console.log(`[Orchestrator] Queries:        ${args.queries}`);
  console.log(`[Orchestrator] Data Dir:       ${args.dataDir}`);
  console.log(`[Orchestrator] Scale Factor:   ${args.scaleFactor}`);
  console.log(`[Orchestrator] Max Iterations: ${args.maxIterations}`);
  console.log(`[Orchestrator] Model:          ${args.model}`);
  console.log(`[Orchestrator] Workload:       ${workload}`);
  console.log(`[Orchestrator] Run ID:         ${runId}`);
  console.log(`[Orchestrator] Run Dir:        ${runDir}`);

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

  console.log("\n[Orchestrator] Pipeline complete.");
  console.log(`[Orchestrator] Run Dir:  ${runDir}`);
  console.log(`[Orchestrator] Latest symlink: output/${workload}/latest → ${runId}`);
}

main().catch((err) => {
  console.error("[Orchestrator] Fatal error:", err.message);
  process.exit(1);
});
