/**
 * GenDB Orchestrator
 *
 * Spawns specialized agents via the Claude CLI to analyze workloads,
 * design storage, generate C++ code, and evaluate the results.
 *
 * Pipeline: Workload Analyzer → Storage/Index Designer → Code Generator → Evaluator
 *
 * Usage: node src/gendb/orchestrator.mjs [--schema <path>] [--queries <path>] [--data-dir <path>]
 */

import { spawn } from "child_process";
import { readFile, writeFile, mkdir } from "fs/promises";
import { resolve } from "path";
import { existsSync } from "fs";
import { DEFAULT_SCHEMA, DEFAULT_QUERIES, BENCHMARKS_DIR } from "./config.mjs";
import { config as workloadAnalyzerConfig } from "./agents/workload-analyzer/index.mjs";
import { config as storageDesignerConfig } from "./agents/storage-index-designer/index.mjs";
import { config as codeGeneratorConfig } from "./agents/code-generator/index.mjs";
import { config as evaluatorConfig } from "./agents/evaluator/index.mjs";
import { createRunId, getWorkloadName, createRunDir, updateLatestSymlink } from "./utils/paths.mjs";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Parse simple CLI args */
function parseArgs(argv) {
  const args = {
    schema: DEFAULT_SCHEMA,
    queries: DEFAULT_QUERIES,
    dataDir: resolve(BENCHMARKS_DIR, "tpc-h/data"),
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--schema" && argv[i + 1]) args.schema = resolve(argv[++i]);
    if (argv[i] === "--queries" && argv[i + 1]) args.queries = resolve(argv[++i]);
    if (argv[i] === "--data-dir" && argv[i + 1]) args.dataDir = resolve(argv[++i]);
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
    console.error(`[Orchestrator] Run 'bash benchmarks/tpc-h/setup_data.sh' first to generate TPC-H data.`);
    process.exit(1);
  }

  // Determine workload and create run directory
  const workload = getWorkloadName(args.schema);
  const runId = createRunId();
  const runDir = await createRunDir(workload, runId);

  console.log("[Orchestrator] GenDB Pipeline");
  console.log(`[Orchestrator] Schema:   ${args.schema}`);
  console.log(`[Orchestrator] Queries:  ${args.queries}`);
  console.log(`[Orchestrator] Data Dir: ${args.dataDir}`);
  console.log(`[Orchestrator] Workload: ${workload}`);
  console.log(`[Orchestrator] Run ID:   ${runId}`);
  console.log(`[Orchestrator] Run Dir:  ${runDir}`);

  // Initialize step tracking in run.json
  await updateRunMeta(runDir, (meta) => {
    meta.dataDir = args.dataDir;
    meta.steps = {
      workload_analysis: { status: "pending" },
      storage_design: { status: "pending" },
      code_generation: { status: "pending" },
      evaluation: { status: "pending" },
    };
  });

  // -------------------------------------------------------------------------
  // Step 1: Workload Analysis
  // -------------------------------------------------------------------------
  console.log("\n[Orchestrator] === Step 1: Workload Analysis ===");

  await updateRunMeta(runDir, (meta) => {
    meta.steps.workload_analysis.status = "running";
    meta.steps.workload_analysis.startedAt = new Date().toISOString();
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
    model: workloadAnalyzerConfig.model,
    cwd: runDir,
  });

  // Verify workload analysis output
  try {
    const analysis = JSON.parse(await readFile(workloadAnalysisPath, "utf-8"));
    console.log("\n[Orchestrator] Workload analysis written successfully.");
    console.log(`[Orchestrator] Tables analyzed: ${Object.keys(analysis.tables || {}).length}`);
    console.log(`[Orchestrator] Joins found: ${(analysis.joins || []).length}`);
    console.log(`[Orchestrator] Filters found: ${(analysis.filters || []).length}`);

    await updateRunMeta(runDir, (meta) => {
      meta.steps.workload_analysis.status = "completed";
      meta.steps.workload_analysis.completedAt = new Date().toISOString();
    });
  } catch (err) {
    console.error(`[Orchestrator] Error: could not read/parse workload_analysis.json: ${err.message}`);
    await updateRunMeta(runDir, (meta) => {
      meta.steps.workload_analysis.status = "failed";
      meta.steps.workload_analysis.error = err.message;
      meta.status = "failed";
    });
    throw new Error("Workload analysis failed — cannot continue pipeline.");
  }

  // -------------------------------------------------------------------------
  // Step 2: Storage/Index Design
  // -------------------------------------------------------------------------
  console.log("\n[Orchestrator] === Step 2: Storage/Index Design ===");

  await updateRunMeta(runDir, (meta) => {
    meta.steps.storage_design.status = "running";
    meta.steps.storage_design.startedAt = new Date().toISOString();
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
    model: storageDesignerConfig.model,
    cwd: runDir,
  });

  // Verify storage design output
  try {
    const design = JSON.parse(await readFile(storageDesignPath, "utf-8"));
    console.log("\n[Orchestrator] Storage design written successfully.");
    console.log(`[Orchestrator] Tables designed: ${Object.keys(design.tables || {}).length}`);

    await updateRunMeta(runDir, (meta) => {
      meta.steps.storage_design.status = "completed";
      meta.steps.storage_design.completedAt = new Date().toISOString();
    });
  } catch (err) {
    console.error(`[Orchestrator] Error: could not read/parse storage_design.json: ${err.message}`);
    await updateRunMeta(runDir, (meta) => {
      meta.steps.storage_design.status = "failed";
      meta.steps.storage_design.error = err.message;
      meta.status = "failed";
    });
    throw new Error("Storage design failed — cannot continue pipeline.");
  }

  // -------------------------------------------------------------------------
  // Step 3: Code Generation
  // -------------------------------------------------------------------------
  console.log("\n[Orchestrator] === Step 3: Code Generation ===");

  await updateRunMeta(runDir, (meta) => {
    meta.steps.code_generation.status = "running";
    meta.steps.code_generation.startedAt = new Date().toISOString();
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
    model: codeGeneratorConfig.model,
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
      meta.steps.code_generation.status = "failed";
      meta.steps.code_generation.error = `Missing files: ${missingFiles.join(", ")}`;
      meta.status = "failed";
    });
    throw new Error(`Code generation incomplete — missing: ${missingFiles.join(", ")}`);
  }

  console.log("\n[Orchestrator] Code generation completed. All files present.");
  await updateRunMeta(runDir, (meta) => {
    meta.steps.code_generation.status = "completed";
    meta.steps.code_generation.completedAt = new Date().toISOString();
  });

  // -------------------------------------------------------------------------
  // Step 4: Evaluation
  // -------------------------------------------------------------------------
  console.log("\n[Orchestrator] === Step 4: Evaluation ===");

  await updateRunMeta(runDir, (meta) => {
    meta.steps.evaluation.status = "running";
    meta.steps.evaluation.startedAt = new Date().toISOString();
  });

  const evaluatorSystemPrompt = await readFile(evaluatorConfig.promptPath, "utf-8");
  const evaluationPath = resolve(runDir, "evaluation.json");

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
    model: evaluatorConfig.model,
    cwd: runDir,
  });

  // Read evaluation results
  try {
    const evaluation = JSON.parse(await readFile(evaluationPath, "utf-8"));
    console.log("\n[Orchestrator] Evaluation completed.");
    console.log(`[Orchestrator] Overall status: ${evaluation.overall_status}`);
    if (evaluation.summary) {
      console.log(`[Orchestrator] Summary: ${evaluation.summary}`);
    }

    await updateRunMeta(runDir, (meta) => {
      meta.steps.evaluation.status = "completed";
      meta.steps.evaluation.completedAt = new Date().toISOString();
      meta.steps.evaluation.overall_status = evaluation.overall_status;
    });
  } catch (err) {
    console.error(`[Orchestrator] Warning: could not read/parse evaluation.json: ${err.message}`);
    await updateRunMeta(runDir, (meta) => {
      meta.steps.evaluation.status = "completed_with_warnings";
      meta.steps.evaluation.warning = err.message;
    });
  }

  // -------------------------------------------------------------------------
  // Finalize
  // -------------------------------------------------------------------------
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
