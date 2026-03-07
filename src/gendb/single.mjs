/**
 * GenDB Single-Agent Mode
 *
 * Launches one LLM agent to handle the entire GenDB pipeline end-to-end.
 * The agent has full freedom in approach — no prescribed intermediate artifacts.
 *
 * Usage: node src/gendb/single.mjs [--benchmark <name>] [--sf <N>] [--max-iterations <N>]
 *        [--model <name>] [--single-agent-prompt <high-level|guided>]
 */

import { readFile, writeFile, mkdir } from "fs/promises";
import { resolve, dirname } from "path";
import { existsSync, readdirSync } from "fs";
import { fileURLToPath } from "url";
import {
  BENCHMARKS_DIR,
  getDataDir,
  getSchemaPath,
  getQueriesPath,
} from "./config.mjs";
import { defaults, getProviderConfig } from "./gendb.config.mjs";
import {
  createRunId,
  getWorkloadName,
  createRunDir,
  updateLatestSymlink,
} from "./utils/paths.mjs";
import {
  renderTemplate,
  runAgent,
  parseQueryFile,
  readJSON,
  formatDuration,
  setAgentProvider,
  getAgentProviderName,
} from "./shared.mjs";
import { config as singleAgentConfig } from "./agents/single-agent/index.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const COMPARE_TOOL_PATH = resolve(__dirname, "tools", "compare_results.py");
const UTILS_PATH = resolve(__dirname, "utils");

// ---------------------------------------------------------------------------
// CLI Parsing
// ---------------------------------------------------------------------------

function parseArgsSingle(argv) {
  const sa = defaults.singleAgent;
  const args = {
    schema: null,
    queries: null,
    dataDir: null,
    gendbDir: null,        // null = auto-generate run-specific path after runId is known
    targetBenchmark: defaults.targetBenchmark,
    scaleFactor: defaults.scaleFactor,
    maxIterations: sa.maxOptimizationIterations,
    model: null, // resolved after provider is set
    modelOverride: null,
    optimizationTarget: defaults.optimizationTarget,
    agentProvider: defaults.agentProvider,
    promptVariant: sa.promptVariant,
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--schema" && argv[i + 1]) args.schema = resolve(argv[++i]);
    if (argv[i] === "--queries" && argv[i + 1]) args.queries = resolve(argv[++i]);
    if (argv[i] === "--data-dir" && argv[i + 1]) args.dataDir = resolve(argv[++i]);
    if (argv[i] === "--gendb-dir" && argv[i + 1]) args.gendbDir = resolve(argv[++i]);
    if (argv[i] === "--benchmark" && argv[i + 1]) args.targetBenchmark = argv[++i];
    if (argv[i] === "--sf" && argv[i + 1]) args.scaleFactor = parseInt(argv[++i], 10);
    if (argv[i] === "--max-iterations" && argv[i + 1]) args.maxIterations = parseInt(argv[++i], 10);
    if (argv[i] === "--model" && argv[i + 1]) args.model = argv[++i];
    if (argv[i] === "--model-override" && argv[i + 1]) args.modelOverride = argv[++i];
    if (argv[i] === "--optimization-target" && argv[i + 1]) args.optimizationTarget = argv[++i];
    if (argv[i] === "--agent-provider" && argv[i + 1]) args.agentProvider = argv[++i];
    if (argv[i] === "--single-agent-prompt" && argv[i + 1]) args.promptVariant = argv[++i];
  }
  if (!args.schema) args.schema = getSchemaPath(args.targetBenchmark);
  if (!args.queries) args.queries = getQueriesPath(args.targetBenchmark);
  if (!args.dataDir) args.dataDir = getDataDir(args.targetBenchmark, args.scaleFactor);
  // gendbDir resolved later (after runId) to ensure isolation — see main()
  return args;
}

// ---------------------------------------------------------------------------
// Post-Run: Collect Results
// ---------------------------------------------------------------------------

async function collectResults(runDir, parsedQueries) {
  console.log(`\n[Single] === Per-Query Results ===\n`);

  const queryData = [];
  let maxIter = 0;

  for (const query of parsedQueries) {
    const qDir = resolve(runDir, "queries", query.id);
    const iters = new Map();
    let dirs = [];
    try { dirs = readdirSync(qDir); } catch { /* query dir may not exist */ }
    for (const d of dirs) {
      const m = d.match(/^iter_(\d+)$/);
      if (!m) continue;
      const iterNum = parseInt(m[1], 10);
      const execPath = resolve(qDir, d, "execution_results.json");
      const exec = await readJSON(execPath);
      if (!exec) continue;
      iters.set(iterNum, {
        timing_ms: exec.timing_ms,
        validation: exec.validation?.status || "-",
      });
    }
    const maxIterNum = iters.size > 0 ? Math.max(...iters.keys()) + 1 : 0;
    const iterArray = [];
    for (let i = 0; i < maxIterNum; i++) {
      iterArray.push(iters.get(i) || { timing_ms: null, validation: "-" });
    }
    if (maxIterNum > maxIter) maxIter = maxIterNum;
    queryData.push({ id: query.id, iters: iterArray });
  }

  if (queryData.length === 0 || maxIter === 0) {
    console.log("[Single] No query results found.");
    return;
  }

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

  for (const q of queryData) {
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
      } else {
        timingRow += "-".padStart(colW) + " |";
        validRow += ((i < q.iters.length ? q.iters[i].validation : "-") || "-").toUpperCase().padStart(colW) + " |";
      }
    }

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
// Format Benchmark Context
// ---------------------------------------------------------------------------

function formatBenchmarkContext(benchmarkResults) {
  if (!benchmarkResults) return "";
  const isHotMode = defaults.optimizationTarget === "hot";
  const lines = ["## Performance Targets by Query", "| Query | System | Time (ms) |", "|-------|--------|-----------|"];

  const queryIds = new Set();
  for (const [, data] of Object.entries(benchmarkResults)) {
    for (const key of Object.keys(data)) {
      if (key.match(/^Q\d+$/i)) queryIds.add(key);
      if (data.queries) {
        for (const qk of Object.keys(data.queries)) {
          if (qk.match(/^Q\d+$/i)) queryIds.add(qk);
        }
      }
    }
  }

  for (const queryId of [...queryIds].sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)))) {
    let bestTime = Infinity, bestSystem = "";
    for (const [system, data] of Object.entries(benchmarkResults)) {
      const qData = data?.[queryId] || data?.queries?.[queryId];
      if (!qData) continue;
      const allMs = qData.all_ms;
      let time;
      if (allMs && allMs.length >= 2) {
        time = isHotMode ? Math.min(...allMs.slice(1)) : allMs[0];
      } else {
        time = qData.min_ms || qData.average_ms || qData.time_ms;
      }
      if (time == null) continue;
      if (time < bestTime) { bestTime = time; bestSystem = system; }
    }
    if (bestTime < Infinity) {
      lines.push(`| ${queryId} | ${bestSystem} | ${Math.round(bestTime)} |`);
    }
  }

  return lines.length > 3 ? lines.join("\n") : "";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const startTime = Date.now();
  const args = parseArgsSingle(process.argv);

  // Initialize agent provider
  setAgentProvider(args.agentProvider);
  console.log(`[Single] Agent provider: ${getAgentProviderName()}`);

  // Resolve model from provider config if not set via CLI
  const providerCfg = getProviderConfig(args.agentProvider);
  if (!args.model) args.model = providerCfg.singleAgent.model;
  const effectiveModel = args.modelOverride || args.model;

  // Read schema and queries
  const schema = await readFile(args.schema, "utf-8");
  const queries = await readFile(args.queries, "utf-8");
  const parsedQueries = parseQueryFile(queries);

  // Validate data dir
  if (!existsSync(args.dataDir)) {
    console.error(`[Single] Error: data directory not found: ${args.dataDir}`);
    process.exit(1);
  }

  // Create run directory
  const workload = getWorkloadName(args.schema);
  const runId = createRunId();
  const runDir = await createRunDir(workload, runId);

  // Place gendb storage inside run directory for isolation — the agent cannot
  // see or reuse storage from previous runs. benchmark.py reads the gendb path
  // from run.json's gendbDir field, so this works without any benchmark changes.
  if (!args.gendbDir) {
    args.gendbDir = resolve(runDir, "gendb");
  }
  await mkdir(args.gendbDir, { recursive: true });

  // Load benchmark results
  const benchmarkResultsPath = resolve(
    BENCHMARKS_DIR, args.targetBenchmark,
    "results", `sf${args.scaleFactor}`, "metrics", "benchmark_results.json"
  );
  const benchmarkResults = await readJSON(benchmarkResultsPath);

  // Check ground truth
  const groundTruthDir = resolve(
    BENCHMARKS_DIR, args.targetBenchmark,
    "results", `sf${args.scaleFactor}`, "ground_truth"
  );
  const hasGroundTruth = existsSync(groundTruthDir);

  // Select system prompt
  const promptVariant = args.promptVariant;
  const promptPath = singleAgentConfig.promptPaths[promptVariant];
  if (!promptPath) {
    console.error(`[Single] Unknown prompt variant: ${promptVariant}. Use "high-level" or "guided".`);
    process.exit(1);
  }
  const systemPrompt = await readFile(promptPath, "utf-8");

  // Build user prompt from template
  const userPromptTemplate = await readFile(singleAgentConfig.userPromptPath, "utf-8");
  const userPrompt = renderTemplate(userPromptTemplate, {
    schema,
    queries,
    data_dir: args.dataDir,
    gendb_dir: args.gendbDir,
    run_dir: runDir,
    utils_path: UTILS_PATH,
    compare_tool: COMPARE_TOOL_PATH,
    ground_truth_dir: hasGroundTruth ? groundTruthDir : "",
    max_iterations: String(args.maxIterations),
    max_iterations_minus_1: String(args.maxIterations - 1),
    timeout_sec: String(defaults.queryExecutionTimeoutSec),
    benchmark_context: formatBenchmarkContext(benchmarkResults),
  });

  // Write run metadata (gendbDir is read by benchmark.py to locate storage)
  const runMeta = {
    runId,
    workload,
    mode: "single-agent",
    agentProvider: args.agentProvider,
    promptVariant,
    model: effectiveModel,
    gendbDir: args.gendbDir,
    maxIterations: args.maxIterations,
    scaleFactor: args.scaleFactor,
    optimizationTarget: args.optimizationTarget,
    startedAt: new Date().toISOString(),
    status: "running",
  };
  await writeFile(resolve(runDir, "run.json"), JSON.stringify(runMeta, null, 2));

  // Print config
  console.log("[Single] GenDB Single-Agent Mode");
  console.log(`[Single] Schema:              ${args.schema}`);
  console.log(`[Single] Queries:             ${args.queries} (${parsedQueries.length} queries)`);
  console.log(`[Single] Data Dir:            ${args.dataDir}`);
  console.log(`[Single] GenDB Dir:           ${args.gendbDir}`);
  console.log(`[Single] Scale Factor:        ${args.scaleFactor}`);
  console.log(`[Single] Max Iterations:      ${args.maxIterations}`);
  console.log(`[Single] Model:               ${effectiveModel}`);
  console.log(`[Single] Prompt Variant:      ${promptVariant}`);
  console.log(`[Single] Optimization Target: ${args.optimizationTarget}`);
  console.log(`[Single] Workload:            ${workload}`);
  console.log(`[Single] Run ID:              ${runId}`);
  console.log(`[Single] Run Dir:             ${runDir}`);
  if (benchmarkResults) {
    console.log(`[Single] Benchmark data:      loaded`);
  }
  if (hasGroundTruth) {
    console.log(`[Single] Ground truth:        ${groundTruthDir}`);
  }

  // Launch single agent
  console.log(`\n[Single] Launching single agent...`);
  const agentResult = await runAgent(singleAgentConfig.name, {
    systemPrompt,
    userPrompt,
    allowedTools: singleAgentConfig.allowedTools,
    model: effectiveModel,
    cwd: runDir,
    timeoutMs: defaults.singleAgent.timeoutMs,
    configName: singleAgentConfig.configKey,
    useSkills: false,
    verbose: true,
  });

  // Post-run: collect results (if agent used the queries/Qi/iter_j structure)
  await collectResults(runDir, parsedQueries);

  // Determine final status
  const status = agentResult.error ? "failed" : "completed";

  // Always update run metadata (even on timeout/error)
  const completedRunMeta = JSON.parse(await readFile(resolve(runDir, "run.json"), "utf-8"));
  completedRunMeta.status = status;
  completedRunMeta.completedAt = new Date().toISOString();
  if (agentResult.error) completedRunMeta.error = agentResult.error;
  await writeFile(resolve(runDir, "run.json"), JSON.stringify(completedRunMeta, null, 2));

  // Always write telemetry (cost data preserved even on failure)
  const totalMs = Date.now() - startTime;
  const telemetry = {
    mode: "single-agent",
    prompt_variant: promptVariant,
    model: effectiveModel,
    status,
    total_wall_clock_ms: totalMs,
    agent_duration_ms: agentResult.durationMs,
    tokens: agentResult.tokens,
    cost_usd: agentResult.costUsd,
    ...(agentResult.error ? { error: agentResult.error } : {}),
  };
  await writeFile(resolve(runDir, "telemetry.json"), JSON.stringify(telemetry, null, 2));

  // Update latest symlink
  await updateLatestSymlink(workload, runId);

  // Print summary
  console.log(`\n[Single] === Run Summary ===`);
  if (agentResult.error) {
    console.log(`[Single] Status: FAILED — ${agentResult.error}`);
  }
  console.log(`[Single] Total time: ${formatDuration(totalMs)}`);
  console.log(`[Single] Agent time: ${formatDuration(agentResult.durationMs)}`);
  console.log(`[Single] Tokens: ${Math.round(agentResult.tokens.input / 1000)}K input, ${Math.round(agentResult.tokens.output / 1000)}K output`);
  if (agentResult.tokens.cache_read > 0) {
    console.log(`[Single] Cache tokens: ${Math.round(agentResult.tokens.cache_read / 1000)}K cache_read, ${Math.round(agentResult.tokens.cache_creation / 1000)}K cache_creation`);
  }
  console.log(`[Single] Cost: $${agentResult.costUsd.toFixed(2)}`);
  console.log(`\n[Single] Pipeline ${agentResult.error ? 'failed' : 'complete'}.`);
  console.log(`[Single] Run Dir:  ${runDir}`);
  console.log(`[Single] Latest symlink: output/${workload}/latest → ${runId}`);

  if (agentResult.error) process.exit(1);
}

main().catch((err) => {
  console.error("[Single] Fatal error:", err.message);
  process.exit(1);
});
