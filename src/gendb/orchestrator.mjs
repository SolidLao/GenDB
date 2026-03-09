/**
 * GenDB Orchestrator v34
 *
 * Three-phase agentic system with 7 agents and 4-layer prompt architecture:
 *   Phase 1 (Offline Data Storage Optimization):
 *     Workload Analyzer → Storage/Index Designer → DBA Stage A (optional, --dba-stage-a)
 *   Phase 2 (Online Per-Query Pipeline-Parallel Optimization):
 *     Iter 0: Query Planner → Code Generator (compile+run+validate) → Inspector → Execute
 *     Iter 1+: Query Optimizer (revised plan) → Code Generator (implement plan) → Inspector → Execute
 *     LLM calls gated by Semaphore, execution serialized via ExecutionQueue.
 *   Phase 3 (Post-Run):
 *     DBA Stage B — Retrospective, experience evolution
 *
 * v34: 4-layer prompt architecture (identity → experience → domain skills → user prompt),
 * skill system replaces knowledge base, user prompt templates, lean I/O contracts,
 * configurable hot/cold optimization target.
 *
 * Usage: node src/gendb/orchestrator.mjs [--benchmark <name>] [--schema <path>] [--queries <path>]
 *        [--data-dir <path>] [--gendb-dir <path>] [--sf <N>] [--max-iterations <N>]
 *        [--stall-threshold <N>] [--model <name>] [--model-override <name>]
 *        [--optimization-target <target>] [--max-concurrent <N>]
 */

import { spawn, execSync } from "child_process";
import { readFile, writeFile, mkdir, cp } from "fs/promises";
import { resolve, dirname } from "path";
import { existsSync, readFileSync, rmSync, readdirSync } from "fs";
import { fileURLToPath } from "url";
import {
  DEFAULT_SCHEMA,
  DEFAULT_QUERIES,
  BENCHMARKS_DIR,
  getDataDir,
  getSchemaPath,
  getQueriesPath,
} from "./config.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const COMPARE_TOOL_PATH = resolve(__dirname, "tools", "compare_results.py");
const UTILS_PATH = resolve(__dirname, "utils");
let EXPERIENCE_PATH = resolve(__dirname, "../../.claude/skills/experience/SKILL.md");
import { defaults, getProviderConfig } from "./gendb.config.mjs";
import { config as workloadAnalyzerConfig } from "./agents/workload-analyzer/index.mjs";
import { config as storageDesignerConfig } from "./agents/storage-index-designer/index.mjs";
import { config as queryOptimizerConfig } from "./agents/query-optimizer/index.mjs";
import { config as codeGeneratorConfig } from "./agents/code-generator/index.mjs";
import { config as codeInspectorConfig } from "./agents/code-inspector/index.mjs";
import { config as queryPlannerConfig } from "./agents/query-planner/index.mjs";
import { config as dbaConfig } from "./agents/dba/index.mjs";
import {
  createRunId,
  getWorkloadName,
  createRunDir,
  updateLatestSymlink,
  createQueryDir,
} from "./utils/paths.mjs";
import {
  renderTemplate,
  runAgent,
  parseQueryFile,
  MODEL_PRICING,
  estimateCost,
  readJSON,
  formatDuration,
  setAgentProvider,
  getAgentProviderName,
} from "./shared.mjs";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Parse CLI args, merging over gendb.config defaults. */
function parseArgs(argv) {
  const args = {
    schema: null,
    queries: null,
    dataDir: null,
    gendbDir: null,
    targetBenchmark: defaults.targetBenchmark,
    scaleFactor: defaults.scaleFactor,
    maxIterations: defaults.maxOptimizationIterations,
    stallThreshold: defaults.stallThreshold,
    model: null, // resolved after provider is set
    modelOverride: null,
    optimizationTarget: defaults.optimizationTarget,  // "hot", "cold", or legacy "execution_time"
    maxConcurrent: defaults.maxConcurrentQueries,
    agentProvider: defaults.agentProvider,
    dbaStageA: false,
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--schema" && argv[i + 1]) args.schema = resolve(argv[++i]);
    if (argv[i] === "--queries" && argv[i + 1]) args.queries = resolve(argv[++i]);
    if (argv[i] === "--data-dir" && argv[i + 1]) args.dataDir = resolve(argv[++i]);
    if (argv[i] === "--gendb-dir" && argv[i + 1]) args.gendbDir = resolve(argv[++i]);
    if (argv[i] === "--benchmark" && argv[i + 1]) args.targetBenchmark = argv[++i];
    if (argv[i] === "--sf" && argv[i + 1]) args.scaleFactor = parseInt(argv[++i], 10);
    if (argv[i] === "--max-iterations" && argv[i + 1]) args.maxIterations = parseInt(argv[++i], 10);
    if (argv[i] === "--stall-threshold" && argv[i + 1]) args.stallThreshold = parseInt(argv[++i], 10);
    if (argv[i] === "--model" && argv[i + 1]) args.model = argv[++i];
    if (argv[i] === "--model-override" && argv[i + 1]) args.modelOverride = argv[++i];
    if (argv[i] === "--optimization-target" && argv[i + 1]) args.optimizationTarget = argv[++i];
    if (argv[i] === "--max-concurrent" && argv[i + 1]) args.maxConcurrent = parseInt(argv[++i], 10);
    if (argv[i] === "--agent-provider" && argv[i + 1]) args.agentProvider = argv[++i];
    if (argv[i] === "--dba-stage-a") args.dbaStageA = true;
    if (argv[i] === "--no-skills") args.useSkills = false;
    if (argv[i] === "--no-dba") args.useDba = false;
  }
  if (args.useSkills === undefined) args.useSkills = defaults.useSkills;
  if (args.useDba === undefined) args.useDba = defaults.useDba;
  // Resolve schema/queries from benchmark dir if not explicitly provided
  if (!args.schema) {
    args.schema = getSchemaPath(args.targetBenchmark);
  }
  if (!args.queries) {
    args.queries = getQueriesPath(args.targetBenchmark);
  }
  if (!args.dataDir) {
    args.dataDir = getDataDir(args.targetBenchmark, args.scaleFactor);
  }
  // gendbDir resolved later (after runDir creation) to place inside run directory.
  // Explicit --gendb-dir overrides this.
  return args;
}

/**
 * Resolve which LLM model to use for a given agent.
 * CLI --model-override forces all agents to one model (for testing).
 * Otherwise use agentModels config, falling back to args.model.
 */
function getAgentModel(agentConfigName, args) {
  if (args.modelOverride) return args.modelOverride;
  const providerCfg = getProviderConfig(args.agentProvider);
  return providerCfg.agentModels[agentConfigName] || args.model || providerCfg.model;
}

function getAgentTimeout(agentConfigName) {
  return defaults.agentTimeoutOverrides?.[agentConfigName] || defaults.agentTimeoutMs;
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

// MODEL_PRICING, estimateCost imported from shared.mjs

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

// formatDuration, runAgent imported from shared.mjs

/** Trim validation details: limit to 2 columns, 2 sample rows each. */
function trimValidationDetails(details) {
  if (!details?.queries) return details;
  const trimmed = { ...details, queries: {} };
  for (const [qid, qResult] of Object.entries(details.queries)) {
    const tq = { ...qResult };
    if (tq.column_failures) {
      const cols = Object.entries(tq.column_failures);
      const limited = cols.slice(0, 2);
      tq.column_failures = {};
      for (const [col, failure] of limited) {
        tq.column_failures[col] = {
          ...failure,
          samples: (failure.samples || []).slice(0, 2),
        };
      }
    }
    trimmed.queries[qid] = tq;
  }
  return trimmed;
}

// readJSON imported from shared.mjs

/** Read column_versions/registry.json from the gendb data directory. Returns formatted string for agent prompts. */
async function getColumnVersionsContext(gendbDir) {
  const registryPath = resolve(gendbDir, "column_versions", "registry.json");
  const registry = await readJSON(registryPath);
  if (!registry?.versions?.length) return '';
  const lines = [
    '## Available Column Versions',
    'The following derived column representations exist in `<gendb_dir>/column_versions/` and can be used directly (mmap the files):',
    '',
  ];
  for (const v of registry.versions) {
    lines.push(`### ${v.id}`);
    lines.push(`- Source: ${v.source.table}.${v.source.column} (v0: ${v.source.v0_encoding})`);
    lines.push(`- Encoding: ${v.encoding}, ${v.row_count} rows, ${v.unique_values} unique values`);
    for (const [key, path] of Object.entries(v.files)) {
      if (!key.endsWith('_format')) {
        const fmt = v.files[`${key}_format`] || '';
        lines.push(`- File: \`${path}\`${fmt ? ` (${fmt})` : ''}`);
      }
    }
    lines.push('');
  }
  return lines.join('\n');
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

/** Format benchmark comparison for a single query — adapts to optimization target. */
function formatPerQueryBenchmarkContext(benchmarkResults, queryId, coldMs, hotMs) {
  if (!benchmarkResults) return "";
  const isHotMode = defaults.optimizationTarget === 'hot';
  const isColdMode = defaults.optimizationTarget === 'cold';
  const rows = [];
  let bestTime = Infinity, bestSystem = "";
  for (const [system, data] of Object.entries(benchmarkResults)) {
    const qData = data?.[queryId] || data?.queries?.[queryId];
    if (!qData) continue;
    const allMs = qData.all_ms;
    let time;
    if (allMs && allMs.length >= 2) {
      if (isHotMode) {
        time = Math.min(...allMs.slice(1));
      } else if (isColdMode) {
        time = allMs[0];
      } else {
        time = allMs[0] + allMs.slice(1).reduce((a, b) => a + b, 0) / (allMs.length - 1);
      }
    } else {
      time = qData.min_ms || qData.average_ms || qData.time_ms;
    }
    if (time == null) continue;
    rows.push({ system, time });
    if (time < bestTime) { bestTime = time; bestSystem = system; }
  }
  if (rows.length === 0) return "";
  rows.sort((a, b) => a.time - b.time);
  const metricLabel = isHotMode ? "Hot (ms)" : isColdMode ? "Cold (ms)" : "Time (ms)";
  const lines = [
    "",
    `## Performance Comparison for ${queryId}`,
    `| System | ${metricLabel} |`,
    `|--------|${"-".repeat(metricLabel.length + 2)}|`,
    ...rows.map(r => `| ${r.system} | ${Math.round(r.time)} |`),
  ];
  const currentTime = isHotMode ? hotMs : isColdMode ? coldMs : (coldMs != null && hotMs != null ? coldMs + hotMs : coldMs);
  if (currentTime != null) {
    lines.push(`| Current GenDB | ${Math.round(currentTime)} |`);
    lines.push("", `Target: Beat best system (${bestSystem} ${Math.round(bestTime)}ms).`);
  }
  return lines.join("\n");
}

// parseQueryFile imported from shared.mjs

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
    if (!qData) continue;
    const allMs = qData.all_ms;
    let time;
    if (allMs && allMs.length >= 2) {
      if (defaults.optimizationTarget === 'hot') {
        // Hot mode: compare avg(hot) only
        time = allMs.slice(1).reduce((a, b) => a + b, 0) / (allMs.length - 1);
      } else if (defaults.optimizationTarget === 'cold') {
        time = allMs[0];
      } else {
        const cold = allMs[0];
        const hotAvg = allMs.slice(1).reduce((a, b) => a + b, 0) / (allMs.length - 1);
        time = cold + hotAvg;
      }
    } else {
      time = qData.min_ms || qData.average_ms || qData.time_ms || qData.median_ms;
    }
    if (time && time < best) best = time;
  }
  return best === Infinity ? null : best;
}

function shouldContinue(queryId, history, benchmarkResults, execResults, iteration, maxIter, stallThreshold) {
  if (iteration > maxIter) return { action: 'stop', reason: 'Max iterations reached' };

  // Never stop if results are still incorrect — escalate after repeated failures
  if (execResults?.validation?.status === 'fail') {
    const consecutiveFails = history.iterations.filter(i => i.validation === 'fail').length;
    const cap = defaults.correctnessFailureCap || 3;
    if (consecutiveFails >= cap * 2) return { action: 'stop', reason: 'Too many correctness failures even after escalation — needs manual review' };
    if (consecutiveFails >= cap) {
      const providerCfg = getProviderConfig();
      return { action: 'escalate', reason: `${consecutiveFails} correctness failures — escalating to ${providerCfg.escalationModel} Code Generator` };
    }
    return { action: 'continue', reason: 'Fix correctness first' };
  }

  const timing = execResults?.timing_ms;
  if (timing && timing < 50) return { action: 'stop', reason: 'Already fast (<50ms)' };

  // Already competitive with baseline
  if (benchmarkResults && timing) {
    const best = getBestBaselineTime(benchmarkResults, queryId);
    if (best && timing < best / 1.2) return { action: 'stop', reason: `Competitive with baseline (${timing.toFixed(1)}ms vs ${best.toFixed(1)}ms)` };
  }

  // Adaptive: stop after stallThreshold consecutive non-improvements
  const thresh = stallThreshold || defaults.stallThreshold;
  const recent = history.iterations.slice(-thresh);
  if (recent.length >= thresh && recent.every(i => !i.improved)) {
    return { action: 'stop', reason: `Stalled: ${thresh} consecutive non-improving iterations` };
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
// Validation failure diagnosis: detect common patterns in column mismatches
// ---------------------------------------------------------------------------

function diagnoseValidationFailure(execResults) {
  const details = execResults?.validation?.details;
  if (!details) return "";
  const hints = [];

  for (const [qid, qResult] of Object.entries(details.queries || {})) {
    for (const [col, failure] of Object.entries(qResult.column_failures || {})) {
      const samples = failure.samples || [];

      // Detect consistent Nx multiplier
      const ratios = samples
        .filter(s => parseFloat(s.expected) !== 0)
        .map(s => parseFloat(s.actual) / parseFloat(s.expected));

      if (ratios.length > 0) {
        const avgRatio = ratios.reduce((a, b) => a + b, 0) / ratios.length;
        if (Math.abs(avgRatio - 100) < 5) {
          hints.push(`Column "${col}": values ~100× too large. Check arithmetic — possible extra multiplication or missing division in the output formula.`);
        } else if (Math.abs(avgRatio - 0.01) < 0.005) {
          hints.push(`Column "${col}": values ~100× too small. Check arithmetic — possible extra division in the output formula.`);
        }
      }

      // Detect zero output
      if (samples.every(s => parseFloat(s.actual) === 0 && parseFloat(s.expected) !== 0)) {
        hints.push(`Column "${col}": all values are 0 when expected non-zero. A filter predicate may be too restrictive — check that filter thresholds match SQL values directly (DECIMAL columns are stored as double).`);
      }
    }

    // Row count mismatch
    if (qResult.rows_expected !== undefined && qResult.rows_actual !== undefined && qResult.rows_expected !== qResult.rows_actual) {
      hints.push(`Row count mismatch: expected ${qResult.rows_expected}, got ${qResult.rows_actual}. Check join/filter predicate logic.`);
    }
  }

  return hints.length > 0
    ? "\n**Diagnosis**: " + hints.join(" ") + " Refer to the Query Guide's Column Reference for correct column types and conversion patterns."
    : "";
}

// ---------------------------------------------------------------------------
// Correctness Anchors: extract validated constants from passing code
// ---------------------------------------------------------------------------

function extractCorrectnessAnchors(cppPath) {
  try {
    const code = readFileSync(cppPath, 'utf-8');
    const anchors = [];

    // Extract date threshold constants
    const dateMatches = code.matchAll(/(?:date_str_to_epoch_days|epoch_days)\s*\(\s*"([^"]+)"\s*\)/g);
    for (const m of dateMatches) anchors.push({ type: 'date_literal', value: m[1] });

    // Extract add_years/add_months/add_days calls with their arguments
    const dateArithMatches = code.matchAll(/(?:add_years|add_months|add_days)\s*\([^,]+,\s*(-?\d+)\s*\)/g);
    for (const m of dateArithMatches) anchors.push({ type: 'date_arithmetic', value: m[0] });

    // Extract scaled threshold constants (large int literals with scale comments)
    const scaleMatches = code.matchAll(/(\d{4,})LL?\s*(?:\/\*.*?scale.*?\*\/|\/\/.*?scale)/gi);
    for (const m of scaleMatches) anchors.push({ type: 'scaled_constant', value: m[1] });

    // Extract numeric threshold constants from comparison operators
    const knownThresholds = code.matchAll(/(?:>=?|<=?|==|!=)\s*(\d{4,})(?:LL)?(?:\s|;|\))/g);
    for (const m of knownThresholds) {
      const val = parseInt(m[1]);
      if (val >= 100 && val % 100 === 0) {
        anchors.push({ type: 'threshold_constant', value: m[1] });
      }
    }

    // Extract revenue formula patterns
    const revenueMatches = code.matchAll(/((?:\w+price\w*|\w+cost\w*|\w+revenue\w*)\s*\*\s*\([^)]+\))/gi);
    for (const m of revenueMatches) anchors.push({ type: 'revenue_formula', value: m[1].trim() });

    return anchors;
  } catch {
    return [];
  }
}

function formatCorrectnessAnchors(anchors) {
  if (!anchors || anchors.length === 0) return "";
  const lines = [
    "",
    "## Correctness Anchors (DO NOT MODIFY)",
    "These constants were validated in a passing iteration. Changing them will break correctness:",
  ];
  for (const a of anchors) {
    lines.push(`- ${a.type}: ${a.value}`);
  }
  lines.push("Modify only the data structures, parallelism, and execution strategy around these anchors.");
  lines.push("");
  return lines.join("\n");
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
    meta.model = args.modelOverride || args.model;
    meta.agentProvider = args.agentProvider;
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

  // --- Step 1: Workload Analysis ---
  console.log("\n[Orchestrator] === Step 1: Workload Analysis ===");
  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.workload_analysis.status = "running";
    meta.phase1.steps.workload_analysis.startedAt = new Date().toISOString();
  });

  const analyzerSystemPrompt = await readFile(workloadAnalyzerConfig.promptPath, "utf-8");
  const workloadAnalysisPath = resolve(runDir, "workload_analysis.json");

  const waTemplatePath = resolve(__dirname, "agents/workload-analyzer/user-prompt.md");
  const waTemplate = readFileSync(waTemplatePath, 'utf-8');
  const analyzerUserPrompt = renderTemplate(waTemplate, {
    schema: schema.trim(),
    queries: queries.trim(),
    data_dir: args.dataDir,
    output_path: workloadAnalysisPath,
  });

  const waResult = await runAgent(workloadAnalyzerConfig.name, {
    systemPrompt: analyzerSystemPrompt,
    userPrompt: analyzerUserPrompt,
    allowedTools: workloadAnalyzerConfig.allowedTools,
    model: getAgentModel("workload_analyzer", args),
    configName: "workload_analyzer",
    cwd: runDir,
    useSkills: args.useSkills,
    domainSkillsPrompt: workloadAnalyzerConfig.domainSkillsPrompt,
  });
  recordAgentTelemetry("phase1", "workload_analyzer", waResult.durationMs, waResult.tokens, waResult.costUsd);
  if (waResult.error) throw new Error(`Workload Analyzer failed: ${waResult.error}`);

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

  const sdTemplatePath = resolve(__dirname, "agents/storage-index-designer/user-prompt.md");
  const sdTemplate = readFileSync(sdTemplatePath, 'utf-8');
  const queriesSection = parsedQueries.map(q => `### ${q.id}\n\`\`\`sql\n${q.sql}\n\`\`\``).join('\n\n');
  const designerUserPrompt = renderTemplate(sdTemplate, {
    workload_analysis_path: workloadAnalysisPath,
    schema: schema.trim(),
    queries_section: queriesSection,
    data_dir: args.dataDir,
    gendb_dir: args.gendbDir,
    generated_ingest_dir: generatedIngestDir,
    storage_design_path: storageDesignPath,
  });

  const sdResult = await runAgent(storageDesignerConfig.name, {
    systemPrompt: designerSystemPrompt,
    userPrompt: designerUserPrompt,
    allowedTools: storageDesignerConfig.allowedTools,
    model: getAgentModel("storage_designer", args),
    configName: "storage_designer",
    cwd: runDir,
    timeoutMs: getAgentTimeout("storage_designer"),
    useSkills: args.useSkills,
    domainSkillsPrompt: storageDesignerConfig.domainSkillsPrompt,
  });
  recordAgentTelemetry("phase1", "storage_designer", sdResult.durationMs, sdResult.tokens, sdResult.costUsd);
  if (sdResult.error) throw new Error(`Storage Designer failed: ${sdResult.error}`);

  const design = await readJSON(storageDesignPath);
  if (!design) {
    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.storage_design.status = "failed";
      meta.phase1.status = "failed";
      meta.status = "failed";
    });
    throw new Error("Storage design failed — could not read/parse storage_design.json.");
  }

  // Copy storage_design.json into gendb data directory so generated code can access it at runtime
  await writeFile(resolve(args.gendbDir, "storage_design.json"), JSON.stringify(design, null, 2));
  console.log(`[Orchestrator] Copied storage_design.json to ${args.gendbDir}`);

  console.log("\n[Orchestrator] Storage design + ingestion + index building completed (pass 1).");
  console.log(`[Orchestrator] Tables designed: ${Object.keys(design.tables || {}).length}`);

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.steps.storage_design.status = "completed";
    meta.phase1.steps.storage_design.completedAt = new Date().toISOString();
    meta.phase1.steps.data_ingestion.status = "completed";
    meta.phase1.steps.data_ingestion.completedAt = new Date().toISOString();
    meta.phase1.steps.index_building.status = "completed";
    meta.phase1.steps.index_building.completedAt = new Date().toISOString();
  });

  // --- Pass 2: Generate query guides (separate invocation for consistency) ---
  console.log("\n[Orchestrator] === Storage Designer Pass 2: Generate Query Guides ===");
  const sdPass2TemplatePath = resolve(__dirname, "agents/storage-index-designer/user-prompt-pass2.md");
  const sdPass2Template = readFileSync(sdPass2TemplatePath, 'utf-8');
  const sdPass2UserPrompt = renderTemplate(sdPass2Template, {
    storage_design_path: storageDesignPath,
    build_indexes_cpp_path: resolve(generatedIngestDir, "build_indexes.cpp"),
    ingest_cpp_path: resolve(generatedIngestDir, "ingest.cpp"),
    workload_analysis_path: workloadAnalysisPath,
    queries_section: queriesSection,
    gendb_dir: args.gendbDir,
    query_guides_dir: queryGuidesDir,
  });

  const sdResult2 = await runAgent(storageDesignerConfig.name, {
    systemPrompt: designerSystemPrompt,
    userPrompt: sdPass2UserPrompt,
    allowedTools: storageDesignerConfig.allowedTools,
    model: getAgentModel("storage_designer", args),
    configName: "storage_designer",
    cwd: runDir,
    useSkills: args.useSkills,
    domainSkillsPrompt: storageDesignerConfig.domainSkillsPrompt,
  });
  recordAgentTelemetry("phase1", "storage_designer_pass2", sdResult2.durationMs, sdResult2.tokens, sdResult2.costUsd);
  if (sdResult2.error) throw new Error(`Storage Designer pass 2 failed: ${sdResult2.error}`);
  console.log("[Orchestrator] Storage designer pass 2 (query guides) completed.");

  // --- Phase 1 Step 3: DBA Stage A (predict risks, extend utilities) --- OPTIONAL
  if (args.dbaStageA && args.useDba) {
    console.log("\n[Orchestrator] === Phase 1 Step 3: DBA Stage A (Pre-Generation Risk Analysis) ===");
    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.dba_stage_a = { status: "running", startedAt: new Date().toISOString() };
    });

    const dbaSystemPrompt = await readFile(dbaConfig.promptPath, "utf-8");
    const dbaATemplatePath = resolve(__dirname, "agents/dba/user-prompt.md");
    const dbaATemplate = readFileSync(dbaATemplatePath, 'utf-8');
    const dbaStageAPrompt = renderTemplate(dbaATemplate, {
      stage_a: true,
      workload_analysis_path: workloadAnalysisPath,
      storage_design_path: storageDesignPath,
      queries_path: args.queries,
      utils_path: UTILS_PATH,
      experience_path: EXPERIENCE_PATH,
    });

    const dbaResult = await runAgent(dbaConfig.name, {
      systemPrompt: dbaSystemPrompt,
      userPrompt: dbaStageAPrompt,
      allowedTools: dbaConfig.allowedTools,
      model: getAgentModel("dba", args),
      configName: "dba",
      cwd: runDir,
      useSkills: args.useSkills,
      domainSkillsPrompt: dbaConfig.domainSkillsPrompt,
    });
    recordAgentTelemetry("phase1", "dba_stage_a", dbaResult.durationMs, dbaResult.tokens, dbaResult.costUsd);
    if (dbaResult.error) {
      console.error(`[Orchestrator] DBA Stage A failed (non-fatal): ${dbaResult.error}`);
    } else {
      console.log("[Orchestrator] DBA Stage A completed.");
    }

    await updateRunMeta(runDir, (meta) => {
      meta.phase1.steps.dba_stage_a.status = "completed";
      meta.phase1.steps.dba_stage_a.completedAt = new Date().toISOString();
    });
  } else {
    console.log("\n[Orchestrator] === Phase 1 Step 3: DBA Stage A SKIPPED (use --dba-stage-a to enable) ===");
  }

  await updateRunMeta(runDir, (meta) => {
    meta.phase1.status = "completed";
    meta.phase1.completedAt = new Date().toISOString();
  });

  return { workloadAnalysisPath, storageDesignPath };
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
  const groundTruthDir = resolve(BENCHMARKS_DIR, args.targetBenchmark, "query_results");
  const hasGroundTruth = existsSync(groundTruthDir);

  // Shared resources
  const executionQueue = new ExecutionQueue();
  const semaphore = new Semaphore(args.maxConcurrent);
  const qpSystemPrompt = await readFile(queryPlannerConfig.promptPath, "utf-8");
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
          executionQueue, semaphore, qpSystemPrompt, cgSystemPrompt, qoSystemPrompt,
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
// Code Inspector: review generated code against experience base
// ---------------------------------------------------------------------------

async function runCodeInspection(queryId, cppPath, args, queryPhase, previousPassingCppPath, runDir) {
  const inspectorSystemPrompt = await readFile(codeInspectorConfig.promptPath, "utf-8");

  // Read query guide for encoding verification
  let inspectorGuide = "";
  if (runDir) {
    const inspectorGuidePath = resolve(runDir, "query_guides", `${queryId}_guide.md`);
    try { inspectorGuide = await readFile(inspectorGuidePath, "utf-8"); } catch {}
  }

  const ciTemplatePath = resolve(__dirname, "agents/code-inspector/user-prompt.md");
  const ciTemplate = readFileSync(ciTemplatePath, 'utf-8');
  const inspectorUserPrompt = renderTemplate(ciTemplate, {
    query_id: queryId,
    cpp_path: cppPath,
    experience_path: EXPERIENCE_PATH,
    query_guide: inspectorGuide,
    previous_passing_cpp: previousPassingCppPath,
  });

  const result = await runAgent(codeInspectorConfig.name, {
    systemPrompt: inspectorSystemPrompt,
    userPrompt: inspectorUserPrompt,
    allowedTools: codeInspectorConfig.allowedTools,
    model: getAgentModel("code_inspector", args),
    configName: "code_inspector",
    cwd: dirname(cppPath),
    useSkills: args.useSkills,
    domainSkillsPrompt: codeInspectorConfig.domainSkillsPrompt,
  });
  recordAgentTelemetry(queryPhase, "code_inspector", result.durationMs, result.tokens, result.costUsd);
  if (result.error) return { verdict: "PASS", issues: [] };

  // Parse inspection result
  try {
    const jsonMatch = result.result.match(/\{[\s\S]*"verdict"[\s\S]*\}/);
    if (jsonMatch) {
      return JSON.parse(jsonMatch[0]);
    }
  } catch {}
  return { verdict: "PASS", issues: [] };
}

function hasCriticalIssues(inspection) {
  return inspection.issues?.some(i => i.severity === "critical");
}

// ---------------------------------------------------------------------------
// Per-Query Full Pipeline (Code Gen → Inspector → Execute → [Optimize → Inspector → Execute]*)
// ---------------------------------------------------------------------------

/**
 * Full lifecycle pipeline for a single query.
 * Code generation (LLM, gated by Semaphore)
 * → ExecutionQueue (serial, enters immediately after code gen)
 * → [shouldContinue → Query Optimizer (revised plan) → Code Generator (implement plan) → ExecutionQueue]*
 * → done
 *
 * Each query progresses independently. No waiting for other queries between stages.
 */
async function runQueryFullPipeline(
  query, args, runDir, workloadAnalysisPath, storageDesignPath,
  schema, groundTruthDir, hasGroundTruth,
  executionQueue, semaphore, qpSystemPrompt, cgSystemPrompt, qoSystemPrompt,
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

  // Read hardware info
  const storageDesign = await readJSON(storageDesignPath);
  const hw = storageDesign?.hardware_config || {};

  // Read per-query guide if available
  const guidePath = resolve(runDir, "query_guides", `${queryId}_guide.md`);
  let queryGuide = "";
  try { queryGuide = await readFile(guidePath, "utf-8"); } catch {}

  // Load workload analysis once for skill selection across all iterations
  const workloadAnalysis = await readJSON(workloadAnalysisPath);

  // === ITERATION 0: QUERY PLANNER (gated by semaphore) ===
  const planPath = resolve(iterDir, "plan.json");
  progressTracker.update(queryId, "planner");
  await semaphore.acquire();
  try {
    console.log(`\n[Orchestrator] [${queryId}] Iteration 0: Query Planner`);

    const qpTemplatePath = resolve(__dirname, "agents/query-planner/user-prompt.md");
    const qpTemplate = readFileSync(qpTemplatePath, 'utf-8');
    const qpUserPrompt = renderTemplate(qpTemplate, {
      query_id: queryId,
      cpu_cores: hw.cpu_cores || 'unknown',
      disk_type: hw.disk_type || 'unknown',
      l3_cache_mb: hw.l3_cache_mb || 'unknown',
      total_memory_gb: hw.total_memory_gb || 'unknown',
      query_guide: queryGuide,
      query_sql: query.sql,
      gendb_dir: args.gendbDir,
      benchmark_context: formatPerQueryBenchmarkContext(args.benchmarkResults, queryId, null),
      plan_path: planPath,
    });

    const qpResult = await runAgent(queryPlannerConfig.name, {
      systemPrompt: qpSystemPrompt,
      userPrompt: qpUserPrompt,
      allowedTools: queryPlannerConfig.allowedTools,
      model: getAgentModel("query_planner", args),
      configName: "query_planner",
      cwd: iterDir,
      useSkills: args.useSkills,
      domainSkillsPrompt: queryPlannerConfig.domainSkillsPrompt,
    });
    recordAgentTelemetry(queryPhase, "query_planner", qpResult.durationMs, qpResult.tokens, qpResult.costUsd);
    if (qpResult.error) throw new Error(`Query Planner failed for ${queryId}: ${qpResult.error}`);
  } finally {
    semaphore.release();
  }

  // Read the plan (may be null if planner failed — coder proceeds without it)
  let planJson = null;
  try { planJson = JSON.parse(await readFile(planPath, "utf-8")); } catch {}

  // === ITERATION 0: CODE GENERATION (gated by semaphore) ===
  progressTracker.update(queryId, "codegen");
  await semaphore.acquire();
  try {
    console.log(`\n[Orchestrator] [${queryId}] Iteration 0: Code Generator`);

    const cgTemplatePath = resolve(__dirname, "agents/code-generator/user-prompt.md");
    const cgTemplate = readFileSync(cgTemplatePath, 'utf-8');

    // Format storage_extensions from initial plan for Code Generator (if planner included them)
    const iter0StorageExtSection = planJson?.storage_extensions?.length
      ? `## Storage Extensions\nThe Query Planner has referenced derived column versions. Use these files instead of the original columns where specified:\n${JSON.stringify(planJson.storage_extensions, null, 2)}\nAll file paths are relative to the GenDB storage directory (argv[1]).`
      : '';
    const iter0ColumnVersions = await getColumnVersionsContext(args.gendbDir);

    const cgUserPrompt = renderTemplate(cgTemplate, {
      query_id: queryId,
      plan_json: planJson ? JSON.stringify(planJson, null, 2) : '',
      storage_extensions: iter0StorageExtSection,
      column_versions: iter0ColumnVersions,
      cpu_cores: hw.cpu_cores || 'unknown',
      disk_type: hw.disk_type || 'unknown',
      l3_cache_mb: hw.l3_cache_mb || 'unknown',
      total_memory_gb: hw.total_memory_gb || 'unknown',
      query_guide: queryGuide,
      schema_path: args.schema,
      benchmark_context: formatPerQueryBenchmarkContext(args.benchmarkResults, queryId, null),
      query_sql: query.sql,
      gendb_dir: args.gendbDir,
      ground_truth: hasGroundTruth ? groundTruthDir : 'No ground truth available',
      has_ground_truth: hasGroundTruth,
      ground_truth_dir: groundTruthDir,
      cpp_path: iterCppPath,
      utils_path: UTILS_PATH,
      binary_name: queryId.toLowerCase(),
      cpp_name: cppName,
      timeout_sec: defaults.queryExecutionTimeoutSec,
      results_dir: iterResultsDir,
      compare_tool: COMPARE_TOOL_PATH,
    });

    // Run code generator — resilient to timeout (still try executeQuery on whatever .cpp exists)
    const cgResult = await runAgent(codeGeneratorConfig.name, {
      systemPrompt: cgSystemPrompt,
      userPrompt: cgUserPrompt,
      allowedTools: codeGeneratorConfig.allowedTools,
      model: getAgentModel("code_generator", args),
      configName: "code_generator",
      cwd: iterDir,
      useSkills: args.useSkills,
      domainSkillsPrompt: codeGeneratorConfig.domainSkillsPrompt,
    });
    recordAgentTelemetry(queryPhase, "code_generator", cgResult.durationMs, cgResult.tokens, cgResult.costUsd);
    const cgTimedOut = cgResult.error?.includes("timed out");
    if (cgResult.error && !cgTimedOut) throw new Error(cgResult.error);
    if (cgTimedOut) console.log(`[Orchestrator] [${queryId}] Code Generator timed out — will try executeQuery on existing .cpp`);

    if (!existsSync(iterCppPath)) {
      throw new Error(`Code Generator failed to produce ${iterCppPath}`);
    }
  } finally {
    semaphore.release();
  }

  // === ITERATION 0: CODE INSPECTOR (skip when skills disabled) ===
  if (args.useSkills) {
    progressTracker.update(queryId, "inspect-0");
    console.log(`[Orchestrator] [${queryId}] Iteration 0: Code Inspector`);
    const inspection0 = await runCodeInspection(queryId, iterCppPath, args, queryPhase, null, runDir);
    if (inspection0.verdict === "NEEDS_FIX" && hasCriticalIssues(inspection0)) {
      console.log(`[Orchestrator] [${queryId}] Inspector found critical issues, requesting fix`);
      await semaphore.acquire();
      try {
        const fixPrompt = [
          `The Code Inspector found critical issues in your generated code for ${queryId}.`,
          `Fix the following issues in: ${iterCppPath}`,
          ``,
          `## Issues`,
          JSON.stringify(inspection0.issues, null, 2),
          ``,
          `Use the Edit tool to fix each issue. Then recompile:`,
          `g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -I${UTILS_PATH} -o ${queryId.toLowerCase()} ${cppName}`,
        ].join("\n");
        const fixResult = await runAgent(codeGeneratorConfig.name, {
          systemPrompt: cgSystemPrompt,
          userPrompt: fixPrompt,
          allowedTools: codeGeneratorConfig.allowedTools,
          model: getAgentModel("code_generator", args),
          configName: "code_generator",
          cwd: iterDir,
          useSkills: args.useSkills,
          domainSkillsPrompt: codeGeneratorConfig.domainSkillsPrompt,
        });
        recordAgentTelemetry(queryPhase, "code_generator_fix", fixResult.durationMs, fixResult.tokens, fixResult.costUsd);
        if (fixResult.error) console.error(`[Orchestrator] [${queryId}] Fix agent failed (non-fatal): ${fixResult.error}`);
      } finally {
        semaphore.release();
      }
    }
  }

  // === ITERATION 0: EXECUTE (orchestrator safety net — always runs, even if agent already validated) ===
  progressTracker.update(queryId, "exec-0");
  console.log(`[Orchestrator] [${queryId}] Iteration 0: Executor (compile + run + validate)`);
  await executionQueue.requestExecution(queryId, async () => {
    return await executeQuery(query, iterDir, iterCppPath, args.gendbDir, groundTruthDir, iterResultsDir, defaults.optimizationRuns, args.targetBenchmark);
  });

  // Report to Iter0 barrier (for storage checkpoint coordination)
  iter0Barrier.report({ query, queryDir, iterDir, iterCppPath, cppName });

  // === ITERATIONS 1+: OPTIMIZE → EXECUTE loop ===
  let bestIterDir = iterDir;
  let bestExecResultsPath = resolve(iterDir, "execution_results.json");

  // Track best result for fault tolerance — return even if later iterations throw.
  // Start as "failed"; upgrade to "completed" when any iteration produces a valid run.
  const iter0ExecResults = await readJSON(bestExecResultsPath);
  const iter0Passed = iter0ExecResults?.run?.status === "pass";
  let bestCppPath = iter0Passed ? iterCppPath : null;
  let bestPlanPath = existsSync(planPath) ? planPath : null;
  let bestResult = iter0Passed
    ? { queryId, status: "completed", bestCppPath: iterCppPath, iterations: 0 }
    : { queryId, status: "failed", bestCppPath: null, iterations: 0 };

  // Record iter_0 baseline in optimization history
  if (iter0Passed) {
    optimizationHistory.iterations.push({
      iteration: 0,
      improved: true,
      categories: ["initial"],
      timing_ms: iter0ExecResults?.timing_ms || null,
      cold_timing_ms: iter0ExecResults?.cold_timing_ms || null,
      hot_timing_ms: iter0ExecResults?.hot_timing_ms || null,
      validation: iter0ExecResults?.validation?.status || null,
      strategy: "initial implementation",
      operation_timings: iter0ExecResults?.hot_operation_timings || iter0ExecResults?.operation_timings || null,
    });
    await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));
  }

  const maxIter = args.maxIterations;
  let previousIterationOutcome = null;

  for (let iteration = 1; iteration <= maxIter; iteration++) {
  try {
    // --- Programmatic shouldContinue() check ---
    const bestExecResults = await readJSON(bestExecResultsPath);
    const continueDecision = shouldContinue(queryId, optimizationHistory, args.benchmarkResults, bestExecResults, iteration, maxIter, args.stallThreshold);

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

    // --- Model escalation: re-invoke Code Generator with stronger model on repeated correctness failures ---
    if (continueDecision.action === 'escalate') {
      console.log(`[Orchestrator] [${queryId}] ESCALATING: ${continueDecision.reason}`);
      const providerCfg = getProviderConfig(args.agentProvider);
      const escalationModel = providerCfg.escalationModel;
      const escalationEffort = providerCfg.escalationEffortLevel;

      // Build accumulated error context from all failed iterations
      const failedIters = optimizationHistory.iterations.filter(i => i.validation === 'fail');
      const errorContext = [];
      for (const fi of failedIters) {
        const fiDir = resolve(queryDir, `iter_${fi.iteration}`);
        const fiExecPath = resolve(fiDir, "execution_results.json");
        try {
          const fiExec = JSON.parse(await readFile(fiExecPath, "utf-8"));
          if (fiExec?.validation?.details) {
            errorContext.push(`Iter ${fi.iteration}: ${JSON.stringify(fiExec.validation.details)}`);
          }
        } catch {}
      }

      // Read query guide
      const escalationGuidePath = resolve(runDir, "query_guides", `${queryId}_guide.md`);
      let escalationGuide = "";
      try { escalationGuide = await readFile(escalationGuidePath, "utf-8"); } catch {}

      // Copy plan.json for reference
      const planPath = resolve(queryDir, "iter_0", "plan.json");
      const optIterPlanPath = resolve(optIterDir, "plan.json");
      if (existsSync(planPath)) {
        try { await writeFile(optIterPlanPath, await readFile(planPath, "utf-8")); } catch {}
      }

      // Copy best code as starting point (fallback to iter_0 code if no passing iteration exists)
      if (bestCppPath && existsSync(bestCppPath)) {
        await writeFile(optIterCppPath, await readFile(bestCppPath, "utf-8"));
      } else if (!existsSync(optIterCppPath) && existsSync(iterCppPath)) {
        console.log(`[Orchestrator] [${queryId}] No passing iteration — seeding escalation with iter_0 code`);
        await writeFile(optIterCppPath, await readFile(iterCppPath, "utf-8"));
      }

      const escalationPrompt = [
        `## MODEL ESCALATION — Correctness Fix for ${queryId}`,
        `The previous ${failedIters.length} iterations ALL produced incorrect results.`,
        `You are being invoked as a stronger model (${escalationModel}) to fix this.`,
        ``,
        `## Accumulated Error Context`,
        ...errorContext.slice(-3),  // last 3 failure details
        ``,
        `## Current Code`,
        `Path: ${optIterCppPath}`,
        `Read it, understand the bug, and fix it using the Edit tool.`,
        ``,
        `## Query Guide`,
        escalationGuide,
        ``,
        `## SQL`,
        query.sql,
        ``,
        `Compile after fixing:`,
        `g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -I${UTILS_PATH} -o ${resolve(optIterDir, queryId.toLowerCase())} ${optIterCppPath}`,
      ].join("\n");

      await semaphore.acquire();
      try {
        const savedEffort = providerCfg.agentEffortLevels.code_generator;
        providerCfg.agentEffortLevels.code_generator = escalationEffort;
        const escalationResult = await runAgent(codeGeneratorConfig.name, {
          systemPrompt: cgSystemPrompt,
          userPrompt: escalationPrompt,
          allowedTools: codeGeneratorConfig.allowedTools,
          model: escalationModel,
          configName: "code_generator",
          cwd: optIterDir,
          useSkills: args.useSkills,
          domainSkillsPrompt: codeGeneratorConfig.domainSkillsPrompt,
        });
        providerCfg.agentEffortLevels.code_generator = savedEffort;
        recordAgentTelemetry(queryPhase, "code_generator_escalation", escalationResult.durationMs, escalationResult.tokens, escalationResult.costUsd);
        if (escalationResult.error) console.error(`[Orchestrator] [${queryId}] Escalation failed (non-fatal): ${escalationResult.error}`);
      } finally {
        semaphore.release();
      }

      // Execute escalated code
      progressTracker.update(queryId, `exec-${iteration}-escalated`);
      await executionQueue.requestExecution(queryId, async () => {
        return await executeQuery(query, optIterDir, optIterCppPath, args.gendbDir, groundTruthDir, optIterResultsDir, defaults.optimizationRuns, args.targetBenchmark);
      });

      const newExecResults = await readJSON(optIterExecResultsPath);
      const improved = checkExecutionImprovement(await readJSON(bestExecResultsPath), newExecResults);
      optimizationHistory.iterations.push({
        iteration,
        improved,
        categories: ["escalation"],
        timing_ms: newExecResults?.timing_ms || null,
        cold_timing_ms: newExecResults?.cold_timing_ms || null,
        hot_timing_ms: newExecResults?.hot_timing_ms || null,
        validation: newExecResults?.validation?.status || null,
        strategy: `escalation to ${escalationModel}`,
        operation_timings: newExecResults?.hot_operation_timings || newExecResults?.operation_timings || null,
      });
      await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

      if (improved) {
        console.log(`[Orchestrator] [${queryId}] Escalation iteration ${iteration} improved. Keeping changes.`);
        bestIterDir = optIterDir;
        bestCppPath = optIterCppPath;
        const escalationPlanPath = resolve(optIterDir, "plan.json");
        if (existsSync(escalationPlanPath)) bestPlanPath = escalationPlanPath;
        bestExecResultsPath = optIterExecResultsPath;
        bestResult = { queryId, status: "completed", bestCppPath: optIterCppPath, iterations: iteration };
        previousIterationOutcome = `Iteration ${iteration} (ESCALATED to ${escalationModel}) IMPROVED. Changes kept.`;
      } else {
        console.log(`[Orchestrator] [${queryId}] Escalation iteration ${iteration} did not improve.`);
        previousIterationOutcome = `Iteration ${iteration} (ESCALATED to ${escalationModel}) did not fix correctness.`;
      }
      continue;  // Skip normal optimizer path for this iteration
    }

    // Copy best code as starting point (fallback to iter_0 code if no passing iteration exists)
    if (bestCppPath && existsSync(bestCppPath)) {
      await writeFile(optIterCppPath, await readFile(bestCppPath, "utf-8"));
    } else if (!existsSync(optIterCppPath) && existsSync(iterCppPath)) {
      console.log(`[Orchestrator] [${queryId}] No passing iteration — seeding optimizer with iter_0 code`);
      await writeFile(optIterCppPath, await readFile(iterCppPath, "utf-8"));
    }

    // --- Stall detection (trigger after 2 non-improving iterations with 3x gap) ---
    let stallSection = "";
    const recentHistory = optimizationHistory.iterations.slice(-2);
    if (recentHistory.length >= 2 && recentHistory.every(it => !it.improved)) {
      const bestBaseline = getBestBaselineTime(args.benchmarkResults, queryId);
      const currentBest = bestExecResults?.timing_ms;
      if (bestBaseline && currentBest && currentBest > bestBaseline * 3) {
        stallSection = [
          "",
          "## OPTIMIZATION STALL DETECTED",
          `${recentHistory.length} consecutive iterations failed to improve. Current: ${Math.round(currentBest)}ms, best engine: ${Math.round(bestBaseline)}ms (${(currentBest / bestBaseline).toFixed(1)}x gap).`,
          "The current code architecture is fundamentally limited. See optimizer system prompt for stall recovery guidance.",
          "Read the Query Guide below for pre-built indexes you may not be using.",
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
        ...(args.useSkills ? ["See the join-optimization skill for the technique."] : []),
        "DO NOT skip this step. The current join order has not been validated.",
      ].join("\n");
    }

    // Build correctness failure section
    let correctnessSection = "";
    if (bestExecResults?.validation?.status === 'fail') {
      const details = bestExecResults.validation.details
        ? JSON.stringify(bestExecResults.validation.details, null, 2)
        : (bestExecResults.validation.output || "unknown mismatch");
      const diagnosis = diagnoseValidationFailure(bestExecResults);
      correctnessSection = [
        "",
        "## CORRECTNESS FAILURE — FIX THIS FIRST",
        "The current code produces WRONG results. You MUST fix correctness before any performance optimization.",
        "Mismatch details:",
        "```json",
        details,
        "```",
        diagnosis,
        "Common causes: wrong filter predicate, encoding/decoding bug, wrong join logic, incorrect aggregation.",
        "",
      ].join("\n");
    } else if (bestExecResults?.run?.status === 'fail' && (bestExecResults?.run?.stderr || "").includes("timed out")) {
      correctnessSection = [
        "",
        "## EXECUTION TIMEOUT — FIX THIS FIRST",
        `The previous run timed out (${defaults.queryExecutionTimeoutSec}s limit). The binary did not produce results.`,
        "Timeout likely caused by:",
        "1. Infinite loop in hash table — check memset sentinel initialization (see C20 in experience base).",
        "   memset(buf, 0x80, n) sets each BYTE to 0x80, producing 0x80808080 ≠ INT32_MIN. Use std::fill() instead.",
        "2. Algorithmic complexity explosion — nested loops with O(n²) or worse over large tables.",
        "3. Hash table at 100% load factor — ensure power-of-2 sizing with ≤50% load factor.",
        "4. Thread-local aggregation hash table too small — each thread may see ALL distinct groups,",
        "   not just total/nthreads. Check that per-thread capacity >= estimated aggregate groups.",
        "   If plan.json group estimate seems too low (e.g., <10K for a multi-table join),",
        "   re-derive from qualifying row counts.",
        "Check the code for these patterns and fix the root cause.",
        "",
      ].join("\n");
    }

    // Read query guide
    const qoGuidePath = resolve(runDir, "query_guides", `${queryId}_guide.md`);
    let qoQueryGuide = "";
    try { qoQueryGuide = await readFile(qoGuidePath, "utf-8"); } catch {}

    // --- Extract correctness anchors from best passing code ---
    let anchorsSection = "";
    if (bestExecResults?.validation?.status === 'pass' && bestCppPath) {
      const anchors = extractCorrectnessAnchors(bestCppPath);
      anchorsSection = formatCorrectnessAnchors(anchors);
    }

    // --- Build optimization history summary (lean, one-line per iteration) ---
    const historySummary = optimizationHistory.iterations.map(it => {
      const isHot = defaults.optimizationTarget === 'hot';
      const isCold = defaults.optimizationTarget === 'cold';
      const displayTime = isHot ? it.hot_timing_ms : isCold ? it.cold_timing_ms : it.timing_ms;
      const timingStr = displayTime ? `${Math.round(displayTime)}ms` : 'N/A';
      const label = isHot ? ' (hot)' : isCold ? ' (cold)' : '';
      let line = `Iter ${it.iteration}: ${timingStr}${label} ${(it.validation || 'unknown').toUpperCase()} (${it.improved ? 'improved' : 'no improvement'})`;

      // Add strategy note if available
      if (it.strategy) {
        line += ` — ${it.strategy}`;
      }

      // Add operation timing breakdown if available
      if (it.operation_timings) {
        const phases = Object.entries(it.operation_timings)
          .filter(([k]) => k !== 'total' && k !== 'output')
          .map(([k, v]) => `${k}=${Math.round(parseFloat(v))}ms`)
          .join(', ');
        if (phases) line += `\n  Phases: ${phases}`;
      }

      return line;
    }).join("\n");

    // --- Query Optimizer (gated by semaphore) ---
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Query Optimizer`);
    const revisedPlanPath = resolve(optIterDir, "plan.json");

    // Build performance profile section (needed by both QO and CG)
    const performanceProfile = (() => {
        const targetMetric = defaults.optimizationTarget === 'hot' ? 'hot' : 'combined';
        const hotOt = bestExecResults?.hot_operation_timings || {};
        const hotTotal = parseFloat(hotOt.total || bestExecResults?.hot_timing_ms || 0);

        if (targetMetric === 'hot' && hotTotal > 0) {
          // Hot-only optimization: show only hot profile
          const lines = ["## Current Performance Profile (Hot — optimization target)"];
          let dominantOp = "", dominantMs = 0;
          for (const [op, ms] of Object.entries(hotOt)) {
            if (op === "total" || op === "output") continue;
            const pct = ((parseFloat(ms) / hotTotal) * 100).toFixed(0);
            lines.push(`- ${op}: ${Math.round(parseFloat(ms))}ms (${pct}%)`);
            if (parseFloat(ms) > dominantMs) { dominantMs = parseFloat(ms); dominantOp = op; }
          }
          lines.push(`- total: ${Math.round(hotTotal)}ms`);
          if (dominantOp) lines.push(`Dominant: ${dominantOp} (${((dominantMs / hotTotal) * 100).toFixed(0)}%)`);
          if (bestExecResults?.cold_timing_ms) lines.push(`\nCold time (informational): ${Math.round(bestExecResults.cold_timing_ms)}ms`);
          return lines.join("\n");
        }

        // Combined metric: show cold vs hot side-by-side
        const coldOt = bestExecResults?.operation_timings || {};
        const coldTotal = parseFloat(coldOt.total || bestExecResults?.cold_timing_ms || bestExecResults?.timing_ms || 0);
        if (coldTotal <= 0) return "";

        if (hotTotal > 0 && coldTotal > hotTotal * 1.5) {
          const lines = [
            "## Current Performance Profile (Cold vs Hot)",
            "| Phase | Cold (ms) | Hot (ms) | Cold % | Hot % |",
            "|-------|-----------|----------|--------|-------|",
          ];
          const allOps = new Set([...Object.keys(coldOt), ...Object.keys(hotOt)]);
          let dominantColdOp = "", dominantColdMs = 0;
          for (const op of allOps) {
            if (op === "total" || op === "output") continue;
            const cMs = parseFloat(coldOt[op] || 0);
            const hMs = parseFloat(hotOt[op] || 0);
            const cPct = coldTotal > 0 ? ((cMs / coldTotal) * 100).toFixed(0) : "0";
            const hPct = hotTotal > 0 ? ((hMs / hotTotal) * 100).toFixed(0) : "0";
            lines.push(`| ${op} | ${Math.round(cMs)} | ${Math.round(hMs)} | ${cPct}% | ${hPct}% |`);
            if (cMs > dominantColdMs) { dominantColdMs = cMs; dominantColdOp = op; }
          }
          lines.push(`| total | ${Math.round(coldTotal)} | ${Math.round(hotTotal)} | | |`);
          if (dominantColdOp) {
            const coldPct = ((dominantColdMs / coldTotal) * 100).toFixed(0);
            const hMs = parseFloat(hotOt[dominantColdOp] || 0);
            const hPct = hotTotal > 0 ? ((hMs / hotTotal) * 100).toFixed(0) : "0";
            lines.push(`\n${dominantColdOp} is ${coldPct}% of cold time but ${hPct}% of hot time — ${parseFloat(coldPct) > 50 && parseFloat(hPct) < 20 ? 'I/O-bound' : 'compute-bound'}.`);
          }
          return lines.join("\n");
        }

        const lines = ["## Current Performance Profile"];
        let dominantOp = "", dominantPct = 0;
        for (const [op, ms] of Object.entries(coldOt)) {
          if (op === "total" || op === "output") continue;
          const pct = ((parseFloat(ms) / coldTotal) * 100).toFixed(0);
          lines.push(`- ${op}: ${Math.round(parseFloat(ms))}ms (${pct}%)`);
          if (parseFloat(ms) > dominantPct) { dominantPct = parseFloat(ms); dominantOp = op; }
        }
        lines.push(`- total: ${Math.round(coldTotal)}ms`);
        if (dominantOp) lines.push(`Dominant: ${dominantOp} (${((dominantPct / coldTotal) * 100).toFixed(0)}%)`);
        return lines.join("\n");
      })();

    // Read available column versions for the optimizer
    const columnVersionsContext = await getColumnVersionsContext(args.gendbDir);

    await semaphore.acquire();
    try {
      const qoTemplatePath = resolve(__dirname, "agents/query-optimizer/user-prompt.md");
      const qoTemplate = readFileSync(qoTemplatePath, 'utf-8');
      const qoUserPrompt = renderTemplate(qoTemplate, {
        query_id: queryId,
        iteration: iteration,
        stall_section: stallSection,
        correctness_section: correctnessSection,
        anchors_section: anchorsSection,
        plan_path_exists: bestPlanPath ? existsSync(bestPlanPath) : existsSync(planPath),
        plan_path: bestPlanPath || planPath,
        query_guide: qoQueryGuide,
        performance_profile: performanceProfile,
        benchmark_context: formatPerQueryBenchmarkContext(args.benchmarkResults, queryId, bestExecResults?.cold_timing_ms, bestExecResults?.hot_timing_ms),
        history_summary: historySummary,
        cpu_cores: hw.cpu_cores || 'unknown',
        disk_type: hw.disk_type || 'unknown',
        l3_cache_mb: hw.l3_cache_mb || 'unknown',
        total_memory_gb: hw.total_memory_gb || 'unknown',
        join_sampling_mandate: joinSamplingMandate,
        previous_outcome: previousIterationOutcome,
        gendb_dir: args.gendbDir,
        cpp_path: bestCppPath || iterCppPath,
        query_sql: query.sql,
        revised_plan_path: revisedPlanPath,
        column_versions: columnVersionsContext,
      });

      const qoResult = await runAgent(queryOptimizerConfig.name, {
        systemPrompt: qoSystemPrompt,
        userPrompt: qoUserPrompt,
        allowedTools: queryOptimizerConfig.allowedTools,
        model: getAgentModel("query_optimizer", args),
        configName: "query_optimizer",
        cwd: optIterDir,
        useSkills: args.useSkills,
        domainSkillsPrompt: queryOptimizerConfig.domainSkillsPrompt,
      });
      recordAgentTelemetry(queryPhase, "query_optimizer", qoResult.durationMs, qoResult.tokens, qoResult.costUsd);
      if (qoResult.error) throw new Error(`Query Optimizer failed for ${queryId}: ${qoResult.error}`);
    } finally {
      semaphore.release();
    }

    // --- Code Generator: implement the optimizer's revised plan ---
    let revisedPlanJson = null;
    try { revisedPlanJson = JSON.parse(await readFile(revisedPlanPath, "utf-8")); } catch {}

    // Extract strategy note from the optimizer's revised plan (expected: plain text string)
    let strategyNote = '';
    if (revisedPlanJson?.optimization_notes) {
      const notes = revisedPlanJson.optimization_notes;
      strategyNote = typeof notes === 'string' ? notes : String(notes);
    }

    if (revisedPlanJson) {
      progressTracker.update(queryId, `codegen-${iteration}`);
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Code Generator (implementing revised plan)`);
      await semaphore.acquire();
      try {
        const cgTemplatePath = resolve(__dirname, "agents/code-generator/user-prompt.md");
        const cgTemplate = readFileSync(cgTemplatePath, 'utf-8');

        // Build performance context for CG so it knows what to prioritize
        const perfContext = performanceProfile
          ? `Previous implementation: ${performanceProfile}\nThe revised plan addresses the bottleneck identified above.`
          : '';

        // Format storage_extensions from revised plan for Code Generator
        const storageExtSection = revisedPlanJson.storage_extensions?.length
          ? `## Storage Extensions\nThe Query Optimizer has built or referenced derived column versions. Use these files instead of the original columns where specified:\n${JSON.stringify(revisedPlanJson.storage_extensions, null, 2)}\nAll file paths are relative to the GenDB storage directory (argv[1]).`
          : '';

        const optCgUserPrompt = renderTemplate(cgTemplate, {
          query_id: queryId,
          plan_json: JSON.stringify(revisedPlanJson, null, 2),
          anchors_section: anchorsSection,
          performance_context: perfContext,
          storage_extensions: storageExtSection,
          column_versions: columnVersionsContext,
          cpu_cores: hw.cpu_cores || 'unknown',
          disk_type: hw.disk_type || 'unknown',
          l3_cache_mb: hw.l3_cache_mb || 'unknown',
          total_memory_gb: hw.total_memory_gb || 'unknown',
          query_guide: queryGuide,
          schema_path: args.schema,
          benchmark_context: formatPerQueryBenchmarkContext(args.benchmarkResults, queryId, bestExecResults?.cold_timing_ms, bestExecResults?.hot_timing_ms),
          query_sql: query.sql,
          gendb_dir: args.gendbDir,
          ground_truth: hasGroundTruth ? groundTruthDir : 'No ground truth available',
          has_ground_truth: hasGroundTruth,
          ground_truth_dir: groundTruthDir,
          cpp_path: optIterCppPath,
          utils_path: UTILS_PATH,
          binary_name: queryId.toLowerCase(),
          cpp_name: cppName,
          timeout_sec: defaults.queryExecutionTimeoutSec,
          results_dir: optIterResultsDir,
          compare_tool: COMPARE_TOOL_PATH,
        });

        const cgResult = await runAgent(codeGeneratorConfig.name, {
          systemPrompt: cgSystemPrompt,
          userPrompt: optCgUserPrompt,
          allowedTools: codeGeneratorConfig.allowedTools,
          model: getAgentModel("code_generator", args),
          configName: "code_generator",
          cwd: optIterDir,
          useSkills: args.useSkills,
          domainSkillsPrompt: codeGeneratorConfig.domainSkillsPrompt,
        });
        recordAgentTelemetry(queryPhase, "code_generator", cgResult.durationMs, cgResult.tokens, cgResult.costUsd);
        const cgTimedOut = cgResult.error?.includes("timed out");
        if (cgResult.error && !cgTimedOut) throw new Error(cgResult.error);
        if (cgTimedOut) console.log(`[Orchestrator] [${queryId}] Code Generator timed out in iteration ${iteration} — will try executeQuery on existing .cpp`);
      } finally {
        semaphore.release();
      }
    } else {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Optimizer did not produce revised plan — skipping Code Generator`);
    }

    // --- Code Inspector (after optimization, with regression detection; skip when skills disabled) ---
    if (args.useSkills) {
      progressTracker.update(queryId, `inspect-${iteration}`);
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Code Inspector`);
      const previousPassingPath = (bestExecResults?.validation?.status === 'pass') ? bestCppPath : null;
      const inspectionN = await runCodeInspection(queryId, optIterCppPath, args, queryPhase, previousPassingPath, runDir);
      if (inspectionN.verdict === "NEEDS_FIX" && hasCriticalIssues(inspectionN)) {
        console.log(`[Orchestrator] [${queryId}] Inspector found critical issues in iteration ${iteration}, requesting fix`);
        await semaphore.acquire();
        try {
          const fixPrompt = [
            `The Code Inspector found critical issues in optimized code for ${queryId} (iteration ${iteration}).`,
            `Fix the following issues in: ${optIterCppPath}`,
            ``,
            `## Issues`,
            JSON.stringify(inspectionN.issues, null, 2),
            ``,
            `Use the Edit tool to fix each issue. Then recompile:`,
            `g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -I${UTILS_PATH} -o ${resolve(optIterDir, queryId.toLowerCase())} ${optIterCppPath}`,
          ].join("\n");
          const fixResult = await runAgent(codeGeneratorConfig.name, {
            systemPrompt: cgSystemPrompt,
            userPrompt: fixPrompt,
            allowedTools: codeGeneratorConfig.allowedTools,
            model: getAgentModel("code_generator", args),
            configName: "code_generator",
            cwd: optIterDir,
            useSkills: args.useSkills,
            domainSkillsPrompt: codeGeneratorConfig.domainSkillsPrompt,
          });
          recordAgentTelemetry(queryPhase, "code_generator_fix", fixResult.durationMs, fixResult.tokens, fixResult.costUsd);
          if (fixResult.error) console.error(`[Orchestrator] [${queryId}] Fix agent failed (non-fatal): ${fixResult.error}`);
        } finally {
          semaphore.release();
        }
      }
    }

    // --- Execute via ExecutionQueue (enters immediately after QO + Inspector) ---
    progressTracker.update(queryId, `exec-${iteration}`);
    console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: Executor (compile + run + validate)`);
    await executionQueue.requestExecution(queryId, async () => {
      return await executeQuery(query, optIterDir, optIterCppPath, args.gendbDir, groundTruthDir, optIterResultsDir, defaults.optimizationRuns, args.targetBenchmark);
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
      cold_timing_ms: newExecResults?.cold_timing_ms || null,
      hot_timing_ms: newExecResults?.hot_timing_ms || null,
      validation: newExecResults?.validation?.status || null,
      strategy: strategyNote,
      operation_timings: newExecResults?.hot_operation_timings || newExecResults?.operation_timings || null,
    });
    await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));

    if (improved) {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration} improved. Keeping changes.`);
      bestIterDir = optIterDir;
      bestCppPath = optIterCppPath;
      const iterPlanPath = resolve(optIterDir, "plan.json");
      if (existsSync(iterPlanPath)) bestPlanPath = iterPlanPath;
      bestExecResultsPath = optIterExecResultsPath;
      bestResult = { queryId, status: "completed", bestCppPath: optIterCppPath, iterations: iteration };
      previousIterationOutcome = `Iteration ${iteration} IMPROVED. Changes kept.`;
    } else {
      console.log(`[Orchestrator] [${queryId}] Iteration ${iteration} did not improve. Rolling back.`);
      const prevTiming = prevExecResults?.timing_ms;
      const newTiming = newExecResults?.timing_ms;
      const regressionPct = (prevTiming && newTiming) ? Math.round(((newTiming - prevTiming) / prevTiming) * 100) : null;
      const regressionDetail = (prevTiming && newTiming) ? ` (${Math.round(newTiming)}ms vs best ${Math.round(prevTiming)}ms, +${regressionPct}%)` : '';
      const recentRegressions = optimizationHistory.iterations.slice(-3).filter(i => !i.improved).length;
      const regressionAdvice = recentRegressions >= 2 ? ' Multiple regressions. Focus on dominant phase from operation timings.' : '';
      previousIterationOutcome = `Iteration ${iteration} REGRESSED${regressionDetail} — rolled back. Try a DIFFERENT optimization direction.${regressionAdvice}`;
    }
  } catch (err) {
    console.error(`[Orchestrator] [${queryId}] Iteration ${iteration} error: ${err.message}`);
    console.error(err.stack);
    const failureReason = err.message.includes("timed out") ? "TIMEOUT" : "CRASHED";
    previousIterationOutcome = `Iteration ${iteration} ${failureReason}: ${err.message}`;

    // Record failed iteration in optimization history so subsequent iterations can see what happened
    const fallbackExecResultsPath = resolve(queryDir, `iter_${iteration}`, "results", "execution_results.json");
    let fallbackExecResults = null;

    // Attempt to execute whatever .cpp exists in this iteration dir (prevents gaps in execution_results.json)
    try {
      const failedIterDir = resolve(queryDir, `iter_${iteration}`);
      const failedCppPath = resolve(failedIterDir, cppName);
      const failedResultsDir = resolve(failedIterDir, "results");
      if (existsSync(failedCppPath)) {
        console.log(`[Orchestrator] [${queryId}] Iteration ${iteration}: running fallback execution on existing .cpp`);
        await mkdir(failedResultsDir, { recursive: true });
        await executionQueue.requestExecution(queryId, async () => {
          return await executeQuery(query, failedIterDir, failedCppPath, args.gendbDir, groundTruthDir, failedResultsDir, defaults.optimizationRuns, args.targetBenchmark);
        });
        fallbackExecResults = await readJSON(fallbackExecResultsPath);
      }
    } catch (fallbackErr) {
      console.error(`[Orchestrator] [${queryId}] Fallback execution also failed: ${fallbackErr.message}`);
    }

    optimizationHistory.iterations.push({
      iteration,
      improved: false,
      categories: [],
      timing_ms: fallbackExecResults?.timing_ms || null,
      cold_timing_ms: fallbackExecResults?.cold_timing_ms || null,
      hot_timing_ms: fallbackExecResults?.hot_timing_ms || null,
      validation: fallbackExecResults?.validation?.status || "skipped",
      strategy: `${failureReason}: ${err.message}`,
      operation_timings: fallbackExecResults?.hot_operation_timings || fallbackExecResults?.operation_timings || {},
    });
    await writeFile(historyPath, JSON.stringify(optimizationHistory, null, 2));
  }
  }

  progressTracker.update(queryId, "done");

  return bestResult;
}

// ---------------------------------------------------------------------------
// OS Cache Management
// ---------------------------------------------------------------------------

let _cacheClearAvailable = null; // null = untested, true/false = tested

/** Clear OS page cache via sudo. Returns true on success. */
function clearOsCache() {
  try {
    execSync('sudo -n sh -c "sync && echo 3 > /proc/sys/vm/drop_caches"', { stdio: 'pipe', timeout: 10000 });
    return true;
  } catch {
    return false;
  }
}

/** Test cache clearing capability once at startup. */
function checkCacheClearCapability() {
  if (_cacheClearAvailable !== null) return _cacheClearAvailable;
  _cacheClearAvailable = clearOsCache();
  if (_cacheClearAvailable) {
    console.log("[Executor] OS cache clearing available (sudo -n drop_caches works).");
  } else {
    console.warn("[Executor] WARNING: OS cache clearing unavailable (sudo -n failed). Cold-start optimization disabled.");
    console.warn("[Executor] To enable: add 'echo 3 > /proc/sys/vm/drop_caches' to sudoers NOPASSWD.");
  }
  return _cacheClearAvailable;
}

// ---------------------------------------------------------------------------
// Executor (non-LLM): compile, run, validate, parse timing
// ---------------------------------------------------------------------------

/**
 * Execute a query: compile → run (multiple times) → validate → parse timing.
 * Returns execution results JSON and writes execution_results.json to iterDir.
 * @param {number} numRuns - Number of runs. Run 1 = cold (cache cleared), runs 2+ = hot.
 */
async function executeQuery(query, iterDir, cppPath, gendbDir, groundTruthDir, resultsDir, numRuns = 1, targetBenchmark = "tpc-h") {
  const binaryPath = resolve(iterDir, query.id.toLowerCase());
  const execResultsPath = resolve(iterDir, "execution_results.json");
  const results = {
    compile: { status: "fail", output: "" },
    run: { status: "fail", output: "", stderr: "", duration_ms: 0 },
    validation: { status: "skipped", output: "" },
    operation_timings: {},
    timing_ms: null,
    cold_timing_ms: null,
    hot_timing_ms: null,
    hot_operation_timings: {},
    all_runs: [],
  };

  // Clear results directory to prevent stale output from masking silent binary failures
  if (existsSync(resultsDir)) {
    rmSync(resultsDir, { recursive: true, force: true });
  }
  await mkdir(resultsDir, { recursive: true });

  // Step 1: Compile (with -fopenmp)
  console.log(`[Executor] [${query.id}] Compiling...`);
  try {
    const compileOutput = await runProcess("g++", [
      "-O3", "-march=native", "-std=c++17", "-Wall", "-lpthread", "-fopenmp",
      "-DGENDB_PROFILE", `-I${UTILS_PATH}`,
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

  // Step 2: Multi-run execution
  const effectiveRuns = Math.max(1, numRuns);
  const isHotMode = defaults.optimizationTarget === 'hot';
  const isColdMode = defaults.optimizationTarget === 'cold';
  if (isHotMode) {
    console.log(`[Executor] [${query.id}] Hot mode — ${effectiveRuns} hot runs, no cache clearing.`);
  } else {
    console.log(`[Executor] [${query.id}] Cold mode — ${effectiveRuns} cold runs, clearing cache before each.`);
  }
  for (let runIdx = 0; runIdx < effectiveRuns; runIdx++) {
    // Cold mode: clear OS cache before EVERY run
    if (!isHotMode) {
      if (_cacheClearAvailable === null) checkCacheClearCapability();
      if (_cacheClearAvailable) {
        const cleared = clearOsCache();
        if (cleared) {
          console.log(`[Executor] [${query.id}] OS cache cleared before run ${runIdx + 1}.`);
        } else {
          console.warn(`[Executor] [${query.id}] WARNING: OS cache clear FAILED before run ${runIdx + 1}.`);
        }
      } else if (runIdx === 0) {
        console.warn(`[Executor] [${query.id}] WARNING: OS cache clearing unavailable. Runs will NOT be truly cold.`);
      }
    }

    console.log(`[Executor] [${query.id}] Run ${runIdx + 1}/${effectiveRuns}...`);
    const runStart = Date.now();
    try {
      const runOutput = await runProcess(binaryPath, [gendbDir, resultsDir], {
        cwd: iterDir, timeout: defaults.queryExecutionTimeoutSec * 1000,
      });
      const runDurationMs = Date.now() - runStart;
      const runStdout = typeof (runOutput.stdout || runOutput) === "string" ? (runOutput.stdout || runOutput) : "";

      // Parse [TIMING] lines from this run's stdout
      const runTimings = {};
      const timingRegex = /\[TIMING\]\s+(\w+):\s+([\d.]+)\s*ms/g;
      let match;
      while ((match = timingRegex.exec(runStdout)) !== null) {
        runTimings[match[1]] = parseFloat(match[2]);
      }

      // Extract timing_ms for this run
      let runTimingMs = null;
      if (runTimings.total != null && runTimings.output != null) {
        runTimingMs = runTimings.total - runTimings.output;
      } else if (runTimings.total != null) {
        runTimingMs = runTimings.total;
      } else {
        const totalMatch = runStdout.match(/Execution time:\s*([\d.]+)\s*ms/);
        if (totalMatch) runTimingMs = parseFloat(totalMatch[1]);
      }

      results.all_runs.push({ timing_ms: runTimingMs, operation_timings: runTimings });

      // Keep last run's stdout/stderr for validation and status
      results.run = {
        status: "pass",
        output: runStdout,
        stderr: runOutput.stderr || "",
        duration_ms: runDurationMs,
      };

      console.log(`[Executor] [${query.id}] Run ${runIdx + 1}/${effectiveRuns} completed in ${runDurationMs}ms (timing: ${runTimingMs ? Math.round(runTimingMs) + 'ms' : 'N/A'}).`);
    } catch (err) {
      results.run = {
        status: "fail",
        output: err.stdout || "",
        stderr: err.message,
        duration_ms: Date.now() - runStart,
      };
      console.error(`[Executor] [${query.id}] Run ${runIdx + 1}/${effectiveRuns} failed: ${err.message.slice(0, 200)}`);
      await writeFile(execResultsPath, JSON.stringify(results, null, 2));
      return results;
    }
  }

  // Step 3: Derive timing from all_runs based on optimization target
  if (results.all_runs.length > 0) {

    // Find best run (lowest timing) for operation_timings
    let bestIdx = 0, bestTime = Infinity;
    for (let i = 0; i < results.all_runs.length; i++) {
      if (results.all_runs[i].timing_ms != null && results.all_runs[i].timing_ms < bestTime) {
        bestTime = results.all_runs[i].timing_ms;
        bestIdx = i;
      }
    }

    const allTimings = results.all_runs.map(r => r.timing_ms).filter(t => t != null);
    const avgTiming = allTimings.length > 0 ? allTimings.reduce((a, b) => a + b, 0) / allTimings.length : null;

    if (isHotMode) {
      // Hot mode: all runs are hot (no cache clear). Average all runs.
      results.hot_timing_ms = avgTiming;
      results.cold_timing_ms = null;
      results.hot_operation_timings = results.all_runs[bestIdx].operation_timings;
      results.operation_timings = results.hot_operation_timings;
      results.timing_ms = results.hot_timing_ms;
    } else {
      // Cold mode: all runs are cold (cache cleared before each). Average all runs.
      results.cold_timing_ms = avgTiming;
      results.hot_timing_ms = null;
      results.operation_timings = results.all_runs[bestIdx].operation_timings;
      results.hot_operation_timings = {};
      results.timing_ms = results.cold_timing_ms;
    }
  }

  // Step 4: Validate against ground truth (from last run)
  if (groundTruthDir && existsSync(groundTruthDir)) {
    console.log(`[Executor] [${query.id}] Validating results...`);
    try {
      const compareArgs = [COMPARE_TOOL_PATH, groundTruthDir, resultsDir];
      if (targetBenchmark === "tpc-h") {
        compareArgs.push("--tpch");
      } else {
        compareArgs.push("--financial");
      }
      const valOutput = await runProcess("python3", compareArgs, { cwd: iterDir, timeout: 60000 });
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

  // Write lean execution results (mode-aware, no duplicated fields)
  const leanResults = {
    compile: { status: results.compile.status },
    run: { status: results.run.status, duration_ms: results.run.duration_ms },
    validation: {
      status: results.validation.status,
      ...(results.validation.details ? { details: trimValidationDetails(results.validation.details) } : {}),
    },
    optimization_target: defaults.optimizationTarget,
    timing_ms: results.timing_ms,
  };
  if (isHotMode) {
    leanResults.hot_timing_ms = results.hot_timing_ms;
    leanResults.operation_timings = results.hot_operation_timings;
  } else {
    leanResults.cold_timing_ms = results.cold_timing_ms;
    leanResults.hot_timing_ms = results.hot_timing_ms;
    leanResults.operation_timings = results.operation_timings;
    leanResults.hot_operation_timings = results.hot_operation_timings;
  }
  // Include compile output only on failure
  if (results.compile.status === 'fail') leanResults.compile.output = results.compile.output;
  // Include run stderr only on failure
  if (results.run.status === 'fail') leanResults.run.stderr = results.run.stderr;

  await writeFile(execResultsPath, JSON.stringify(leanResults, null, 2));
  console.log(`[Executor] [${query.id}] Results written to ${execResultsPath}`);
  // Return full results for in-memory use
  return results;
}

/**
 * Run a subprocess and return its stdout. Rejects on non-zero exit.
 */
function runProcess(cmd, cmdArgs, opts = {}) {
  const timeoutVal = opts.timeout || 120000;
  return new Promise((resolveP, rejectP) => {
    const startTime = Date.now();
    let timedOut = false;
    const child = spawn(cmd, cmdArgs, {
      cwd: opts.cwd,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });

    const timer = setTimeout(() => {
      timedOut = true;
      try { process.kill(-child.pid, "SIGKILL"); } catch {}
    }, timeoutVal);

    child.on("close", (code) => {
      clearTimeout(timer);
      // Defense-in-depth: ensure entire process group is dead
      try { process.kill(-child.pid, "SIGKILL"); } catch {}
      if (code === 0 && !timedOut) {
        resolveP({ stdout, stderr });
      } else {
        const durationMs = Date.now() - startTime;
        let msg;
        if (timedOut) {
          msg = `Process timed out after ${Math.round(timeoutVal / 1000)}s`;
        } else if (code === null) {
          msg = `Process killed by signal (duration: ${Math.round(durationMs / 1000)}s)`;
        } else {
          msg = `Process exited with code ${code}`;
        }
        const err = new Error(stderr || msg);
        err.stdout = stdout;
        err.stderr = stderr;
        err.timedOut = timedOut;
        rejectP(err);
      }
    });
    child.on("error", (err) => {
      clearTimeout(timer);
      rejectP(err);
    });
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
    `CXXFLAGS = -O3 -march=native -flto -std=c++17 -Wall -lpthread -fopenmp -DGENDB_LIBRARY -I${UTILS_PATH}`,
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
      const finalRun = await runProcess(resolve(finalDir, "main"), [args.gendbDir, finalResultsDir], {
        cwd: finalDir, timeout: defaults.queryExecutionTimeoutSec * 1000,
      });
      if (finalRun.stdout) process.stdout.write(finalRun.stdout);
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
    const iters = new Map(); // iteration number -> data
    // Scan all iter_* directories instead of breaking on first gap
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
    // Convert map to sparse array indexed by iteration number
    const maxIterNum = iters.size > 0 ? Math.max(...iters.keys()) + 1 : 0;
    const iterArray = [];
    for (let i = 0; i < maxIterNum; i++) {
      iterArray.push(iters.get(i) || { timing_ms: null, validation: "-" });
    }
    if (maxIterNum > maxIter) maxIter = maxIterNum;
    queryData.push({ id: query.id, iters: iterArray });
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
// Pre-Run Cleanup
// ---------------------------------------------------------------------------

function cleanupOrphanedProcesses() {
  try {
    const result = execSync("pgrep -f '\\.gendb.*/(q|Q)[0-9]' 2>/dev/null || true", { encoding: "utf-8" }).trim();
    if (result) {
      const pids = result.split("\n").filter(Boolean);
      console.log(`[Orchestrator] Found ${pids.length} orphaned query process(es): ${pids.join(", ")}`);
      try {
        execSync(`pkill -9 -f '\\.gendb.*/(q|Q)[0-9]' 2>/dev/null || true`);
        console.log("[Orchestrator] Orphaned processes killed.");
      } catch {}
    }
  } catch {
    // pgrep/pkill not available or no matches — safe to ignore
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  cleanupOrphanedProcesses();

  const args = parseArgs(process.argv);

  // Initialize agent provider and resolve default model
  setAgentProvider(args.agentProvider);
  if (!args.model) args.model = getProviderConfig(args.agentProvider).model;

  // Conditionally disable experience skill path when skills are off
  if (!args.useSkills) {
    EXPERIENCE_PATH = "";
  }

  const schema = await readFile(args.schema, "utf-8");
  const queries = await readFile(args.queries, "utf-8");

  if (!existsSync(args.dataDir)) {
    console.error(`[Orchestrator] Error: data directory not found: ${args.dataDir}`);
    console.error(`[Orchestrator] Data directory not found. Ensure source data files exist at: ${args.dataDir}`);
    process.exit(1);
  }

  const workload = getWorkloadName(args.schema);
  const runId = createRunId();
  const runDir = await createRunDir(workload, runId);

  // Resolve gendb dir: place inside run directory for isolation unless explicitly overridden.
  // benchmark.py reads gendbDir from run.json, so this works without benchmark changes.
  if (!args.gendbDir) {
    args.gendbDir = resolve(runDir, "gendb");
  }
  await mkdir(args.gendbDir, { recursive: true });

  // Try to load benchmark comparison results
  const benchmarkResultsPath = resolve(
    BENCHMARKS_DIR, args.targetBenchmark,
    "results", `sf${args.scaleFactor}`, "metrics", "benchmark_results.json"
  );
  const benchmarkResults = await readJSON(benchmarkResultsPath);
  args.benchmarkResults = benchmarkResults || null;
  if (benchmarkResults) {
    console.log(`[Orchestrator] Benchmark comparison data loaded from: ${benchmarkResultsPath}`);
  }

  console.log("[Orchestrator] GenDB Pipeline v34");
  console.log(`[Orchestrator] Schema:              ${args.schema}`);
  console.log(`[Orchestrator] Queries:             ${args.queries}`);
  console.log(`[Orchestrator] Data Dir:            ${args.dataDir}`);
  console.log(`[Orchestrator] GenDB Dir:           ${args.gendbDir}`);
  console.log(`[Orchestrator] Scale Factor:        ${args.scaleFactor}`);
  console.log(`[Orchestrator] Max Iterations:      ${args.maxIterations}`);
  console.log(`[Orchestrator] Stall Threshold:     ${args.stallThreshold}`);
  console.log(`[Orchestrator] Max Concurrent:      ${args.maxConcurrent}`);
  console.log(`[Orchestrator] Default Model:       ${args.model}`);
  console.log(`[Orchestrator] Model Override:      ${args.modelOverride || "(none)"}`);
  const activeProviderCfg = getProviderConfig(args.agentProvider);
  console.log(`[Orchestrator] Agent Provider:      ${args.agentProvider}`);
  console.log(`[Orchestrator] Agent Models:        ${JSON.stringify(activeProviderCfg.agentModels)}`);
  console.log(`[Orchestrator] Effort Levels:       ${JSON.stringify(activeProviderCfg.agentEffortLevels)}`);
  console.log(`[Orchestrator] Optimization Target: ${args.optimizationTarget}`);
  const runLabel = args.optimizationTarget === 'hot' ? `${defaults.optimizationRuns} hot` : `${defaults.optimizationRuns} cold`;
  console.log(`[Orchestrator] Optimization Runs:   ${runLabel}`);
  console.log(`[Orchestrator] Workload:            ${workload}`);
  console.log(`[Orchestrator] Run ID:              ${runId}`);
  console.log(`[Orchestrator] Run Dir:             ${runDir}`);

  // Test cache clearing capability at startup (only needed for cold mode)
  if (args.optimizationTarget !== 'hot') {
    checkCacheClearCapability();
  }

  let pipelineError = null;
  try {
    // Phase 1: Offline Data Storage Optimization
    const { workloadAnalysisPath, storageDesignPath } = await runOfflineStorageOptimization(args, runDir, schema, queries);

    // Phase 2: Online Per-Query Parallel Optimization
    await runPerQueryParallelOptimization(args, runDir, workloadAnalysisPath, storageDesignPath);

    // Phase 3: DBA Stage B (Post-Run Retrospective)
    if (args.useDba) {
      console.log("\n[Orchestrator] ========== PHASE 3: DBA RETROSPECTIVE ==========\n");
      const retroDir = resolve(runDir, "retrospective");
      await mkdir(retroDir, { recursive: true });

      try {
        const dbaSystemPrompt = await readFile(dbaConfig.promptPath, "utf-8");
        const dbaBTemplatePath = resolve(__dirname, "agents/dba/user-prompt.md");
        const dbaBTemplate = readFileSync(dbaBTemplatePath, 'utf-8');
        const dbaStageBPrompt = renderTemplate(dbaBTemplate, {
          stage_b: true,
          run_dir: runDir,
          retro_dir: retroDir,
          queries_dir: resolve(runDir, "queries"),
          retro_summary_path: resolve(retroDir, "summary.md"),
          retro_proposals_path: resolve(retroDir, "proposals.json"),
          experience_path: EXPERIENCE_PATH,
        });

        const dbaResult = await runAgent(dbaConfig.name, {
          systemPrompt: dbaSystemPrompt,
          userPrompt: dbaStageBPrompt,
          allowedTools: dbaConfig.allowedTools,
          model: getAgentModel("dba", args),
          configName: "dba",
          cwd: runDir,
          useSkills: args.useSkills,
          domainSkillsPrompt: dbaConfig.domainSkillsPrompt,
        });
        recordAgentTelemetry("phase3", "dba_stage_b", dbaResult.durationMs, dbaResult.tokens, dbaResult.costUsd);
        if (dbaResult.error) {
          console.error(`[Orchestrator] DBA Stage B failed (non-fatal): ${dbaResult.error}`);
        } else {
          console.log("[Orchestrator] DBA Stage B (Retrospective) completed.");
        }
      } catch (err) {
        console.error(`[Orchestrator] DBA Stage B unexpected error (non-fatal): ${err.message}`);
      }
    } else {
      console.log("\n[Orchestrator] ========== PHASE 3: DBA RETROSPECTIVE SKIPPED (--no-dba) ==========\n");
    }

    // Finalize on success
    await updateRunMeta(runDir, (meta) => {
      meta.status = "completed";
      meta.completedAt = new Date().toISOString();
    });
  } catch (err) {
    pipelineError = err;
    console.error(`\n[Orchestrator] Pipeline failed: ${err.message}`);
    try {
      await updateRunMeta(runDir, (meta) => {
        meta.status = "failed";
        meta.completedAt = new Date().toISOString();
        meta.error = err.message;
      });
    } catch {}
  }

  // Always write telemetry (even on failure — cost data is always preserved)
  await updateLatestSymlink(workload, runId);

  const parsedQueries = parseQueryFile(queries);
  await printPerQuerySummary(runDir, parsedQueries);

  telemetryData.total_wall_clock_ms = Date.now() - runStartTime;
  telemetryData.status = pipelineError ? "failed" : "completed";
  if (pipelineError) telemetryData.error = pipelineError.message;
  const telemetryPath = resolve(runDir, "telemetry.json");
  await writeFile(telemetryPath, JSON.stringify(telemetryData, null, 2));

  // Print cost summary
  console.log(`\n[Orchestrator] === Run Summary ===`);
  if (pipelineError) {
    console.log(`[Orchestrator] Status: FAILED — ${pipelineError.message}`);
  }
  console.log(`[Orchestrator] Total time: ${formatDuration(telemetryData.total_wall_clock_ms)}`);
  console.log(`[Orchestrator] Total tokens: ${Math.round(telemetryData.total_tokens.input / 1000)}K input, ${Math.round(telemetryData.total_tokens.output / 1000)}K output`);
  if (telemetryData.total_tokens.cache_read > 0) {
    console.log(`[Orchestrator] Cache tokens: ${Math.round(telemetryData.total_tokens.cache_read / 1000)}K cache_read, ${Math.round(telemetryData.total_tokens.cache_creation / 1000)}K cache_creation`);
  }
  console.log(`[Orchestrator] Cost: $${telemetryData.total_cost_usd.toFixed(2)}`);

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

  console.log(`\n[Orchestrator] Pipeline ${pipelineError ? 'failed' : 'complete'}.`);
  console.log(`[Orchestrator] Run Dir:  ${runDir}`);
  console.log(`[Orchestrator] Latest symlink: output/${workload}/latest → ${runId}`);

  if (pipelineError) process.exit(1);
}

main().catch((err) => {
  console.error("[Orchestrator] Fatal error:", err.message);
  process.exit(1);
});
