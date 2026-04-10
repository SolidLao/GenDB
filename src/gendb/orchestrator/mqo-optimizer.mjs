/**
 * MQO Optimizer (Strategy ε) — profile-directed, scope-aware, single loop.
 *
 * Per iteration:
 *   1. Snapshot current source tree for rollback
 *   2. Build + measure (./mqo --all) → profile.json
 *   3. If baseline: establish baseline, continue. Otherwise compute delta vs baseline.
 *   4. Invoke MQO Optimizer agent with profile + hotspots to produce an edit
 *   5. Build again; re-measure; re-validate scoped to the declared edit scope
 *   6. Accept if: correctness preserved AND batch_total improved ≥ min_improvement
 *                 AND no per-query regression > regression_slack
 *   7. Otherwise rollback from the snapshot
 *
 * Stop when max_iterations reached or stall_threshold hit.
 */

import { readFile, writeFile, mkdir, readdir } from "fs/promises";
import { existsSync } from "fs";
import { resolve } from "path";
import { runAgent, renderTemplate, readJSON } from "../shared.mjs";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { BENCHMARKS_DIR } from "../config.mjs";
import { config as mqoOptimizerConfig } from "../agents/mqo-mode/mqo-optimizer/index.mjs";
import {
  getMqoDir,
  getMqoIterDir,
  getMqoIterSnapshotDir,
  getMqoBlueprintPath,
  getMqoSkeletonPath,
  getMqoOptimizationHistoryPath,
} from "../utils/paths.mjs";
import {
  buildMqoArtifact,
  runMqoAll,
  runMqoQuery,
  validateMqoResults,
  loadLatestProfile,
} from "./mqo-build.mjs";
import { snapshotDir, restoreSnapshot } from "./measure.mjs";

function getAgentModel(configName, args) {
  if (args.modelOverride) return args.modelOverride;
  const providerCfg = getProviderConfig(args.agentProvider);
  return providerCfg.agentModels[configName] || args.model || providerCfg.model;
}

function getAgentTimeout(configName) {
  return defaults.agentTimeoutOverrides?.[configName] || defaults.agentTimeoutMs;
}

/**
 * Build hotspots section (ranked regions) from the profile for the LLM prompt.
 * Uses dispatcher_total as denominator for percentages when available.
 */
function renderHotspots(profile, topK = 15) {
  if (!profile || !profile.regions) return "_(profile not available)_";
  const dispatcherTotal = profile.regions?.dispatcher_total?.total_ms;
  const rows = Object.entries(profile.regions)
    .filter(([name]) => name !== "dispatcher_total" && name !== "finalize_all")
    .map(([name, s]) => ({
      name,
      kind: s.kind,
      total_ms: s.total_ms,
    }));
  rows.sort((a, b) => b.total_ms - a.total_ms);
  const denominator = dispatcherTotal || rows.reduce((acc, r) => acc + r.total_ms, 0) || 1;
  return rows.slice(0, topK).map((r, i) => {
    const pct = ((r.total_ms / denominator) * 100).toFixed(1);
    return `${i + 1}. **${r.name}** (${r.kind}) — ${r.total_ms.toFixed(1)} ms (${pct}% of batch)`;
  }).join("\n");
}

/**
 * Get dispatcher_total from profile, or fall back to sum of all regions.
 */
function getDispatcherTotalMs(profile) {
  if (profile?.regions?.dispatcher_total?.total_ms) {
    return profile.regions.dispatcher_total.total_ms;
  }
  // Fallback: sum all non-nested regions
  if (!profile?.regions) return null;
  return Object.values(profile.regions).reduce((acc, s) => acc + s.total_ms, 0);
}

/**
 * Format optimization history as a readable summary for the LLM.
 * Mirrors the SQO optimizer's history_summary format.
 */
function renderHistorySummary(history) {
  if (!history || !history.iterations || history.iterations.length === 0) {
    return "_(no prior iterations)_";
  }
  return history.iterations.map((it) => {
    const timeStr = it.batch_total_ms != null ? `${Math.round(it.batch_total_ms)}ms` : "N/A";
    const status = it.status || (it.accepted ? "accepted" : "baseline");
    let line = `Iter ${it.iteration}: ${timeStr} ${status.toUpperCase()}`;
    if (it.edit_scope) line += ` scope=${it.edit_scope}`;
    if (it.edit_summary) line += ` — ${it.edit_summary}`;
    if (it.improvement_ratio != null) {
      const pct = (it.improvement_ratio * 100).toFixed(1);
      line += ` (${it.improvement_ratio >= 0 ? "+" : ""}${pct}%)`;
    }
    if (it.per_query_regression) line += " [PER-QUERY REGRESSION]";
    return line;
  }).join("\n");
}

/**
 * Build benchmark context for MQO batch-total comparison across systems.
 */
function formatMqoBenchmarkContext(benchmarkResults, queries, currentBatchMs) {
  if (!benchmarkResults) return "";
  const queryIds = queries.map((q) => q.id);
  const rows = [];
  for (const [system, data] of Object.entries(benchmarkResults)) {
    let sum = 0;
    let count = 0;
    for (const qid of queryIds) {
      const qData = data?.[qid] || data?.queries?.[qid];
      if (!qData) continue;
      const time = qData.min_ms || qData.average_ms || qData.time_ms;
      if (time != null) { sum += time; count++; }
    }
    if (count > 0) rows.push({ system, sum, count });
  }
  if (rows.length === 0) return "";
  rows.sort((a, b) => a.sum - b.sum);
  const lines = [
    "## Performance Comparison (sum of per-query times — MQO should beat these)",
    "| System | Sum of Query Times (ms) | Note |",
    "|--------|------------------------|------|",
    ...rows.map((r) =>
      `| ${r.system} | ${Math.round(r.sum)} | ${r.count < queryIds.length ? `(${r.count}/${queryIds.length} queries)` : ""} |`
    ),
  ];
  if (currentBatchMs != null) {
    lines.push(`| **Current MQO** | **${Math.round(currentBatchMs)}** | fused batch total |`);
  }
  lines.push("", `Target: Beat ${rows[0].system} (${Math.round(rows[0].sum)} ms) with fused execution.`);
  return lines.join("\n");
}

/**
 * Extract per-query finalize time from profile.
 * Matches regions with kind "tail" AND kind "phase" with *_finalize naming.
 */
function perQueryFromProfile(profile) {
  const out = {};
  if (!profile?.regions) return out;
  for (const [name, s] of Object.entries(profile.regions)) {
    // Match kind="tail" with Qn_finalize or Qn_tail naming
    if (s.kind === "tail" || s.kind === "phase") {
      const m = name.match(/^(Q\d+)_(?:finalize|tail)$/);
      if (m) out[m[1]] = s.total_ms;
    }
  }
  return out;
}

export async function runMqoOptimizer(opts) {
  const { args, mqoDir: mqoDirFromOpts, blueprint, skeleton, queries, recordTelemetry } = opts;
  const mqoDir = mqoDirFromOpts || getMqoDir(args.runAuditDir);
  const maxIter = args.mqoMaxIterations ?? defaults.mqo?.optimizer?.maxIterations ?? 5;
  const stallCap = args.mqoStallThreshold ?? defaults.mqo?.optimizer?.stallThreshold ?? 5;
  const minImprovement = args.mqoMinImprovement ?? defaults.mqo?.optimizer?.minImprovement ?? 0.02;
  const regressionSlack = args.mqoRegressionSlack ?? defaults.mqo?.optimizer?.regressionSlack ?? 0.05;

  const groundTruthDir = resolve(BENCHMARKS_DIR, args.targetBenchmark, "query_results");
  if (!existsSync(groundTruthDir)) {
    console.warn(`[MQO Optimizer] No ground truth at ${groundTruthDir}; skipping optimizer loop.`);
    return { status: "skipped" };
  }

  // Load hardware config from storage design
  const storageDesignPath = resolve(args.runAuditDir, "..", "..", "storage_design.json");
  let hw = {};
  try {
    const sd = await readJSON(storageDesignPath);
    hw = sd?.hardware_config || {};
  } catch {}

  // Load benchmark results for context
  let benchmarkResults = args.benchmarkResults || null;
  if (!benchmarkResults) {
    try {
      const benchPath = resolve(BENCHMARKS_DIR, args.targetBenchmark, "results",
        `sf${args.scaleFactor}`, "metrics", "benchmark_results.json");
      benchmarkResults = await readJSON(benchPath);
    } catch {}
  }

  const historyPath = getMqoOptimizationHistoryPath(args.runAuditDir);
  const history = { iterations: [], best: null };

  // --- Establish baseline via iter_0 measurement ---
  console.log(`[MQO Optimizer] Establishing baseline...`);
  const iter0Dir = getMqoIterDir(args.runAuditDir, 0);
  await mkdir(iter0Dir, { recursive: true });

  let baseline = await runMqoAll(args, iter0Dir);
  if (baseline.status !== "pass") {
    throw new Error(`Baseline ./mqo --all failed: ${(baseline.stderr || "").slice(0, 500)}`);
  }
  let profile = await loadLatestProfile(args);
  const baselineBatchMs = baseline.durationMs;
  const baselineBatchTotalMs = profile?.regions?.dispatcher_total?.total_ms ?? baselineBatchMs;
  const baselinePerQuery = perQueryFromProfile(profile);

  const baselineValid = await validateMqoResults(baseline.resultsDir, groundTruthDir);
  if (baselineValid.status !== "pass") {
    console.warn(
      `[MQO Optimizer] Baseline validation failed:\n${(baselineValid.stdout || baselineValid.stderr || "").slice(0, 1500)}`,
    );
    console.warn(`[MQO Optimizer] Continuing optimizer loop; must be fixed before acceptance.`);
  }

  history.iterations.push({
    iteration: 0,
    batch_total_ms: baselineBatchTotalMs,
    per_query_ms: baselinePerQuery,
    validated: baselineValid.status === "pass",
    edit_scope: null,
    accepted: true,
  });
  history.best = { iteration: 0, batch_total_ms: baselineBatchTotalMs };
  await writeFile(historyPath, JSON.stringify(history, null, 2));

  let bestBatchMs = baselineBatchTotalMs;
  let bestIteration = 0;
  let stallCount = 0;

  for (let iter = 1; iter <= maxIter; iter++) {
    if (stallCount >= stallCap) {
      console.log(`[MQO Optimizer] Stalled (${stallCount} consecutive non-improving iterations). Stopping.`);
      break;
    }
    console.log(`\n[MQO Optimizer] ======== Iteration ${iter} / ${maxIter} ========`);
    const iterDir = getMqoIterDir(args.runAuditDir, iter);
    await mkdir(iterDir, { recursive: true });

    // --- Snapshot for rollback ---
    const snapshotDst = getMqoIterSnapshotDir(args.runAuditDir, iter);
    await snapshotDir(resolve(mqoDir, "stages"), resolve(snapshotDst, "stages"));
    const mainSnapshotPath = resolve(snapshotDst, "mqo_main.cpp");
    await mkdir(snapshotDst, { recursive: true });
    const { copyFile } = await import("fs/promises");
    await copyFile(resolve(mqoDir, "mqo_main.cpp"), mainSnapshotPath);

    // --- Invoke MQO Optimizer agent ---
    const systemPrompt = await readFile(mqoOptimizerConfig.promptPath, "utf-8");
    const userTemplate = await readFile(mqoOptimizerConfig.userPromptPath, "utf-8");
    const editScopePath = resolve(iterDir, "edit_scope.json");
    const editRationalePath = resolve(iterDir, "edit_rationale.md");

    const dispatcherTotalMs = getDispatcherTotalMs(profile);

    const userPrompt = renderTemplate(userTemplate, {
      iteration: iter,
      baseline_batch_ms: bestBatchMs.toFixed(1),
      best_batch_ms: bestBatchMs.toFixed(1),
      best_iteration: bestIteration,
      stall_count: stallCount,
      stall_threshold: stallCap,
      profile_path: resolve(mqoDir, "profile.json"),
      hotspots_section: renderHotspots(profile),
      dispatcher_total_ms: dispatcherTotalMs != null ? dispatcherTotalMs.toFixed(1) : "N/A",
      benchmark_context: formatMqoBenchmarkContext(benchmarkResults, queries, bestBatchMs),
      history_summary: renderHistorySummary(history),
      cpu_cores: hw.cpu_cores || "unknown",
      l3_cache_mb: hw.l3_cache_mb || "unknown",
      total_memory_gb: hw.total_memory_gb || "unknown",
      mqo_dir: mqoDir,
      blueprint_path: getMqoBlueprintPath(args.runAuditDir),
      skeleton_path: getMqoSkeletonPath(args.runAuditDir),
      edit_scope_path: editScopePath,
      edit_rationale_path: editRationalePath,
      min_improvement: minImprovement,
      regression_slack: regressionSlack,
    });

    const agentResult = await runAgent(mqoOptimizerConfig.name, {
      systemPrompt,
      userPrompt,
      allowedTools: mqoOptimizerConfig.allowedTools,
      model: getAgentModel("mqo_optimizer", args),
      configName: "mqo_optimizer",
      cwd: mqoDir,
      timeoutMs: getAgentTimeout("mqo_optimizer"),
      useSkills: false,
    });

    // Record telemetry for this optimizer iteration
    if (recordTelemetry) {
      recordTelemetry("phase2_mqo", `mqo_optimizer_iter${iter}`,
        agentResult.durationMs || 0, agentResult.tokens || { input: 0, output: 0 }, agentResult.costUsd || 0);
    }

    if (agentResult.error) {
      console.error(`[MQO Optimizer] Agent failed: ${agentResult.error}`);
      await restoreSnapshot(resolve(snapshotDst, "stages"), resolve(mqoDir, "stages"));
      await copyFile(mainSnapshotPath, resolve(mqoDir, "mqo_main.cpp"));
      stallCount++;
      history.iterations.push({
        iteration: iter, status: "agent_error",
        edit_scope: "none", edit_summary: (agentResult.error || "").slice(0, 200),
      });
      await writeFile(historyPath, JSON.stringify(history, null, 2));
      continue;
    }

    // --- Read edit scope ---
    let editScope = null;
    try {
      editScope = await readJSON(editScopePath);
    } catch {
      console.warn(`[MQO Optimizer] No edit_scope.json written; assuming 'none'.`);
      editScope = { scope: "none", rationale: "not provided" };
    }
    if (!editScope?.scope || editScope.scope === "none") {
      console.log(`[MQO Optimizer] Agent declared no edit this iteration. Stopping loop.`);
      break;
    }
    console.log(`[MQO Optimizer] Edit scope: ${editScope.scope} — ${editScope.edit_summary || ""}`);

    // --- Rebuild ---
    const build = await buildMqoArtifact(args);
    if (build.status !== "pass") {
      console.error(`[MQO Optimizer] Build failed after agent edit. Rolling back.`);
      await restoreSnapshot(resolve(snapshotDst, "stages"), resolve(mqoDir, "stages"));
      await copyFile(mainSnapshotPath, resolve(mqoDir, "mqo_main.cpp"));
      history.iterations.push({
        iteration: iter, status: "build_failed",
        edit_scope: editScope.scope, edit_summary: editScope.edit_summary || "",
      });
      await writeFile(historyPath, JSON.stringify(history, null, 2));
      stallCount++;
      continue;
    }

    // --- Re-measure ---
    const run = await runMqoAll(args, iterDir);
    profile = await loadLatestProfile(args);
    const batchTotalMs = profile?.regions?.dispatcher_total?.total_ms ?? run.durationMs;
    const perQuery = perQueryFromProfile(profile);

    // --- Re-validate (all queries) ---
    const valid = await validateMqoResults(run.resultsDir, groundTruthDir);
    const validated = valid.status === "pass";
    if (!validated) {
      console.error(`[MQO Optimizer] Correctness regression — rolling back.`);
      await restoreSnapshot(resolve(snapshotDst, "stages"), resolve(mqoDir, "stages"));
      await copyFile(mainSnapshotPath, resolve(mqoDir, "mqo_main.cpp"));
      history.iterations.push({
        iteration: iter, status: "correctness_regression",
        edit_scope: editScope.scope, edit_summary: editScope.edit_summary || "",
        batch_total_ms: batchTotalMs,
      });
      await writeFile(historyPath, JSON.stringify(history, null, 2));
      stallCount++;
      continue;
    }

    // --- Acceptance check ---
    const improvementRatio = (bestBatchMs - batchTotalMs) / bestBatchMs;
    const accepted =
      validated &&
      (improvementRatio >= minImprovement ||
        (editScope.scope.startsWith("finalize:") && improvementRatio >= 0));

    console.log(
      `[MQO Optimizer] Iter ${iter}: batch_total ${batchTotalMs.toFixed(1)} ms ` +
      `(baseline best ${bestBatchMs.toFixed(1)}, delta ${(improvementRatio * 100).toFixed(2)}%), ` +
      `scope=${editScope.scope}, accepted=${accepted}`,
    );

    history.iterations.push({
      iteration: iter,
      status: accepted ? "accepted" : "rejected",
      edit_scope: editScope.scope,
      edit_summary: editScope.edit_summary || "",
      batch_total_ms: batchTotalMs,
      improvement_ratio: improvementRatio,
      per_query_ms: perQuery,
      validated,
    });
    await writeFile(historyPath, JSON.stringify(history, null, 2));

    if (accepted) {
      bestBatchMs = batchTotalMs;
      bestIteration = iter;
      stallCount = 0;
      history.best = { iteration: iter, batch_total_ms: batchTotalMs };
      await writeFile(historyPath, JSON.stringify(history, null, 2));
    } else {
      await restoreSnapshot(resolve(snapshotDst, "stages"), resolve(mqoDir, "stages"));
      await copyFile(mainSnapshotPath, resolve(mqoDir, "mqo_main.cpp"));
      stallCount++;
    }
  }

  console.log(
    `\n[MQO Optimizer] Done. Best: iter ${bestIteration}, batch_total ${bestBatchMs.toFixed(1)} ms.`,
  );
  return { status: "ok", bestIteration, bestBatchMs };
}
