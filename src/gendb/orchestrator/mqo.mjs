/**
 * MQO (Multiple Query Optimization) Phase 2 driver.
 *
 * Called from orchestrator.mjs when `--optimization-mode mqo` is set, after
 * Phase 1 (Workload Analyzer + Storage/Index Designer) has produced the
 * run-wide workload_analysis.json and storage_design.json.
 *
 * Pipeline (see plan file §4 for full design):
 *   2.1  MQO Analyzer       (adaptive: small-batch single agent OR
 *                            large-batch Surveyor → parallel Clusters → Integrator)
 *                           → shared_component_blueprint.json
 *   2.2  Global Skeleton Planner
 *                           → batch_skeleton.json
 *   2.3  Per-query Tail Planners (parallel; reuse Query Planner with mqoMode)
 *                           → queries/qN_plan.json
 *   2.4  Batch Code Generator
 *                           → shared/*.hpp + mqo_main.cpp + manifest.json + Makefile
 *   2.5  Per-query Code Generator (parallel; reuse Code Generator with mqoMode)
 *                           → queries/qN.cpp
 *   2.6  MQO Optimizer (Strategy ε — profile-directed, scope-aware, single loop)
 *
 * This file is intentionally a thin orchestrator: each stage delegates to a
 * helper module. The stages land incrementally (per the plan's Stage
 * checklist). In the scaffold phase this function only creates the MQO
 * output dir and logs that each stage is not yet implemented.
 */

import { mkdir, writeFile, readFile } from "fs/promises";
import { existsSync } from "fs";
import { resolve } from "path";
import { defaults } from "../gendb.config.mjs";
import { parseQueryFile } from "../shared.mjs";

// MQO spawns many sequential agents; each adds exit listeners via the SDK.
// Raise the limit to avoid spurious warnings.
process.setMaxListeners(Math.max(process.getMaxListeners(), 30));
import {
  getMqoDir,
  getMqoSharedDir,
  getMqoBlueprintPath,
  getMqoSkeletonPath,
  getMqoManifestPath,
} from "../utils/paths.mjs";
import { BENCHMARKS_DIR } from "../config.mjs";
import { listQueries } from "../tools/mqo-tools.mjs";

// ---------------------------------------------------------------------------
// Public entry point — called from orchestrator.mjs Phase 1 → Phase 2 branch
// ---------------------------------------------------------------------------

/**
 * Drive the MQO Phase 2 pipeline end-to-end.
 *
 * @param {object} args            — parsed CLI args (parseArgs result), including:
 *                                    optimizationMode, mqoAnalyzerThreshold, mqo* knobs,
 *                                    queries, schema, targetBenchmark, scaleFactor, runAuditDir
 * @param {string} runDir          — persistent workload dir (output/<benchmark>-sf<N>/)
 * @param {string} workloadAnalysisPath
 * @param {string} storageDesignPath
 * @param {function} recordTelemetry  — callback(phase, agentName, durationMs, tokens, costUsd) from orchestrator
 * @returns {Promise<{ status: string, mqoDir: string }>}
 */
export async function runMqoPhase2(args, runDir, workloadAnalysisPath, storageDesignPath, recordTelemetry) {
  // Telemetry helper — record agent cost into the orchestrator's telemetry
  const record = (agentName, result) => {
    if (recordTelemetry && result) {
      recordTelemetry("phase2_mqo", agentName, result.durationMs || 0, result.tokens || { input: 0, output: 0 }, result.costUsd || 0);
    }
  };
  const mqoDir = getMqoDir(args.runAuditDir);
  const sharedDir = getMqoSharedDir(args.runAuditDir);
  await mkdir(mqoDir, { recursive: true });
  await mkdir(sharedDir, { recursive: true });

  // --- Banner ---
  console.log(`\n[Orchestrator] ========== PHASE 2: MQO (MULTIPLE QUERY OPTIMIZATION) ==========`);
  console.log(`[MQO] Artifact dir:          ${mqoDir}`);
  console.log(`[MQO] Workload analysis:     ${workloadAnalysisPath}`);
  console.log(`[MQO] Storage design:        ${storageDesignPath}`);
  console.log(`[MQO] Analyzer threshold:    ${args.mqoAnalyzerThreshold}`);
  console.log(`[MQO] Max iterations:        ${args.mqoMaxIterations}`);
  console.log(`[MQO] Stall threshold:       ${args.mqoStallThreshold}`);
  console.log(`[MQO] Min improvement:       ${args.mqoMinImprovement}`);
  console.log(`[MQO] Regression slack:      ${args.mqoRegressionSlack}`);

  // --- Load query batch ---
  const queriesSource = await readFile(args.queries, "utf-8");
  const parsedQueries = parseQueryFile(queriesSource);
  console.log(`[MQO] Query batch size:      ${parsedQueries.length}`);

  // Pre-compute a structural summary of the batch. This is used by both the
  // small-batch and large-batch analyzer paths — we always want the LLM to
  // see the concrete structural facts, not just the raw SQL.
  let batchSummary;
  try {
    batchSummary = listQueries(args.queries);
  } catch (err) {
    console.warn(`[MQO] mqo-tools batch summary failed (non-fatal): ${err.message}`);
    batchSummary = null;
  }

  // --- Route small-batch vs large-batch ---
  const useLargeBatchPath = parsedQueries.length > (args.mqoAnalyzerThreshold ?? 30);
  const analyzerMode = useLargeBatchPath ? "large-batch" : "small-batch";
  console.log(`[MQO] Analyzer path:         ${analyzerMode} (threshold=${args.mqoAnalyzerThreshold})`);

  // ===========================================================================
  // 2.1 MQO Analyzer
  // ===========================================================================
  console.log(`\n[MQO] === Step 2.1: MQO Analyzer (${analyzerMode}) ===`);
  let blueprint = null;
  try {
    if (useLargeBatchPath) {
      // Lazy import — large-batch path lands in Stage 10
      const mod = await import("./mqo-analyzer-large.mjs").catch(() => null);
      if (!mod || !mod.runLargeBatchMqoAnalyzer) {
        console.warn(`[MQO] Large-batch path not yet wired (Stage 10). Falling back to small-batch.`);
        const smallMod = await import("./mqo-analyzer-small.mjs");
        blueprint = await smallMod.runSmallBatchMqoAnalyzer({
          args, mqoDir, workloadAnalysisPath, storageDesignPath,
          queries: parsedQueries, batchSummary,
        });
      } else {
        blueprint = await mod.runLargeBatchMqoAnalyzer({
          args, mqoDir, workloadAnalysisPath, storageDesignPath,
          queries: parsedQueries, batchSummary,
        });
      }
    } else {
      const smallMod = await import("./mqo-analyzer-small.mjs").catch(() => null);
      if (!smallMod || !smallMod.runSmallBatchMqoAnalyzer) {
        console.warn(`[MQO] Small-batch analyzer not yet wired (Stage 3). Writing stub blueprint.`);
        blueprint = stubBlueprint(parsedQueries);
        await writeFile(getMqoBlueprintPath(args.runAuditDir), JSON.stringify(blueprint, null, 2));
      } else {
        blueprint = await smallMod.runSmallBatchMqoAnalyzer({
          args, mqoDir, workloadAnalysisPath, storageDesignPath,
          queries: parsedQueries, batchSummary,
        });
      }
    }
    if (blueprint?._agentMeta) record("mqo_analyzer", blueprint._agentMeta);
  } catch (err) {
    console.error(`[MQO] MQO Analyzer failed: ${err.message}`);
    throw err;
  }

  // ===========================================================================
  // 2.2 Global Skeleton Planner
  // ===========================================================================
  console.log(`\n[MQO] === Step 2.2: Global Skeleton Planner ===`);
  let skeleton = null;
  try {
    const mod = await import("./mqo-skeleton-planner.mjs").catch(() => null);
    if (!mod || !mod.runGlobalSkeletonPlanner) {
      console.warn(`[MQO] Global Skeleton Planner not yet wired (Stage 4). Writing stub skeleton.`);
      skeleton = stubSkeleton(blueprint, parsedQueries);
      await writeFile(getMqoSkeletonPath(args.runAuditDir), JSON.stringify(skeleton, null, 2));
    } else {
      skeleton = await mod.runGlobalSkeletonPlanner({
        args, mqoDir, workloadAnalysisPath, storageDesignPath,
        blueprint, queries: parsedQueries,
      });
    }
    if (skeleton?._agentMeta) record("mqo_skeleton_planner", skeleton._agentMeta);
  } catch (err) {
    console.error(`[MQO] Global Skeleton Planner failed: ${err.message}`);
    throw err;
  }

  // ===========================================================================
  // 2.3 Batch Code Generator (fused execution — produces ALL code)
  // ===========================================================================
  console.log(`\n[MQO] === Step 2.3: Batch Code Generator (fused execution) ===`);
  try {
    const mod = await import("./mqo-batch-codegen.mjs").catch(() => null);
    if (!mod || !mod.runBatchCodeGenerator) {
      console.warn(`[MQO] Batch Code Generator not yet wired (Stage 6). Skipping.`);
    } else {
      const codegenResult = await mod.runBatchCodeGenerator({
        args, mqoDir, workloadAnalysisPath, storageDesignPath,
        blueprint, skeleton, queries: parsedQueries,
      });
      if (codegenResult?._agentMeta) record("mqo_batch_code_generator", codegenResult._agentMeta);
    }
  } catch (err) {
    console.error(`[MQO] Batch Code Generator failed: ${err.message}`);
    throw err;
  }

  // ===========================================================================
  // Build: compile the fused artifact with repair passes on failure.
  // This sits between codegen (2.3) and the ε optimizer (2.4) so that the
  // optimizer always starts from a known-good compiled baseline.
  // ===========================================================================
  console.log(`\n[MQO] === Build: compiling MQO artifact ===`);
  try {
    const { buildMqoArtifactWithRepair, runMqoAll, runMqoQuery, validateMqoResults } =
      await import("./mqo-build.mjs");
    const buildResult = await buildMqoArtifactWithRepair(args);
    if (buildResult.status !== "pass") {
      throw new Error(`MQO artifact failed to compile after repair attempts. See logs above.`);
    }

    // Smoke test — only runs if ground truth is available for this benchmark
    const groundTruthDir = resolve(
      BENCHMARKS_DIR, args.targetBenchmark, "query_results",
    );
    if (existsSync(groundTruthDir)) {
      // Create an iter_0 directory for baseline measurement
      const { getMqoIterDir } = await import("../utils/paths.mjs");
      const iter0Dir = getMqoIterDir(args.runAuditDir, 0);
      await mkdir(iter0Dir, { recursive: true });

      console.log(`[MQO] Smoke test: running ./mqo --all and validating...`);
      const allRun = await runMqoAll(args, iter0Dir);
      if (allRun.status === "pass") {
        const valid = await validateMqoResults(allRun.resultsDir, groundTruthDir);
        console.log(`[MQO] --all validation: ${valid.status}`);
        if (valid.status !== "pass") {
          console.warn(`[MQO] Validation details:\n${(valid.stdout || valid.stderr || "").slice(0, 1500)}`);
        }
      } else {
        console.error(`[MQO] ./mqo --all FAILED: ${(allRun.stderr || "").slice(0, 500)}`);
      }
    } else {
      console.warn(`[MQO] Ground truth dir not found (${groundTruthDir}); skipping smoke test.`);
    }
  } catch (err) {
    console.error(`[MQO] Build/smoke-test stage failed: ${err.message}`);
    throw err;
  }

  // ===========================================================================
  // 2.4 MQO Optimizer (Strategy ε)
  // ===========================================================================
  if ((args.mqoMaxIterations ?? 0) > 0) {
    console.log(`\n[MQO] === Step 2.4: MQO Optimizer (Strategy ε) ===`);
    try {
      const mod = await import("./mqo-optimizer.mjs").catch(() => null);
      if (!mod || !mod.runMqoOptimizer) {
        console.warn(`[MQO] MQO Optimizer not yet wired (Stage 9). Skipping.`);
      } else {
        await mod.runMqoOptimizer({
          args, mqoDir, workloadAnalysisPath, storageDesignPath,
          blueprint, skeleton, queries: parsedQueries,
          recordTelemetry,
        });
      }
    } catch (err) {
      console.error(`[MQO] MQO Optimizer failed: ${err.message}`);
      throw err;
    }
  } else {
    console.log(`\n[MQO] === Step 2.4: MQO Optimizer skipped (mqoMaxIterations=0) ===`);
  }

  // ===========================================================================
  // MQO Summary — print results table similar to SQO mode
  // ===========================================================================
  await printMqoSummary(args, mqoDir, parsedQueries);

  console.log(`\n[MQO] Phase 2 (MQO mode) complete. Artifact dir: ${mqoDir}`);
  return { status: "ok", mqoDir };
}

// ---------------------------------------------------------------------------
// MQO Summary — mirrors the SQO per-query results table
// ---------------------------------------------------------------------------

async function printMqoSummary(args, mqoDir, queries) {
  const { readJSON } = await import("../shared.mjs");
  const { BENCHMARKS_DIR } = await import("../config.mjs");

  console.log(`\n[MQO] ========== MQO RESULTS SUMMARY ==========\n`);

  // Load optimizer history
  const historyPath = resolve(mqoDir, "optimization_history.json");
  let history = null;
  try { history = await readJSON(historyPath); } catch {}

  // Load latest profile
  const profilePath = resolve(mqoDir, "profile.json");
  let profile = null;
  try { profile = await readJSON(profilePath); } catch {}

  // Load SQO baseline for comparison (if available)
  let sqoBaseline = null;
  try {
    const benchPath = resolve(BENCHMARKS_DIR, args.targetBenchmark, "results",
      `sf${args.scaleFactor}`, "metrics", "benchmark_results.json");
    const benchData = await readJSON(benchPath);
    sqoBaseline = benchData?.GenDB || null;
  } catch {}

  // Validate iter_0 results against ground truth
  const groundTruthDir = resolve(BENCHMARKS_DIR, args.targetBenchmark, "query_results");
  const iter0ResultsDir = resolve(mqoDir, "iter_0", "results", "all");
  let validationResult = null;
  if (existsSync(iter0ResultsDir) && existsSync(groundTruthDir)) {
    try {
      const { validateCsvAgainstGroundTruth } = await import("./measure.mjs");
      const raw = await validateCsvAgainstGroundTruth(iter0ResultsDir, groundTruthDir);
      validationResult = JSON.parse(raw.stdout || "{}");
    } catch {}
  }

  // --- Optimizer iterations table ---
  if (history && history.iterations && history.iterations.length > 0) {
    const best = history.best || {};
    console.log(`[MQO] Optimizer: ${history.iterations.length} iteration(s), best = iter ${best.iteration ?? 0} (${(best.batch_total_ms ?? 0).toFixed(0)} ms)\n`);

    const colW = 14;
    let header = "Iteration".padEnd(12) + "|" + "Batch Total".padStart(colW) + " |" +
      "Status".padStart(colW) + " |" + "Scope".padStart(20) + " |";
    console.log(header);
    console.log("-".repeat(header.length));

    for (const it of history.iterations) {
      const ms = it.batch_total_ms != null ? `${Math.round(it.batch_total_ms)}ms` : "-";
      const status = it.status || (it.iteration === 0 ? "baseline" : "?");
      const scope = it.edit_scope || "-";
      let row = `iter ${it.iteration}`.padEnd(12) + "|" +
        ms.padStart(colW) + " |" + status.padStart(colW) + " |" + scope.padStart(20) + " |";
      console.log(row);
    }
    console.log();
  }

  // --- Per-query timing from profile ---
  if (profile && profile.regions) {
    console.log(`[MQO] === Stage Breakdown ===\n`);
    const regions = Object.entries(profile.regions).sort((a, b) => b[1].total_ms - a[1].total_ms);
    const total = regions.reduce((s, [, r]) => s + r.total_ms, 0);

    for (const [name, r] of regions.slice(0, 12)) {
      const pct = ((r.total_ms / total) * 100).toFixed(1);
      console.log(`  ${name.padEnd(40)} ${r.total_ms.toFixed(1).padStart(10)} ms  (${pct.padStart(5)}%)`);
    }
    if (regions.length > 12) {
      const rest = regions.slice(12).reduce((s, [, r]) => s + r.total_ms, 0);
      console.log(`  ${"(other)".padEnd(40)} ${rest.toFixed(1).padStart(10)} ms`);
    }
    console.log();
  }

  // --- Per-query validation ---
  let passCount = 0;
  const failedQueries = [];

  const sortedQueries = [...queries].sort((a, b) => {
    const na = parseInt(a.id.slice(1)), nb = parseInt(b.id.slice(1));
    return na - nb;
  });

  for (const q of sortedQueries) {
    let valid = "?";
    if (validationResult?.queries?.[q.id]) {
      valid = validationResult.queries[q.id].match ? "PASS" : "FAIL";
    } else if (validationResult?.match === true) {
      valid = "PASS";
    }
    if (valid === "PASS") passCount++;
    else failedQueries.push(q.id);
  }

  console.log(`[MQO] === Validation ===\n`);
  console.log(`[MQO] ${passCount}/${queries.length} queries correct`);
  if (failedQueries.length > 0) {
    console.log(`[MQO] Failed: ${failedQueries.join(", ")}`);
  }
  console.log();

  // --- Batch total comparison ---
  const batchMs = history?.best?.batch_total_ms;
  let sqoTotal = 0;
  if (sqoBaseline) {
    for (const q of sortedQueries) {
      const sqoMs = sqoBaseline[q.id]?.average_ms;
      if (sqoMs != null) sqoTotal += sqoMs;
    }
  }

  console.log(`[MQO] === Performance ===\n`);
  if (batchMs != null) {
    console.log(`[MQO] MQO batch total (fused):    ${Math.round(batchMs)} ms  (${queries.length} queries, single process)`);
  }
  if (sqoTotal > 0) {
    console.log(`[MQO] SQO sum (per-query best):   ${Math.round(sqoTotal)} ms  (${queries.length} queries, individual binaries)`);
    if (batchMs != null) {
      const ratio = batchMs / sqoTotal;
      const label = ratio < 1 ? `${(1 / ratio).toFixed(2)}x faster` : `${ratio.toFixed(2)}x slower`;
      console.log(`[MQO] MQO vs SQO:                ${label}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Minimal stub helpers used before real agents land (Stages 3/4).
// These let Stage 2 (scaffold) produce a valid end-to-end directory layout
// without any LLM calls — useful for testing the wiring.
// ---------------------------------------------------------------------------

function stubBlueprint(queries) {
  return {
    version: "0.1-stub",
    stub: true,
    generated_by: "mqo.mjs stub (pre-Stage-3)",
    shared_components: [],
    per_query_dependencies: Object.fromEntries(queries.map((q) => [q.id, []])),
  };
}

function stubSkeleton(blueprint, queries) {
  return {
    version: "2.0-stub",
    stub: true,
    generated_by: "mqo.mjs stub (pre-skeleton-planner)",
    execution_model: "fused",
    stages: [
      {
        stage_id: "finalize_output",
        kind: "finalize",
        description: "Per-query finalization (stub — no fused scans)",
        operations: queries.map((q) => ({ query: q.id, action: "full standalone execution + output" })),
      },
    ],
    query_stage_mask: Object.fromEntries(
      queries.map((q) => [q.id, { stages: ["finalize_output"], hash_deps: [] }]),
    ),
  };
}
