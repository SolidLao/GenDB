/**
 * MQO build + smoke-test helper (Stage 8 of the plan).
 *
 * After Stages 3–7 run, we have shared/*.hpp, queries/*.cpp, mqo_main.cpp,
 * manifest.json, and a Makefile in <runAuditDir>/mqo/. This module:
 *   1. Invokes `make` (with a bounded retry that feeds compile errors back
 *      to the Batch Code Generator for one repair pass)
 *   2. Runs `./mqo --all` and `./mqo --query Qi` for each query
 *   3. Validates per-query CSV output against ground truth
 *   4. Reads profile.json into a structured object
 *
 * The optimizer (Stage 9) will call these helpers on every iteration.
 */

import { readFile, writeFile, mkdir, cp, rm } from "fs/promises";
import { existsSync, rmSync } from "fs";
import { resolve } from "path";
import {
  runMake,
  runBinary,
  validateCsvAgainstGroundTruth,
  readProfileJson,
  parseTimingLines,
} from "./measure.mjs";
import {
  getMqoDir,
  getMqoBinaryPath,
  getMqoIterDir,
} from "../utils/paths.mjs";
import { defaults } from "../gendb.config.mjs";

/**
 * Compile the MQO artifact. Returns { status, stdout, stderr }. On failure,
 * the caller may invoke repairBatchCodeGenerator() to retry.
 */
export async function buildMqoArtifact(args) {
  const mqoDir = getMqoDir(args.runAuditDir);
  console.log(`[MQO] Building artifact: make -C ${mqoDir}`);
  const result = await runMake(mqoDir, {
    timeoutMs: 10 * 60 * 1000,  // 10 minutes
    jobs: Math.max(1, Math.min(8, args.maxConcurrent || 4)),
  });
  if (result.status === "pass") {
    console.log(`[MQO] Build succeeded.`);
  } else {
    console.error(`[MQO] Build FAILED:\n${(result.stderr || "").slice(0, 2000)}`);
  }
  return result;
}

/**
 * One-shot build + repair. If the first build fails, invoke the Batch Code
 * Generator again with the compile errors and try once more.
 */
export async function buildMqoArtifactWithRepair(args) {
  let result = await buildMqoArtifact(args);
  if (result.status === "pass") return result;

  const retryCap = defaults.mqo?.optimizer?.compilationRetryCap ?? 1;
  for (let attempt = 1; attempt <= retryCap; attempt++) {
    console.log(`[MQO] Build failed; requesting repair pass ${attempt}/${retryCap}...`);
    try {
      const { repairBatchCodeGeneratorOutput } = await import("./mqo-repair.mjs").catch(() => ({}));
      if (!repairBatchCodeGeneratorOutput) {
        console.warn(`[MQO] Repair helper not available; aborting.`);
        return result;
      }
      await repairBatchCodeGeneratorOutput({
        args,
        buildStdout: result.stdout,
        buildStderr: result.stderr,
      });
    } catch (err) {
      console.error(`[MQO] Repair call failed: ${err.message}`);
      return result;
    }
    result = await buildMqoArtifact(args);
    if (result.status === "pass") return result;
  }
  return result;
}

/**
 * Execute ./mqo --all into <iterDir>/results/all/.
 * Returns { status, stdout, stderr, durationMs, timings }.
 */
export async function runMqoAll(args, iterDir) {
  const binaryPath = getMqoBinaryPath(args.runAuditDir);
  const resultsDir = resolve(iterDir, "results", "all");
  if (existsSync(resultsDir)) rmSync(resultsDir, { recursive: true, force: true });
  await mkdir(resultsDir, { recursive: true });

  // The MQO binary expects --gendb-dir to be the workload dir (parent of storage/).
  // args.gendbDir is the storage dir itself, so we pass its parent.
  const workloadDir = resolve(args.gendbDir, "..");

  console.log(`[MQO] Running ./mqo --gendb-dir ${workloadDir} --output-dir ${resultsDir} --all`);
  const run = await runBinary(binaryPath, [
    "--gendb-dir", workloadDir,
    "--output-dir", resultsDir,
    "--all",
  ], {
    cwd: resolve(binaryPath, ".."),
    timeoutMs: (defaults.queryExecutionTimeoutSec || 300) * 1000 * 5, // allow 5x since batch
  });
  const timings = parseTimingLines(run.stdout);
  return { ...run, resultsDir, timings };
}

/**
 * Execute ./mqo --query Qi for one query. Used in both smoke testing
 * (Stage 8) and the per-query probe that the ε optimizer does before each
 * iteration.
 */
export async function runMqoQuery(args, iterDir, queryId) {
  const binaryPath = getMqoBinaryPath(args.runAuditDir);
  const resultsDir = resolve(iterDir, "results", "single", queryId);
  if (existsSync(resultsDir)) rmSync(resultsDir, { recursive: true, force: true });
  await mkdir(resultsDir, { recursive: true });
  const workloadDir = resolve(args.gendbDir, "..");
  const run = await runBinary(binaryPath, [
    "--gendb-dir", workloadDir,
    "--output-dir", resultsDir,
    "--query", queryId,
  ], {
    cwd: resolve(binaryPath, ".."),
    timeoutMs: (defaults.queryExecutionTimeoutSec || 300) * 1000,
  });
  const timings = parseTimingLines(run.stdout);
  return { ...run, queryId, resultsDir, timings };
}

/**
 * Validate per-query CSV outputs in a results directory against a ground-truth
 * directory. Returns { status, details: { [queryId]: "pass"|"fail"|"missing" } }.
 *
 * NOTE: compare_results.py already handles directory-level comparison. We call
 * it once per results directory.
 */
export async function validateMqoResults(resultsDir, groundTruthDir, queryIds = null) {
  return validateCsvAgainstGroundTruth(resultsDir, groundTruthDir, { queryIds });
}

/**
 * Read the profile.json emitted by the last run (if any). Profile is written
 * to the dispatcher's CWD by the MQO_PROFILE_FLUSH call in mqo_main.cpp.
 */
export async function loadLatestProfile(args) {
  const mqoDir = getMqoDir(args.runAuditDir);
  // The generator may write profile.json relative to the binary's CWD, which
  // is mqoDir in our invocation. Try there first.
  const candidate = resolve(mqoDir, "profile.json");
  const profile = await readProfileJson(candidate);
  return profile;
}
