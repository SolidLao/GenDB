/**
 * Measurement + validation + rollback helpers shared by MQO Phase 2 and
 * (potentially) future refactors of the single-query Phase 2.
 *
 * This module intentionally stays self-contained: it does NOT import from
 * orchestrator.mjs to avoid circular deps. It duplicates a small amount of
 * plumbing (runProcess) on purpose — orchestrator.mjs keeps its own copy to
 * avoid destabilizing the mature single-query pipeline during MQO landing.
 *
 * Exported helpers:
 *   - compileCppFile(cppPath, binaryPath, {cwd, extraFlags, includeDirs, timeoutMs})
 *   - runBinary(binaryPath, argv, {cwd, timeoutMs, env})
 *   - validateCsvAgainstGroundTruth(resultsDir, groundTruthDir, {queryIds})
 *   - snapshotDir(srcDir, snapshotDir)   — deep copy for rollback
 *   - restoreSnapshot(snapshotDir, dstDir) — deep copy back
 *   - parseTimingLines(stdout) → {metric: ms}
 *   - readProfileJson(profilePath) → object or null
 */

import { spawn } from "child_process";
import { readFile, writeFile, mkdir, cp, rm } from "fs/promises";
import { existsSync, rmSync } from "fs";
import { resolve, dirname, basename } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const GENDB_DIR = resolve(__dirname, "..");
const COMPARE_TOOL_PATH = resolve(GENDB_DIR, "tools", "compare_results.py");
const UTILS_PATH = resolve(GENDB_DIR, "utils");

// ---------------------------------------------------------------------------
// Low-level process runner (self-contained copy)
// ---------------------------------------------------------------------------

export function runProcess(cmd, cmdArgs, opts = {}) {
  const timeoutVal = opts.timeout || opts.timeoutMs || 120000;
  return new Promise((resolveP, rejectP) => {
    const startTime = Date.now();
    let timedOut = false;
    const child = spawn(cmd, cmdArgs, {
      cwd: opts.cwd,
      detached: true,
      env: opts.env || process.env,
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
      try { process.kill(-child.pid, "SIGKILL"); } catch {}
      const durationMs = Date.now() - startTime;
      if (code === 0 && !timedOut) {
        resolveP({ stdout, stderr, durationMs });
      } else {
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
        err.durationMs = durationMs;
        err.exitCode = code;
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
// Compile a single C++ translation unit (used for single-file fallback)
// ---------------------------------------------------------------------------

export async function compileCppFile(cppPath, binaryPath, opts = {}) {
  const {
    cwd = dirname(cppPath),
    extraFlags = [],
    includeDirs = [UTILS_PATH],
    timeoutMs = 180000,
  } = opts;
  const args = [
    "-O3", "-march=native", "-std=c++17", "-Wall", "-lpthread", "-fopenmp",
    "-DGENDB_PROFILE",
    ...includeDirs.map((d) => `-I${d}`),
    ...extraFlags,
    "-o", binaryPath, cppPath,
  ];
  try {
    const { stdout, stderr } = await runProcess("g++", args, { cwd, timeoutMs });
    return { status: "pass", stdout, stderr };
  } catch (err) {
    return { status: "fail", stdout: err.stdout || "", stderr: err.stderr || err.message };
  }
}

// ---------------------------------------------------------------------------
// Invoke `make` in a directory (used by MQO for the multi-file artifact)
// ---------------------------------------------------------------------------

export async function runMake(mqoDir, { target = "", timeoutMs = 300000, jobs = 4 } = {}) {
  const args = ["-C", mqoDir, `-j${jobs}`];
  if (target) args.push(target);
  try {
    const { stdout, stderr } = await runProcess("make", args, { timeoutMs });
    return { status: "pass", stdout, stderr };
  } catch (err) {
    return { status: "fail", stdout: err.stdout || "", stderr: err.stderr || err.message };
  }
}

// ---------------------------------------------------------------------------
// Execute a binary with argv, capturing stdout/stderr and duration
// ---------------------------------------------------------------------------

export async function runBinary(binaryPath, argv = [], opts = {}) {
  const { cwd = dirname(binaryPath), timeoutMs = 300000, env } = opts;
  const start = Date.now();
  try {
    const { stdout, stderr, durationMs } = await runProcess(
      binaryPath, argv, { cwd, timeoutMs, env },
    );
    return { status: "pass", stdout, stderr, durationMs };
  } catch (err) {
    return {
      status: "fail",
      stdout: err.stdout || "",
      stderr: err.stderr || err.message,
      durationMs: Date.now() - start,
      timedOut: !!err.timedOut,
    };
  }
}

// ---------------------------------------------------------------------------
// Parse [TIMING] lines emitted by GenDB utility code
// Format: "[TIMING] <metric>: <ms> ms"
// ---------------------------------------------------------------------------

export function parseTimingLines(stdout) {
  const timings = {};
  if (!stdout) return timings;
  const regex = /\[TIMING\]\s+(\w+):\s+([\d.]+)\s*ms/g;
  let m;
  while ((m = regex.exec(stdout)) !== null) {
    timings[m[1]] = parseFloat(m[2]);
  }
  return timings;
}

export function deriveTimingMs(timings) {
  if (timings.total != null && timings.output != null) return timings.total - timings.output;
  if (timings.total != null) return timings.total;
  return null;
}

// ---------------------------------------------------------------------------
// Validate produced CSVs against ground-truth directory using compare_results.py
// Returns { status: "pass"|"fail", details: [{queryId, status, diff}], stdout, stderr }
// ---------------------------------------------------------------------------

export async function validateCsvAgainstGroundTruth(resultsDir, groundTruthDir, opts = {}) {
  const { queryIds = null, timeoutMs = 120000 } = opts;
  // compare_results.py takes two directories and compares matching CSVs.
  const args = [COMPARE_TOOL_PATH, groundTruthDir, resultsDir];
  if (queryIds && queryIds.length > 0) {
    args.push("--queries", queryIds.join(","));
  }
  try {
    const { stdout, stderr } = await runProcess("python3", args, { timeoutMs });
    return { status: "pass", stdout, stderr };
  } catch (err) {
    return {
      status: "fail",
      stdout: err.stdout || "",
      stderr: err.stderr || err.message,
    };
  }
}

// ---------------------------------------------------------------------------
// Snapshot helpers for rollback (used by MQO ε optimizer)
// ---------------------------------------------------------------------------

export async function snapshotDir(srcDir, snapshotDir) {
  if (existsSync(snapshotDir)) {
    rmSync(snapshotDir, { recursive: true, force: true });
  }
  await mkdir(dirname(snapshotDir), { recursive: true });
  await cp(srcDir, snapshotDir, { recursive: true, force: true });
}

export async function restoreSnapshot(snapshotDir, dstDir) {
  if (!existsSync(snapshotDir)) {
    throw new Error(`restoreSnapshot: source snapshot does not exist: ${snapshotDir}`);
  }
  if (existsSync(dstDir)) {
    rmSync(dstDir, { recursive: true, force: true });
  }
  await cp(snapshotDir, dstDir, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// Read an MQO profile.json emitted by the runtime (if present)
// ---------------------------------------------------------------------------

export async function readProfileJson(profilePath) {
  try {
    const raw = await readFile(profilePath, "utf-8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}
