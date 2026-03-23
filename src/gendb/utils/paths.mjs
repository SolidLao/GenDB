/**
 * Run ID generation, directory creation, and symlink helpers.
 */

import { mkdir, writeFile, readdir, symlink, unlink, lstat } from "fs/promises";
import { resolve, relative } from "path";
import { existsSync, readFileSync } from "fs";
import { OUTPUT_DIR, BENCHMARKS_DIR } from "../config.mjs";

/** Returns a timestamp string like `2025-06-15T14-30-00` (filesystem-safe). */
export function createRunId() {
  return new Date().toISOString().replace(/:/g, "-").replace(/\.\d+Z$/, "");
}

/** Extracts workload name from a benchmark path (e.g. `benchmarks/tpc-h/schema.sql` → `tpc-h`). */
export function getWorkloadName(schemaPath) {
  const rel = relative(BENCHMARKS_DIR, schemaPath);
  return rel.split("/")[0] || "default";
}

/**
 * Creates `output/<workload>/<runId>/` and writes an initial `run.json`.
 * Returns the absolute path to the run directory.
 */
export async function createRunDir(workload, runId) {
  const runDir = resolve(OUTPUT_DIR, workload, runId);
  await mkdir(runDir, { recursive: true });

  const runMeta = {
    runId,
    workload,
    startedAt: new Date().toISOString(),
    status: "running",
  };
  await writeFile(resolve(runDir, "run.json"), JSON.stringify(runMeta, null, 2));

  return runDir;
}

/**
 * Creates `iterations/<i>/generated/` inside the run directory.
 * Returns the absolute path to the iteration directory.
 */
export async function createIterationDir(runDir, iteration) {
  const iterDir = resolve(runDir, "iterations", String(iteration));
  await mkdir(resolve(iterDir, "generated"), { recursive: true });
  return iterDir;
}

/**
 * Creates `queries/<queryId>/` inside the run directory.
 * Returns the absolute path to the query directory.
 */
export async function createQueryDir(runDir, queryId) {
  const queryDir = resolve(runDir, "queries", queryId);
  await mkdir(queryDir, { recursive: true });
  return queryDir;
}

/** Updates (or creates) the `output/<workload>/latest` symlink to point at `<runId>`. */
export async function updateLatestSymlink(workload, runId) {
  const linkPath = resolve(OUTPUT_DIR, workload, "latest");
  try {
    await lstat(linkPath);
    await unlink(linkPath);
  } catch {
    // symlink doesn't exist yet — fine
  }
  await symlink(runId, linkPath);
}

// ---------------------------------------------------------------------------
// Persistent Workload Directory Helpers
// ---------------------------------------------------------------------------

/** Returns workload dir name: `<benchmark>-sf<N>` (e.g., `tpc-h-sf10`). */
export function getWorkloadDirName(benchmark, sf) {
  return `${benchmark}-sf${sf}`;
}

/** Returns the absolute path to the workload directory. */
export function getWorkloadDir(benchmark, sf) {
  return resolve(OUTPUT_DIR, getWorkloadDirName(benchmark, sf));
}

/**
 * Creates the workload directory and writes `workload.json` if it doesn't exist.
 * Returns the absolute path to the workload directory.
 */
export async function ensureWorkloadDir(benchmark, sf) {
  const workloadDir = getWorkloadDir(benchmark, sf);
  await mkdir(workloadDir, { recursive: true });

  // Create subdirectories
  await mkdir(resolve(workloadDir, "queries"), { recursive: true });
  await mkdir(resolve(workloadDir, "storage"), { recursive: true });
  await mkdir(resolve(workloadDir, "runs"), { recursive: true });
  await mkdir(resolve(workloadDir, "ingest"), { recursive: true });

  // Write workload.json if it doesn't exist
  const metaPath = resolve(workloadDir, "workload.json");
  if (!existsSync(metaPath)) {
    const meta = {
      benchmark,
      scaleFactor: sf,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await writeFile(metaPath, JSON.stringify(meta, null, 2));
  }

  return workloadDir;
}

/** Returns the query template directory: `<workloadDir>/queries/<Qi>/`. */
export function getQueryTemplateDir(workloadDir, queryId) {
  return resolve(workloadDir, "queries", queryId);
}

/**
 * Scans existing `v<N>` directories and returns the next version number.
 * Returns 1 if no versions exist yet.
 */
export async function getNextVersionNumber(workloadDir, queryId) {
  const queryDir = getQueryTemplateDir(workloadDir, queryId);
  let maxVersion = 0;
  try {
    const entries = await readdir(queryDir);
    for (const entry of entries) {
      const match = entry.match(/^v(\d+)$/);
      if (match) {
        const num = parseInt(match[1], 10);
        if (num > maxVersion) maxVersion = num;
      }
    }
  } catch {
    // Directory doesn't exist yet
  }
  return maxVersion + 1;
}

/**
 * Updates the `best` symlink in `<workloadDir>/queries/<Qi>/` to point at `v<N>`.
 */
export async function updateBestSymlink(workloadDir, queryId, version) {
  const queryDir = getQueryTemplateDir(workloadDir, queryId);
  const linkPath = resolve(queryDir, "best");
  try {
    await lstat(linkPath);
    await unlink(linkPath);
  } catch {
    // symlink doesn't exist yet — fine
  }
  await symlink(`v${version}`, linkPath);
}

/**
 * Creates a run audit entry in `<workloadDir>/runs/<timestamp>/`.
 * Writes initial `run.json` and returns the run directory path.
 */
export async function createRunAuditEntry(workloadDir, runId) {
  const timestamp = runId || createRunId();
  const runAuditDir = resolve(workloadDir, "runs", timestamp);
  await mkdir(runAuditDir, { recursive: true });

  const meta = {
    timestamp,
    startedAt: new Date().toISOString(),
    status: "running",
  };
  await writeFile(resolve(runAuditDir, "run.json"), JSON.stringify(meta, null, 2));

  return runAuditDir;
}

/**
 * Updates the `runs/latest` symlink in a workload directory.
 */
export async function updateRunsLatestSymlink(workloadDir, runId) {
  const linkPath = resolve(workloadDir, "runs", "latest");
  try {
    await lstat(linkPath);
    await unlink(linkPath);
  } catch {
    // symlink doesn't exist yet — fine
  }
  await symlink(runId, linkPath);
}

// ---------------------------------------------------------------------------
// Run-Scoped Path Helpers
// ---------------------------------------------------------------------------

/** Returns run-scoped query dir: `<runAuditDir>/queries/<queryId>/`. */
export function getRunQueryDir(runAuditDir, queryId) {
  return resolve(runAuditDir, "queries", queryId);
}

/** Returns run-scoped iteration dir: `<runAuditDir>/queries/<queryId>/iter_<N>/`. */
export function getRunQueryIterDir(runAuditDir, queryId, iteration) {
  return resolve(runAuditDir, "queries", queryId, `iter_${iteration}`);
}

/** Returns the query guide path: `<workloadDir>/queries/<queryId>/guide.md`. */
export function getQueryGuidePath(workloadDir, queryId) {
  return resolve(workloadDir, "queries", queryId, "guide.md");
}

/** Returns the routing.json path: `<workloadDir>/queries/<queryId>/routing.json`. */
export function getQueryRoutingPath(workloadDir, queryId) {
  return resolve(workloadDir, "queries", queryId, "routing.json");
}

/** Returns the version meta.json path: `<workloadDir>/queries/<queryId>/v<N>/meta.json`. */
export function getVersionMetaPath(workloadDir, queryId, version) {
  return resolve(workloadDir, "queries", queryId, `v${version}`, "meta.json");
}

/** Returns the ingest directory: `<workloadDir>/ingest/`. */
export function getIngestDir(workloadDir) {
  return resolve(workloadDir, "ingest");
}
