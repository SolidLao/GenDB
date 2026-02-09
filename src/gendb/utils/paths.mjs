/**
 * Run ID generation, directory creation, and symlink helpers.
 */

import { mkdir, writeFile, symlink, unlink, lstat } from "fs/promises";
import { resolve, relative } from "path";
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
