#!/usr/bin/env node
/**
 * migrate-output.mjs — Move old timestamp-based run directories to output/deprecated/
 * and optionally import best .cpp files into persistent workload directories.
 *
 * Usage:
 *   node src/gendb/tools/migrate-output.mjs --benchmark tpc-h
 *   node src/gendb/tools/migrate-output.mjs --benchmark tpc-h --import-best
 *   node src/gendb/tools/migrate-output.mjs                      # all benchmarks
 */

import { resolve, dirname } from "path";
import { existsSync, readdirSync, readFileSync } from "fs";
import { mkdir, readFile, writeFile, rename, readdir, copyFile } from "fs/promises";
import { PROJECT_ROOT, OUTPUT_DIR } from "../config.mjs";
import { getNextVersionNumber, updateBestSymlink, getQueryTemplateDir } from "../utils/paths.mjs";

// Timestamp dir pattern: e.g. 2026-03-11T05-06-27
const TIMESTAMP_RE = /^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}$/;
// Workload dir pattern: e.g. tpc-h-sf10
const WORKLOAD_DIR_RE = /^.+-sf\d+$/;

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { benchmark: null, importBest: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--benchmark" && args[i + 1]) {
      opts.benchmark = args[++i];
    } else if (args[i] === "--import-best") {
      opts.importBest = true;
    }
  }
  return opts;
}

/** List benchmark directories under output/ that contain timestamp-based runs. */
function discoverBenchmarks(filter) {
  const entries = readdirSync(OUTPUT_DIR, { withFileTypes: true });
  return entries
    .filter((e) => e.isDirectory())
    .map((e) => e.name)
    .filter((name) => !WORKLOAD_DIR_RE.test(name) && name !== "deprecated")
    .filter((name) => !filter || name === filter);
}

/** Migrate timestamp dirs for a single benchmark into output/deprecated/<benchmark>/. */
async function migrateBenchmark(benchmark) {
  const benchDir = resolve(OUTPUT_DIR, benchmark);
  const entries = readdirSync(benchDir, { withFileTypes: true });

  const timestampDirs = entries
    .filter((e) => e.isDirectory() && TIMESTAMP_RE.test(e.name))
    .map((e) => e.name);

  if (timestampDirs.length === 0) {
    console.log(`  [${benchmark}] No timestamp dirs to migrate.`);
    return [];
  }

  const deprecatedDir = resolve(OUTPUT_DIR, "deprecated", benchmark);
  await mkdir(deprecatedDir, { recursive: true });

  const migrated = [];
  for (const ts of timestampDirs) {
    const src = resolve(benchDir, ts);
    const dst = resolve(deprecatedDir, ts);
    if (existsSync(dst)) {
      console.log(`  [${benchmark}] Already migrated: ${ts}`);
      continue;
    }
    await rename(src, dst);
    console.log(`  [${benchmark}] Moved ${ts} -> deprecated/${benchmark}/${ts}`);
    migrated.push({ benchmark, timestamp: ts, path: dst });
  }
  return migrated;
}

/** Import best .cpp files from a deprecated run into the persistent workload dir. */
async function importBestFromRun({ benchmark, timestamp, path: runPath }) {
  const runJsonPath = resolve(runPath, "run.json");
  if (!existsSync(runJsonPath)) return;

  let runMeta;
  try {
    runMeta = JSON.parse(await readFile(runJsonPath, "utf-8"));
  } catch {
    console.log(`  [import] Skipping ${timestamp}: invalid run.json`);
    return;
  }

  const pipelines = runMeta?.phase2?.pipelines;
  if (!pipelines) return;

  // Determine scale factor from run metadata or default
  const sf = runMeta.scaleFactor || runMeta.sf || 10;
  const workloadDir = resolve(OUTPUT_DIR, `${benchmark}-sf${sf}`);
  if (!existsSync(workloadDir)) {
    console.log(`  [import] No workload dir for ${benchmark}-sf${sf}, skipping.`);
    return;
  }

  for (const [queryKey, pipeline] of Object.entries(pipelines)) {
    const bestPath = pipeline.bestCppPath;
    if (!bestPath || !existsSync(bestPath)) continue;

    const queryId = queryKey; // e.g. Q1, Q2, ...
    const queryDir = getQueryTemplateDir(workloadDir, queryId);
    await mkdir(queryDir, { recursive: true });

    const version = await getNextVersionNumber(workloadDir, queryId);
    const versionDir = resolve(queryDir, `v${version}`);
    await mkdir(versionDir, { recursive: true });

    const destCpp = resolve(versionDir, "query.cpp");
    await copyFile(bestPath, destCpp);
    await updateBestSymlink(workloadDir, queryId, version);

    console.log(`  [import] ${benchmark} ${queryId}: imported as v${version}`);
  }
}

async function main() {
  const opts = parseArgs();
  console.log(`Migrating output directories...`);
  if (opts.benchmark) console.log(`  Filtering to benchmark: ${opts.benchmark}`);
  if (opts.importBest) console.log(`  --import-best enabled`);

  const benchmarks = discoverBenchmarks(opts.benchmark);
  if (benchmarks.length === 0) {
    console.log("No benchmarks found to migrate.");
    return;
  }

  const allMigrated = [];
  for (const bm of benchmarks) {
    const migrated = await migrateBenchmark(bm);
    allMigrated.push(...migrated);
  }

  if (opts.importBest && allMigrated.length > 0) {
    console.log(`\nImporting best .cpp files from ${allMigrated.length} migrated run(s)...`);
    for (const entry of allMigrated) {
      await importBestFromRun(entry);
    }
  }

  console.log(`\nDone. Migrated ${allMigrated.length} run(s).`);
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
