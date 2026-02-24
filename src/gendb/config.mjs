/**
 * Centralized path resolution & constants for GenDB.
 * All paths computed relative to project root.
 */

import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { defaults } from "./gendb.config.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const PROJECT_ROOT = resolve(__dirname, "..", "..");
export const BENCHMARKS_DIR = resolve(PROJECT_ROOT, "benchmarks");
export const OUTPUT_DIR = resolve(PROJECT_ROOT, "output");
export const AGENTS_DIR = resolve(__dirname, "agents");

export const DEFAULT_SCHEMA = resolve(BENCHMARKS_DIR, defaults.targetBenchmark, "schema.sql");
export const DEFAULT_QUERIES = resolve(BENCHMARKS_DIR, defaults.targetBenchmark, "queries.sql");

/** Resolve the data directory for a given benchmark and scale factor. */
export function getDataDir(benchmark, scaleFactor) {
  return resolve(BENCHMARKS_DIR, benchmark, "data", `sf${scaleFactor}`);
}

/** Resolve the GenDB persistent storage directory for a given benchmark and scale factor. */
export function getGendbDir(benchmark, scaleFactor) {
  return resolve(BENCHMARKS_DIR, benchmark, "gendb", `sf${scaleFactor}.gendb`);
}
