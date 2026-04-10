/**
 * Node.js wrapper for the MQO tool layer (src/gendb/tools/mqo-tools.py).
 *
 * Each exported function shells out to the Python CLI and returns parsed JSON.
 * These wrappers exist for two reasons:
 *   1. They give JS code (orchestrator, MQO Phase 2 helpers) a typed interface.
 *   2. They are the basis for exposing the tools to LLM agents via the provider
 *      tool-exposure pattern (see src/gendb/providers/{claude,codex}.mjs).
 *
 * Design: the tools are stateless except for the registry-* family, which
 * accepts a `registryPath` argument so concurrent agents can point to
 * different shards (critical for the large-batch parallel Cluster Analyzer
 * flow — see the plan file).
 */

import { execFileSync } from "child_process";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const TOOL_SCRIPT = resolve(__dirname, "mqo-tools.py");

const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_MAX_BUFFER = 16 * 1024 * 1024; // 16 MB

function runTool(args, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  try {
    const stdout = execFileSync("python3", [TOOL_SCRIPT, ...args], {
      encoding: "utf-8",
      timeout: timeoutMs,
      maxBuffer: DEFAULT_MAX_BUFFER,
    });
    return JSON.parse(stdout.trim());
  } catch (err) {
    // Surface the tool stderr if available for easier debugging
    const detail = err.stderr ? `: ${err.stderr.toString().slice(0, 500)}` : "";
    throw new Error(`mqo-tools ${args[0]} failed${detail} (${err.message})`);
  }
}

// ---------------------------------------------------------------------------
// Read-only structural lookups
// ---------------------------------------------------------------------------

export function listQueries(queriesFile) {
  return runTool(["list-queries", "--queries-file", queriesFile]);
}

export function getQuery(queriesFile, qid) {
  return runTool(["get-query", "--queries-file", queriesFile, "--qid", qid]);
}

export function canonicalSignature(queriesFile, qid, scope) {
  return runTool(["canonical-signature", "--queries-file", queriesFile, "--qid", qid, "--scope", scope]);
}

export function findQueriesTouching(queriesFile, table) {
  return runTool(["find-queries-touching", "--queries-file", queriesFile, "--table", table]);
}

export function findQueriesWithJoin(queriesFile, tableA, tableB) {
  return runTool(["find-queries-with-join", "--queries-file", queriesFile, "--table-a", tableA, "--table-b", tableB]);
}

export function predicateOverlap(queriesFile, qidA, qidB, table, column) {
  return runTool([
    "predicate-overlap",
    "--queries-file", queriesFile,
    "--qid-a", qidA, "--qid-b", qidB,
    "--table", table, "--column", column,
  ]);
}

export function aggSignature(queriesFile, qid) {
  return runTool(["agg-signature", "--queries-file", queriesFile, "--qid", qid]);
}

// ---------------------------------------------------------------------------
// Registry operations (shard-safe — pass a different registryPath per agent)
// ---------------------------------------------------------------------------

export function registryRead(registryPath) {
  return runTool(["registry-read", "--registry-path", registryPath]);
}

export function registryAddCandidate(registryPath, spec) {
  return runTool([
    "registry-add-candidate",
    "--registry-path", registryPath,
    "--spec-json", JSON.stringify(spec),
  ]);
}

export function registryUpdateConsumers(registryPath, componentId, qids) {
  return runTool([
    "registry-update-consumers",
    "--registry-path", registryPath,
    "--component-id", componentId,
    "--qids", qids.join(","),
  ]);
}

// ---------------------------------------------------------------------------
// Convenience: batch summary (caller wants structural facts for all queries)
// Useful for small-batch MQO Analyzer agent when it wants a single blob.
// ---------------------------------------------------------------------------

export function batchStructuralSummary(queriesFile) {
  const queries = listQueries(queriesFile);
  // Collect the union of tables and all pairwise join edges seen
  const allTables = new Set();
  const allEdges = new Set();
  for (const q of queries) {
    (q.tables || []).forEach((t) => allTables.add(t));
    (q.join_edges || []).forEach(([a, b]) => allEdges.add(`${a}|${b}`));
  }
  return {
    query_count: queries.length,
    queries,
    all_tables: [...allTables].sort(),
    all_join_edges: [...allEdges].map((s) => s.split("|")),
  };
}
