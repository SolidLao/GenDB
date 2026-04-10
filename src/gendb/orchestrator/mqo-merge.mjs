/**
 * Deterministic signature-based merge of cluster shards for the large-batch
 * MQO Analyzer path.
 *
 * Inputs:
 *   - global_registry.json             — all candidates from the Surveyor
 *   - cluster_<k>/blueprint_shard.json  — per-cluster decisions/new candidates
 *
 * Output:
 *   - deterministic_merge.json          — structured merge result with:
 *       merged_components: components that could be merged cleanly by canonical_signature
 *       ambiguous_cases:   cases that need LLM arbitration (Global Integrator)
 *       unassigned_queries: queries with no shared components after merge
 *       per_query_dependencies: union across all clusters
 *
 * The Global Integrator LLM then consumes deterministic_merge.json and
 * writes the FINAL shared_component_blueprint.json (with LLM judgment on
 * ambiguous cases only).
 */

import { readFile, writeFile, readdir } from "fs/promises";
import { resolve } from "path";
import { existsSync } from "fs";

/**
 * @param {object} opts
 * @param {string} opts.globalRegistryPath
 * @param {string} opts.mqoDir               path to mqo/ (parent of cluster_X/ dirs)
 * @param {Array<{id:string}>} opts.queries  full batch
 * @param {string} opts.outPath              where to write deterministic_merge.json
 */
export async function deterministicMerge(opts) {
  const { globalRegistryPath, mqoDir, queries, outPath } = opts;

  // --- Load global registry ---
  const registry = JSON.parse(await readFile(globalRegistryPath, "utf-8"));
  const bySig = new Map();
  for (const c of registry.candidates || []) {
    const sig = c.canonical_signature || c.component_id;
    bySig.set(sig, { ...c, consumers: new Set(c.consumers || []), origin: "surveyor" });
  }

  // --- Discover cluster shard files ---
  const entries = await readdir(mqoDir, { withFileTypes: true });
  const clusterDirs = entries
    .filter((e) => e.isDirectory() && e.name.startsWith("cluster_"))
    .map((e) => resolve(mqoDir, e.name));

  const perQueryDeps = {};
  for (const q of queries) perQueryDeps[q.id] = new Set();

  const ambiguous = [];

  for (const cdir of clusterDirs) {
    const shardPath = resolve(cdir, "blueprint_shard.json");
    if (!existsSync(shardPath)) continue;
    let shard;
    try {
      shard = JSON.parse(await readFile(shardPath, "utf-8"));
    } catch (err) {
      console.warn(`[MQO merge] Failed to read shard ${shardPath}: ${err.message}`);
      continue;
    }

    // Decisions: accept materializes; track rejects
    for (const dec of shard.decisions || []) {
      const sig = dec.canonical_signature || dec.component_id;
      if (!sig) continue;
      const existing = bySig.get(sig);
      if (dec.decision === "materialize") {
        if (existing) {
          for (const q of dec.consumers || existing.consumers) existing.consumers.add(q);
          // Merge metadata opportunistically
          for (const k of ["output_schema", "build_cost_estimate_ms", "cardinality_estimate", "source_tables", "kind", "rationale"]) {
            if (dec[k] != null && existing[k] == null) existing[k] = dec[k];
          }
        } else {
          // Cluster added a new candidate
          bySig.set(sig, {
            ...dec,
            consumers: new Set(dec.consumers || []),
            origin: `cluster:${shard.cluster_id || "?"}`,
          });
        }
      } else if (dec.decision === "reject") {
        if (existing) {
          existing._rejections = (existing._rejections || 0) + 1;
          existing._rejectReasons = (existing._rejectReasons || []).concat(dec.rationale || "");
        }
      } else {
        ambiguous.push({
          reason: "unknown decision field",
          from: shard.cluster_id,
          candidate: dec,
        });
      }
    }

    // New candidates: add if signature is new, else merge consumers
    for (const nc of shard.new_candidates || []) {
      const sig = nc.canonical_signature || nc.component_id;
      if (!sig) continue;
      const existing = bySig.get(sig);
      if (existing) {
        for (const q of nc.consumers || []) existing.consumers.add(q);
      } else {
        bySig.set(sig, {
          ...nc,
          consumers: new Set(nc.consumers || []),
          origin: `cluster:${shard.cluster_id || "?"}`,
        });
      }
    }

    // Merge per-query dependencies
    for (const [qid, cids] of Object.entries(shard.per_query_dependencies || {})) {
      if (!perQueryDeps[qid]) perQueryDeps[qid] = new Set();
      for (const cid of cids) perQueryDeps[qid].add(cid);
    }
  }

  // --- Finalize ---
  const merged = [];
  for (const [sig, c] of bySig.entries()) {
    const rejections = c._rejections || 0;
    const accepted = rejections < 2;  // unanimous rejection from ≥ 2 clusters → drop
    const consumers = [...c.consumers].sort();
    const entry = {
      component_id: c.component_id,
      canonical_signature: sig,
      kind: c.kind,
      source_tables: c.source_tables || [],
      output_schema: c.output_schema || null,
      consumers,
      build_cost_estimate_ms: c.build_cost_estimate_ms ?? null,
      cardinality_estimate: c.cardinality_estimate ?? null,
      rationale: c.rationale || "",
      origin: c.origin,
      _accepted: accepted,
      _rejections: rejections,
    };
    if (consumers.length >= 2 && accepted) {
      merged.push(entry);
    } else if (consumers.length < 2) {
      ambiguous.push({ reason: "fewer than 2 consumers", candidate: entry });
    } else {
      ambiguous.push({ reason: `rejected by ${rejections} clusters`, candidate: entry });
    }
  }

  const unassignedQueries = Object.entries(perQueryDeps)
    .filter(([, deps]) => deps.size === 0)
    .map(([q]) => q);

  const result = {
    version: "1.0",
    generated_by: "mqo-merge.mjs deterministic",
    merged_components: merged,
    ambiguous_cases: ambiguous,
    unassigned_queries: unassignedQueries,
    per_query_dependencies: Object.fromEntries(
      Object.entries(perQueryDeps).map(([q, s]) => [q, [...s]]),
    ),
  };
  await writeFile(outPath, JSON.stringify(result, null, 2));
  return result;
}
