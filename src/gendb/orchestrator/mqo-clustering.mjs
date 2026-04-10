/**
 * Deterministic query clustering for the large-batch MQO path.
 *
 * Given a global registry (with candidate shared components and their
 * consumer lists), form clusters such that:
 *   - Queries that share ≥ K candidates co-cluster
 *   - Every query appears in exactly one cluster
 *   - Candidates whose consumer set straddles multiple clusters are marked
 *     `scope: "cross"` and will be lifted by the Global Integrator
 *
 * No LLM calls. This is intentional — determinism preserves global
 * information: cluster membership only depends on registry state (which was
 * written by the Surveyor with full-batch visibility).
 */

import { readFile, writeFile } from "fs/promises";
import { defaults } from "../gendb.config.mjs";

/**
 * @param {string} globalRegistryPath   path to global_registry.json
 * @param {Array<{id:string}>} queries  full batch of queries
 * @param {object} opts                 { targetClusters, minSharedCandidates }
 * @returns {Promise<{clusters:Array, crossCluster:Array}>}
 */
export async function formClusters(globalRegistryPath, queries, opts = {}) {
  const targetClusters = opts.targetClusters ?? defaults.mqo?.clustering?.targetClustersPerLarge ?? 8;
  const minShared = opts.minSharedCandidates ?? defaults.mqo?.clustering?.minSharedCandidates ?? 2;

  const registryRaw = await readFile(globalRegistryPath, "utf-8");
  const registry = JSON.parse(registryRaw);
  const candidates = registry.candidates || [];

  // Build per-query candidate multiset
  const queryCandidates = new Map();
  for (const q of queries) queryCandidates.set(q.id, new Set());
  for (const c of candidates) {
    for (const qid of c.consumers || []) {
      if (queryCandidates.has(qid)) queryCandidates.get(qid).add(c.component_id);
    }
  }

  // Greedy clustering by Jaccard similarity — for each unassigned query,
  // find its closest already-formed cluster; if similarity < threshold,
  // start a new cluster. Threshold adapts so we land near targetClusters.
  const unassigned = [...queries].map((q) => q.id);
  const clusters = [];

  function jaccardSimilarity(aSet, bSet) {
    if (aSet.size === 0 && bSet.size === 0) return 0;
    let inter = 0;
    for (const x of aSet) if (bSet.has(x)) inter++;
    const uni = aSet.size + bSet.size - inter;
    return uni === 0 ? 0 : inter / uni;
  }

  // Order queries by their candidate-set size descending — anchors form first
  unassigned.sort((a, b) => (queryCandidates.get(b)?.size || 0) - (queryCandidates.get(a)?.size || 0));

  const SIM_THRESHOLD_INITIAL = 0.2; // lenient initially
  let simThreshold = SIM_THRESHOLD_INITIAL;

  while (unassigned.length > 0 && clusters.length < targetClusters) {
    const anchorQid = unassigned.shift();
    const anchorCands = queryCandidates.get(anchorQid) || new Set();
    const cluster = { members: [anchorQid], candidateUnion: new Set(anchorCands) };

    // Pull in queries that share ≥ minShared candidates (and exceed sim threshold)
    const stillUnassigned = [];
    for (const qid of unassigned) {
      const qCands = queryCandidates.get(qid) || new Set();
      let sharedCount = 0;
      for (const x of qCands) if (cluster.candidateUnion.has(x)) sharedCount++;
      const sim = jaccardSimilarity(qCands, cluster.candidateUnion);
      if (sharedCount >= minShared && sim >= simThreshold) {
        cluster.members.push(qid);
        for (const x of qCands) cluster.candidateUnion.add(x);
      } else {
        stillUnassigned.push(qid);
      }
    }
    unassigned.length = 0;
    unassigned.push(...stillUnassigned);
    clusters.push(cluster);
  }

  // Any remaining unassigned queries form a catch-all cluster
  if (unassigned.length > 0) {
    clusters.push({
      members: unassigned.slice(),
      candidateUnion: new Set(
        unassigned.flatMap((qid) => [...(queryCandidates.get(qid) || new Set())]),
      ),
    });
  }

  // Mark cross-cluster candidates: any candidate whose consumers appear in > 1 cluster
  const clusterOfQuery = new Map();
  clusters.forEach((cl, idx) => cl.members.forEach((qid) => clusterOfQuery.set(qid, idx)));
  const crossCluster = [];
  for (const c of candidates) {
    const clIdxs = new Set((c.consumers || []).map((qid) => clusterOfQuery.get(qid)).filter((x) => x != null));
    if (clIdxs.size > 1) {
      crossCluster.push({ component_id: c.component_id, clusters: [...clIdxs] });
    }
  }

  return {
    clusters: clusters.map((cl, idx) => ({
      cluster_id: `cluster_${idx}`,
      members: cl.members,
      candidate_ids: [...cl.candidateUnion],
    })),
    crossCluster,
  };
}
