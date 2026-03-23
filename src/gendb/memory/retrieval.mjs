/**
 * HAG Retrieval v3 — hierarchical 3-tier matching.
 *
 * Given a SQL query, classifies it as:
 *   - EXACT (score >= 1.0): same template → fast path
 *   - STRUCTURAL (score 0.5–0.99): similar structure → accelerated path
 *   - NOVEL (score < 0.5): no match → full path
 *
 * v3: L2-L5 knowledge is delivered via native Claude skills (.claude/skills/).
 * The catalog is a brief summary of available skills; agents discover and invoke
 * skills via Claude's native semantic matching. Pre-injection (L0/L1 data) remains.
 *
 * Returns structured context for the orchestrator:
 *   { tier, score, matchedL1, matchedL0, preInjected, catalog, bestCppPath }
 */

import { readFile } from "fs/promises";
import { existsSync } from "fs";
import { getNodesByLayer, readNode, getConnectedNodes, generateCatalog, writeNode } from "./graph.mjs";
import { extractStructuralFeatures } from "../tools/sql-parser.mjs";
import { extractTemplate } from "../tools/template-extractor.mjs";

/** Compute structural similarity between two feature sets (0-1). */
function structuralSimilarity(f1, f2) {
  let score = 0;
  let weights = 0;

  // Join count similarity (weight: 3)
  const joinDiff = Math.abs((f1.join_count ?? 0) - (f2.join_count ?? 0));
  score += (1 - Math.min(joinDiff / 5, 1)) * 3;
  weights += 3;

  // Boolean features (weight: 1 each)
  const boolFeatures = [
    "has_aggregation", "has_group_by", "has_order_by", "has_subquery",
    "has_like", "has_in", "has_between", "has_case", "has_distinct",
    "has_limit", "has_having",
  ];
  for (const feat of boolFeatures) {
    if ((f1[feat] ?? false) === (f2[feat] ?? false)) score += 1;
    weights += 1;
  }

  // Table overlap (weight: 3 — strong signal for exact match)
  const t1 = f1.tables || [];
  const t2 = f2.tables || [];
  if (t1.length > 0 && t2.length > 0) {
    const intersection = t1.filter(t => t2.includes(t));
    const union = [...new Set([...t1, ...t2])];
    score += (intersection.length / union.length) * 3;
  }
  weights += 3;

  // Aggregate function overlap (weight: 1)
  const a1 = f1.aggregate_functions || [];
  const a2 = f2.aggregate_functions || [];
  if (a1.length > 0 && a2.length > 0) {
    const aIntersection = a1.filter(f => a2.includes(f));
    const aUnion = [...new Set([...a1, ...a2])];
    score += aIntersection.length / aUnion.length;
  } else if (!f1.has_aggregation && !f2.has_aggregation) {
    score += 1;
  }
  weights += 1;

  return score / weights;
}

// ---------------------------------------------------------------------------
// Main Retrieval: classifyQuery
// ---------------------------------------------------------------------------

/**
 * Classify a query against L1 templates and retrieve appropriate context.
 *
 * @param {string} querySql - The SQL query
 * @param {string} queryId - Query identifier (e.g., "Q1")
 * @param {string} benchmark - Benchmark name (e.g., "tpc-h")
 * @param {number} scaleFactor - Scale factor
 * @param {string} memoryDir - Path to memory directory
 * @param {object} config - Memory config from gendb.config
 * @returns {object} { tier, score, matchedL1, matchedL0, preInjected, catalog }
 */
export async function classifyQuery(querySql, queryId, benchmark, scaleFactor, memoryDir, config = {}) {
  const minStructuralScore = config.structuralMatchMinScore || 0.5;
  const queryFeatures = extractStructuralFeatures(querySql);

  // Extract template SQL for more precise matching (same template = different params only)
  let queryTemplateSql = null;
  try {
    const { templateSql } = extractTemplate(querySql);
    queryTemplateSql = templateSql?.trim() || null;
  } catch {}

  // --- Match against L1 templates ---
  const l1Nodes = await getNodesByLayer(1, memoryDir);
  const l1Matches = [];

  for (const node of l1Nodes) {
    const sig = node.content?.structural_signature;
    if (!sig) continue;

    // L1 signatures are produced by the same extractStructuralFeatures() tool,
    // so they can be compared directly. Use ?? defaults for safety.
    let similarity = structuralSimilarity(queryFeatures, sig);

    // Boost score to exact (1.0) if template SQL matches exactly —
    // same template with different params should be treated as exact match
    if (queryTemplateSql && node.content?.template_sql) {
      const l1Template = node.content.template_sql.trim();
      if (queryTemplateSql === l1Template) {
        similarity = 1.0;
      }
    }

    l1Matches.push({ node, similarity });
  }

  l1Matches.sort((a, b) => b.similarity - a.similarity);
  const bestMatch = l1Matches[0] || null;
  const bestScore = bestMatch?.similarity || 0;

  // --- Determine tier ---
  let tier, matchedL1 = null, matchedL0 = null;

  if (bestScore >= 1.0) {
    tier = "exact";
    matchedL1 = bestMatch.node;
  } else if (bestScore >= minStructuralScore) {
    tier = "structural";
    matchedL1 = bestMatch.node;
  } else {
    tier = "novel";
  }

  // --- Retrieve L0 instances for matched L1 ---
  if (matchedL1) {
    const l0Nodes = await getConnectedNodes(matchedL1.id, "instance_of", memoryDir, "incoming");
    // Prefer same benchmark + scale factor
    const sameBenchmark = l0Nodes.filter(n =>
      n.content?.benchmark === benchmark && n.content?.scale_factor === scaleFactor
    );
    const others = l0Nodes.filter(n =>
      !(n.content?.benchmark === benchmark && n.content?.scale_factor === scaleFactor)
    );
    // Pick best by timing
    const sorted = [...sameBenchmark, ...others].sort((a, b) =>
      (a.content?.best_timing_ms || Infinity) - (b.content?.best_timing_ms || Infinity)
    );
    matchedL0 = sorted[0] || null;
  }

  // --- Build pre-injected content (tier-dependent) ---
  let preInjected = "";

  if (tier === "exact" && matchedL0) {
    preInjected = await buildExactPreInjection(matchedL0, matchedL1, memoryDir);
  } else if (tier === "structural" && matchedL1) {
    preInjected = await buildStructuralPreInjection(matchedL1, memoryDir);
  }
  // Novel tier: no structural pre-injection

  // --- Generate catalog (L2/L3/L4 summaries) ---
  const catalog = await generateCatalog(memoryDir);

  // For exact match, provide the best_cpp_path so orchestrator can copy it directly
  const bestCppPath = (tier === "exact" && matchedL0?.content?.best_cpp_path) || null;

  return {
    tier,
    score: bestScore,
    matchedL1: matchedL1?.id || null,
    matchedL0: matchedL0?.id || null,
    bestCppPath,
    preInjected,
    catalog,
  };
}

// ---------------------------------------------------------------------------
// Pre-injection builders
// ---------------------------------------------------------------------------

/** Build pre-injection for exact match (fast path): L1 strategies only (code is copied by orchestrator). */
async function buildExactPreInjection(l0Node, l1Node, memoryDir) {
  const lines = [];
  lines.push("## Prior Knowledge — Exact Template Match (Fast Path)");
  lines.push("The reference C++ implementation has been copied into your working directory as the starting point.\n");

  // L1 proven strategies
  const c1 = l1Node.content;
  if (c1.proven_strategies?.length > 0) {
    lines.push("### Proven Strategies");
    for (const s of c1.proven_strategies) lines.push(`- ${s}`);
    lines.push("");
  }
  if (c1.anti_patterns?.length > 0) {
    lines.push("### Anti-Patterns (avoid these)");
    for (const a of c1.anti_patterns) lines.push(`- ${a}`);
    lines.push("");
  }

  // L0 reference info (no code — it's already in the file)
  const c0 = l0Node.content;
  lines.push(`### Reference: ${l0Node.id}`);
  lines.push(`- Best timing: ${c0.best_timing_ms ? Math.round(c0.best_timing_ms) + "ms" : "N/A"}`);
  if (c0.key_optimizations?.length > 0) {
    lines.push(`- Key optimizations: ${c0.key_optimizations.join(", ")}`);
  }

  lines.push("");
  return lines.join("\n");
}

/** Build pre-injection for structural match (accelerated path): L1 strategies + L2 operator summaries. */
async function buildStructuralPreInjection(l1Node, memoryDir) {
  const lines = [];
  lines.push("## Prior Knowledge — Structural Match (Accelerated Path)");
  lines.push("A similar query template was found. The strategies below may apply.\n");

  // L1 strategies
  const c1 = l1Node.content;
  lines.push(`### Similar Template: ${c1.template_name || c1.pattern_name || l1Node.id}`);
  if (c1.proven_strategies?.length > 0) {
    lines.push("**Proven strategies**:");
    for (const s of c1.proven_strategies) lines.push(`- ${s}`);
  }
  if (c1.anti_patterns?.length > 0) {
    lines.push("**Anti-patterns**:");
    for (const a of c1.anti_patterns) lines.push(`- ${a}`);
  }
  lines.push("");

  // L3 operator techniques connected to this L1 (via uses_operator edges)
  // In v3, these are also available as .claude/skills/operators/ — agents can invoke them directly
  const l3Nodes = await getConnectedNodes(l1Node.id, "uses_operator", memoryDir, "outgoing");
  if (l3Nodes.length > 0) {
    lines.push("### Relevant Operator Techniques");
    lines.push("_These are also available as agent skills and will be loaded automatically._");
    for (const n of l3Nodes.slice(0, 5)) {
      const c = n.content;
      const skillRef = n.skill_path ? ` (skill: ${n.skill_path})` : "";
      lines.push(`- **${n.id}**${skillRef}: ${c.description || ""}`);
    }
    lines.push("");
  }

  // L2 sub-structure patterns connected to this L1 (via exhibits_pattern edges)
  const l2Nodes = await getConnectedNodes(l1Node.id, "exhibits_pattern", memoryDir, "outgoing");
  if (l2Nodes.length > 0) {
    lines.push("### Relevant Patterns (composed operator recipes)");
    lines.push("_These are also available as agent skills and will be loaded automatically._");
    for (const n of l2Nodes.slice(0, 3)) {
      const c = n.content;
      const skillRef = n.skill_path ? ` (skill: ${n.skill_path})` : "";
      lines.push(`- **${n.id}**${skillRef}: ${c.description || n.summary || ""}`);
    }
    lines.push("");
  }

  // L0 reference instances (brief, no full code)
  const l0Nodes = await getConnectedNodes(l1Node.id, "instance_of", memoryDir, "incoming");
  if (l0Nodes.length > 0) {
    lines.push("### Reference Results");
    for (const n of l0Nodes.slice(0, 3)) {
      const c = n.content;
      const timing = c.best_timing_ms ? `${Math.round(c.best_timing_ms)}ms` : "N/A";
      lines.push(`- ${n.id}: ${timing}${c.key_optimizations?.length ? " — " + c.key_optimizations.join(", ") : ""}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Legacy-compatible retrieveContext (wraps classifyQuery)
// ---------------------------------------------------------------------------

/**
 * Retrieve relevant memory context for a query (legacy interface).
 * Returns a formatted Markdown string combining pre-injection + catalog.
 */
export async function retrieveContext(querySql, storageDesign, hardware, memoryDir, config = {}) {
  const maxTokens = config.maxPreInjectionTokens || 4000;

  // Use classifyQuery with default params
  const result = await classifyQuery(querySql, "unknown", "unknown", 0, memoryDir, config);

  const sections = [];

  if (result.preInjected) {
    sections.push(result.preInjected);
  }

  if (result.catalog) {
    sections.push(result.catalog);
  }

  if (sections.length === 0) return "";

  sections.unshift("Prior optimization experience will be provided from GenDB's memory system.");
  sections.unshift("Try the suggested approaches first. If they do not lead to good performance, identify bottlenecks yourself and propose new implementations.\n");

  let text = sections.join("\n");
  const maxChars = maxTokens * 4;
  if (text.length > maxChars) {
    text = text.slice(0, maxChars) + "\n\n*[Memory context truncated]*";
  }

  return text;
}

// ---------------------------------------------------------------------------
// patchL1Signatures: programmatic backup to fix string-based structural_signatures
// ---------------------------------------------------------------------------

/**
 * Ensure all L1 nodes have proper feature-object structural_signatures.
 * If a L1 node has a string signature (from LLM agent), replace it with
 * a proper feature object extracted from connected L0 node's SQL.
 */
export async function patchL1Signatures(memoryDir) {
  const l1Nodes = await getNodesByLayer(1, memoryDir);
  let patched = 0;
  for (const l1 of l1Nodes) {
    // Only patch if signature is a string (not already an object)
    if (typeof l1.content?.structural_signature !== 'object' || l1.content.structural_signature === null) {
      // Find connected L0 nodes via instance_of edges
      const l0Nodes = await getConnectedNodes(l1.id, "instance_of", memoryDir, "incoming");
      const l0 = l0Nodes.find(n => n.content?.sql);
      if (!l0) continue;

      // Extract features using the same tool the retrieval system uses
      const features = extractStructuralFeatures(l0.content.sql);
      if (features && typeof features === 'object') {
        l1.content.structural_signature = features;
        patched++;
      }

      // Also extract template SQL if missing
      if (!l1.content.template_sql) {
        try {
          const { templateSql } = extractTemplate(l0.content.sql);
          if (templateSql) l1.content.template_sql = templateSql;
        } catch {}
      }

      await writeNode(l1, memoryDir);
    }
  }
  if (patched > 0) {
    console.log(`[Memory] Patched ${patched} L1 structural signatures.`);
  }
}
