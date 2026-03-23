/**
 * Hierarchical Abstraction Graph (HAG) — CRUD operations.
 *
 * Manages nodes at 5 layers (L0–L4) and edges between them.
 * All data stored as JSON files in gendb-memory/graph/.
 */

import { readFile, writeFile, mkdir, readdir } from "fs/promises";
import { resolve } from "path";
import { existsSync } from "fs";

const LAYERS = ["L0", "L1", "L2", "L3", "L4", "L5"];

// v3 layer semantics:
// L0: Query Instances (HAG, structural retrieval)
// L1: Query Templates (HAG, structural retrieval)
// L2: Sub-Structure Patterns (Skills, semantic retrieval) [NEW in v3]
// L3: Operator Techniques (Skills, semantic retrieval) [was L2 in v2]
// L4: Optimization Strategies (Skills, semantic retrieval) [was L3 in v2]
// L5: Performance Principles (Skills, semantic retrieval) [was L4 in v2]
const HAG_LAYERS = [0, 1];      // Stored in gendb-memory/graph/nodes/
const SKILL_LAYERS = [2, 3, 4, 5]; // Stored as .claude/skills/ (graph stores lightweight refs)

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

/** Create graph directory structure if it doesn't exist. */
export async function initGraphDirs(memoryDir) {
  const graphDir = resolve(memoryDir, "graph", "nodes");
  for (const layer of LAYERS) {
    await mkdir(resolve(graphDir, layer), { recursive: true });
  }
  // Initialize edges.json and index.json if they don't exist
  const edgesPath = resolve(memoryDir, "graph", "edges.json");
  if (!existsSync(edgesPath)) {
    await writeFile(edgesPath, JSON.stringify([], null, 2));
  }
  const indexPath = resolve(memoryDir, "graph", "index.json");
  if (!existsSync(indexPath)) {
    await writeFile(indexPath, JSON.stringify({ nodes: {}, stats: { total: 0, by_layer: {} } }, null, 2));
  }
}

// ---------------------------------------------------------------------------
// Node Operations
// ---------------------------------------------------------------------------

/** Read a node by ID. Returns null if not found. */
export async function readNode(id, memoryDir) {
  const layer = id.split("_")[0]; // e.g., "L0" from "L0_tpch_Q1_sf10"
  const nodePath = resolve(memoryDir, "graph", "nodes", layer, `${id}.json`);
  try {
    return JSON.parse(await readFile(nodePath, "utf-8"));
  } catch {
    return null;
  }
}

/** Write (create or update) a node. */
export async function writeNode(node, memoryDir) {
  const layer = `L${node.layer}`;
  const dirPath = resolve(memoryDir, "graph", "nodes", layer);
  await mkdir(dirPath, { recursive: true });
  const nodePath = resolve(dirPath, `${node.id}.json`);
  node.updated_at = new Date().toISOString();
  if (!node.created_at) node.created_at = node.updated_at;
  await writeFile(nodePath, JSON.stringify(node, null, 2));

  // Update index
  await updateIndex(memoryDir);
}

/** Delete a node by ID. */
export async function deleteNode(id, memoryDir) {
  const layer = id.split("_")[0];
  const nodePath = resolve(memoryDir, "graph", "nodes", layer, `${id}.json`);
  try {
    const { unlink } = await import("fs/promises");
    await unlink(nodePath);
    await updateIndex(memoryDir);
  } catch {}
}

/** Get all nodes at a given layer (0-4). */
export async function getNodesByLayer(layer, memoryDir) {
  const layerStr = `L${layer}`;
  const dirPath = resolve(memoryDir, "graph", "nodes", layerStr);
  if (!existsSync(dirPath)) return [];

  const files = await readdir(dirPath);
  const nodes = [];
  for (const file of files) {
    if (!file.endsWith(".json")) continue;
    try {
      const node = JSON.parse(await readFile(resolve(dirPath, file), "utf-8"));
      nodes.push(node);
    } catch {}
  }
  return nodes;
}

/** Get all nodes across all layers. */
export async function getAllNodes(memoryDir) {
  const allNodes = [];
  for (let layer = 0; layer <= 5; layer++) {
    const nodes = await getNodesByLayer(layer, memoryDir);
    allNodes.push(...nodes);
  }
  return allNodes;
}

/** Find nodes by tag. */
export async function findNodesByTag(tag, memoryDir) {
  const allNodes = await getAllNodes(memoryDir);
  return allNodes.filter(n => n.tags?.includes(tag));
}

/** Find nodes matching a predicate. */
export async function findNodes(predicate, memoryDir) {
  const allNodes = await getAllNodes(memoryDir);
  return allNodes.filter(predicate);
}

// ---------------------------------------------------------------------------
// Edge Operations
// ---------------------------------------------------------------------------

/** Read all edges. */
export async function readAllEdges(memoryDir) {
  const edgesPath = resolve(memoryDir, "graph", "edges.json");
  try {
    return JSON.parse(await readFile(edgesPath, "utf-8"));
  } catch {
    return [];
  }
}

/** Write all edges (replaces entire edges array). */
export async function writeEdges(edges, memoryDir) {
  const edgesPath = resolve(memoryDir, "graph", "edges.json");
  await writeFile(edgesPath, JSON.stringify(edges, null, 2));
}

/** Add a single edge (deduplicates by source+target+type). */
export async function addEdge(edge, memoryDir) {
  const edges = await readAllEdges(memoryDir);
  const newSrc = edgeSource(edge);
  const newTgt = edgeTarget(edge);
  const existing = edges.find(
    e => edgeSource(e) === newSrc && edgeTarget(e) === newTgt && e.type === edge.type
  );
  if (!existing) {
    edges.push({ ...edge, created_at: new Date().toISOString() });
    await writeEdges(edges, memoryDir);
  }
}

// Helper: edges may use "from"/"to" or "source"/"target" — normalize access
const edgeSource = (e) => e.source ?? e.from;
const edgeTarget = (e) => e.target ?? e.to;

/** Remove edges involving a node. */
export async function removeEdgesForNode(nodeId, memoryDir) {
  const edges = await readAllEdges(memoryDir);
  const filtered = edges.filter(e => edgeSource(e) !== nodeId && edgeTarget(e) !== nodeId);
  await writeEdges(filtered, memoryDir);
}

/** Get connected nodes by edge type from a given node. */
export async function getConnectedNodes(nodeId, edgeType, memoryDir, direction = "outgoing") {
  const edges = await readAllEdges(memoryDir);
  const connectedIds = edges
    .filter(e => {
      if (edgeType && e.type !== edgeType) return false;
      if (direction === "outgoing") return edgeSource(e) === nodeId;
      if (direction === "incoming") return edgeTarget(e) === nodeId;
      return edgeSource(e) === nodeId || edgeTarget(e) === nodeId;
    })
    .map(e => (edgeSource(e) === nodeId ? edgeTarget(e) : edgeSource(e)));

  const nodes = [];
  for (const id of [...new Set(connectedIds)]) {
    const node = await readNode(id, memoryDir);
    if (node) nodes.push(node);
  }
  return nodes;
}

/** Get edges of a specific type from a node. */
export async function getEdgesFrom(nodeId, edgeType, memoryDir) {
  const edges = await readAllEdges(memoryDir);
  return edges.filter(e => edgeSource(e) === nodeId && (!edgeType || e.type === edgeType));
}

/** Get edges of a specific type to a node. */
export async function getEdgesTo(nodeId, edgeType, memoryDir) {
  const edges = await readAllEdges(memoryDir);
  return edges.filter(e => edgeTarget(e) === nodeId && (!edgeType || e.type === edgeType));
}

// ---------------------------------------------------------------------------
// Index Management
// ---------------------------------------------------------------------------

/** Read the graph index. */
export async function readIndex(memoryDir) {
  const indexPath = resolve(memoryDir, "graph", "index.json");
  try {
    return JSON.parse(await readFile(indexPath, "utf-8"));
  } catch {
    return { nodes: {}, stats: { total: 0, by_layer: {} } };
  }
}

/** Rebuild index from disk. */
export async function updateIndex(memoryDir) {
  const index = { nodes: {}, stats: { total: 0, by_layer: {} } };

  for (const layer of LAYERS) {
    const dirPath = resolve(memoryDir, "graph", "nodes", layer);
    if (!existsSync(dirPath)) continue;

    const files = await readdir(dirPath);
    const layerCount = files.filter(f => f.endsWith(".json")).length;
    index.stats.by_layer[layer] = layerCount;
    index.stats.total += layerCount;

    for (const file of files) {
      if (!file.endsWith(".json")) continue;
      const id = file.replace(".json", "");
      index.nodes[id] = { layer, file: `nodes/${layer}/${file}` };
    }
  }

  const indexPath = resolve(memoryDir, "graph", "index.json");
  await writeFile(indexPath, JSON.stringify(index, null, 2));
  return index;
}

// ---------------------------------------------------------------------------
// Catalog Generation
// ---------------------------------------------------------------------------

/**
 * Generate a concise Markdown catalog of L2-L5 skill references for agent context.
 *
 * In v3, L2-L5 nodes are stored as .claude/skills/ and discovered natively by the
 * Claude SDK. This catalog provides a brief summary for pre-injection context —
 * agents use native skill invocation (not load_memory) to access full content.
 */
export async function generateCatalog(memoryDir) {
  const lines = [];

  // L2-L5 nodes in the graph are lightweight references to skills
  const layerInfo = [
    { layer: 2, label: "Sub-Structure Patterns", sublabel: "Learned compositions of operators for recurring structural motifs" },
    { layer: 3, label: "Operator Techniques", sublabel: "Atomic building-block techniques" },
    { layer: 4, label: "Optimization Strategies", sublabel: "High-level decision guides" },
    { layer: 5, label: "Performance Principles", sublabel: "Cross-cutting performance principles" },
  ];

  let hasContent = false;
  for (const { layer, label, sublabel } of layerInfo) {
    const nodes = await getNodesByLayer(layer, memoryDir);
    if (nodes.length > 0) {
      if (!hasContent) {
        lines.push("## Available Optimization Skills");
        lines.push("These are available as agent skills and will be loaded automatically when relevant.\n");
        hasContent = true;
      }
      lines.push(`### ${label}`);
      lines.push(`_${sublabel}_`);
      for (const n of nodes) {
        const summary = n.summary || n.content?.description || "";
        const skillPath = n.skill_path || "";
        lines.push(`- **${n.id}**${skillPath ? ` (${skillPath})` : ""}: ${summary.slice(0, 100)}`);
      }
      lines.push("");
    }
  }

  return hasContent ? lines.join("\n") : "";
}

/**
 * Load a single memory node by ID and format as Markdown for agent consumption.
 */
export async function loadMemoryNode(nodeId, memoryDir) {
  const node = await readNode(nodeId, memoryDir);
  if (!node) return `Node "${nodeId}" not found.`;

  const layerLabels = ["Query Instance", "Query Template", "Sub-Structure Pattern", "Operator Technique", "Optimization Strategy", "Performance Principle"];
  const label = layerLabels[node.layer] || `Layer ${node.layer}`;
  const lines = [`## [${label}] ${node.id}\n`];
  const c = node.content;

  switch (node.layer) {
    case 0: {
      if (c.sql) lines.push(`**SQL**: \`${c.sql.slice(0, 200)}\``);
      lines.push(`**Benchmark**: ${c.benchmark || "?"}, SF${c.scale_factor || "?"}`);
      lines.push(`**Best timing**: ${c.best_timing_ms ? Math.round(c.best_timing_ms) + "ms" : "N/A"}`);
      if (c.best_cpp_path) lines.push(`**Best C++ path**: ${c.best_cpp_path}`);
      if (c.key_optimizations?.length > 0) {
        lines.push(`**Key optimizations**: ${c.key_optimizations.join(", ")}`);
      }
      if (c.optimization_trajectory?.length > 0) {
        lines.push("\n**Optimization trajectory**:");
        for (const t of c.optimization_trajectory) {
          const timing = t.timing_ms ? `${Math.round(t.timing_ms)}ms` : "N/A";
          lines.push(`- Iter ${t.iter}: ${timing} — ${t.strategy || ""}${t.regression ? " (REGRESSION)" : ""}`);
        }
      }
      break;
    }
    case 1: {
      lines.push(`**Template**: ${c.template_name || c.pattern_name || "?"}`);
      if (c.sql_template) lines.push(`**SQL template**: \`${c.sql_template.slice(0, 200)}\``);
      if (c.structural_signature) lines.push(`**Signature**: ${JSON.stringify(c.structural_signature)}`);
      if (c.proven_strategies?.length > 0) lines.push(`**Proven strategies**: ${c.proven_strategies.join(", ")}`);
      if (c.anti_patterns?.length > 0) lines.push(`**Anti-patterns**: ${c.anti_patterns.join(", ")}`);
      break;
    }
    case 2: {
      // L2: Sub-Structure Patterns (skill-backed in v3)
      if (n.skill_path) lines.push(`**Skill**: ${n.skill_path}`);
      if (c.description) lines.push(`**Description**: ${c.description}`);
      if (c.operators?.length > 0) lines.push(`**Operators**: ${c.operators.join(", ")}`);
      if (c.preconditions?.length > 0) lines.push(`**Preconditions**: ${c.preconditions.join("; ")}`);
      if (c.source_examples?.length > 0) lines.push(`**Source examples**: ${c.source_examples.join(", ")}`);
      break;
    }
    case 3: {
      // L3: Operator Techniques (skill-backed in v3, was L2 in v2)
      if (n.skill_path) lines.push(`**Skill**: ${n.skill_path}`);
      lines.push(`**Category**: ${c.category || c.operator || "?"}`);
      lines.push(`**Name**: ${c.operator_name || c.sub_type || "?"}`);
      if (c.description) lines.push(`**Description**: ${c.description}`);
      if (c.preconditions?.length > 0) lines.push(`**Preconditions**: ${c.preconditions.join("; ")}`);
      if (c.code_pattern) lines.push(`\n**Code pattern**:\n\`\`\`cpp\n${c.code_pattern}\n\`\`\``);
      if (c.performance_characteristics) {
        const pc = c.performance_characteristics;
        if (pc.observed_speedups) lines.push(`**Observed speedups**: ${pc.observed_speedups}`);
        if (pc.memory_footprint) lines.push(`**Memory footprint**: ${pc.memory_footprint}`);
        if (pc.cache_behavior) lines.push(`**Cache behavior**: ${pc.cache_behavior}`);
      }
      if (c.source_examples?.length > 0) lines.push(`**Source examples**: ${c.source_examples.join(", ")}`);
      break;
    }
    case 4: {
      // L4: Optimization Strategies (skill-backed in v3, was L3 in v2)
      if (n.skill_path) lines.push(`**Skill**: ${n.skill_path}`);
      lines.push(`**Strategy**: ${c.strategy_name || "?"}`);
      if (c.problem_signature) lines.push(`**Problem**: ${c.problem_signature}`);
      if (c.solution) lines.push(`**Solution**: ${c.solution}`);
      if (c.applicability) lines.push(`**When to use**: ${c.applicability}`);
      if (c.trade_offs) lines.push(`**Trade-offs**: ${c.trade_offs}`);
      if (c.evidence?.length > 0) {
        lines.push("\n**Evidence**:");
        for (const e of c.evidence.slice(0, 5)) {
          lines.push(`- ${e.query}: ${e.before_ms}ms → ${e.after_ms}ms (${e.speedup})`);
        }
      }
      lines.push(`**Confidence**: ${c.confidence || "?"}, Evidence count: ${c.evidence_count || 0}`);
      break;
    }
    case 5: {
      // L5: Performance Principles (skill-backed in v3, was L4 in v2)
      if (n.skill_path) lines.push(`**Skill**: ${n.skill_path}`);
      lines.push(`**Principle**: ${c.principle || "?"}`);
      if (c.reasoning) lines.push(`**Reasoning**: ${c.reasoning}`);
      if (c.derived_from?.length > 0) lines.push(`**Derived from**: ${c.derived_from.join(", ")}`);
      if (c.counter_examples?.length > 0) lines.push(`**Counter-examples**: ${c.counter_examples.join("; ")}`);
      if (c.applies_when) lines.push(`**Applies when**: ${c.applies_when}`);
      break;
    }
  }

  if (node.tags?.length > 0) lines.push(`\n**Tags**: ${node.tags.join(", ")}`);

  return lines.join("\n");
}
