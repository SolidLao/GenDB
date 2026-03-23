/**
 * load_memory tool — agent-driven runtime memory access.
 *
 * Agents call load_memory("L2_direct_array_agg") to get full node content.
 * Can also be invoked as CLI:
 *   node src/gendb/memory/search-tool.mjs --id <node_id> --memory-dir gendb-memory/
 *   node src/gendb/memory/search-tool.mjs --query "..." --memory-dir gendb-memory/
 */

import { resolve } from "path";
import { loadMemoryNode, readNode, getAllNodes, getNodesByLayer } from "./graph.mjs";

// ---------------------------------------------------------------------------
// Tool definition (for agent SDK registration)
// ---------------------------------------------------------------------------

/**
 * Get the load_memory tool definition for Claude agent SDK.
 * The handler reads the specified node and returns formatted content.
 */
export function getLoadMemoryToolDefinition(memoryDir) {
  return {
    name: "load_memory",
    description: "Load full details of a knowledge node from GenDB's memory system. Use the node ID from the catalog (e.g., 'L2_direct_array_agg', 'L3_bandwidth_reduction'). Returns the complete node content including code patterns, strategies, evidence, and performance data.",
    input_schema: {
      type: "object",
      properties: {
        node_id: {
          type: "string",
          description: "The ID of the memory node to load (e.g., 'L2_direct_array_agg', 'L3_bandwidth_reduction', 'L4_bandwidth_vs_compute')",
        },
      },
      required: ["node_id"],
    },
  };
}

/**
 * Handle a load_memory tool call.
 * @param {object} input - Tool input with node_id
 * @param {string} memoryDir - Path to memory directory
 * @returns {string} Formatted node content
 */
export async function handleLoadMemory(input, memoryDir) {
  const { node_id } = input;
  if (!node_id) return "Error: node_id is required.";
  return await loadMemoryNode(node_id, memoryDir);
}

// ---------------------------------------------------------------------------
// Search function (keyword-based fallback for CLI)
// ---------------------------------------------------------------------------

/**
 * Search memory nodes by keyword (simple text matching).
 * Used as CLI fallback when no node_id is provided.
 */
export async function searchMemory(queryText, layers, tags, memoryDir, config = {}) {
  const maxResults = config.vectorSearchK || 10;
  let candidates = await getAllNodes(memoryDir);

  // Layer filter
  if (layers && layers.length > 0) {
    candidates = candidates.filter(n => layers.includes(n.layer));
  }

  // Tag filter
  if (tags && tags.length > 0) {
    candidates = candidates.filter(n =>
      n.tags?.some(t => tags.includes(t))
    );
  }

  // Text matching score
  const queryWords = queryText.toLowerCase().split(/\s+/);
  const scored = candidates.map(node => {
    const text = (node.embedding_text || JSON.stringify(node.content)).toLowerCase();
    const matchCount = queryWords.filter(w => text.includes(w)).length;
    return { node, score: matchCount / queryWords.length };
  }).filter(({ score }) => score > 0.2);

  scored.sort((a, b) => b.score - a.score);
  const results = scored.slice(0, maxResults);

  if (results.length === 0) {
    return "No relevant memory entries found.";
  }

  const lines = [`## Memory Search Results (${results.length} matches)\n`];
  for (const { node, score } of results) {
    const layerLabels = ["Query Instance", "Query Template", "Operator Pattern", "Optimization Strategy", "Performance Principle"];
    const label = layerLabels[node.layer] || `Layer ${node.layer}`;
    const relevance = (score * 100).toFixed(0);
    const summary = node.content?.strategy_name || node.content?.template_name || node.content?.operator_name || node.content?.principle || node.id;
    lines.push(`- [${label}] **${node.id}** (${relevance}%): ${summary}`);
  }

  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

async function main() {
  const args = process.argv.slice(2);
  let nodeId = "";
  let queryText = "";
  let layers = null;
  let tags = null;
  let memoryDir = "gendb-memory";

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--id" && args[i + 1]) nodeId = args[++i];
    if (args[i] === "--query" && args[i + 1]) queryText = args[++i];
    if (args[i] === "--layers" && args[i + 1]) layers = args[++i].split(",").map(Number);
    if (args[i] === "--tags" && args[i + 1]) tags = args[++i].split(",");
    if (args[i] === "--memory-dir" && args[i + 1]) memoryDir = args[++i];
  }

  if (nodeId) {
    const result = await handleLoadMemory({ node_id: nodeId }, memoryDir);
    console.log(result);
  } else if (queryText) {
    const result = await searchMemory(queryText, layers, tags, memoryDir);
    console.log(result);
  } else {
    console.error("Usage:");
    console.error("  node search-tool.mjs --id <node_id> [--memory-dir <path>]");
    console.error("  node search-tool.mjs --query <text> [--layers 2,3] [--tags scan] [--memory-dir <path>]");
    process.exit(1);
  }
}

const isMain = process.argv[1]?.endsWith("search-tool.mjs");
if (isMain) {
  main().catch(err => {
    console.error(`Error: ${err.message}`);
    process.exit(1);
  });
}
