/**
 * GenDB Memory System v3 — public API.
 *
 * v3 architecture: 6-layer HAG with skill-based knowledge delivery.
 * - L0/L1: HAG (structural retrieval via SQL features)
 * - L2-L5: Skills in .claude/skills/ (semantic retrieval via Claude)
 *
 * Exports:
 * - initMemory: Initialize/validate memory directory + skill dirs
 * - classifyQuery: 3-tier query classification (exact/structural/novel)
 * - retrieveContext: Pre-inject memory context for agents (L0/L1 only)
 * - generateCatalog: L2-L5 skill reference catalog
 * - loadMemoryNode: Load full node content by ID
 *
 * Deprecated (kept for backward compat, will be removed):
 * - searchMemory, getLoadMemoryToolDefinition, handleLoadMemory
 *   (replaced by native Claude skill system)
 */

import { resolve } from "path";
import { existsSync } from "fs";
import { mkdir, writeFile, readFile } from "fs/promises";
import { initGraphDirs, readIndex, generateCatalog, loadMemoryNode } from "./graph.mjs";
import { classifyQuery, retrieveContext, patchL1Signatures } from "./retrieval.mjs";

// Re-export for convenience
export {
  classifyQuery,
  retrieveContext,
  patchL1Signatures,
  generateCatalog,
  loadMemoryNode,
};

// Deprecated: load_memory tool replaced by native Claude skills in v3
// Keep exports for backward compatibility during migration
export { searchMemory, getLoadMemoryToolDefinition, handleLoadMemory } from "./search-tool.mjs";

/**
 * Initialize the memory system.
 * Creates directory structure and config if they don't exist.
 *
 * @param {string} memoryDir - Path to memory directory
 * @param {object} config - Memory config from gendb.config
 * @returns {boolean} true if memory system is ready
 */
export async function initMemory(memoryDir, config = {}) {
  if (!memoryDir) return false;

  try {
    await mkdir(memoryDir, { recursive: true });

    // Initialize graph directories (6 layers in v3)
    await initGraphDirs(memoryDir);

    // Initialize skill directory (flat structure — SDK discovers .claude/skills/<name>/SKILL.md)
    const skillsDir = config.skillsDir || ".claude/skills";
    await mkdir(skillsDir, { recursive: true });

    // Write/update config.json
    const configPath = resolve(memoryDir, "config.json");
    const memConfig = {
      schemaVersion: 3,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      skillsDir,
    };

    if (existsSync(configPath)) {
      try {
        const existing = JSON.parse(await readFile(configPath, "utf-8"));
        memConfig.createdAt = existing.createdAt;
      } catch {}
    }

    const index = await readIndex(memoryDir);
    memConfig.stats = index.stats;
    await writeFile(configPath, JSON.stringify(memConfig, null, 2));

    console.log(`[Memory v3] Initialized at ${memoryDir} (${index.stats.total} nodes, skills: ${skillsDir})`);
    return true;
  } catch (err) {
    console.error(`[Memory] Failed to initialize: ${err.message}`);
    return false;
  }
}

/**
 * Get a summary of memory contents for logging.
 */
export async function getMemorySummary(memoryDir) {
  if (!memoryDir || !existsSync(memoryDir)) return "Memory: disabled";

  const index = await readIndex(memoryDir);
  const layerCounts = Object.entries(index.stats.by_layer || {})
    .map(([layer, count]) => `${layer}:${count}`)
    .join(", ");

  return `Memory: ${index.stats.total} nodes (${layerCounts || "empty"})`;
}
