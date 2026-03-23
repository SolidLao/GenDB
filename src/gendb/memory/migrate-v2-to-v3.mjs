#!/usr/bin/env node
/**
 * Migration script: HAG v2 (5-layer) → v3 (6-layer).
 *
 * Renumbers:
 *   v2 L2 (operators)   → v3 L3
 *   v2 L3 (strategies)  → v3 L4
 *   v2 L4 (principles)  → v3 L5
 *
 * Creates lightweight skill-reference nodes for L3/L4/L5.
 * L2 (sub-structure patterns) starts empty — synthesized by Memory Manager.
 *
 * Usage:
 *   node src/gendb/memory/migrate-v2-to-v3.mjs [--memory-dir gendb-memory] [--dry-run]
 */

import { readFile, writeFile, readdir, mkdir, rename, unlink } from "fs/promises";
import { resolve, basename } from "path";
import { existsSync } from "fs";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const memoryDirIdx = args.indexOf("--memory-dir");
const memoryDir = memoryDirIdx >= 0 ? args[memoryDirIdx + 1] : "gendb-memory";

const SKILL_SUBDIRS = { 3: "operators", 4: "strategies", 5: "principles" };

async function migrate() {
  console.log(`[Migration] HAG v2 → v3 (memory: ${memoryDir}, dry-run: ${dryRun})`);

  // Check config version
  const configPath = resolve(memoryDir, "config.json");
  if (existsSync(configPath)) {
    const config = JSON.parse(await readFile(configPath, "utf-8"));
    if (config.schemaVersion >= 3) {
      console.log("[Migration] Already at v3 or later. Nothing to do.");
      return;
    }
  }

  const nodesDir = resolve(memoryDir, "graph", "nodes");

  // 1. Renumber nodes: L2→L3, L3→L4, L4→L5 (process in reverse to avoid collisions)
  const renameMap = [
    { from: "L4", to: "L5" },
    { from: "L3", to: "L4" },
    { from: "L2", to: "L3" },
  ];

  for (const { from, to } of renameMap) {
    const fromDir = resolve(nodesDir, from);
    const toDir = resolve(nodesDir, to);

    if (!existsSync(fromDir)) {
      console.log(`[Migration] ${from}/ does not exist, skipping.`);
      continue;
    }

    await mkdir(toDir, { recursive: true });
    const files = (await readdir(fromDir)).filter(f => f.endsWith(".json"));
    console.log(`[Migration] Moving ${files.length} nodes from ${from} → ${to}`);

    for (const file of files) {
      const node = JSON.parse(await readFile(resolve(fromDir, file), "utf-8"));

      // Update node layer
      const oldLayer = node.layer;
      const newLayer = oldLayer + 1; // L2→L3, L3→L4, L4→L5
      node.layer = newLayer;

      // Update node ID prefix
      const oldId = node.id;
      const newId = oldId.replace(/^L\d+/, `L${newLayer}`);
      node.id = newId;

      // Add skill reference for L3/L4/L5
      const skillSubdir = SKILL_SUBDIRS[newLayer];
      if (skillSubdir) {
        const skillName = newId.replace(`L${newLayer}_`, "").replace(/_/g, "-");
        node.skill_path = `.claude/skills/${skillSubdir}/${skillName}/`;
        node.summary = node.content?.description || node.content?.strategy_name || node.content?.principle || "";
      }

      node.migrated_from = { version: "v2", layer: oldLayer, id: oldId };
      node.updated_at = new Date().toISOString();

      const newFile = `${newId}.json`;
      console.log(`  ${file} → ${to}/${newFile} (layer ${oldLayer} → ${newLayer})`);

      if (!dryRun) {
        await writeFile(resolve(toDir, newFile), JSON.stringify(node, null, 2));
        // Remove old file (only if source and target dirs differ)
        if (from !== to) {
          await unlink(resolve(fromDir, file));
        }
      }
    }
  }

  // 2. Create empty L2 directory for sub-structure patterns
  const l2Dir = resolve(nodesDir, "L2");
  if (!existsSync(l2Dir)) {
    console.log("[Migration] Creating empty L2/ for sub-structure patterns.");
    if (!dryRun) await mkdir(l2Dir, { recursive: true });
  }

  // 3. Update edges: renumber references
  const edgesPath = resolve(memoryDir, "graph", "edges.json");
  if (existsSync(edgesPath)) {
    const edges = JSON.parse(await readFile(edgesPath, "utf-8"));
    let edgesUpdated = 0;

    const renumberId = (id) => {
      if (!id || typeof id !== "string") return id;
      const match = id.match(/^L(\d+)_(.*)/);
      if (!match) return id;
      const layer = parseInt(match[1]);
      if (layer >= 2) {
        return `L${layer + 1}_${match[2]}`;
      }
      return id;
    };

    const renameEdgeType = (type) => {
      // v2: instance_of (L0→L1), uses_operator (L1→L2), implements_strategy (L2→L3)
      // v3: instance_of (L0→L1), uses_operator (L1→L3), implements_strategy (L3→L4)
      // Note: exhibits_pattern (L1→L2) is NEW and will be added by Memory Manager
      return type;
    };

    for (const edge of edges) {
      // Edges may use "from"/"to" or "source"/"target" — handle both
      const srcKey = edge.source !== undefined ? "source" : "from";
      const tgtKey = edge.target !== undefined ? "target" : "to";
      const newSource = renumberId(edge[srcKey]);
      const newTarget = renumberId(edge[tgtKey]);
      if (newSource !== edge[srcKey] || newTarget !== edge[tgtKey]) {
        edge[srcKey] = newSource;
        edge[tgtKey] = newTarget;
        edge.type = renameEdgeType(edge.type);
        edgesUpdated++;
      }
    }

    console.log(`[Migration] Updated ${edgesUpdated} edges.`);
    if (!dryRun) {
      await writeFile(edgesPath, JSON.stringify(edges, null, 2));
    }
  }

  // 4. Update config
  if (!dryRun) {
    const config = existsSync(configPath) ? JSON.parse(await readFile(configPath, "utf-8")) : {};
    config.schemaVersion = 3;
    config.updatedAt = new Date().toISOString();
    config.skillsDir = ".claude/skills";
    config.migration = { from: "v2", to: "v3", migratedAt: new Date().toISOString() };
    await writeFile(configPath, JSON.stringify(config, null, 2));
  }

  // 5. Rebuild index
  if (!dryRun) {
    const { updateIndex } = await import("./graph.mjs");
    const index = await updateIndex(memoryDir);
    console.log(`[Migration] Index rebuilt: ${index.stats.total} nodes`);
    console.log(`  Layers: ${JSON.stringify(index.stats.by_layer)}`);
  }

  console.log(`[Migration] Done.${dryRun ? " (DRY RUN — no changes written)" : ""}`);
}

migrate().catch(err => {
  console.error(`[Migration] Error: ${err.message}`);
  process.exit(1);
});
