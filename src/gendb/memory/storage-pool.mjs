/**
 * Storage Pool — content-addressed storage reuse across runs.
 *
 * Enables reuse of gendb/ directories when running the same benchmark
 * at the same scale factor. Avoids re-running Phase 1 data ingestion.
 */

import { readFile, writeFile, mkdir, symlink, readlink, rm } from "fs/promises";
import { resolve } from "path";
import { existsSync } from "fs";
import { createHash } from "crypto";

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/** Compute a content-addressed hash for a storage configuration. */
export function computeStorageHash(benchmark, scaleFactor, schemaContent) {
  const hash = createHash("sha256");
  hash.update(`benchmark:${benchmark}`);
  hash.update(`sf:${scaleFactor}`);
  hash.update(`schema:${schemaContent.trim()}`);
  return hash.digest("hex").slice(0, 16);
}

/** Compute a hash for the storage design (includes column encodings, indexes, etc.). */
export function computeDesignHash(storageDesign) {
  const hash = createHash("sha256");
  hash.update(JSON.stringify(storageDesign, null, 0));
  return hash.digest("hex").slice(0, 16);
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

async function readRegistry(memoryDir) {
  const registryPath = resolve(memoryDir, "storage-pool", "registry.json");
  try {
    return JSON.parse(await readFile(registryPath, "utf-8"));
  } catch {
    return { entries: {} };
  }
}

async function writeRegistry(registry, memoryDir) {
  const poolDir = resolve(memoryDir, "storage-pool");
  await mkdir(poolDir, { recursive: true });
  const registryPath = resolve(poolDir, "registry.json");
  await writeFile(registryPath, JSON.stringify(registry, null, 2));
}

// ---------------------------------------------------------------------------
// Lookup & Register
// ---------------------------------------------------------------------------

/**
 * Look up existing storage for a benchmark+SF combination.
 * Returns { gendbPath, designHash, runId } if found, null otherwise.
 */
export async function lookupStorage(benchmark, scaleFactor, schemaContent, memoryDir) {
  if (!memoryDir) return null;

  const storageHash = computeStorageHash(benchmark, scaleFactor, schemaContent);
  const registry = await readRegistry(memoryDir);
  const entry = registry.entries[storageHash];

  if (!entry) return null;

  // Verify the storage directory still exists
  if (!existsSync(entry.gendbPath)) {
    console.warn(`[Memory] Storage pool entry for ${storageHash} points to missing directory: ${entry.gendbPath}`);
    return null;
  }

  return {
    gendbPath: entry.gendbPath,
    designHash: entry.designHash,
    runId: entry.runId,
    storageHash,
  };
}

/**
 * Register a storage directory in the pool.
 */
export async function registerStorage(benchmark, scaleFactor, schemaContent, storageDesign, gendbDir, runId, memoryDir) {
  if (!memoryDir) return;

  const storageHash = computeStorageHash(benchmark, scaleFactor, schemaContent);
  const designHash = computeDesignHash(storageDesign);
  const registry = await readRegistry(memoryDir);

  registry.entries[storageHash] = {
    benchmark,
    scaleFactor,
    storageHash,
    designHash,
    gendbPath: gendbDir,
    runId,
    registeredAt: new Date().toISOString(),
  };

  await writeRegistry(registry, memoryDir);
  console.log(`[Memory] Registered storage in pool: ${storageHash} → ${gendbDir}`);
}

/**
 * Create a symlink from target gendb dir to the pooled storage.
 * Returns true on success.
 */
export async function symlinkStorage(sourceGendbDir, targetGendbDir) {
  try {
    // Check if target already exists
    if (existsSync(targetGendbDir)) {
      // Check if it's already a symlink to the source
      try {
        const linkTarget = await readlink(targetGendbDir);
        if (linkTarget === sourceGendbDir) {
          console.log(`[Memory] Storage symlink already exists: ${targetGendbDir} → ${sourceGendbDir}`);
          return true;
        }
        // Symlink to a different source — replace it
        console.log(`[Memory] Replacing existing symlink (pointed to ${linkTarget})`);
        await rm(targetGendbDir);
      } catch {
        // Not a symlink — it's a real directory (e.g. from a killed run). Replace it.
        console.log(`[Memory] Removing existing gendb directory to create symlink: ${targetGendbDir}`);
        await rm(targetGendbDir, { recursive: true });
      }
    }

    await symlink(sourceGendbDir, targetGendbDir);
    console.log(`[Memory] Created storage symlink: ${targetGendbDir} → ${sourceGendbDir}`);
    return true;
  } catch (err) {
    console.error(`[Memory] Failed to create storage symlink: ${err.message}`);
    return false;
  }
}
