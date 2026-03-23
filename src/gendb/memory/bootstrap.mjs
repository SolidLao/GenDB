/**
 * Bootstrap Script for GenDB Memory System v2.
 *
 * DEPRECATED: Legacy bootstrap (experience.md parsing, skill file parsing) has been removed.
 * Use `populate.mjs` for LLM-based population from actual run outputs.
 *
 * This file is kept only for backward compatibility.
 * Usage: node src/gendb/memory/populate.mjs --runs <run1>,<run2>,...
 */

import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function bootstrap(options = {}) {
  console.log("[Bootstrap] Legacy bootstrap has been removed in Memory System v2.");
  console.log("[Bootstrap] Use 'node src/gendb/memory/populate.mjs --runs <paths>' instead.");
  console.log("[Bootstrap] See the plan in the codebase for details.");
}

// CLI entry point
async function main() {
  console.log("[Bootstrap] Legacy bootstrap has been removed in Memory System v2.");
  console.log("[Bootstrap] Use 'node src/gendb/memory/populate.mjs --runs <paths>' instead.");
  process.exit(0);
}

const isMain = process.argv[1]?.endsWith("bootstrap.mjs");
if (isMain) {
  main().catch(err => {
    console.error(`[Bootstrap] Fatal error: ${err.message}`);
    process.exit(1);
  });
}

export { bootstrap };
