/**
 * Shared utilities for GenDB orchestrator and single-agent mode.
 * Extracted from orchestrator.mjs to avoid duplication.
 */

import { readFile } from "fs/promises";
import { defaults } from "./gendb.config.mjs";
import { getProvider, setAgentProvider, getAgentProviderName, getAvailableProviders } from "./providers/index.mjs";

// Re-export provider management functions for use by orchestrator/single
export { setAgentProvider, getAgentProviderName, getAvailableProviders };

/**
 * Simple template rendering: replace {{key}} with values, handle {{#if key}}...{{/if}} blocks.
 */
export function renderTemplate(template, vars) {
  // Handle {{#if key}}...{{/if}} blocks
  let result = template.replace(/\{\{#if\s+(\w+)\}\}([\s\S]*?)\{\{\/if\}\}/g, (match, key, content) => {
    const val = vars[key];
    if (val && val !== '' && val !== false && val !== null && val !== undefined) {
      return content;
    }
    return '';
  });
  // Replace {{key}} placeholders
  result = result.replace(/\{\{(\w+)\}\}/g, (match, key) => {
    const val = vars[key];
    return val !== undefined && val !== null ? String(val) : '';
  });
  // Clean up excessive blank lines
  result = result.replace(/\n{4,}/g, '\n\n\n');
  return result;
}

// ---------------------------------------------------------------------------
// Model pricing (per million tokens)
// ---------------------------------------------------------------------------

export const MODEL_PRICING = {
  sonnet: { input: 3, output: 15, cache_read: 0.30, cache_creation: 3.75 },
  haiku: { input: 0.80, output: 4, cache_read: 0.08, cache_creation: 1 },
  opus: { input: 15, output: 75, cache_read: 1.50, cache_creation: 18.75 },
};

export function estimateCost(model, tokens) {
  const pricing = MODEL_PRICING[model] || MODEL_PRICING.sonnet;
  const perM = 1_000_000;
  return (tokens.input * pricing.input) / perM
    + (tokens.output * pricing.output) / perM
    + ((tokens.cache_read || 0) * pricing.cache_read) / perM
    + ((tokens.cache_creation || 0) * pricing.cache_creation) / perM;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export function formatDuration(ms) {
  const totalSec = Math.floor(ms / 1000);
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return min > 0 ? `${min}m ${sec}s` : `${sec}s`;
}

/** Safely read and parse a JSON file, returning null on failure. */
export async function readJSON(path) {
  try {
    return JSON.parse(await readFile(path, "utf-8"));
  } catch {
    return null;
  }
}

/** Parse queries.sql into individual queries with IDs. */
export function parseQueryFile(queriesText) {
  const queries = [];
  // Split on query separators: lines starting with "-- Q" or numbered query comments
  const parts = queriesText.split(/(?=--\s*(?:Q|Query)\s*\d)/i);
  let queryNum = 1;
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed || !trimmed.includes("SELECT")) continue;

    // Try to extract query ID from comment
    const idMatch = trimmed.match(/--\s*(?:Q|Query)\s*(\d+)/i);
    const id = idMatch ? `Q${idMatch[1]}` : `Q${queryNum}`;
    queries.push({ id, sql: trimmed });
    queryNum++;
  }

  // Fallback: if no queries found via comments, split by semicolons
  if (queries.length === 0) {
    const stmts = queriesText.split(";").filter(s => s.trim().toUpperCase().includes("SELECT"));
    for (let i = 0; i < stmts.length; i++) {
      queries.push({ id: `Q${i + 1}`, sql: stmts[i].trim() + ";" });
    }
  }

  return queries;
}

/**
 * Invoke an LLM agent via the active provider (Claude, Codex, etc.).
 * Returns { result, durationMs, tokens, costUsd, error? }.
 * Always returns cost/token data even on timeout or error.
 */
export async function runAgent(name, options) {
  const provider = await getProvider();
  return provider.runAgent(name, options);
}
