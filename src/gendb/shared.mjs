/**
 * Shared utilities for GenDB orchestrator and single-agent mode.
 * Extracted from orchestrator.mjs to avoid duplication.
 */

import { query } from "@anthropic-ai/claude-agent-sdk";
import { readFile } from "fs/promises";
import { defaults } from "./gendb.config.mjs";

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
 * Invoke a Claude Code agent via the Agent SDK.
 * Returns { result, durationMs, tokens, costUsd, error? }.
 * Always returns cost/token data even on timeout or error.
 */
export async function runAgent(name, { systemPrompt, userPrompt, allowedTools, model, cwd, timeoutMs, configName, useSkills, domainSkillsPrompt, verbose = false }) {
  // Filter Skill from allowedTools if skills disabled
  const effectiveTools = useSkills === false
    ? allowedTools.filter(t => t !== "Skill")
    : allowedTools;

  // Inject domain skills section into system prompt if enabled
  const effectivePrompt = (useSkills !== false && domainSkillsPrompt)
    ? systemPrompt + "\n\n" + domainSkillsPrompt
    : systemPrompt;

  const timeout = timeoutMs || defaults.agentTimeoutMs;
  const effortLevel = configName && defaults.agentEffortLevels[configName];

  console.log(`\n[${"=".repeat(60)}]`);
  console.log(`[Orchestrator] Spawning agent: ${name} (timeout: ${formatDuration(timeout)}${effortLevel ? `, effort: ${effortLevel}` : ''})`);
  console.log(`[${"=".repeat(60)}]\n`);

  const startTime = Date.now();
  const abortController = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    console.error(`\n[Orchestrator] Agent "${name}" timed out after ${formatDuration(timeout)}, aborting...`);
    abortController.abort();
  }, timeout);

  let resultText = "";
  let tokens = { input: 0, output: 0, cache_read: 0, cache_creation: 0 };
  let costUsd = 0;
  let agentError = null;

  try {
    const agentQuery = query({
      prompt: userPrompt,
      options: {
        systemPrompt: effectivePrompt,
        allowedTools: effectiveTools,
        model: model || undefined,
        cwd: cwd || process.cwd(),
        permissionMode: "bypassPermissions",
        allowDangerouslySkipPermissions: true,
        abortController,
        effort: effortLevel || undefined,
        maxOutputTokens: 64000,
      },
    });

    for await (const message of agentQuery) {
      if (verbose) {
        if (message.type === "assistant") {
          for (const block of message.message.content) {
            if (block.type === "text" && block.text) {
              console.log(`[${name}] ${block.text.slice(0, 200)}`);
            }
            if (block.type === "tool_use") {
              console.log(`[${name}] Tool: ${block.name}${block.input?.command ? ': ' + String(block.input.command).slice(0, 120) : block.input?.file_path ? ': ' + block.input.file_path : ''}`);
            }
          }
        }
      }

      if (message.type === "result") {
        costUsd = message.total_cost_usd || 0;
        if (message.usage) {
          tokens = {
            input: message.usage.input_tokens || 0,
            output: message.usage.output_tokens || 0,
            cache_read: message.usage.cache_read_input_tokens || 0,
            cache_creation: message.usage.cache_creation_input_tokens || 0,
          };
        }
        if (message.subtype === "success") {
          resultText = message.result || "";
        } else {
          agentError = message.errors?.join("; ") || message.subtype;
        }
      }
    }
  } catch (err) {
    agentError = timedOut
      ? `Agent "${name}" timed out after ${formatDuration(timeout)}`
      : `Agent "${name}" failed: ${err.message}`;
  } finally {
    clearTimeout(timer);
  }

  const durationMs = Date.now() - startTime;

  // Fallback cost estimation if SDK didn't provide it
  if (!costUsd && (tokens.input || tokens.output)) {
    costUsd = estimateCost(model || defaults.model, tokens);
  }

  if (agentError) {
    console.error(`\n[Orchestrator] Agent "${name}" failed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)}): ${agentError}`);
    return { result: resultText, durationMs, tokens, costUsd, error: agentError };
  }

  console.log(`\n[Orchestrator] Agent "${name}" completed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)})`);
  return { result: resultText, durationMs, tokens, costUsd };
}
