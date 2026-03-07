/**
 * Claude Agent SDK provider for GenDB.
 * Wraps @anthropic-ai/claude-agent-sdk query() function.
 */

import { query } from "@anthropic-ai/claude-agent-sdk";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { formatDuration, estimateCost } from "../shared.mjs";

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
  const providerCfg = getProviderConfig("claude");
  const effortLevel = configName && providerCfg.agentEffortLevels[configName];

  console.log(`\n[${"=".repeat(60)}]`);
  console.log(`[Orchestrator] Spawning agent: ${name} (provider: claude, timeout: ${formatDuration(timeout)}${effortLevel ? `, effort: ${effortLevel}` : ''})`);
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
    costUsd = estimateCost(model || providerCfg.model, tokens);
  }

  if (agentError) {
    console.error(`\n[Orchestrator] Agent "${name}" failed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)}): ${agentError}`);
    return { result: resultText, durationMs, tokens, costUsd, error: agentError };
  }

  console.log(`\n[Orchestrator] Agent "${name}" completed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)})`);
  return { result: resultText, durationMs, tokens, costUsd };
}
