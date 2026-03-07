/**
 * OpenAI Codex SDK provider for GenDB.
 * Wraps @openai/codex-sdk to provide the same runAgent interface as the Claude provider.
 *
 * Key mappings from GenDB concepts to Codex SDK:
 *   systemPrompt    → developer_instructions config
 *   userPrompt      → thread.runStreamed(prompt)
 *   allowedTools    → N/A (Codex uses sandbox_mode)
 *   model           → config.model
 *   cwd             → startThread({ workingDirectory })
 *   effort          → config.model_reasoning_effort
 *   permissions     → approval_policy: "never", sandbox_mode: "danger-full-access"
 */

import { Codex } from "@openai/codex-sdk";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { formatDuration } from "../shared.mjs";

export async function runAgent(name, { systemPrompt, userPrompt, allowedTools, model, cwd, timeoutMs, configName, useSkills, domainSkillsPrompt, verbose = false }) {
  // Build the effective system prompt (same logic as Claude provider)
  const effectivePrompt = (useSkills !== false && domainSkillsPrompt)
    ? systemPrompt + "\n\n" + domainSkillsPrompt
    : systemPrompt;

  const timeout = timeoutMs || defaults.agentTimeoutMs;
  const providerCfg = getProviderConfig("codex");
  const codexEffort = (configName && providerCfg.agentEffortLevels[configName]) || "medium";
  const effectiveModel = model || providerCfg.model;

  console.log(`\n[${"=".repeat(60)}]`);
  console.log(`[Orchestrator] Spawning agent: ${name} (provider: codex, model: ${effectiveModel}, timeout: ${formatDuration(timeout)}, effort: ${codexEffort})`);
  console.log(`[${"=".repeat(60)}]\n`);

  const startTime = Date.now();
  let timedOut = false;
  let resultText = "";
  let tokens = { input: 0, output: 0, cache_read: 0, cache_creation: 0 };
  let costUsd = 0;
  let agentError = null;

  let timer;
  const abortController = new AbortController();
  try {
    // developer_instructions goes in the Codex constructor's config
    const codex = new Codex({
      config: { developer_instructions: effectivePrompt },
    });

    // model/sandbox/approval/effort are direct ThreadOptions properties
    const thread = codex.startThread({
      workingDirectory: cwd || process.cwd(),
      skipGitRepoCheck: true,
      model: effectiveModel,
      sandboxMode: "danger-full-access",
      approvalPolicy: "never",
      modelReasoningEffort: codexEffort,
    });

    // Set up timeout with AbortController to actually kill the Codex process
    timer = setTimeout(() => {
      timedOut = true;
      console.error(`\n[Orchestrator] Agent "${name}" timed out after ${formatDuration(timeout)}, aborting...`);
      abortController.abort();
    }, timeout);

    // Use runStreamed to capture progress and usage
    const { events } = await thread.runStreamed(userPrompt, { signal: abortController.signal });

    for await (const event of events) {
      // Collect agent message text from completed items
      if (event.type === "item.completed" && event.item?.type === "agent_message") {
        resultText += event.item.text;
      }

      if (verbose && event.type === "item.completed" && event.item) {
        const item = event.item;
        if (item.type === "agent_message") {
          console.log(`[${name}] ${item.text.slice(0, 200)}`);
        }
        if (item.type === "command_execution") {
          console.log(`[${name}] Command: ${item.command}`);
        }
      }

      // Capture usage from turn.completed events (includes cached_input_tokens)
      if (event.type === "turn.completed" && event.usage) {
        tokens = {
          input: (tokens.input || 0) + (event.usage.input_tokens || 0),
          output: (tokens.output || 0) + (event.usage.output_tokens || 0),
          cache_read: (tokens.cache_read || 0) + (event.usage.cached_input_tokens || 0),
          cache_creation: 0,
        };
      }

      // Surface turn failures with the actual error message
      if (event.type === "turn.failed" && event.error) {
        throw new Error(event.error.message);
      }
    }

  } catch (err) {
    if (!timedOut && err.name === "AbortError") {
      timedOut = true;
    }
    agentError = timedOut
      ? `Agent "${name}" timed out after ${formatDuration(timeout)}`
      : `Agent "${name}" failed: ${err.message}`;
  } finally {
    clearTimeout(timer);
  }

  const durationMs = Date.now() - startTime;

  // Estimate cost for Codex (pricing may differ; use a rough estimate)
  // Codex pricing is not provided via SDK, so we track tokens only
  if (!costUsd && (tokens.input || tokens.output)) {
    costUsd = estimateCodexCost(effectiveModel, tokens);
  }

  if (agentError) {
    console.error(`\n[Orchestrator] Agent "${name}" failed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)}): ${agentError}`);
    return { result: resultText, durationMs, tokens, costUsd, error: agentError };
  }

  console.log(`\n[Orchestrator] Agent "${name}" completed (${formatDuration(durationMs)}, ${tokens.input + tokens.output} tokens, $${costUsd.toFixed(2)})`);
  return { result: resultText, durationMs, tokens, costUsd };
}

/**
 * Rough cost estimation for Codex models.
 * Update these rates as OpenAI publishes official pricing.
 */
function estimateCodexCost(model, tokens) {
  // Pricing per million tokens (from https://developers.openai.com/api/docs/pricing)
  const CODEX_PRICING = {
    "gpt-5.4":           { input: 2.50, cached: 0.25, output: 15 },
    "gpt-5.3-codex":     { input: 1.75, cached: 0.175, output: 14 },
    "gpt-5.2-codex":     { input: 1.75, cached: 0.175, output: 14 },
    "gpt-5.1-codex-max": { input: 1.25, cached: 0.125, output: 10 },
    "gpt-5.1-codex":     { input: 1.25, cached: 0.125, output: 10 },
    "gpt-5-codex":       { input: 1.25, cached: 0.125, output: 10 },
  };
  const pricing = CODEX_PRICING[model] || { input: 1.75, cached: 0.175, output: 14 };
  const perM = 1_000_000;
  const nonCachedInput = tokens.input - (tokens.cache_read || 0);
  return (nonCachedInput * pricing.input) / perM
    + ((tokens.cache_read || 0) * pricing.cached) / perM
    + (tokens.output * pricing.output) / perM;
}
