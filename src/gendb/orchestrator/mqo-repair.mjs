/**
 * One-shot compile repair: if the initial MQO artifact fails to build, feed
 * the compiler errors back to the Batch Code Generator for a single repair
 * pass. Used by buildMqoArtifactWithRepair() in mqo-build.mjs.
 *
 * Kept separate so the main build path doesn't pull in the LLM agent when
 * not needed (e.g., in the optimizer's happy path).
 */

import { readFile } from "fs/promises";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { runAgent } from "../shared.mjs";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { config as batchCodeGenConfig } from "../agents/mqo-mode/batch-code-generator/index.mjs";
import { getMqoDir } from "../utils/paths.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function getAgentModel(configName, args) {
  if (args.modelOverride) return args.modelOverride;
  const providerCfg = getProviderConfig(args.agentProvider);
  return providerCfg.agentModels[configName] || args.model || providerCfg.model;
}

function getAgentTimeout(configName) {
  return defaults.agentTimeoutOverrides?.[configName] || defaults.agentTimeoutMs;
}

export async function repairBatchCodeGeneratorOutput({ args, buildStdout, buildStderr }) {
  const mqoDir = getMqoDir(args.runAuditDir);
  const systemPrompt = await readFile(batchCodeGenConfig.promptPath, "utf-8");

  const userPrompt =
    `# MQO Artifact Build Failed — Repair Pass\n\n` +
    `The fused MQO artifact at \`${mqoDir}\` failed to compile. Your task: read the build output, ` +
    `identify the root cause, and edit ONLY the files needed to fix the compile error(s). ` +
    `You may edit \`${mqoDir}/mqo_main.cpp\`, any \`${mqoDir}/stages/*.hpp\`, and the \`${mqoDir}/Makefile\`.\n\n` +
    `## Build stdout (truncated)\n\n\`\`\`\n${(buildStdout || "").slice(-4000)}\n\`\`\`\n\n` +
    `## Build stderr (truncated)\n\n\`\`\`\n${(buildStderr || "").slice(-6000)}\n\`\`\`\n\n` +
    `Use the Read/Edit tools to make minimal, targeted fixes. Return a one-line summary of what you changed.\n`;

  const result = await runAgent(`${batchCodeGenConfig.name} (repair)`, {
    systemPrompt,
    userPrompt,
    allowedTools: batchCodeGenConfig.allowedTools,
    model: getAgentModel("mqo_batch_code_generator", args),
    configName: "mqo_batch_code_generator",
    cwd: mqoDir,
    timeoutMs: getAgentTimeout("mqo_batch_code_generator"),
    useSkills: false,
  });
  if (result.error) {
    throw new Error(`Batch Code Generator repair failed: ${result.error}`);
  }
  return { status: "ok" };
}
