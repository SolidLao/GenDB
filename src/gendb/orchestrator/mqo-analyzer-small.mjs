/**
 * MQO Analyzer — small-batch variant.
 *
 * Single LLM agent reads the entire query batch (schema + SQL + workload +
 * storage) and produces shared_component_blueprint.json describing which
 * scans/joins/aggregations should be shared across queries.
 *
 * Used when batch size ≤ mqoAnalyzerThreshold (default 30). For larger
 * batches, the large-batch path (Surveyor → Cluster Analyzers → Integrator)
 * is used instead — see mqo-analyzer-large.mjs (Stage 10).
 */

import { readFile, writeFile } from "fs/promises";
import { existsSync } from "fs";
import { resolve } from "path";
import { runAgent, renderTemplate, readJSON } from "../shared.mjs";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { config as mqoAnalyzerConfig } from "../agents/mqo-mode/mqo-analyzer/index.mjs";
import { getMqoBlueprintPath } from "../utils/paths.mjs";

function getAgentModel(configName, args) {
  if (args.modelOverride) return args.modelOverride;
  const providerCfg = getProviderConfig(args.agentProvider);
  return providerCfg.agentModels[configName] || args.model || providerCfg.model;
}

function getAgentTimeout(configName) {
  return defaults.agentTimeoutOverrides?.[configName] || defaults.agentTimeoutMs;
}

/**
 * @param {object} opts
 * @param {object} opts.args                 — parsed CLI args
 * @param {string} opts.mqoDir               — <runAuditDir>/mqo/
 * @param {string} opts.workloadAnalysisPath
 * @param {string} opts.storageDesignPath
 * @param {Array<{id:string,sql:string}>} opts.queries
 * @param {Array|null} opts.batchSummary     — mqo-tools list-queries output (structural facts)
 * @returns {Promise<object>} the parsed blueprint JSON
 */
export async function runSmallBatchMqoAnalyzer(opts) {
  const { args, mqoDir, workloadAnalysisPath, storageDesignPath, queries, batchSummary } = opts;
  const blueprintPath = getMqoBlueprintPath(args.runAuditDir);

  // --- Load schema ---
  const schemaText = await readFile(args.schema, "utf-8");

  // --- Build queries section (markdown) ---
  const queriesSection = queries
    .map((q) => `### ${q.id}\n\n\`\`\`sql\n${q.sql.trim()}\n\`\`\``)
    .join("\n\n");

  // --- Render user prompt ---
  const systemPrompt = await readFile(mqoAnalyzerConfig.promptPath, "utf-8");
  const userTemplate = await readFile(mqoAnalyzerConfig.userPromptPath, "utf-8");
  const userPrompt = renderTemplate(userTemplate, {
    batch_size: queries.length,
    benchmark: args.targetBenchmark,
    scale_factor: args.scaleFactor,
    schema: schemaText.trim(),
    workload_analysis_path: workloadAnalysisPath,
    storage_design_path: storageDesignPath,
    queries_file: args.queries,
    queries_section: queriesSection,
    structural_summary: JSON.stringify(batchSummary || [], null, 2),
    blueprint_output_path: blueprintPath,
  });

  // --- Invoke agent ---
  const result = await runAgent(mqoAnalyzerConfig.name, {
    systemPrompt,
    userPrompt,
    allowedTools: mqoAnalyzerConfig.allowedTools,
    model: getAgentModel("mqo_analyzer_small", args),
    configName: "mqo_analyzer_small",
    cwd: mqoDir,
    timeoutMs: getAgentTimeout("mqo_analyzer_small"),
    useSkills: false,  // skills currently only wired for single-query mode agents
  });

  if (result.error) {
    throw new Error(`MQO Analyzer (small-batch) failed: ${result.error}`);
  }

  // --- Load and validate the written blueprint ---
  if (!existsSync(blueprintPath)) {
    throw new Error(
      `MQO Analyzer finished but did not write blueprint at expected path: ${blueprintPath}. ` +
      `Agent output: ${(result.result || "").slice(0, 500)}`,
    );
  }
  const blueprint = await readJSON(blueprintPath);
  if (!blueprint || !Array.isArray(blueprint.shared_components)) {
    throw new Error(
      `MQO Analyzer wrote an invalid blueprint (missing shared_components array): ${blueprintPath}`,
    );
  }
  if (!blueprint.per_query_dependencies || typeof blueprint.per_query_dependencies !== "object") {
    throw new Error(
      `MQO Analyzer wrote an invalid blueprint (missing per_query_dependencies): ${blueprintPath}`,
    );
  }

  // --- Sanity checks ---
  const componentCount = blueprint.shared_components.length;
  const consumerLinks = blueprint.shared_components.reduce(
    (sum, c) => sum + (Array.isArray(c.consumers) ? c.consumers.length : 0),
    0,
  );
  console.log(`[MQO] Blueprint written: ${componentCount} components, ${consumerLinks} consumer links.`);

  // Warn (don't fail) on components with < 2 consumers
  const nonShared = blueprint.shared_components.filter((c) => !Array.isArray(c.consumers) || c.consumers.length < 2);
  if (nonShared.length > 0) {
    console.warn(
      `[MQO] Warning: ${nonShared.length} "shared" component(s) have < 2 consumers: ` +
      nonShared.map((c) => c.component_id).join(", "),
    );
  }

  // Check that every query appears in per_query_dependencies
  const batchIds = new Set(queries.map((q) => q.id));
  for (const qid of batchIds) {
    if (!(qid in blueprint.per_query_dependencies)) {
      console.warn(`[MQO] Warning: per_query_dependencies missing entry for ${qid} — filling with [].`);
      blueprint.per_query_dependencies[qid] = [];
    }
  }
  await writeFile(blueprintPath, JSON.stringify(blueprint, null, 2));

  blueprint._agentMeta = { durationMs: result.durationMs, tokens: result.tokens, costUsd: result.costUsd };
  return blueprint;
}
