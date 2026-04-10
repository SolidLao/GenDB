/**
 * MQO Global Skeleton Planner driver (fused execution model v2).
 *
 * Reads the shared-component blueprint and produces batch_skeleton.json —
 * a fused execution DAG with stages: hash builds, fused scans (one per
 * fact table with per-query consumer branches), and finalize.
 *
 * One sequential LLM agent.
 */

import { readFile, writeFile } from "fs/promises";
import { existsSync } from "fs";
import { runAgent, renderTemplate, readJSON } from "../shared.mjs";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { config as skeletonPlannerConfig } from "../agents/mqo-mode/global-skeleton-planner/index.mjs";
import { getMqoBlueprintPath, getMqoSkeletonPath } from "../utils/paths.mjs";

function getAgentModel(configName, args) {
  if (args.modelOverride) return args.modelOverride;
  const providerCfg = getProviderConfig(args.agentProvider);
  return providerCfg.agentModels[configName] || args.model || providerCfg.model;
}

function getAgentTimeout(configName) {
  return defaults.agentTimeoutOverrides?.[configName] || defaults.agentTimeoutMs;
}

export async function runGlobalSkeletonPlanner(opts) {
  const { args, mqoDir, workloadAnalysisPath, storageDesignPath, blueprint, queries } = opts;
  const blueprintPath = getMqoBlueprintPath(args.runAuditDir);
  const skeletonPath = getMqoSkeletonPath(args.runAuditDir);

  const systemPrompt = await readFile(skeletonPlannerConfig.promptPath, "utf-8");
  const userTemplate = await readFile(skeletonPlannerConfig.userPromptPath, "utf-8");
  const userPrompt = renderTemplate(userTemplate, {
    blueprint_path: blueprintPath,
    workload_analysis_path: workloadAnalysisPath,
    storage_design_path: storageDesignPath,
    query_id_list: queries.map((q) => q.id).join(", "),
    skeleton_output_path: skeletonPath,
  });

  const result = await runAgent(skeletonPlannerConfig.name, {
    systemPrompt,
    userPrompt,
    allowedTools: skeletonPlannerConfig.allowedTools,
    model: getAgentModel("mqo_skeleton_planner", args),
    configName: "mqo_skeleton_planner",
    cwd: mqoDir,
    timeoutMs: getAgentTimeout("mqo_skeleton_planner"),
    useSkills: false,
  });

  if (result.error) {
    throw new Error(`Global Skeleton Planner failed: ${result.error}`);
  }
  if (!existsSync(skeletonPath)) {
    throw new Error(
      `Global Skeleton Planner finished but did not write skeleton at ${skeletonPath}. ` +
      `Output: ${(result.result || "").slice(0, 500)}`,
    );
  }
  const skeleton = await readJSON(skeletonPath);
  if (!skeleton || !Array.isArray(skeleton.stages)) {
    throw new Error(`Global Skeleton Planner wrote invalid skeleton at ${skeletonPath} (missing stages array)`);
  }

  // --- Validation: basic structural checks ---
  const stageIds = skeleton.stages.map((s) => s.stage_id);
  console.log(`[MQO] Skeleton written: ${stageIds.length} stages — ${stageIds.join(", ")}`);

  // Count fused scans and consumer branches
  let totalConsumerBranches = 0;
  for (const stage of skeleton.stages) {
    if (stage.kind === "fused_scan" && Array.isArray(stage.consumer_branches)) {
      totalConsumerBranches += stage.consumer_branches.length;
      console.log(
        `[MQO]   ${stage.stage_id}: fused scan of ${stage.table} with ${stage.consumer_branches.length} consumer branches`,
      );
    }
  }
  console.log(`[MQO] Total fused consumer branches: ${totalConsumerBranches}`);

  // Check query_stage_mask coverage
  if (skeleton.query_stage_mask) {
    const coveredQueries = Object.keys(skeleton.query_stage_mask);
    for (const q of queries) {
      if (!coveredQueries.includes(q.id)) {
        console.warn(`[MQO] Warning: query_stage_mask missing entry for ${q.id}`);
      }
    }
  } else {
    console.warn(`[MQO] Warning: skeleton missing query_stage_mask — code generator will infer from consumer_branches.`);
  }

  skeleton._agentMeta = { durationMs: result.durationMs, tokens: result.tokens, costUsd: result.costUsd };
  return skeleton;
}
