/**
 * MQO Analyzer — large-batch variant.
 *
 * Flow:
 *   1. Global Surveyor (single LLM agent, tool-driven) writes global_registry.json
 *   2. Deterministic clustering (mqo-clustering.mjs) writes clusters.json
 *   3. Parallel Cluster Analyzers (N LLM agents, each writing its own shard) —
 *      read-only access to global_registry.json, read-write to its own cluster_N/blueprint_shard.json
 *   4. Deterministic merge (mqo-merge.mjs) writes deterministic_merge.json
 *   5. Global Integrator (single LLM agent) writes final shared_component_blueprint.json
 */

import { readFile, writeFile, mkdir } from "fs/promises";
import { existsSync } from "fs";
import { resolve } from "path";
import { runAgent, renderTemplate, readJSON } from "../shared.mjs";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { config as mqoAnalyzerConfig } from "../agents/mqo-mode/mqo-analyzer/index.mjs";
import {
  getMqoDir,
  getMqoBlueprintPath,
  getMqoGlobalRegistryPath,
  getMqoClustersPath,
  getMqoClusterShardDir,
} from "../utils/paths.mjs";
import { formClusters } from "./mqo-clustering.mjs";
import { deterministicMerge } from "./mqo-merge.mjs";

function getAgentModel(configName, args) {
  if (args.modelOverride) return args.modelOverride;
  const providerCfg = getProviderConfig(args.agentProvider);
  return providerCfg.agentModels[configName] || args.model || providerCfg.model;
}
function getAgentTimeout(configName) {
  return defaults.agentTimeoutOverrides?.[configName] || defaults.agentTimeoutMs;
}

async function parallelMap(items, limit, fn) {
  const results = new Array(items.length);
  let idx = 0;
  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      results[i] = await fn(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => worker()));
  return results;
}

export async function runLargeBatchMqoAnalyzer(opts) {
  const { args, mqoDir: mqoDirOpt, workloadAnalysisPath, storageDesignPath, queries, batchSummary } = opts;
  const mqoDir = mqoDirOpt || getMqoDir(args.runAuditDir);
  const globalRegistryPath = getMqoGlobalRegistryPath(args.runAuditDir);
  const clustersPath = getMqoClustersPath(args.runAuditDir);
  const blueprintPath = getMqoBlueprintPath(args.runAuditDir);

  // Start with empty registry
  await writeFile(globalRegistryPath, JSON.stringify({ candidates: [] }, null, 2));

  // --- Step 1: Global Surveyor ---
  console.log(`[MQO] Step 1: Global Surveyor (tool-driven walk of ${queries.length} queries)`);
  const surveyorSystemPrompt = await readFile(mqoAnalyzerConfig.largeBatchSurveyorPromptPath, "utf-8");
  const surveyorUserPrompt =
    `# Global Surveyor — populate global candidate registry\n\n` +
    `## Batch size\n${queries.length} queries\n\n` +
    `## Queries file (for tool calls)\n\`${args.queries}\`\n\n` +
    `## Workload analysis\n\`${workloadAnalysisPath}\`\n\n` +
    `## Storage design\n\`${storageDesignPath}\`\n\n` +
    `## Registry path (WRITE to this)\n\`${globalRegistryPath}\`\n\n` +
    `## Structural summary (pre-computed)\n\`\`\`json\n${JSON.stringify(batchSummary || [], null, 2)}\n\`\`\`\n\n` +
    `Begin the breadth walk and populate the registry via registry-add-candidate / registry-update-consumers tool calls. ` +
    `Return a short summary when done.\n`;

  const surveyorResult = await runAgent(`${mqoAnalyzerConfig.name} (Surveyor)`, {
    systemPrompt: surveyorSystemPrompt,
    userPrompt: surveyorUserPrompt,
    allowedTools: mqoAnalyzerConfig.allowedTools,
    model: getAgentModel("mqo_surveyor", args),
    configName: "mqo_surveyor",
    cwd: mqoDir,
    timeoutMs: getAgentTimeout("mqo_surveyor"),
    useSkills: false,
  });
  if (surveyorResult.error) throw new Error(`Global Surveyor failed: ${surveyorResult.error}`);

  // --- Step 2: Deterministic clustering ---
  console.log(`[MQO] Step 2: Deterministic clustering`);
  const { clusters, crossCluster } = await formClusters(globalRegistryPath, queries);
  await writeFile(clustersPath, JSON.stringify({ clusters, cross_cluster_candidates: crossCluster }, null, 2));
  console.log(`[MQO] Formed ${clusters.length} clusters (${crossCluster.length} cross-cluster candidates).`);

  // --- Step 3: Parallel Cluster Analyzers ---
  console.log(`[MQO] Step 3: Parallel Cluster Analyzers (${clusters.length} agents)`);
  const clusterSystemPrompt = await readFile(mqoAnalyzerConfig.largeBatchClusterPromptPath, "utf-8");

  await parallelMap(clusters, Math.min(clusters.length, args.maxConcurrent || 4), async (cluster, idx) => {
    const shardDir = getMqoClusterShardDir(args.runAuditDir, idx);
    await mkdir(shardDir, { recursive: true });
    const shardPath = resolve(shardDir, "blueprint_shard.json");
    await writeFile(shardPath, JSON.stringify({ cluster_id: cluster.cluster_id, decisions: [], new_candidates: [], per_query_dependencies: {} }, null, 2));

    const clusterQueries = queries.filter((q) => cluster.members.includes(q.id));
    const sqlSection = clusterQueries.map((q) => `### ${q.id}\n\`\`\`sql\n${q.sql.trim()}\n\`\`\``).join("\n\n");

    const userPrompt =
      `# Cluster Analyzer — ${cluster.cluster_id}\n\n` +
      `## Cluster queries\n${cluster.members.join(", ")}\n\n` +
      `## Global registry (READ-ONLY)\n\`${globalRegistryPath}\`\n\n` +
      `## Your shard path (WRITE)\n\`${shardPath}\`\n\n` +
      `## Queries file (for tool calls)\n\`${args.queries}\`\n\n` +
      `## Workload analysis\n\`${workloadAnalysisPath}\`\n\n` +
      `## Storage design\n\`${storageDesignPath}\`\n\n` +
      `## Cluster SQL\n${sqlSection}\n`;

    const result = await runAgent(`${mqoAnalyzerConfig.name} (${cluster.cluster_id})`, {
      systemPrompt: clusterSystemPrompt,
      userPrompt,
      allowedTools: mqoAnalyzerConfig.allowedTools,
      model: getAgentModel("mqo_cluster_analyzer", args),
      configName: "mqo_cluster_analyzer",
      cwd: mqoDir,
      timeoutMs: getAgentTimeout("mqo_cluster_analyzer"),
      useSkills: false,
    });
    if (result.error) throw new Error(`Cluster Analyzer ${cluster.cluster_id} failed: ${result.error}`);
  });

  // --- Step 4: Deterministic merge ---
  console.log(`[MQO] Step 4: Deterministic signature-based merge`);
  const mergePath = resolve(mqoDir, "deterministic_merge.json");
  await deterministicMerge({
    globalRegistryPath,
    mqoDir,
    queries,
    outPath: mergePath,
  });

  // --- Step 5: Global Integrator ---
  console.log(`[MQO] Step 5: Global Integrator`);
  const integratorSystemPrompt = await readFile(mqoAnalyzerConfig.largeBatchIntegratorPromptPath, "utf-8");
  const integratorUserPrompt =
    `# Global Integrator — produce final blueprint\n\n` +
    `## Inputs\n- Deterministic merge: \`${mergePath}\`\n- Global registry: \`${globalRegistryPath}\`\n- Cluster shards: \`${mqoDir}/cluster_*/blueprint_shard.json\`\n- Workload analysis: \`${workloadAnalysisPath}\`\n- Storage design: \`${storageDesignPath}\`\n\n` +
    `## Output path\n\`${blueprintPath}\`\n\n` +
    `Read the deterministic merge first, then handle any ambiguous_cases with LLM judgment, and write the final blueprint.\n`;

  const integratorResult = await runAgent(`${mqoAnalyzerConfig.name} (Integrator)`, {
    systemPrompt: integratorSystemPrompt,
    userPrompt: integratorUserPrompt,
    allowedTools: mqoAnalyzerConfig.allowedTools,
    model: getAgentModel("mqo_global_integrator", args),
    configName: "mqo_global_integrator",
    cwd: mqoDir,
    timeoutMs: getAgentTimeout("mqo_global_integrator"),
    useSkills: false,
  });
  if (integratorResult.error) throw new Error(`Global Integrator failed: ${integratorResult.error}`);

  if (!existsSync(blueprintPath)) {
    throw new Error(`Global Integrator did not write final blueprint at ${blueprintPath}`);
  }
  const blueprint = await readJSON(blueprintPath);
  console.log(`[MQO] Large-batch blueprint written: ${(blueprint.shared_components || []).length} components.`);
  return blueprint;
}
