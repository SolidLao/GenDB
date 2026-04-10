/**
 * MQO Batch Code Generator driver (fused execution model).
 *
 * Invokes the Batch Code Generator agent to emit the complete fused MQO
 * artifact. Passes query guides (from Phase 1 Pass 2), ground truth paths,
 * and compile/validate commands — mirroring the single-query-mode Code
 * Generator's input contract.
 */

import { readFile, copyFile, mkdir, readdir } from "fs/promises";
import { existsSync, readFileSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { runAgent, renderTemplate, readJSON } from "../shared.mjs";
import { defaults, getProviderConfig } from "../gendb.config.mjs";
import { BENCHMARKS_DIR } from "../config.mjs";
import { config as batchCodeGenConfig } from "../agents/mqo-mode/batch-code-generator/index.mjs";
import {
  getMqoBlueprintPath,
  getMqoSkeletonPath,
  getMqoDir,
  getMqoMainPath,
  getMqoManifestPath,
} from "../utils/paths.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const MQO_PROFILE_HPP = resolve(__dirname, "..", "utils", "mqo_profile.hpp");
const COMPARE_TOOL_PATH = resolve(__dirname, "..", "tools", "compare_results.py");

function getAgentModel(configName, args) {
  if (args.modelOverride) return args.modelOverride;
  const providerCfg = getProviderConfig(args.agentProvider);
  return providerCfg.agentModels[configName] || args.model || providerCfg.model;
}

function getAgentTimeout(configName) {
  return defaults.agentTimeoutOverrides?.[configName] || defaults.agentTimeoutMs;
}

/**
 * Build benchmark context for MQO: sum of per-query times per system as batch-total reference.
 */
function formatMqoBenchmarkContext(benchmarkResults, queries) {
  if (!benchmarkResults) return "";
  const queryIds = queries.map((q) => q.id);
  const rows = [];
  for (const [system, data] of Object.entries(benchmarkResults)) {
    let sum = 0;
    let count = 0;
    for (const qid of queryIds) {
      const qData = data?.[qid] || data?.queries?.[qid];
      if (!qData) continue;
      const time = qData.min_ms || qData.average_ms || qData.time_ms;
      if (time != null) { sum += time; count++; }
    }
    if (count > 0) rows.push({ system, sum, count });
  }
  if (rows.length === 0) return "";
  rows.sort((a, b) => a.sum - b.sum);
  const lines = [
    "## Performance Reference (sum of per-query times across all systems)",
    "| System | Sum of Per-Query Times (ms) | Note |",
    "|--------|---------------------------|------|",
    ...rows.map((r) =>
      `| ${r.system} | ${Math.round(r.sum)} | ${r.count < queryIds.length ? `(${r.count}/${queryIds.length} queries)` : ""} |`
    ),
    "",
    `MQO fused execution should beat the sum-of-individual-queries time for each system,`,
    `since fused scans amortize I/O and cache costs across queries. Target: beat ${rows[0].system} (${Math.round(rows[0].sum)} ms).`,
  ];
  return lines.join("\n");
}

/**
 * Load all query guides from the workload directory and format them as a
 * markdown section the agent can read inline (avoiding 22 separate Read calls).
 */
function loadQueryGuides(workloadDir, queries) {
  const lines = [];
  for (const q of queries) {
    const guidePath = resolve(workloadDir, "queries", q.id, "guide.md");
    if (existsSync(guidePath)) {
      const content = readFileSync(guidePath, "utf-8");
      lines.push(`### ${q.id} Guide\n\nPath: \`${guidePath}\`\n\n${content}`);
    } else {
      lines.push(`### ${q.id} Guide\n\n_(not available — read storage_design.json for this query's columns)_`);
    }
  }
  return lines.join("\n\n---\n\n");
}

export async function runBatchCodeGenerator(opts) {
  const { args, mqoDir, workloadAnalysisPath, storageDesignPath, blueprint, skeleton, queries } = opts;
  const mainCppPath = getMqoMainPath(args.runAuditDir);
  const manifestPath = getMqoManifestPath(args.runAuditDir);
  const makefilePath = resolve(mqoDir, "Makefile");

  await mkdir(resolve(mqoDir, "stages"), { recursive: true });
  await copyFile(MQO_PROFILE_HPP, resolve(mqoDir, "mqo_profile.hpp"));

  // The workload dir is the PARENT of the storage dir. The generated binary
  // receives this as --gendb-dir and accesses storage/<table>/<col>.bin.
  // runDir is the workload dir (e.g., output/tpc-h-sf10).
  const workloadDir = resolve(args.runAuditDir, "..", "..");  // runs/<ts>/ → output/<benchmark>-sf<N>/
  const gendbDirParent = workloadDir;

  const schemaText = await readFile(args.schema, "utf-8");
  const queriesSection = queries
    .map((q) => `### ${q.id}\n\n\`\`\`sql\n${q.sql.trim()}\n\`\`\``)
    .join("\n\n");

  // Load query guides inline
  const queryGuidesSection = loadQueryGuides(workloadDir, queries);

  // Ground truth
  const groundTruthDir = resolve(BENCHMARKS_DIR, args.targetBenchmark, "query_results");

  // Load hardware config from storage design
  const storageDesign = await readJSON(storageDesignPath);
  const hw = storageDesign?.hardware_config || {};

  // Build benchmark context for batch-total comparison
  const benchmarkContext = formatMqoBenchmarkContext(args.benchmarkResults, queries);

  const systemPrompt = await readFile(batchCodeGenConfig.promptPath, "utf-8");
  const userTemplate = await readFile(batchCodeGenConfig.userPromptPath, "utf-8");
  const userPrompt = renderTemplate(userTemplate, {
    blueprint_path: getMqoBlueprintPath(args.runAuditDir),
    skeleton_path: getMqoSkeletonPath(args.runAuditDir),
    workload_analysis_path: workloadAnalysisPath,
    storage_design_path: storageDesignPath,
    gendb_dir_parent: gendbDirParent,
    mqo_dir: mqoDir,
    schema: schemaText.trim(),
    batch_size: queries.length,
    queries_section: queriesSection,
    query_id_list: queries.map((q) => q.id).join(", "),
    query_guides_section: queryGuidesSection,
    main_cpp_path: mainCppPath,
    manifest_path: manifestPath,
    makefile_path: makefilePath,
    compare_tool: COMPARE_TOOL_PATH,
    ground_truth_dir: groundTruthDir,
    cpu_cores: hw.cpu_cores || "unknown",
    l3_cache_mb: hw.l3_cache_mb || "unknown",
    total_memory_gb: hw.total_memory_gb || "unknown",
    benchmark_context: benchmarkContext,
  });

  const result = await runAgent(batchCodeGenConfig.name, {
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
    throw new Error(`Batch Code Generator failed: ${result.error}`);
  }

  // Validate outputs
  const missing = [];
  if (!existsSync(mainCppPath)) missing.push(mainCppPath);
  if (!existsSync(manifestPath)) missing.push(manifestPath);
  if (!existsSync(makefilePath)) missing.push(makefilePath);

  if (missing.length > 0) {
    throw new Error(
      `Batch Code Generator finished but did not write expected files:\n  - ` +
      missing.join("\n  - "),
    );
  }

  console.log(`[MQO] Batch Code Generator wrote fused artifact at ${mqoDir}.`);
  return { status: "ok", _agentMeta: { durationMs: result.durationMs, tokens: result.tokens, costUsd: result.costUsd } };
}
