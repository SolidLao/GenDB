/**
 * GenDB Memory System v2 — LLM-based population script.
 *
 * Populates the HAG from actual run outputs using the Memory Manager agent.
 * 3-session bottom-up approach:
 *   Session 1: TPC-H runs → L0 + L1 nodes
 *   Session 2: SEC-EDGAR runs → L0 + L1 nodes
 *   Session 3: Cross-benchmark synthesis → L2 + L3 + L4 nodes + edges
 *
 * Usage:
 *   node src/gendb/memory/populate.mjs --runs <run1>,<run2>,... [--memory-dir <path>]
 *
 * Example:
 *   node src/gendb/memory/populate.mjs \
 *     --runs output/tpc-h/2026-02-26T06-27-28,output/tpc-h/2026-03-07T17-46-23,\
 *     output/sec-edgar/2026-02-26T07-58-24,output/sec-edgar/2026-03-07T19-04-56
 */

import { readFile, writeFile, mkdir, readdir } from "fs/promises";
import { resolve, dirname } from "path";
import { existsSync } from "fs";
import { fileURLToPath } from "url";
import { initMemory, getMemorySummary } from "./index.mjs";
import { writeNode, addEdge, getAllNodes, getNodesByLayer, readAllEdges, updateIndex, readNode, getConnectedNodes } from "./graph.mjs";
import { extractStructuralFeatures } from "../tools/sql-parser.mjs";
import { setAgentProvider } from "../providers/index.mjs";
import { runAgent } from "../shared.mjs";
import { config as memoryManagerConfig } from "../agents/common/memory-manager/index.mjs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = resolve(__dirname, "..", "..", "..");

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = {
    runs: [],
    memoryDir: resolve(PROJECT_ROOT, "gendb-memory"),
    agentProvider: "claude",
    model: "opus",
    verbose: false,
  };

  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--runs" && argv[i + 1]) args.runs = argv[++i].split(",").map(r => resolve(r.trim()));
    if (argv[i] === "--memory-dir" && argv[i + 1]) args.memoryDir = resolve(argv[++i]);
    if (argv[i] === "--provider" && argv[i + 1]) args.agentProvider = argv[++i];
    if (argv[i] === "--model" && argv[i + 1]) args.model = argv[++i];
    if (argv[i] === "--verbose") args.verbose = true;
  }

  return args;
}

// ---------------------------------------------------------------------------
// Run classification
// ---------------------------------------------------------------------------

function classifyRuns(runs) {
  const groups = {};
  for (const runDir of runs) {
    let benchmark = "unknown";
    if (runDir.includes("tpc-h")) benchmark = "tpc-h";
    else if (runDir.includes("sec-edgar")) benchmark = "sec-edgar";
    if (!groups[benchmark]) groups[benchmark] = [];
    groups[benchmark].push(runDir);
  }
  return groups;
}

// ---------------------------------------------------------------------------
// Agent session prompts
// ---------------------------------------------------------------------------

function buildSession1_2Prompt(benchmark, runDirs, memoryDir) {
  const runList = runDirs.map(r => `- ${r}`).join("\n");
  return `# Task: Extract L0 + L1 Knowledge from ${benchmark} Runs

## Memory Directory
${memoryDir}

## Run Directories
${runList}

## Instructions

For each run directory:
1. Read \`run.json\` to get run metadata (scaleFactor, model, etc.)
2. List all query directories in \`queries/\`
3. For each query:
   a. Read \`optimization_history.json\` — extract the full trajectory
   b. Read the best iteration's \`.cpp\` file path from run.json's \`phase2.pipelines.<Q>.bestCppPath\`
   c. Read the best iteration's \`plan.json\`
   d. Read the SQL from the \`iter_0/plan.json\` or infer from query ID
   e. If a \`template.sql\` exists in the query directory (new workload dir structure), include it as \`template_sql\` in the L0 node

## Output: L0 Nodes (one per query per run)

For each query in each run, create an L0 node at \`${memoryDir}/graph/nodes/L0/<id>.json\`.

**ID format**: \`L0_<benchmark>_<queryId>_sf<N>_<runDate>\`
Example: \`L0_tpch_Q1_sf10_20260226\`

When there are multiple runs of the same query (different models/dates), create ONE consolidated L0 node per query that captures the BEST result across runs. Use the run date from the best result.

**Schema**:
\`\`\`json
{
  "id": "L0_tpch_Q1_sf10_20260226",
  "layer": 0,
  "content": {
    "query_id": "Q1",
    "sql": "SELECT l_returnflag, l_linestatus, ...",
    "benchmark": "tpc-h",
    "scale_factor": 10,
    "tables_accessed": ["lineitem"],
    "best_timing_ms": 43.65,
    "best_cpp_path": "/absolute/path/to/best/q1.cpp",
    "key_optimizations": ["compact uint8 columns", "integer arithmetic", "direct-array aggregation"],
    "optimization_trajectory": [
      {"iter": 0, "timing_ms": 80.83, "strategy": "initial parallel scan + long double", "pass": true, "regression": false},
      {"iter": 1, "timing_ms": 111, "strategy": "zone-map + madvise", "pass": true, "regression": true},
      {"iter": 2, "timing_ms": 75.6, "strategy": "removed madvise", "pass": true, "regression": false},
      {"iter": 3, "timing_ms": 43.65, "strategy": "compact uint8 encoding + int64 arithmetic", "pass": true, "regression": false}
    ],
    "hardware": "64-core, 376GB RAM, HDD",
    "template_sql": "SELECT ... FROM lineitem WHERE l_shipdate <= :shipdate_upper ...",
    "param_schema": {"parameters": [{"name": "shipdate_upper", "type": "date", "default": "1998-09-02"}]}
  },
  "tags": ["${benchmark}"],
  "embedding_text": "${benchmark} Q1 SF10: compact columns + integer arithmetic, 44ms"
}
\`\`\`

**Key rules for L0**:
- Read the ACTUAL C++ code and summarize the key optimizations (don't just copy the strategy string)
- \`best_cpp_path\` must be the absolute path to the fastest passing iteration's .cpp file
- \`key_optimizations\` should be 3-5 concrete optimization techniques extracted from reading the code
- \`sql\` should be the actual SQL query text
- \`tables_accessed\` should list tables from the SQL
- Consolidate across runs: if Q1 appears in both run1 and run2, keep only the BEST L0 node

## Output: L1 Nodes (one per query template)

After creating all L0 nodes, synthesize L1 template nodes.

**ID format**: \`L1_<benchmark>_<queryId>\`
Example: \`L1_tpch_Q1\`

**Schema**:
\`\`\`json
{
  "id": "L1_tpch_Q1",
  "layer": 1,
  "content": {
    "template_name": "TPC-H Q1: Single-table scan with low-cardinality aggregation",
    "template_sql": "SELECT l_returnflag, l_linestatus, SUM(...) FROM lineitem WHERE l_shipdate <= :shipdate_upper GROUP BY ...",
    "sql_template": "SELECT l_returnflag, l_linestatus, SUM(...) FROM lineitem WHERE l_shipdate <= ? GROUP BY ...",
    "structural_signature": {
      "join_count": 0,
      "join_types": [],
      "tables": ["lineitem"],
      "has_aggregation": true,
      "aggregate_functions": ["SUM", "COUNT", "AVG"],
      "group_by_cardinality": "low",
      "has_order_by": true,
      "has_subquery": false,
      "filter_types": ["comparison"],
      "estimated_selectivity": "high"
    },
    "representative_queries": ["L0_tpch_Q1_sf10_20260226"],
    "proven_strategies": [
      "Direct-array accumulators (6 groups fits L1 cache)",
      "Compact uint8 column encoding reduces scan bandwidth 3x",
      "Integer-scaled arithmetic avoids floating-point overhead"
    ],
    "anti_patterns": [
      "long double arithmetic (2-3x slower than double/int64)",
      "MAP_POPULATE on HDD (causes 100ms+ madvise stall)"
    ],
    "selectivity_sensitivity": "low",
    "hardware_sensitivity": "bandwidth-bound on HDD"
  },
  "tags": ["${benchmark}"],
  "embedding_text": "..."
}
\`\`\`

**Key rules for L1**:
- Read the ACTUAL C++ code of the best implementations to extract proven strategies
- \`proven_strategies\` should be specific, actionable techniques (not generic advice)
- \`anti_patterns\` should be things that were tried and regressed or are known to hurt
- \`structural_signature\` must be accurate (count actual joins, list actual tables, etc.)
- Create edges: \`instance_of\` from each L0 to its L1

## Edges

Write edges to \`${memoryDir}/graph/edges.json\`. Read existing edges first, then append.

\`\`\`json
{"source": "L0_tpch_Q1_sf10_20260226", "target": "L1_tpch_Q1", "type": "instance_of"}
\`\`\`

## Important

- Read the ACTUAL code files. Do not guess or copy strategy strings blindly.
- Extract concrete, specific optimizations from reading the C++ source.
- If a query has no passing iterations, skip it (no L0 node).
- Write each node as a separate JSON file.
`;
}

function buildSession3Prompt(memoryDir) {
  return `# Task: Cross-Benchmark Synthesis — L2 + L3 + L4 Nodes

## Memory Directory
${memoryDir}

## Instructions

Read all existing L0 and L1 nodes in \`${memoryDir}/graph/nodes/L0/\` and \`${memoryDir}/graph/nodes/L1/\`.
For each L0 node, read the actual C++ code at \`best_cpp_path\` to understand the implementation patterns.

Synthesize higher-level knowledge:

## 1. L2 — Reusable Operator Implementations

Identify reusable operator patterns that appear across multiple queries. Each L2 node should be a concrete, reusable building block.

**Categories**: scan, join, aggregation, filter, io, data_structure, parallelism

**ID format**: \`L2_<category>_<name>\`
Example: \`L2_aggregation_direct_array\`, \`L2_scan_compact_encoding\`, \`L2_join_open_addr_hash\`

**Schema**:
\`\`\`json
{
  "id": "L2_aggregation_direct_array",
  "layer": 2,
  "content": {
    "operator_name": "Direct-array accumulators",
    "category": "aggregation",
    "description": "Use a fixed-size array indexed by group key for aggregation when the number of distinct groups is small (<100) and fits in L1 cache. Avoids hash table overhead entirely.",
    "preconditions": [
      "Number of distinct group keys < 100",
      "Group key can be mapped to dense integer range",
      "Fits in L1 cache (typically <64KB)"
    ],
    "code_pattern": "// Thread-local accumulator array\\nstruct Accum { int64_t sum_qty; int64_t count; ... };\\nAccum local[MAX_GROUPS];\\n// In parallel scan loop:\\nlocal[group_key].sum_qty += value;\\n// After loop: reduce across threads",
    "performance_characteristics": {
      "observed_speedups": "1.5-3x over hash-based aggregation",
      "memory_footprint": "<1KB per thread for <100 groups",
      "cache_behavior": "Stays in L1 cache, no random memory access"
    },
    "source_examples": ["L0_tpch_Q1_sf10_20260226"]
  },
  "tags": ["aggregation"],
  "embedding_text": "Direct-array accumulators for small-group aggregation, L1-cache-resident"
}
\`\`\`

**Key rules for L2**:
- Extract ACTUAL code patterns from the C++ files (not pseudocode)
- Include preconditions that determine when this pattern applies
- Cross-reference: mark which L0 nodes use this pattern
- Target: 15-30 nodes covering the major patterns across all queries

## 2. L3 — Optimization Strategies

Identify transferable optimization strategies that work across different operators and queries.

**ID format**: \`L3_<strategy_name>\`
Example: \`L3_bandwidth_reduction\`, \`L3_parallel_region_fusion\`

**Schema**:
\`\`\`json
{
  "id": "L3_bandwidth_reduction",
  "layer": 3,
  "content": {
    "strategy_name": "Reduce scan bandwidth via compact encodings",
    "problem_signature": "Main scan is bandwidth-bound (>50% of total time spent in sequential reads)",
    "solution": "Build compact column versions (uint8/int16) when value ranges allow. Reduces memory bandwidth 2-4x.",
    "applicability": "When scan phase dominates and column value range fits in a smaller integer type",
    "trade_offs": "Adds one-time build cost for compact columns; slightly more complex decode logic",
    "evidence": [
      {"query": "TPC-H Q1", "before_ms": 80, "after_ms": 44, "speedup": "1.8x"},
      {"query": "TPC-H Q6", "before_ms": 35, "after_ms": 17, "speedup": "2.1x"}
    ],
    "confidence": 0.9,
    "evidence_count": 4
  },
  "tags": ["performance", "scan"],
  "embedding_text": "Bandwidth reduction via compact column encodings, 1.5-3x speedup on scan-bound queries"
}
\`\`\`

**Key rules for L3**:
- Each strategy must have concrete evidence from actual runs
- \`problem_signature\` describes WHEN this applies (what bottleneck to look for)
- \`applicability\` describes the conditions under which this works
- Target: 10-20 nodes

## 3. L4 — Performance Principles

Synthesize fundamental principles from the L3 strategies. Only create a principle when 3+ L3 strategies support it.

**ID format**: \`L4_<principle_name>\`

**Schema**:
\`\`\`json
{
  "id": "L4_bandwidth_vs_compute",
  "layer": 4,
  "content": {
    "principle": "When memory-bandwidth-bound, reducing data volume yields larger gains than algorithmic improvements",
    "reasoning": "Modern CPUs have more compute capacity than memory bandwidth. On HDD systems, I/O dominates. Compact representations reduce the bottleneck directly.",
    "derived_from": ["L3_bandwidth_reduction", "L3_zone_map_skip", "L3_compact_encoding"],
    "counter_examples": ["When data fits entirely in LLC, bandwidth is not the bottleneck — algorithm matters more"],
    "applies_when": "Scan or I/O phase is >50% of total execution time"
  },
  "tags": ["principle"],
  "embedding_text": "Bandwidth-bound → reduce data volume, not algorithm complexity"
}
\`\`\`

**Key rules for L4**:
- Only create when 3+ L3 strategies support the principle
- Include counter-examples
- Target: 3-8 nodes

## 4. Edges

Create edges connecting the layers:
- \`uses_operator\` (L1→L2): template's best impl uses this operator
- \`implements_strategy\` (L2→L3): operator is a concrete realization of strategy
- \`exemplifies_principle\` (L3→L4): strategy is an instance of principle

Append to existing edges in \`${memoryDir}/graph/edges.json\`.

## Important

- Read the ACTUAL C++ source code for every L0 node's best_cpp_path
- Extract real code patterns, not pseudocode
- Be specific and concrete — avoid generic advice
- Cross-reference nodes with their source evidence
`;
}

// ---------------------------------------------------------------------------
// Post-process L1 structural signatures using SQLGlot
// ---------------------------------------------------------------------------

/**
 * Overwrite L1 structural_signature fields with programmatically extracted
 * features from the representative L0's SQL. This ensures consistency
 * between stored signatures and runtime classification.
 */
async function postProcessL1Signatures(memoryDir) {
  const l1Nodes = await getNodesByLayer(1, memoryDir);
  let updated = 0;
  let skipped = 0;

  for (const l1Node of l1Nodes) {
    // Find representative L0 node via edges
    const l0Nodes = await getConnectedNodes(l1Node.id, "instance_of", memoryDir, "incoming");
    // Pick the L0 with the best timing (most optimized)
    const bestL0 = l0Nodes
      .filter(n => n.content?.sql)
      .sort((a, b) => (a.content?.best_timing_ms || Infinity) - (b.content?.best_timing_ms || Infinity))[0];

    if (!bestL0?.content?.sql) {
      // Fallback: try to extract from the L1 sql_template if it's real SQL
      const template = l1Node.content?.sql_template;
      if (template && !template.includes("?") && !template.includes("...")) {
        const features = extractStructuralFeatures(template);
        if (features.tables.length > 0) {
          l1Node.content.structural_signature = features;
          await writeNode(l1Node, memoryDir);
          updated++;
          continue;
        }
      }
      console.warn(`[Populate] L1 ${l1Node.id}: no SQL source found, keeping LLM signature`);
      skipped++;
      continue;
    }

    const features = extractStructuralFeatures(bestL0.content.sql);
    l1Node.content.structural_signature = features;
    await writeNode(l1Node, memoryDir);
    updated++;
  }

  console.log(`[Populate] Post-processed L1 signatures: ${updated} updated, ${skipped} skipped`);
  return { updated, skipped };
}

// ---------------------------------------------------------------------------
// Post-agent validation
// ---------------------------------------------------------------------------

async function validateNodes(memoryDir) {
  const allNodes = await getAllNodes(memoryDir);
  const errors = [];

  for (const node of allNodes) {
    // Check required fields
    if (!node.id) errors.push(`Node missing id: ${JSON.stringify(node).slice(0, 100)}`);
    if (node.layer === undefined) errors.push(`Node ${node.id} missing layer`);
    if (!node.content) errors.push(`Node ${node.id} missing content`);

    // Layer-specific checks
    switch (node.layer) {
      case 0:
        if (!node.content.benchmark) errors.push(`L0 ${node.id}: missing benchmark`);
        if (!node.content.best_timing_ms && node.content.best_timing_ms !== 0) {
          errors.push(`L0 ${node.id}: missing best_timing_ms`);
        }
        break;
      case 1:
        if (!node.content.structural_signature) errors.push(`L1 ${node.id}: missing structural_signature`);
        break;
      case 2:
        if (!node.content.category && !node.content.operator) errors.push(`L2 ${node.id}: missing category`);
        break;
      case 3:
        if (!node.content.strategy_name) errors.push(`L3 ${node.id}: missing strategy_name`);
        break;
      case 4:
        if (!node.content.principle) errors.push(`L4 ${node.id}: missing principle`);
        break;
    }
  }

  // Validate edges
  const edges = await readAllEdges(memoryDir);
  const nodeIds = new Set(allNodes.map(n => n.id));
  for (const edge of edges) {
    if (!nodeIds.has(edge.source)) errors.push(`Edge: source ${edge.source} not found`);
    if (!nodeIds.has(edge.target)) errors.push(`Edge: target ${edge.target} not found`);
  }

  return { valid: errors.length === 0, errors, nodeCount: allNodes.length, edgeCount: edges.length };
}

// ---------------------------------------------------------------------------
// Main populate
// ---------------------------------------------------------------------------

async function populate(args) {
  console.log("[Populate] GenDB Memory System v2 — LLM-based population");
  console.log(`[Populate] Memory dir: ${args.memoryDir}`);
  console.log(`[Populate] Runs: ${args.runs.join(", ")}`);
  console.log(`[Populate] Provider: ${args.agentProvider}, Model: ${args.model}`);

  // Initialize
  setAgentProvider(args.agentProvider);
  await initMemory(args.memoryDir, {});

  // Group runs by benchmark
  const groups = {};
  for (const runDir of args.runs) {
    let benchmark = "unknown";
    if (runDir.includes("tpc-h")) benchmark = "tpc-h";
    else if (runDir.includes("sec-edgar")) benchmark = "sec-edgar";
    if (!groups[benchmark]) groups[benchmark] = [];
    groups[benchmark].push(runDir);
  }

  console.log(`[Populate] Benchmark groups: ${Object.keys(groups).join(", ")}`);

  // Read Memory Manager system prompt
  const mmSystemPrompt = await readFile(memoryManagerConfig.promptPath, "utf-8");

  // --- Sessions 1 & 2: Per-benchmark L0+L1 extraction (parallel) ---
  const benchmarkPromises = Object.entries(groups).map(async ([benchmark, runDirs]) => {
    console.log(`\n[Populate] === Session: ${benchmark} (${runDirs.length} runs) ===`);

    const userPrompt = buildSession1_2Prompt(benchmark, runDirs, args.memoryDir);

    const result = await runAgent(`Memory Manager (${benchmark} L0+L1)`, {
      systemPrompt: mmSystemPrompt,
      userPrompt,
      allowedTools: memoryManagerConfig.allowedTools,
      model: args.model,
      cwd: PROJECT_ROOT,
      timeoutMs: 30 * 60 * 1000, // 30 minutes
      verbose: args.verbose,
    });

    if (result.error) {
      console.error(`[Populate] ${benchmark} session failed: ${result.error}`);
    } else {
      console.log(`[Populate] ${benchmark} session completed (${result.durationMs}ms, $${result.costUsd?.toFixed(2) || "?"})`);
    }
    return { benchmark, result };
  });

  const sessionResults = await Promise.all(benchmarkPromises);

  // Check L0/L1 counts
  const l0Count = (await getNodesByLayer(0, args.memoryDir)).length;
  const l1Count = (await getNodesByLayer(1, args.memoryDir)).length;
  console.log(`\n[Populate] After L0+L1 sessions: ${l0Count} L0 nodes, ${l1Count} L1 nodes`);

  if (l0Count === 0) {
    console.error("[Populate] No L0 nodes created — aborting Session 3");
    return;
  }

  // Post-process: overwrite L1 structural_signature with SQLGlot-extracted features
  console.log("\n[Populate] === Post-processing L1 structural signatures (SQLGlot) ===");
  await postProcessL1Signatures(args.memoryDir);

  // --- Session 3: Cross-benchmark synthesis (L2+L3+L4) ---
  console.log(`\n[Populate] === Session 3: Cross-benchmark synthesis ===`);

  const session3Prompt = buildSession3Prompt(args.memoryDir);

  const session3Result = await runAgent("Memory Manager (L2+L3+L4 synthesis)", {
    systemPrompt: mmSystemPrompt,
    userPrompt: session3Prompt,
    allowedTools: memoryManagerConfig.allowedTools,
    model: args.model,
    cwd: PROJECT_ROOT,
    timeoutMs: 30 * 60 * 1000,
    verbose: args.verbose,
  });

  if (session3Result.error) {
    console.error(`[Populate] Session 3 failed: ${session3Result.error}`);
  } else {
    console.log(`[Populate] Session 3 completed (${session3Result.durationMs}ms, $${session3Result.costUsd?.toFixed(2) || "?"})`);
  }

  // --- Post-agent validation ---
  console.log("\n[Populate] === Post-agent validation ===");
  const validation = await validateNodes(args.memoryDir);

  if (validation.valid) {
    console.log(`[Populate] Validation PASSED: ${validation.nodeCount} nodes, ${validation.edgeCount} edges`);
  } else {
    console.warn(`[Populate] Validation found ${validation.errors.length} issues:`);
    for (const err of validation.errors.slice(0, 20)) {
      console.warn(`  - ${err}`);
    }
  }

  // Rebuild index
  await updateIndex(args.memoryDir);

  // Final summary
  const summary = await getMemorySummary(args.memoryDir);
  console.log(`\n[Populate] === Population Complete ===`);
  console.log(`[Populate] ${summary}`);

  const l2Count = (await getNodesByLayer(2, args.memoryDir)).length;
  const l3Count = (await getNodesByLayer(3, args.memoryDir)).length;
  const l4Count = (await getNodesByLayer(4, args.memoryDir)).length;
  console.log(`[Populate] L0: ${l0Count}, L1: ${l1Count}, L2: ${l2Count}, L3: ${l3Count}, L4: ${l4Count}`);
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs(process.argv);

  // One-time patch: re-extract L1 signatures from L0 SQL using SQLGlot
  if (process.argv.includes("--patch-l1-signatures")) {
    console.log("[Populate] Patching existing L1 structural signatures with SQLGlot...");
    await initMemory(args.memoryDir, {});
    await postProcessL1Signatures(args.memoryDir);
    await updateIndex(args.memoryDir);
    console.log("[Populate] Done.");
    return;
  }

  if (args.runs.length === 0) {
    console.error("Usage: node src/gendb/memory/populate.mjs --runs <run1>,<run2>,... [--memory-dir <path>] [--provider claude] [--model sonnet]");
    console.error("       node src/gendb/memory/populate.mjs --patch-l1-signatures [--memory-dir <path>]");
    console.error("\nExample:");
    console.error("  node src/gendb/memory/populate.mjs --runs output/tpc-h/2026-02-26T06-27-28,output/tpc-h/2026-03-07T17-46-23,output/sec-edgar/2026-02-26T07-58-24,output/sec-edgar/2026-03-07T19-04-56");
    process.exit(1);
  }

  // Validate all run directories exist.
  // Support both legacy timestamp dirs (with run.json) and new workload dirs (with workload.json).
  const expandedRuns = [];
  for (const runDir of args.runs) {
    if (!existsSync(runDir)) {
      console.error(`[Populate] Error: directory not found: ${runDir}`);
      process.exit(1);
    }
    if (existsSync(resolve(runDir, "workload.json"))) {
      // New workload dir — scan runs/ subdirectory for audit entries
      const runsSubdir = resolve(runDir, "runs");
      if (existsSync(runsSubdir)) {
        try {
          const entries = await readdir(runsSubdir);
          for (const entry of entries) {
            const entryPath = resolve(runsSubdir, entry);
            if (existsSync(resolve(entryPath, "run.json"))) {
              expandedRuns.push(entryPath);
            }
          }
        } catch {}
      }
      // Also treat the workload dir itself as a run source (for queries/ structure)
      expandedRuns.push(runDir);
      console.log(`[Populate] Expanded workload dir ${runDir} to ${expandedRuns.length} run sources`);
    } else if (existsSync(resolve(runDir, "run.json"))) {
      expandedRuns.push(runDir);
    } else {
      console.error(`[Populate] Error: neither run.json nor workload.json found in ${runDir}`);
      process.exit(1);
    }
  }
  args.runs = expandedRuns;

  await populate(args);
}

const isMain = process.argv[1]?.endsWith("populate.mjs");
if (isMain) {
  main().catch(err => {
    console.error(`[Populate] Fatal error: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  });
}

export { populate };
