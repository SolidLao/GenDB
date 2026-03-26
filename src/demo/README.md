# GenDB Demo Agent

Generates interactive visualization data for the GenDB demo app (`docs/demo/`). Takes raw GenDB output (SQL templates, execution plans, optimization histories, benchmarks) and uses an LLM to produce structured VIZ objects that drive the progressive two-column canvas.

## Prerequisites

- Node.js 18+
- `@anthropic-ai/claude-agent-sdk` (installed via `npm install`)
- GenDB output directories:
  - `output/tpc-h-sf10-3.19/` — TPC-H SF10 results
  - `output/sec-edgar-sf3-3.19/` — SEC-EDGAR SF3 results

## Usage

```bash
# Generate VIZ data for all queries (10 queries, ~3 min)
node src/demo/generate-viz.mjs

# Single benchmark
node src/demo/generate-viz.mjs --benchmark tpch
node src/demo/generate-viz.mjs --benchmark secedgar

# Single query
node src/demo/generate-viz.mjs --benchmark tpch --query Q1

# Preview extraction without calling the LLM
node src/demo/generate-viz.mjs --dry-run

# Use a different model
node src/demo/generate-viz.mjs --model claude-sonnet-4-6

# Verbose mode (show prompt snippets and debug info)
node src/demo/generate-viz.mjs -v
```

**Output:** `docs/demo/js/data/generated-viz.js` — a JS file defining `GENERATED_VIZ`, a registry keyed by `"benchmark:queryId"`.

## Queries

| Benchmark  | Queries                        |
|------------|--------------------------------|
| TPC-H      | Q1, Q6, Q9, Q18               |
| SEC-EDGAR  | Q1, Q2, Q3, Q4, Q6, Q24       |

TPC-H Q3 is excluded — it uses a hand-crafted `Q3_VIZ` in `docs/demo/js/data/q3-viz.js`.

## How It Works

```
GenDB output files ──→ extract.mjs ──→ LLM (Claude) ──→ generated-viz.js
                       (raw data +      (understands      (VIZ objects for
                        deterministic    SQL semantics,     the demo app)
                        math)            builds operator
                                         trees, writes
                                         agent narratives)
```

1. **`viz-agent/extract.mjs`** reads raw files per query (SQL template, params, plan.json, optimization_history, telemetry, workload_analysis, storage_design) and computes deterministic values (benchmark comparisons, generation cost/time, speedup factors).

2. **`generate-viz.mjs`** builds a user prompt containing the full raw data + pre-computed numbers, then calls the LLM with the system prompt (`viz-agent/prompt.md`). Queries run in parallel (5 concurrent).

3. The LLM reads the raw data, identifies relevant tables/joins/filters, designs the operator tree, writes agent discovery narratives, and produces a complete VIZ JSON object — using pre-computed benchmark numbers verbatim.

4. Output is written to `docs/demo/js/data/generated-viz.js`, which `app.js` loads via the `GENERATED_VIZ` registry.

## File Structure

```
src/demo/
├── README.md              ← you are here
├── generate-viz.mjs       ← CLI entry point
├── viz-agent/
│   ├── extract.mjs        ← data extraction (files + deterministic math)
│   └── prompt.md          ← LLM system prompt (schema + Q3 example)
├── extract-data.mjs       ← legacy: extracts raw data bundles for the demo app
└── server.mjs             ← live generation server (separate feature)
```

## Adding a New Benchmark

1. Place GenDB output in `output/<benchmark-name>/` with the standard directory structure (queries/, runs/, workload_analysis.json, storage_design.json).
2. Add a benchmark entry in `generate-viz.mjs` under `BENCHMARKS`:
   ```js
   newbench: {
     outputDir: join(ROOT, 'output/new-bench'),
     runTimestamp: '2026-XX-XXTXX-XX-XX',
     benchmarkDir: join(ROOT, 'benchmarks/new-bench'),
     sfDir: 'sf1',
     queries: ['Q1', 'Q2', ...]
   }
   ```
3. Add query titles in `viz-agent/extract.mjs` under `QUERY_TITLES`.
4. Run `node src/demo/generate-viz.mjs --benchmark newbench`.
