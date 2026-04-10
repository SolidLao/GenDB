# MQO Analyzer — Small-Batch Invocation

Analyze the following query batch and produce the shared-component blueprint.

## Batch size
{{batch_size}} queries

## Benchmark
{{benchmark}} (SF {{scale_factor}})

## Schema

```sql
{{schema}}
```

## Workload analysis

Path: `{{workload_analysis_path}}`
Read this JSON first via `Read` before making decisions — it contains table cardinalities, column statistics, and the join graph. **This file is the source of truth for cost estimation.**

## Storage design

Path: `{{storage_design_path}}`
Read this JSON to understand which indexes and storage layouts already exist. A filter that already hits a zone map may not benefit from being promoted to a shared scan.

## Query batch

Queries file (for tool invocations): `{{queries_file}}`

{{queries_section}}

## Structural summary (pre-computed via SQLGlot)

```json
{{structural_summary}}
```

## Tool layer

You can run deterministic lookups via:

```bash
python3 src/gendb/tools/mqo-tools.py list-queries --queries-file {{queries_file}}
python3 src/gendb/tools/mqo-tools.py canonical-signature --queries-file {{queries_file}} --qid Q3 --scope filter
python3 src/gendb/tools/mqo-tools.py find-queries-touching --queries-file {{queries_file}} --table lineitem
python3 src/gendb/tools/mqo-tools.py find-queries-with-join --queries-file {{queries_file}} --table-a customer --table-b orders
python3 src/gendb/tools/mqo-tools.py predicate-overlap --queries-file {{queries_file}} --qid-a Q1 --qid-b Q3 --table lineitem --column l_shipdate
python3 src/gendb/tools/mqo-tools.py agg-signature --queries-file {{queries_file}} --qid Q3
```

## Output

Write the blueprint to this exact path using the Write tool:

```
{{blueprint_output_path}}
```

Then return a brief (< 300 word) summary of:
- Total shared components you identified (by kind)
- Total consumer links (sum of |consumers| across all components)
- Top-3 highest-savings components by `build_cost_estimate_ms × (|consumers| - 1)`
- Any notable rejected candidates

**Do not generate C++ code.** Blueprint only.
