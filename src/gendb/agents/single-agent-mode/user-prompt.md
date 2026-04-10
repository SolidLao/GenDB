# Task: Build a high-performance query engine for this workload

## Schema
```sql
{{schema}}
```

## Queries
```sql
{{queries}}
```

## Paths
- **Source data files**: {{data_dir}}
- **GenDB storage directory** (for any transformed/preprocessed data): {{gendb_dir}}
- **Run directory** (all outputs go here): {{run_dir}}
- **C++ utility headers** (compile with `-I{{utils_path}}`): {{utils_path}}
- **Validation tool**: {{compare_tool}}
{{#if ground_truth_dir}}
- **Ground truth directory**: {{ground_truth_dir}}

Validate correctness: `python3 {{compare_tool}} {{ground_truth_dir}} <results_dir>`
{{/if}}

## Configuration
- **Hard limit — max optimization iterations per query: {{max_iterations}}**. Do NOT create more than {{max_iterations}} iteration directories (`iter_0` … `iter_{{max_iterations_minus_1}}`) per query. Stop optimizing once this limit is reached.
- Query execution timeout: {{timeout_sec}} seconds

{{#if benchmark_context}}
## Benchmark Comparison
{{benchmark_context}}
Target: beat the best system for each query.
{{/if}}

## Reminders
- Place per-query code at: `{{run_dir}}/queries/<Qi>/iter_<j>/<qi>.cpp`
- Compile each query binary in the same directory as its .cpp file
- After each compile+run+validate cycle, write `{{run_dir}}/queries/<Qi>/iter_<j>/execution_results.json`:
  ```json
  { "timing_ms": <[TIMING] total minus [TIMING] output if present, else total>, "validation": { "status": "pass" or "fail" } }
  ```
- If you do data ingestion/indexing, record timing in `{{run_dir}}/ingestion_results.json`: `{ "ingestion_time_ms": <ms> }`
- After finishing, update `{{run_dir}}/run.json` with `phase2.pipelines.<Qi>.bestCppPath` pointing to the fastest **passing** iteration's .cpp file
- Each binary: `./binary {{gendb_dir}} <results_dir>` — must print `[TIMING] total: <ms> ms`
