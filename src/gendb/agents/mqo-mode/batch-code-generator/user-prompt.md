# MQO Batch Code Generator — Invocation

## Step 1: Read these inputs

- Fused execution skeleton: `{{skeleton_path}}`
- Shared-component blueprint: `{{blueprint_path}}`
- Storage design: `{{storage_design_path}}`
- Workload analysis: `{{workload_analysis_path}}`

## Step 2: Read ALL query guides (CRITICAL)

These guides document exact file paths, column types, index layouts, and encodings.
They are the authoritative reference — follow them precisely.

{{query_guides_section}}

## Hardware Configuration
- CPU cores: {{cpu_cores}}
- L3 cache: {{l3_cache_mb}} MB
- Total memory: {{total_memory_gb}} GB

## Step 3: Read the database schema

```sql
{{schema}}
```

{{#if benchmark_context}}
{{benchmark_context}}
{{/if}}

## Batch: {{batch_size}} queries

{{query_id_list}}

## All SQL queries

{{queries_section}}

## Paths

- GenDB workload dir: `{{gendb_dir_parent}}` (data at `{{gendb_dir_parent}}/storage/<table>/`)
- Output dir for Write: `{{mqo_dir}}`
- Write main source to: `{{main_cpp_path}}`
- Write manifest to: `{{manifest_path}}`
- Write Makefile to: `{{makefile_path}}`
- `mqo_profile.hpp` is already at `{{mqo_dir}}/mqo_profile.hpp`

## Validation loop

After writing the code, compile and test:

```bash
# Compile
cd {{mqo_dir}} && g++ -O3 -march=native -std=c++17 -Wall -fopenmp -DGENDB_PROFILE -I. -o mqo mqo_main.cpp

# Run
mkdir -p {{mqo_dir}}/test_results && ./mqo --gendb-dir {{gendb_dir_parent}} --output-dir {{mqo_dir}}/test_results --all

# Validate
python3 {{compare_tool}} {{ground_truth_dir}} {{mqo_dir}}/test_results
```

If compilation or validation fails, fix and retry (up to 2 attempts).

## Output

Return a summary:
- Files written with line counts
- Per fused-scan stage: which queries are fused
- Validation result (pass/fail, which queries if fail)
