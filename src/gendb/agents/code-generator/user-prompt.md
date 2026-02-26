# Task: Generate C++ implementation for {{query_id}}

{{#if plan_json}}
## Execution Plan
Implement this plan in C++. Follow the plan faithfully — it was produced by the Query Optimizer based on profiling data.
```json
{{plan_json}}
```
{{/if}}

{{#if anchors_section}}
{{anchors_section}}
{{/if}}

{{#if performance_context}}
## Performance Context
{{performance_context}}
{{/if}}

## Hardware Configuration
- CPU cores: {{cpu_cores}}
- Disk type: {{disk_type}}
- L3 cache: {{l3_cache_mb}} MB
- Total memory: {{total_memory_gb}} GB

{{#if query_guide}}
## Query Guide (from Storage/Index Designer)
Authoritative reference for column types, encodings, index layouts, table stats.
{{query_guide}}
{{/if}}

- Schema: {{schema_path}}

{{#if benchmark_context}}
{{benchmark_context}}
{{/if}}

## Query to Implement
```sql
{{query_sql}}
```

## GenDB Storage Directory
Binary columnar data: {{gendb_dir}}

{{#if storage_extensions}}
{{storage_extensions}}
{{/if}}

{{#if column_versions}}
{{column_versions}}
{{/if}}

## Ground Truth
{{ground_truth}}

## Output
- Write .cpp file to: {{cpp_path}}

## Validation Loop
Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -I{{utils_path}} -o {{binary_name}} {{cpp_name}}
Run: timeout {{timeout_sec}}s ./{{binary_name}} {{gendb_dir}} {{results_dir}}
{{#if has_ground_truth}}
Validate: python3 {{compare_tool}} {{ground_truth_dir}} {{results_dir}}
If validation fails: analyze root cause, fix, retry (up to 2 fix attempts)
{{/if}}
