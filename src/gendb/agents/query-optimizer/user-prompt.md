# Task: Optimize C++ code for {{query_id}} (iteration {{iteration}})

{{#if stall_section}}
{{stall_section}}
{{/if}}

{{#if correctness_section}}
{{correctness_section}}
{{/if}}

{{#if anchors_section}}
{{anchors_section}}
{{/if}}

{{#if plan_path_exists}}
## Current Execution Plan
You may modify the plan for architectural changes: {{plan_path}}
{{/if}}

{{#if query_guide}}
## Query Guide
{{query_guide}}
{{/if}}

{{#if performance_profile}}
{{performance_profile}}
{{/if}}

{{#if benchmark_context}}
{{benchmark_context}}
{{/if}}

{{#if history_summary}}
## Optimization History
{{history_summary}}
{{/if}}

## Hardware Configuration
- CPU cores: {{cpu_cores}}
- Disk type: {{disk_type}}
- L3 cache: {{l3_cache_mb}} MB
- Total memory: {{total_memory_gb}} GB

{{#if join_sampling_mandate}}
{{join_sampling_mandate}}
{{/if}}

{{#if previous_outcome}}
## Previous Iteration Outcome
{{previous_outcome}}
{{/if}}

## GenDB Storage Directory
{{gendb_dir}}

## Query Code
Read and modify: {{cpp_path}}

## Original SQL
```sql
{{query_sql}}
```

## Compilation
Compile (do NOT run — Executor handles validation):
  {{compile_command}}
Ensure the code compiles successfully (up to 3 fix attempts).
