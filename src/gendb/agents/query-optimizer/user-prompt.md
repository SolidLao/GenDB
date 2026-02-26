# Task: Optimize execution plan for {{query_id}} (iteration {{iteration}})

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
Read the current plan for reference: {{plan_path}}
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

{{#if column_versions}}
{{column_versions}}
{{/if}}

## Current Implementation (read for analysis)
Read the current code to understand what was implemented: {{cpp_path}}

## Original SQL
```sql
{{query_sql}}
```

## Output
Write your revised execution plan to: {{revised_plan_path}}
Follow the Plan JSON Structure from your system prompt exactly. "optimization_notes" must be
a plain text string (not a dict). Include "correctness_anchors" if anchors are provided above.
