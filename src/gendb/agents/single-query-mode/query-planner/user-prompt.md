# Task: Design execution plan for {{query_id}}

## Hardware Configuration
- CPU cores: {{cpu_cores}}
- Disk type: {{disk_type}}
- L3 cache: {{l3_cache_mb}} MB
- Total memory: {{total_memory_gb}} GB

{{#if query_guide}}
## Query Guide
{{query_guide}}
{{/if}}

{{#if memory_pre_injection}}
{{memory_pre_injection}}
{{/if}}

{{#if memory_catalog}}
{{memory_catalog}}
{{/if}}

{{#if template_sql}}
## Parameterized Template SQL
The query uses named parameters. Plan for the template pattern, not specific values.
```sql
{{template_sql}}
```
Parameters: {{params_json}}
{{/if}}

## Query to Plan
```sql
{{query_sql}}
```

## GenDB Storage Directory
Binary columnar data: {{gendb_dir}}

{{#if benchmark_context}}
{{benchmark_context}}
{{/if}}

{{#if memory_note}}
## Memory System Note
Prior optimization experience is provided above from GenDB's memory system.
Try the suggested approaches first. If they do not lead to good performance,
identify bottlenecks yourself and propose new implementations.
{{/if}}

## Output
Write plan JSON to: {{plan_path}}
