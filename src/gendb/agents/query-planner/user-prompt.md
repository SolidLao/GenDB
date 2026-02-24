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

## Query to Plan
```sql
{{query_sql}}
```

## GenDB Storage Directory
Binary columnar data: {{gendb_dir}}

{{#if benchmark_context}}
{{benchmark_context}}
{{/if}}

## Output
Write plan JSON to: {{plan_path}}
