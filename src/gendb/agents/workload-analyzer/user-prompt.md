# Task: Analyze OLAP workload

## Schema
```sql
{{schema}}
```

## Queries
```sql
{{queries}}
```

## Data Directory (source data files for sampling)
{{data_dir}}
Use this to profile actual data: row counts (wc -l), column samples (head -100), selectivity estimates.

IMPORTANT: You MUST use the Write tool to write your JSON analysis to EXACTLY this path: {{output_path}}
Use this EXACT file path — do NOT change the filename or extension. Do NOT just print it.
