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

{{#if previous_analysis_path}}
## Previous Analysis Available
A workload analysis from a prior run already exists at: {{output_path}}
Read it, then INCREMENTALLY UPDATE it:
- PRESERVE all existing query analysis entries — do NOT remove or rewrite them
- Check if hardware, table row counts, or column stats have changed
- Re-estimate filter selectivity for the current query constants
- Use the Edit tool to update what changed — do NOT regenerate from scratch
{{#if new_queries_section}}

### New Queries (added to workload)
The following queries are NEW additions not covered by the existing analysis.
ADD analysis entries for these queries into the existing JSON — do NOT modify
or remove entries for existing queries.

{{new_queries_section}}
{{/if}}
{{/if}}

IMPORTANT: You MUST use the Write tool (or Edit tool if updating) to ensure your JSON analysis is at EXACTLY this path: {{output_path}}
Use this EXACT file path — do NOT change the filename or extension. Do NOT just print it.
