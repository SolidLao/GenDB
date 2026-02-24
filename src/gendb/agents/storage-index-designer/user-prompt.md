# Task: Design storage layout, generate ingestion + index building code

## Workload Analysis
Read the workload analysis from: {{workload_analysis_path}}

## Schema
```sql
{{schema}}
```

## Queries in Workload
{{queries_section}}

## Data Directory (source data files)
{{data_dir}}

## GenDB Storage Directory (output)
Write binary columnar data to: {{gendb_dir}}

## Output Directory for Generated Code
Write ingest.cpp, build_indexes.cpp, and Makefile to: {{generated_ingest_dir}}

IMPORTANT: You MUST:
1. Write your JSON design to EXACTLY this path: {{storage_design_path}}
2. Generate ingest.cpp, build_indexes.cpp, and Makefile in: {{generated_ingest_dir}}
3. Compile: cd {{generated_ingest_dir}} && make clean && make all
4. Run ingestion: cd {{generated_ingest_dir}} && ./ingest {{data_dir}} {{gendb_dir}}
5. Run index building: cd {{generated_ingest_dir}} && ./build_indexes {{gendb_dir}}
6. Do NOT write query guides — they will be generated in a separate pass
