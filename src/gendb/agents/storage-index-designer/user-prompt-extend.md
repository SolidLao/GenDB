# Task: Extend storage layout for new queries

New queries have been added to an existing workload. The storage, ingestion code,
and indexes from previous queries are already built and working. Your job is to
EXTEND the storage — add any new columns, encodings, or indexes needed for the
new queries — WITHOUT disrupting what already exists.

## Critical Rules
1. **DO NOT remove** any existing tables, columns, indexes, or encodings from storage_design.json
2. **DO NOT re-ingest** data that already exists — only ingest NEW columns
3. **DO NOT rebuild** existing indexes — only build NEW ones
4. You MAY enhance existing columns (e.g., add a zone map to an existing column)
   but the existing binary format must remain compatible
5. All new code must compile and run without errors

## Existing Storage Design (READ FIRST)
Read: {{existing_storage_design_path}}

## Existing Ingest Code (for reference — do NOT overwrite)
Read: {{existing_ingest_cpp_path}}

## Existing Index Building Code (for reference — do NOT overwrite)
Read: {{existing_build_indexes_cpp_path}}

## Schema
```sql
{{schema}}
```

## New Queries (need storage support)
{{new_queries_section}}

## Existing Queries (for context — storage already supports these)
{{existing_queries_section}}

## Workload Analysis
Read: {{workload_analysis_path}}

## Data Directory (source data files)
{{data_dir}}

## GenDB Storage Directory
{{gendb_dir}}

## Output
1. UPDATE the storage design JSON at: {{storage_design_path}}
   - Read the existing file, ADD new entries, write it back
   - Add new columns under existing tables or add new table sections as needed
   - Add new index definitions
   - Keep ALL existing entries unchanged

2. If new columns need ingestion, write INCREMENTAL ingest code to: {{extend_ingest_cpp_path}}
   - Only ingest columns that don't already exist in {{gendb_dir}}
   - Check with file existence before ingesting: skip columns whose files already exist
   - Compile and run: cd {{generated_ingest_dir}} && g++ -O3 -std=c++17 -o extend_ingest extend_ingest.cpp && ./extend_ingest {{data_dir}} {{gendb_dir}}

3. If new indexes are needed, write INCREMENTAL index building code to: {{extend_build_indexes_cpp_path}}
   - Only build indexes that don't already exist in {{gendb_dir}}
   - Compile and run: cd {{generated_ingest_dir}} && g++ -O3 -std=c++17 -o extend_build_indexes extend_build_indexes.cpp && ./extend_build_indexes {{gendb_dir}}

4. Do NOT write query guides — they will be generated in a separate pass
