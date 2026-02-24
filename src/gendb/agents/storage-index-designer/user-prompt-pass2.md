# Task: Generate per-query storage guides

You previously designed the storage layout and built indexes. Now write query guides
that are **exactly consistent** with the actual implementation.

## Critical Consistency Requirement
Every hash function, struct layout, and index format in the guides MUST match the
actual code. Read the source files FIRST, then document what you read.

## Storage Design
Read: {{storage_design_path}}

## Build Indexes Source (AUTHORITATIVE for hash functions and index layouts)
Read: {{build_indexes_cpp_path}}

## Ingest Source (AUTHORITATIVE for column encodings and file layouts)
Read: {{ingest_cpp_path}}

## Workload Analysis (for selectivities, join patterns, aggregation estimates)
Read: {{workload_analysis_path}}

## Queries
{{queries_section}}

## GenDB Storage Directory
{{gendb_dir}}

## Output
Write per-query guides to: {{query_guides_dir}}
Each file: <QUERY_ID>_guide.md

## Guide Rules
1. For each index: read the EXACT hash function from build_indexes.cpp and copy the
   C++ computation verbatim — include 64-bit intermediates, fold, mask, everything
2. For each index: copy the exact struct layout (field names, types, order)
3. For each index: document the exact empty-slot sentinel value
4. For each column: cross-reference storage_design.json for encoding, file path, row count
5. Only include columns and indexes relevant to the query
6. NEVER hardcode dictionary code values — show runtime loading pattern
7. For multi-value indexes (1:N joins), document the multi-value format explicitly
