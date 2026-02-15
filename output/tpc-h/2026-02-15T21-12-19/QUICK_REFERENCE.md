# Phase 1 Quick Reference

## File Locations

### Storage Design
```
/home/jl4492/GenDB/output/tpc-h/2026-02-15T21-12-19/storage_design.json
```
- JSON schema defining all tables, columns, types, encodings, sort orders, indexes

### Generated Code
```
/home/jl4492/GenDB/output/tpc-h/2026-02-15T21-12-19/generated_ingest/
  ├── ingest.cpp
  ├── build_indexes.cpp
  ├── Makefile
  ├── ingest (compiled binary)
  └── build_indexes (compiled binary)
```

### Ingested Data
```
/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/
  ├── lineitem/        (60M rows, 6.0 GB)
  ├── orders/          (15M rows, 1.3 GB)
  ├── customer/        (1.5M rows, 239 MB)
  ├── part/            (2.0M rows, 370 MB)
  ├── partsupp/        (8.0M rows, 780 MB)
  ├── supplier/        (100K rows, 14 MB)
  ├── nation/          (25 rows)
  ├── region/          (5 rows)
  └── indexes/
      ├── lineitem_shipdate_zonemap.bin
      ├── lineitem_orderkey_hash.bin
      ├── orders_orderkey_sorted.bin
      ├── orders_custkey_hash.bin
      ├── customer_custkey_sorted.bin
      └── customer_mktsegment_hash.bin
```

### Query Guides
```
/home/jl4492/GenDB/output/tpc-h/2026-02-15T21-12-19/query_guides/
  ├── Q1_storage_guide.md
  ├── Q3_storage_guide.md
  └── Q6_storage_guide.md
```

## Key Constants

### Date Encoding (int32_t days since epoch)
- 1970-01-01: 0
- 1994-01-01: 8766
- 1995-03-15: 9204
- 1998-12-01: 10561

### Decimal Encoding (int64_t scaled by 100)
- 0.05 → 5
- 0.07 → 7
- 24.99 → 2499

### Dictionary Codes (int32_t)
**l_returnflag**: 0=N, 1=A, 2=R
**l_linestatus**: 0=F, 1=O
**c_mktsegment**: 0=AUTOMOBILE, 1=BUILDING, 2=FURNITURE, 3=HOUSEHOLD, 4=MACHINERY

## Column Type Mappings

| Semantic Type | C++ Type | Encoding | Notes |
|---------------|----------|----------|-------|
| INTEGER | int32_t | none | Direct binary |
| DECIMAL | int64_t | none | Scaled by 100 |
| DATE | int32_t | none | Days since 1970-01-01 |
| STRING (low-cardinality) | int32_t | dictionary | Code → dict file |
| STRING (high-cardinality) | std::string | none | Variable-length |

## Index Binary Formats

### Zone Map
```
[uint32_t num_zones]
[int32_t min_val, int32_t max_val, uint32_t count] × num_zones
```

### Hash (Multi-Value)
```
[uint32_t num_unique_keys]
[int32_t key, uint32_t offset, uint32_t count] × num_unique_keys
[uint32_t position_count]
[uint32_t position] × position_count
```

### Sorted
```
[uint32_t num_entries]
[<key_type> key, uint32_t position] × num_entries
```

## Quick Commands

### Verify Ingestion
```bash
# Check data sizes
du -sh /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/*

# Count column files
find /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb -name "*.bin" | wc -l

# List indexes
ls /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/indexes/
```

### Read Storage Design
```bash
cat /home/jl4492/GenDB/output/tpc-h/2026-02-15T21-12-19/storage_design.json | jq .
```

### View Query Guides
```bash
cat /home/jl4492/GenDB/output/tpc-h/2026-02-15T21-12-19/query_guides/Q1_storage_guide.md
cat /home/jl4492/GenDB/output/tpc-h/2026-02-15T21-12-19/query_guides/Q3_storage_guide.md
cat /home/jl4492/GenDB/output/tpc-h/2026-02-15T21-12-19/query_guides/Q6_storage_guide.md
```

## For Phase 2

Use these files to generate optimized query code:
1. **storage_design.json**: Get schema, types, encodings, sort orders
2. **Query Guides**: Get query-specific column layouts and index information
3. **Data Location**: Read from `/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/`

All data is in binary columnar format, mmap-able, pre-sorted for efficient access.
