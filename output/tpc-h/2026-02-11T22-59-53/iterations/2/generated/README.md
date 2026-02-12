# GenDB Generated Code for TPC-H Workload

## Overview
This directory contains generated C++ code for a high-performance TPC-H database implementation optimized for execution time.

## Architecture
- **Two-program design**:
  - `ingest`: Parses .tbl text files → writes binary columnar storage (one-time)
  - `main`: Reads binary storage → executes queries (fast, repeated)
- **Storage**: Binary columnar format with mmap zero-copy access
- **Parallelism**: 64-core thread parallelism for scans, joins, and aggregations
- **Optimization**: Sorted lineitem by shipdate for zone map effectiveness

## Build
```bash
make clean
make all
```

## Usage

### Ingestion (one-time)
```bash
./ingest <data_dir> <gendb_dir>
```
Example:
```bash
./ingest /path/to/tpc-h/data/sf10 /path/to/gendb/tpch_sf10.gendb
```

### Query Execution
```bash
./main <gendb_dir> [results_dir]
```
Example:
```bash
./main /path/to/gendb/tpch_sf10.gendb
./main /path/to/gendb/tpch_sf10.gendb /path/to/results  # With CSV output
```

## Performance (SF10 on 64-core machine)
- **Ingestion**: ~4.3 minutes (60M lineitem rows)
- **Query execution**: ~1.7 seconds total
  - Q1: 0.06s
  - Q3: 1.3s
  - Q6: 0.16s

## Files
- `utils/date_utils.h`: Date conversion utilities
- `storage/storage.{h,cpp}`: Binary columnar storage with mmap
- `index/index.h`: Hash index typedefs
- `queries/q{1,3,6}.cpp`: Query implementations
- `ingest.cpp`: Ingestion entry point
- `main.cpp`: Query execution entry point
