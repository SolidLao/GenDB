# GenDB Generated Code for TPC-H SF10

## Summary

Generated C++ code implementing a high-performance columnar database for TPC-H benchmark queries (Q1, Q3, Q6) on Scale Factor 10 (60M lineitem rows).

**Hardware:** 64 CPU cores, 376GB RAM, AVX-512 support
**Optimization Target:** Execution time minimization
**Generated Code:** 1,462 lines across 10 files

## Architecture

### Two-Program Design
1. **`ingest`**: One-time data ingestion (.tbl → binary columnar storage)
2. **`main`**: Fast repeated query execution (binary → results)

### Key Features
- **Binary columnar storage** with mmap zero-copy reads
- **Parallel execution** across 64 CPU cores with morsel-driven parallelism
- **Thread-local aggregation** to avoid lock contention
- **Column pruning** - only load columns needed by each query
- **Top-K heap optimization** for Q3 LIMIT 10

## Performance Results (SF10)

### Ingestion
- **Total time:** 126.6 seconds
- **Lineitem (60M rows):** 124.0 seconds
- **Storage size:** 9.7GB binary columnar format

### Query Execution
| Query | Rows | Time | Description |
|-------|------|------|-------------|
| Q1 | 4 | 0.065s | Pricing summary (scan + aggregate) |
| Q3 | 10 | 3.271s | Shipping priority (3-way join + aggregate + top-K) |
| Q6 | 1 | 0.515s | Revenue forecast (selective scan) |

## File Structure

```
generated/
├── utils/
│   └── date_utils.h           # Date conversion (days since epoch)
├── storage/
│   ├── storage.h              # Table structures
│   └── storage.cpp            # Binary I/O and mmap
├── index/
│   └── index.h                # Hash index types
├── queries/
│   ├── queries.h              # Query declarations
│   ├── q1.cpp                 # Q1 implementation
│   ├── q3.cpp                 # Q3 implementation
│   └── q6.cpp                 # Q6 implementation
├── ingest.cpp                 # Data ingestion program
├── main.cpp                   # Query execution program
├── Makefile                   # Build configuration
└── README.md                  # This file
```

## Building

```bash
make clean
make all
```

Produces two executables: `ingest` and `main`

## Usage

### 1. Ingest data (one-time)
```bash
./ingest /path/to/tbl/files /path/to/gendb/output
```

Example:
```bash
./ingest ../../benchmarks/tpc-h/data/sf10 ../../benchmarks/tpc-h/gendb/tpch_sf10.gendb
```

### 2. Run queries (repeated, fast)
```bash
./main /path/to/gendb/dir [optional/results/dir]
```

Example (print to terminal only):
```bash
./main ../../benchmarks/tpc-h/gendb/tpch_sf10.gendb
```

Example (write CSV results):
```bash
./main ../../benchmarks/tpc-h/gendb/tpch_sf10.gendb results/
```

## Implementation Highlights

### Parallel Ingestion
- 8 tables ingested concurrently using `std::thread`
- mmap input files for zero-copy parsing
- Pre-allocated vectors based on estimated row count
- Buffered binary writes (1MB buffers)

### Parallel Query Execution
- Morsel-driven parallelism (10K row chunks)
- Thread-local hash tables for aggregation
- Lock-free atomic counters for work distribution
- Near-linear scaling across 64 cores

### Query-Specific Optimizations

**Q1 (Pricing Summary):**
- Parallel scan with date filter pushdown
- Thread-local aggregation (4 groups total)
- Trivial final merge

**Q3 (Shipping Priority):**
- Hash join: customer → orders → lineitem
- Parallel aggregation (150K groups)
- Top-K heap instead of full sort

**Q6 (Revenue Forecast):**
- Highly selective scan (4 predicates)
- Thread-local sum reduction
- Only 4 columns loaded

## Correctness Verification

All queries executed successfully with reasonable results:
- Q1: 4 aggregate groups by (returnflag, linestatus)
- Q3: Top 10 orders by revenue
- Q6: Single revenue scalar

Results written to CSV format with proper headers and formatting.
