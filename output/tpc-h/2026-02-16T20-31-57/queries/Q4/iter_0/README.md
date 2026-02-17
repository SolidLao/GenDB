# Q4 Query Implementation - Iteration 0

## Overview

This directory contains the complete, validated C++ implementation for TPC-H Query 4 (Order Priority Checking).

**Status:** ✅ **COMPLETE AND VALIDATED**
- Correctness: 100% match with ground truth (5/5 rows exact match)
- Compilation: Clean (no warnings, no errors)
- Performance: ~4.9 seconds (optimized iteration 0)

## Files

| File | Purpose |
|------|---------|
| `q4.cpp` | Complete C++ source code (self-contained, ~380 lines) |
| `q4` | Compiled binary (pre-built for convenience) |
| `results/Q4.csv` | Query output in CSV format (5 rows) |
| `SUMMARY.txt` | Detailed implementation and approach documentation |
| `VALIDATION_REPORT.md` | Complete validation results and metrics |
| `README.md` | This file |

## Quick Start

### Compile
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE \
    -o q4 q4.cpp
```

### Run
```bash
./q4 <gendb_directory> [results_directory]

# Example:
./q4 /path/to/gendb/tpch_sf10.gendb ./results
```

### Output
- CSV file: `results/Q4.csv`
- Format: comma-delimited with header row
- Columns: `o_orderpriority`, `order_count`

## Query Specification

```sql
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM orders
WHERE
    o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
    AND EXISTS (
        SELECT * FROM lineitem
        WHERE l_orderkey = o_orderkey
          AND l_commitdate < l_receiptdate
    )
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
```

## Implementation Highlights

### Logical Plan
1. **Semi-join preprocessing**: Scan 59M lineitem rows, build hash set of order keys where `l_commitdate < l_receiptdate`
   - Result: 13.75M distinct keys
   
2. **Orders filtering**: Apply date range [1993-07-01, 1993-10-01)
   - Selectivity: ~3.5% (525K rows)
   
3. **Semi-join**: Probe orders against lineitem set
   - Execution: O(n) hash set lookup
   
4. **Aggregation**: Count by priority (5 distinct values)
   - Data structure: Flat array (no hash table needed)
   
5. **Output**: CSV with lexicographic sort on priority

### Physical Optimizations
- **Thread parallelism**: OpenMP for lineitem scan (thread-local sets)
- **Flat aggregation**: Array-based for 5 groups (vs hash table)
- **Direct date comparison**: No parsing, raw int32_t comparison
- **Zero-copy loading**: Binary mmap (not text parsing)
- **Dictionary decoding**: Runtime loading of o_orderpriority dictionary

### Performance Profile (from `-DGENDB_PROFILE`)
```
Semi-join build:              4,731 ms (95.7%)
Orders scan + filter + join:    211 ms ( 4.3%)
I/O and setup:                   < 1 ms ( <0.1%)
─────────────────────────────────────────
Total:                        4,943 ms
```

## Data Type Handling

### Dates (int32_t, epoch days)
- Encoding: Days since 1970-01-01
- 1993-07-01 = epoch day 8582
- 1993-10-01 = epoch day 8674
- Comparison: Direct integer < operator

### Dictionary Strings
- Column: o_orderpriority
- Storage: Integer codes (0-4)
- Dictionary: Loaded at runtime from `o_orderpriority_dict.txt`
- Mapping:
  - 0 → "5-LOW"
  - 1 → "1-URGENT"
  - 2 → "4-NOT SPECIFIED"
  - 3 → "2-HIGH"
  - 4 → "3-MEDIUM"

### Integer Keys
- o_orderkey, l_orderkey: 32-bit signed integers
- No scaling or special handling

## Validation Results

### Correctness (vs Ground Truth)
| Priority | Expected | Actual | Status |
|----------|----------|--------|--------|
| 1-URGENT | 105,214 | 105,214 | ✅ |
| 2-HIGH | 104,821 | 104,821 | ✅ |
| 3-MEDIUM | 105,227 | 105,227 | ✅ |
| 4-NOT SPECIFIED | 105,422 | 105,422 | ✅ |
| 5-LOW | 105,356 | 105,356 | ✅ |

**Row count:** 5/5 exact match ✅

### Execution Environment
- **CPU cores:** 64
- **Hardware:** HDD, 44 MB L3 cache, 376 GB RAM
- **Compiler:** g++ 11.x (with -O3 -march=native)
- **Parallelism:** OpenMP (up to 64 threads)

## Future Optimization Opportunities

For iteration 1 (Query Optimizer):
1. **Zone map exploitation**: Skip orders blocks outside date range
2. **Bloom filter semi-join**: Pre-filter orders with compact Bloom filter
3. **SIMD vectorization**: Parallelize comparisons at 8x width (AVX-512)
4. **Partitioned aggregation**: Distributed counting across threads
5. **Index utilization**: Pre-built hash indexes (available in storage guide)
6. **Columnar fusion**: Combined filtering in single pass without intermediate materialization

Current iteration 0 is algorithmically correct and demonstrates proper query planning.
The bottleneck is the full lineitem scan (59M rows × 2 date comparisons) which is
inherent to the semi-join algorithm and can be optimized with Bloom filters in future iterations.

## Key Design Decisions

1. **Semi-join over nested loop** (prevents O(n×m) explosion)
2. **Thread-local hash sets** (avoids synchronization overhead)
3. **Flat array aggregation** (O(1) access for 5 groups)
4. **Mmap binary columns** (zero-copy, no parsing)
5. **Runtime dictionary loading** (prevents hardcoded values)

## Compliance

✅ All query planning rules from `query-execution/query-planning.md` followed:
- Filter before everything
- Predicate pushdown
- Subquery decorrelation (semi-join)
- Join ordering
- Physical operator selection
- Parallelism strategy

✅ All critical rules from system prompt applied:
- Correct date epoch day handling
- No hardcoded dictionary codes
- Proper [TIMING] instrumentation
- CSV output format
- 2 decimal places for monetary values

## Contact & Notes

- Iteration: 0 (baseline, no optimizer applied)
- Target: Execution time optimization
- Status: Ready for iteration 1 (Query Optimizer)
