# Q1 Query Implementation - Iteration 0

## Overview
Self-contained C++ implementation of TPC-H Query 1 (Pricing Summary Report) targeting execution_time optimization.

**Status**: ✅ **COMPLETE & VALIDATED**
- Output byte-for-byte matches ground truth
- All aggregations verified to required precision
- Compilation successful with all required instrumentation
- Performance: 52M rows/second on 59.9M row dataset

## Quick Start

### Compilation
```bash
# With profiling instrumentation
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o q1 q1.cpp

# Final production version
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o q1_final q1.cpp
```

### Execution
```bash
./q1 <gendb_dir> [results_dir]

# Example:
./q1 /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb ./results
```

### Output
- **File**: `<results_dir>/Q1.csv`
- **Format**: CSV with comma delimiter, CRLF line endings
- **Columns**: 10 (l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
- **Rows**: 4 (distinct combinations of return flag and line status)

## Implementation Details

### Query Structure
```sql
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

### Execution Strategy

**Logical Plan**:
1. Filter lineitem table on `l_shipdate <= 1998-09-02`
2. Group by two low-cardinality columns (max 8 groups)
3. Compute 8 aggregations with complex arithmetic
4. Sort by return flag and line status
5. Output to CSV

**Physical Plan**:
- **Scan**: Sequential full scan with dictionary and scaled integer decoding
- **Filter**: Date predicate applied during scan (threshold: epoch day 10471)
- **Aggregation**: Flat array (6 elements) indexed by `returnflag_code * 2 + linestatus_code`
- **Output**: CRLF-delimited CSV with `long double` precision

### Key Decisions

1. **Low-Cardinality Grouping**: 6-element flat array instead of hash table
   - Much faster for <256 groups
   - Zero allocation overhead
   - Cache-friendly access patterns

2. **Integer Arithmetic Throughout**:
   - All scaled values kept as `int64_t` during aggregation
   - Prevents IEEE 754 floating-point precision loss
   - Scale factors: 100 (base), 10,000 (discount price), 1,000,000 (charge)
   - Division to double only at final output stage

3. **Dictionary Encoding**:
   - Load dictionary files at runtime (not hardcoded)
   - Store codes as `int32_t` during computation
   - Decode only for CSV output
   - l_returnflag: 0=N, 1=R, 2=A
   - l_linestatus: 0=O, 1=F

4. **Date Handling**:
   - Epoch days (1970-01-01 = day 0)
   - Computed dynamically: 1998-09-02 = day 10471
   - Integer comparison, YYYY-MM-DD output only

## Performance Characteristics

### Time Breakdown
- Dictionary load: 0.06 ms
- Column mmap: 0.05 ms
- Scan + filter + aggregate: 983 ms (85% of total)
- Sort + output: 0.28 ms
- **Total**: 1.15 seconds for 59.9M rows

### Throughput
- **52 million rows per second**
- Linear scaling (no algorithmic bottleneck)
- Bounded by memory bandwidth

### Cache Efficiency
- Working set: 6 aggregate values + dictionary cache
- Sequential access pattern enables prefetching
- L1 cache: Aggregate state (48 bytes)
- L3 cache: Column data (44 MB system capacity)

## Correctness Verification

✅ **Binary Equality**: Output matches ground truth byte-for-byte

✅ **Aggregation Precision**:
- `sum_qty`: Exact integer match
- `sum_base_price`: 2 decimal places
- `sum_disc_price`: 4 decimal places
- `sum_charge`: 6 decimal places
- `avg_*`: 2 decimal places
- `count_order`: Exact integer

✅ **Encoding Handling**:
- Dictionary codes loaded and applied correctly
- Scaled arithmetic maintains precision
- Date filtering applies correct threshold
- Output format matches specification

## File Organization

```
q1.cpp                          # Main implementation (384 lines)
q1                              # Compiled with profiling (46 KB)
q1_final                        # Production binary (46 KB)
results/
  └─ Q1.csv                     # Output file (515 bytes, 4 data rows)
IMPLEMENTATION_SUMMARY.md       # Technical summary
VALIDATION_REPORT.txt          # Detailed validation results
README.md                       # This file
```

## Code Quality

- **Standards**: C++17
- **Compilation**: Clean (0 errors, 0 warnings)
- **Instrumentation**: `#ifdef GENDB_PROFILE` timing guards on all major operations
- **Error Handling**: File open failures checked, proper cleanup
- **Memory**: Zero-copy mmap access to binary columns
- **Dependencies**: None (standard library only)

## Implementation Notes

### Why No Parallelization in Iteration 0?
This iteration establishes correct single-threaded baseline. Parallelization is reserved for Iteration 1+ with expected 7-8x speedup on 64-core CPU.

### Why Flat Array Instead of Hash Table?
- 6-element array costs: O(1) lookup, 48 bytes storage
- Hash table costs: O(1) avg lookup, malloc overhead, pointer chasing
- For 6 groups, array is 2-5x faster

### Why long double for Output?
- Accumulated sums are `int64_t` (no intermediate precision loss)
- Division creates `long double` temporaries for formatting
- Prevents IEEE 754 rounding errors in final output

### Why CRLF Line Endings?
Ground truth uses Windows-style line endings (`\r\n`), required for byte-exact validation.

## Future Optimizations (Iteration 1+)

1. **Parallelization**: OpenMP parallel for on scan with thread-local aggregation
2. **SIMD**: Vectorized filtering and aggregation (8-16 rows/iteration)
3. **Zone Map Pruning**: Skip blocks where max_shipdate > threshold (though selectivity is high)
4. **Morsel-Driven**: Chunk execution for cache efficiency
5. **Columnar Compression**: Delta encoding on dates, lightweight string compression

## References

- **Knowledge Base**: `/home/jl4492/GenDB/src/gendb/knowledge/`
- **Query Planning**: `query-execution/query-planning.md`
- **Storage Encoding**: `storage/encoding-handling.md`
- **Zone Maps**: `indexing/zone-maps.md`

---

**Generated**: 2026-02-16
**Validation**: ✅ PASSED
**Status**: Ready for optimization in Iteration 1
