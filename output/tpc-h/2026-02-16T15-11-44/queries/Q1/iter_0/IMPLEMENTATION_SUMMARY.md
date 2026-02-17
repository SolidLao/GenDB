# Q1 Implementation Summary

## Query
Pricing Summary Report - TPC-H Q1
- **Predicate**: `l_shipdate <= 1998-09-02` (1998-12-01 - 90 days)
- **GROUP BY**: `l_returnflag, l_linestatus` (8 groups)
- **Aggregations**: SUM/AVG/COUNT with complex expressions

## Execution Strategy

### Logical Plan
1. Single table scan: lineitem (59.986M rows)
2. Apply date filter during scan
3. GROUP BY on 2 low-cardinality columns (returns ~4 groups)
4. Compute aggregations with scaled integer arithmetic
5. Sort by return flag and line status
6. Output to CSV

### Physical Plan
1. **Scan**: Full sequential scan with zone map (not used for pruning as filter is permissive)
2. **Filter**: Date predicate (l_shipdate <= 10471 epoch days) applied during scan
3. **Aggregation**: Flat array indexed by (returnflag_code × 2 + linestatus_code)
   - 6-element array (low cardinality → fastest approach)
   - No hash table overhead
4. **Sorting**: Built-in sort on small result set (4 groups)
5. **Output**: CSV with CRLF line endings

## Key Implementation Details

### Encoding Handling
- **Dictionary encoding**: Load `*_dict.txt` at runtime
  - `l_returnflag`: codes 0=N, 1=R, 2=A
  - `l_linestatus`: codes 0=O, 1=F
  - Store as int32_t, decode only for output
- **Scaled decimals**: All monetary values are int64_t × scale_factor (100)
  - No double precision used during calculation to avoid IEEE 754 boundary errors
  - Scale-factor conversions done explicitly:
    - `sum_qty`: scaled by 100 (divide by 100 for output)
    - `sum_base_price`: scaled by 100
    - `sum_disc_price`: scaled by 10,000 (price × (100-discount))
    - `sum_charge`: scaled by 1,000,000 (price × (100-discount) × (100+tax))
- **Date arithmetic**: Epoch days (int32_t) from 1970-01-01
  - Computed dynamically: 1998-09-02 = 10,471 days
  - Comparison as integers, YYYY-MM-DD output only for CSV

### Aggregation Math
For each row matching the date filter:
1. `sum_qty += l_quantity`
2. `sum_base_price += l_extendedprice`
3. `sum_disc_price += l_extendedprice × (100 - l_discount)` [stored as int64_t × 10000]
4. `sum_charge += l_extendedprice × (100 - l_discount) × (100 + l_tax)` [stored as int64_t × 1000000]
5. `sum_discount += l_discount` [for AVG calculation]
6. `count_order++`

Averages computed as: `sum / count / scale_factor`

### Precision
- Used `long double` for final output calculations to avoid floating-point precision loss
- Integer-only accumulation during scan (no intermediate floating-point conversion)

## Performance

### Execution Time (profiled run)
- **Dictionary load**: 0.06 ms
- **Column load (mmap)**: 0.05 ms
- **Scan + filter + aggregate**: 983 ms (for 59.9M rows)
- **Sort + output**: 0.28 ms
- **Total**: 1153 ms

### Scalability
- Linear scaling with number of rows (no hash table overhead)
- Minimal memory overhead (6 aggregate rows)
- Cache-friendly array indexing

## Correctness Validation
✓ Output matches ground truth byte-for-byte
✓ All aggregation values match to 6 decimal places
✓ Row count: 4 groups (A/F, N/F, N/O, R/F)

## Code Quality
- TIMING instrumentation for all major operations
- Proper error handling for file operations
- mmap for zero-copy column access
- Standard C++17, -O3 optimization, -march=native SIMD

## Future Optimization Opportunities (Iterations 1+)
1. Parallelization: OpenMP parallel for on row scan with thread-local aggregation
2. SIMD: Vectorized filtering and aggregation (8-16 rows per iteration)
3. Zone map pruning: Statically prune blocks where max_shipdate > 10471 (though selectivity is high)
4. Memory layout: SOA layout for better cache efficiency
5. Columnar compression: Delta encoding on l_shipdate, lightweight compression on dictionaries
