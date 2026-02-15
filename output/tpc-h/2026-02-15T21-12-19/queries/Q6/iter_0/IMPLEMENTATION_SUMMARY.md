# Q6 Implementation Summary (Iteration 0)

## Query
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE
    l_shipdate >= DATE '1994-01-01' (8766 days)
    AND l_shipdate < DATE '1995-01-01' (9131 days)
    AND l_discount BETWEEN 0.05 AND 0.07 (5 ≤ scaled ≤ 7)
    AND l_quantity < 24 (scaled: < 2400)
```

## Implementation Details

### Data Loading
- **Columns loaded via mmap**: l_shipdate (int32_t), l_discount (int64_t), l_quantity (int64_t), l_extendedprice (int64_t)
- **Total rows**: 59,986,052 (lineitem table)
- **Scale factor**: 100 for all DECIMAL columns (int64_t storage)

### Filtering Strategy
- **Single-pass parallel scan** over all rows using OpenMP
- **Branch-free predicate evaluation** for all three conditions:
  - l_shipdate >= 8766 AND < 9131
  - l_discount >= 5 AND <= 7
  - l_quantity < 2400
- **Dynamic scheduling** with 100K-row chunks for load balancing across 32 cores

### Aggregation
- **Product computation**: `l_extendedprice * l_discount` (both int64_t scaled by 100)
- **Result scaling**: Product is scaled by 10000 (100 × 100)
- **Thread-local accumulation**: Each thread maintains its own int64_t sum
- **Final merge**: Sum thread-local values to get total
- **Output scaling**: Divide total by 10000.0 to convert to decimal format (2 decimal places)

### Timing Instrumentation (with -DGENDB_PROFILE)
- `mmap_columns`: Data loading time
- `scan_filter_aggregate`: Core computation (filtering + multiplication + summation)
- `output`: CSV writing time
- `total`: Total execution time (excluding I/O)

## Performance (32 cores, HDD storage)
- **Execution time (with profiling)**: ~24 ms
- **Expected filtered rows**: ~780K rows (1.3% selectivity of 60M)
- **Parallelization**: Full vectorized scan across all 32 cores with thread-local aggregation

## Validation
- **Expected output**: revenue = 1230113636.0101
- **Actual output**: revenue = 1230113636.0101
- **Status**: ✓ PASS (exact match)

## File Structure
- `q6.cpp`: Self-contained C++ implementation
- Output: `results/Q6.csv` (comma-delimited with header)

## Key Correctness Rules Applied
1. ✓ DATE columns treated as int32_t epoch days (>3000 range)
2. ✓ DECIMAL columns preserved as int64_t throughout computation (no IEEE 754 errors)
3. ✓ Decimal thresholds computed from scale_factor (0.05 × 100 = 5, etc.)
4. ✓ Product aggregation at full precision (int64_t) before scaling down once
5. ✓ CSV output with 4 decimal places for monetary values
