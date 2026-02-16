# Q14 Implementation Summary (Iteration 0)

## Query: Promotion Effect

Calculates the percentage of revenue from products with types starting with "PROMO".

### SQL
```sql
SELECT
    100.00 * SUM(CASE
        WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
        ELSE 0
    END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem, part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;
```

## Implementation Strategy

### 1. Data Loading (mmap)
- Load 4 lineitem columns: l_partkey, l_extendedprice, l_discount, l_shipdate
- Load 2 part columns: p_partkey, p_type (dictionary-encoded)
- Load p_type dictionary for decoding type strings

### 2. Date Handling
- Converted SQL dates to epoch days:
  - 1995-09-01 = 9374 days since 1970-01-01
  - 1995-10-01 = 9404 days since 1970-01-01
- Filter applies: `shipdate >= 9374 AND shipdate < 9404`
- Selectivity: ~1.25% (749,223 rows out of 59,986,052)

### 3. Join Strategy (Hash Join)
- Build: Hash table mapping p_partkey → p_type_code
  - Size: 2,000,000 entries (all part rows)
- Probe: For each filtered lineitem row, probe hash table for part type
- Dictionary decode: Look up p_type_code to get string value
- String matching: Check if type starts with "PROMO"

### 4. Aggregation
- Thread-local accumulators for parallel execution
- For each qualifying lineitem row:
  - Compute revenue: `extended_price * (1 - discount / 100)`
  - Add to total_revenue
  - If type starts with "PROMO", add to promo_revenue
- Merge thread-local results at end

### 5. Decimal Arithmetic
- Both l_extendedprice and l_discount are scaled by 100 (int64_t)
- Revenue formula: `extended * (100 - discount) / 100`
  - This keeps the result in the same scale (×100)
- Percentage calculation: `(100.0 * promo_revenue) / total_revenue`

### 6. Output
- Single row with promo_revenue percentage
- Format: 2 decimal places
- File: Q14.csv with header "promo_revenue"

## Performance Characteristics

### Compilation
- Flags: `-O3 -march=native -std=c++17 -fopenmp`
- Profiling variant adds `-DGENDB_PROFILE` for timing instrumentation

### Execution Timing (from profiled run)
- Load lineitem: 0.07 ms
- Load part: 0.19 ms
- Build part hash table: 184.65 ms
- Scan + filter + join + aggregate: 138.22 ms
- Merge results: 0.00 ms
- Compute final result: 0.00 ms
- Output: 0.23 ms
- **Total: 323.43 ms**

### Parallelization
- Uses OpenMP with dynamic scheduling
- Morsel size: 100,000 rows
- Thread-local aggregation to avoid synchronization
- 64 CPU cores available (uses all via omp_get_max_threads)

## Correctness Checks

### Storage Format Validation
- ✓ l_shipdate values verified > 3000 (valid epoch days)
- ✓ l_discount values non-zero (verified decimal encoding)
- ✓ p_type dictionary loaded successfully (~150 entries)
- ✓ Date constants computed correctly

### Dictionary Encoding
- ✓ p_type is int16_t dictionary-encoded
- ✓ Dictionary loaded from p_type_dict.txt at runtime
- ✓ Type strings decoded before LIKE matching

### Join Correctness
- ✓ Hash table built on all part rows (2M entries)
- ✓ Probe uses p_partkey from lineitem as key
- ✓ Missing keys gracefully handled (skip row)

### Aggregation Correctness
- ✓ Integer arithmetic preserves precision (scale ×100)
- ✓ Thread-local buffers prevent race conditions
- ✓ Final merge accumulates all thread results

## Result

**promo_revenue: 16.65%**

This means 16.65% of the total revenue from lineitem rows shipped between 1995-09-01 and 1995-10-01 is from products with types starting with "PROMO".

## Known Limitations

1. **No zone map index used**: The lineitem_l_shipdate_zone index is available but not used in iteration 0. Optimization iterations could leverage this for block-level skipping.

2. **Hash table not pre-built**: Part type hash table is built at runtime. Could be pre-computed and mmap'd in later iterations.

3. **Dictionary not cached**: p_type dictionary is parsed at runtime. Could be binarized and mmap'd.

## Files Generated

- `q14.cpp`: Self-contained C++ implementation
- `IMPLEMENTATION_SUMMARY.md`: This document
