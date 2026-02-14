# Scan & Filter Optimization

## What It Is

Techniques to accelerate table scans and predicate evaluation — the foundation of query execution. Filters are the first opportunity to reduce data volume before expensive operations (joins, aggregations).

## Key Implementation Ideas

### Predicate Ordering
- **Most selective predicates first**: Evaluate the predicate that eliminates the most rows before others. If predicate A keeps 10% of rows and predicate B keeps 50%, evaluate A first — B then runs on 10% of data instead of 100%.
- **Cheap predicates before expensive ones**: Integer comparisons before string operations, equality before LIKE.
- **Short-circuit compound predicates**: For `AND`, skip remaining predicates once one is false. For `OR`, skip once one is true.

### Branch-Free Filtering
- **Arithmetic predicate evaluation**: Convert boolean conditions to 0/1 integers using arithmetic to avoid branch misprediction.
  ```cpp
  // Branchy (bad for unpredictable predicates):
  if (col[i] > threshold) count++;
  // Branch-free (good for ~50% selectivity):
  count += (col[i] > threshold);
  ```
- **When to use**: Selectivity between 20-80% (branches are unpredictable). For very high (>95%) or very low (<5%) selectivity, branches predict well.
- **Batch selection vectors**: Produce an array of qualifying row indices, then apply subsequent operations only to those rows.

### Vectorized Filtering
- **Process in batches of 1024-4096 rows**: Better cache utilization and enables SIMD.
- **SIMD comparisons**: Use AVX2 to compare 8 int32 values or 4 int64 values simultaneously:
  ```cpp
  __m256i data = _mm256_loadu_si256((__m256i*)(col + i));
  __m256i thresh = _mm256_set1_epi32(threshold);
  __m256i mask = _mm256_cmpgt_epi32(data, thresh);
  ```
- **Combine filter results with bitwise ops**: AND/OR filter bitmasks for compound predicates before materializing qualifying rows.

### Predicate Pushdown
- **Push filters before joins**: Filter each table independently before the join to reduce the build/probe cardinality.
- **Push filters before aggregation**: Filter rows before GROUP BY to reduce aggregation input.
- **Push into mmap reads**: Combine zone map skipping (block-level min/max) with predicate evaluation to skip entire memory pages.

### String Predicate Optimization
- **LIKE prefix optimization**: `LIKE 'ABC%'` → range scan `>= 'ABC' AND < 'ABD'`. No regex needed.
- **Dictionary-encoded filtering**: For dictionary-encoded string columns, filter on dictionary codes (integers) instead of strings. Resolve matching codes once, then scan the code array.
- **Length pre-check**: For fixed-pattern LIKE, check string length before expensive character comparison.

### Scan Parallelism
- **Partition scan range across threads**: Divide row range into morsels (10K-100K rows each), assign to thread pool.
- **Thread-local result buffers**: Each thread collects qualifying rows into its own buffer, merge at the end.
- **Avoid false sharing**: Ensure thread-local buffers are on separate cache lines (align to 64 bytes).
