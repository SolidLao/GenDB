# Sorted Aggregation

## What It Is
Sorted aggregation processes input tuples in sorted order by group key, accumulating aggregate state for each group and emitting results when group boundaries are detected. Achieves O(n) time with O(1) memory overhead by streaming through sorted data.

## When To Use
- Input already sorted (index scan, prior ORDER BY, sorted table)
- Low-cardinality group keys where sorting is cheap (dates, enums, small integers)
- Memory-constrained environments where hash table doesn't fit
- Query result requires ORDER BY on group key (combine aggregation + sort)
- Streaming queries where incremental results are needed

## Key Implementation Ideas
- **Streaming group-boundary detection**: Track current group key; emit result and reset state when key changes, single-pass O(n) with O(1) memory
- **Runtime sortedness detection**: Sample initial tuples to detect if input is already sorted; fall back to hash aggregation if not
- **Exploiting index scans**: Use B-tree index ordered iterator to produce a sorted stream, avoiding explicit sort entirely
- **Early termination with LIMIT**: Stop scanning once the required number of groups have been emitted
- **Multi-column key comparison optimization**: Check most selective (most significant) column first for fast group-boundary detection
- **Key encoding for composite keys**: Normalize multi-column keys into single comparable values to reduce per-tuple comparison cost
- **SIMD-vectorized aggregate updates**: Batch 4-8 tuples per iteration for vectorized aggregate computation within a group run
- **External sort fallback**: For unsorted inputs exceeding memory, use external merge sort before streaming aggregation
- **Pipelining with downstream operators**: Emit groups incrementally so downstream operators can begin processing without waiting for full aggregation

## Performance Characteristics
- O(n) time for sorted input; 200-800M rows/sec/core with sequential access and 95%+ cache hit rate
- O(1) memory overhead (single aggregate state active at a time)
- Sorting cost if needed: O(n log n), 100-200 CPU cycles per tuple, dominates when input unsorted

## Pitfalls
- **Sorting unsorted data**: Sort cost dominates; hash aggregation is usually better for unsorted input
- **Ignoring existing order**: Failing to detect index scans or prior sorts leads to redundant sorting
- **Assuming sortedness without validation**: Unsorted input silently produces wrong results
