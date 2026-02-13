# Sorted Aggregation

## What It Is
Sorted aggregation processes input tuples in sorted order by group key, accumulating aggregate state for each group and emitting results when group boundaries are detected. Achieves O(n) time with O(1) memory overhead by streaming through sorted data.

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
