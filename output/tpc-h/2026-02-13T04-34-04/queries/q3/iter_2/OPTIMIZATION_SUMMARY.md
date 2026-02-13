# Q3 Optimization Summary - Iteration 2

## Optimization Applied: Array-Based Top-K Aggregation

### Changes Made

**Replaced hash map merge with vector-based aggregation:**

1. **Thread-local storage**: Changed from `vector<OpenHashMap<OrderGroup>>` to `vector<vector<OrderGroup>>` with ~2000 element capacity per thread
   
2. **Aggregation logic**: Replaced hash table lookups with linear search within each thread's vector
   - Linear search is efficient for small result sets (typically <2000 unique orderkeys per thread)
   - Better cache locality compared to hash table probing
   
3. **Merge phase**: Simplified merge using `std::unordered_map` instead of OpenHashMap
   - Handles duplicate orderkeys across threads
   - More straightforward code
   - Reduced memory overhead

### Performance Impact

- **Previous (iter_1)**: 4376 ms
- **Current (iter_2)**: 5838 ms
- **Change**: +1462 ms (33% slower)

**Note**: This optimization did NOT improve performance. The linear search overhead in aggregation outweighs the reduced hash map merge cost.

### Why It Didn't Help

1. **Result set size**: Each thread processes many unique orderkeys, making linear search O(n²) expensive
2. **Hash table efficiency**: The OpenHashMap implementation is already well-optimized
3. **Wrong assumption**: The guidance assumed ~2000 unique keys total, but there are likely many more distributed across 64 threads

### Correctness Verification

Results are **identical** to iter_1:
- Same 10 rows
- Same revenue values
- Same orderdate and shippriority values
- Semantic equivalence preserved ✓

### Recommendation

**Revert this optimization** and keep iter_1 implementation. The hash map approach is superior for this workload.

### Hardware Context

- 376GB RAM available
- 64 CPU cores
- L1d/L1i: 1MB (32 instances)
- L2: 32MB (32 instances)  
- L3: 44MB (2 instances)

