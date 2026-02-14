# Sort & Top-K Optimization

## What It Is

Techniques to optimize ORDER BY and LIMIT operations. Full sorts are O(n log n) and often unnecessary — Top-K queries only need the K smallest/largest elements, and some sorts can be eliminated entirely.

## Key Implementation Ideas

### Partial Sort for Top-K
- **`std::partial_sort`**: When query has `ORDER BY ... LIMIT K`, use partial sort instead of full sort. Finds the K smallest elements in O(n log K) instead of O(n log n).
  ```cpp
  std::partial_sort(data.begin(), data.begin() + K, data.end(), comparator);
  ```
- **Priority queue (heap) approach**: Maintain a max-heap of size K. For each element, if smaller than heap top, pop top and push new element. O(n log K) time, O(K) memory.
  ```cpp
  std::priority_queue<T> topk;
  for (auto& val : data) {
      if (topk.size() < K) topk.push(val);
      else if (val < topk.top()) { topk.pop(); topk.push(val); }
  }
  ```
- **When K is small relative to N** (K < N/10): Heap approach is significantly faster. For K close to N, full sort may be comparable.

### Radix Sort for Integer Keys
- **O(n) sorting for integers**: Radix sort processes digits from LSB to MSB. For 32-bit keys with 8-bit radix: 4 passes, each O(n). Beats comparison sorts for n > ~10K.
- **Key extraction**: For multi-column ORDER BY, compose a single integer key (pack columns into a uint64_t with proper sign handling).
- **When to use**: Integer/date keys, large arrays (>10K elements), no complex comparison needed.

### Sort Elimination
- **Already-sorted data**: If data comes from a sorted index scan or merge join output, ORDER BY is free — skip the sort entirely.
- **Clustered data**: If the ORDER BY column matches the table's physical sort order (from ingestion), no sort needed.
- **Single-group aggregation**: `SELECT SUM(x) FROM t` — no ORDER BY needed even if specified, since there's one output row.

### Sort-Limit Fusion
- **Early termination**: When processing sorted input with LIMIT K, stop after K qualifying rows. No need to process the entire dataset.
- **Partial scan + partial sort**: If data is partially sorted (e.g., sorted by first ORDER BY column), only sort within groups of the first column.

### Parallel Merge Sort
- **For large datasets requiring full sort**:
  1. Partition data into chunks (one per thread)
  2. Sort each chunk independently in parallel (e.g., `std::sort`)
  3. K-way merge the sorted chunks
- **Parallel Top-K**: Each thread finds local Top-K, then merge K local results to find global Top-K. Only merges K × num_threads elements.
  ```cpp
  // Thread-local Top-K
  std::vector<std::vector<T>> local_topk(num_threads);
  // parallel: each thread finds its K best
  // merge: std::partial_sort on concatenated local results
  ```

### Multi-Key Sort Optimization
- **Compound sort keys**: For `ORDER BY a DESC, b ASC`, pack into a single comparable value where possible (negate a, keep b).
- **Cascade sort**: Sort by primary key first, then stable-sort sub-ranges by secondary key. Avoids repeated comparisons on primary key.
- **Avoid string comparisons in inner loop**: For string ORDER BY, compute integer sort keys (dictionary codes or hash-based) and sort those, then reorder the original data by the resulting permutation.
