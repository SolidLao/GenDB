# Sort-Merge Join

## What It Is
Sort-merge join sorts both input relations on join keys, then merges them in a single linear scan. Exploits sorted order to match tuples without building hash tables, particularly efficient when inputs are pre-sorted or sorting is cheap.

## When To Use
- One or both inputs already sorted (e.g., index scans, prior ORDER BY)
- Band joins or range joins (e.g., `a.start BETWEEN b.low AND b.high`)
- Low-cardinality join keys where sorting is fast (e.g., date, enum, small integers)
- Memory-constrained environments where hash table doesn't fit
- Many-to-many joins with high match rates (hash join probes repeatedly, merge scans once)

## Key Implementation Ideas

### Basic Merge Algorithm
```cpp
void SortMergeJoin(std::vector<Tuple>& left, std::vector<Tuple>& right) {
    // Phase 1: Sort both sides (skip if already sorted)
    if (!left.IsSorted()) std::sort(left.begin(), left.end(), KeyComparator);
    if (!right.IsSorted()) std::sort(right.begin(), right.end(), KeyComparator);

    // Phase 2: Merge
    size_t left_idx = 0, right_idx = 0;
    while (left_idx < left.size() && right_idx < right.size()) {
        if (left[left_idx].key < right[right_idx].key) {
            left_idx++;
        } else if (left[left_idx].key > right[right_idx].key) {
            right_idx++;
        } else {
            // Match found: emit all combinations with same key
            size_t left_start = left_idx;
            size_t right_start = right_idx;

            while (right_idx < right.size() &&
                   right[right_idx].key == left[left_start].key) {
                left_idx = left_start;
                while (left_idx < left.size() &&
                       left[left_idx].key == right[right_start].key) {
                    EmitJoinResult(left[left_idx], right[right_idx]);
                    left_idx++;
                }
                right_idx++;
            }
        }
    }
}
```

### Exploiting Existing Sort Order
```cpp
// Detect if input is already sorted (avoid redundant sort)
bool CheckSorted(const std::vector<Tuple>& data) {
    for (size_t i = 1; i < data.size(); i++) {
        if (data[i].key < data[i-1].key) return false;
    }
    return true;
}

// Use index scan to produce sorted stream
auto sorted_stream = index->ScanOrdered(join_column);  // B-tree scan
```

### Band Join Optimization
```cpp
// For range joins: a.x BETWEEN b.low AND b.high
// Sort left on x, right on low
void BandJoin(std::vector<Tuple>& left, std::vector<Tuple>& right) {
    std::sort(left.begin(), left.end(), [](auto& a, auto& b) {
        return a.x < b.x;
    });
    std::sort(right.begin(), right.end(), [](auto& a, auto& b) {
        return a.low < b.low;
    });

    size_t right_start = 0;
    for (auto& left_tuple : left) {
        // Advance right_start to first candidate
        while (right_start < right.size() &&
               right[right_start].high < left_tuple.x) {
            right_start++;
        }

        // Scan candidates
        for (size_t i = right_start; i < right.size(); i++) {
            if (right[i].low > left_tuple.x) break;  // Beyond range
            if (right[i].low <= left_tuple.x &&
                left_tuple.x <= right[i].high) {
                EmitJoinResult(left_tuple, right[i]);
            }
        }
    }
}
```

### Cache-Conscious Merge
```cpp
// Process in cache-sized chunks to improve locality
constexpr size_t CHUNK_SIZE = 8192;  // ~64KB chunks

void ChunkedMerge(std::vector<Tuple>& left, std::vector<Tuple>& right) {
    for (size_t left_chunk = 0; left_chunk < left.size(); left_chunk += CHUNK_SIZE) {
        size_t left_end = std::min(left_chunk + CHUNK_SIZE, left.size());

        // Binary search to find right starting position
        auto right_start = std::lower_bound(right.begin(), right.end(),
                                            left[left_chunk].key);

        // Merge this chunk
        MergeChunk(left, left_chunk, left_end, right, right_start);
    }
}
```

## Performance Characteristics
- Sorting cost: O(n log n) for each side if not pre-sorted
- Merge cost: O(n + m) single-pass scan, highly cache-efficient
- Beats hash join when sort cost amortized: pre-sorted inputs, or results used by ORDER BY
- Memory overhead: O(1) if streaming, O(n) if materializing sort
- Best case: 2-3x faster than hash join when both sides pre-sorted
- Worst case: 1.5-2x slower than hash join when sorting from scratch

## Real-World Examples
- **PostgreSQL**: Uses merge join when inputs have useful ORDER BY, or for merge semi-joins
- **DuckDB**: Detects sorted inputs dynamically, falls back to hash join if sort cost high
- **SQL Server**: Merge join for range queries and when nonclustered index provides sort order
- **Oracle**: Sort-merge join as fallback when hash join memory exhausted

## Pitfalls
- **Redundant sorting**: Check if input already sorted (e.g., from index scan, prior ORDER BY)
- **High-cardinality keys**: Sorting strings or large keys is expensive, hash join often better
- **Skewed data**: Long runs of duplicate keys cause quadratic behavior in match phase
- **Ignoring spill cost**: External sorting to disk can be 10x slower than in-memory hash join
- **Many-to-many joins**: Match phase emits O(n*m) results for duplicate keys, buffer carefully
