# Sorted Aggregation

## What It Is
Sorted aggregation processes input tuples in sorted order by group key, accumulating aggregate state for each group and emitting results when group boundaries are detected. Achieves O(n) time complexity with O(1) memory overhead by streaming through sorted data.

## When To Use
- Input already sorted (index scan, prior ORDER BY, sorted table)
- Low-cardinality group keys where sorting is cheap (dates, enums, small integers)
- Memory-constrained environments where hash table doesn't fit
- Query result requires ORDER BY on group key (combine aggregation + sort)
- Streaming queries where incremental results are needed
- High-cardinality aggregation where hash table would spill to disk

## Key Implementation Ideas

### Basic Streaming Aggregation
```cpp
void SortedAggregation(const std::vector<Tuple>& sorted_input) {
    if (sorted_input.empty()) return;

    GroupKey current_group = sorted_input[0].group_key;
    AggregateState current_state = InitializeAggregate(sorted_input[0]);

    for (size_t i = 1; i < sorted_input.size(); i++) {
        if (sorted_input[i].group_key != current_group) {
            // Group boundary: emit result
            EmitResult(current_group, FinalizeAggregate(current_state));

            // Start new group
            current_group = sorted_input[i].group_key;
            current_state = InitializeAggregate(sorted_input[i]);
        } else {
            // Same group: update aggregate
            UpdateAggregate(current_state, sorted_input[i]);
        }
    }

    // Emit final group
    EmitResult(current_group, FinalizeAggregate(current_state));
}
```

### Runtime Sortedness Detection
```cpp
// Detect if input is sorted at runtime to avoid unnecessary sorting
class AdaptiveSortedAggregation {
    bool is_sorted = true;
    GroupKey last_key;
    size_t sample_count = 0;

    bool CheckSorted(const Tuple& tuple) {
        if (!is_sorted) return false;

        if (sample_count > 0 && tuple.group_key < last_key) {
            is_sorted = false;
            return false;
        }

        last_key = tuple.group_key;
        sample_count++;

        // After sampling, commit to strategy
        if (sample_count >= 1000) {
            return is_sorted;
        }
        return true;
    }

    void Process(std::vector<Tuple>& input) {
        // Sample first batch
        for (size_t i = 0; i < std::min(1000UL, input.size()); i++) {
            CheckSorted(input[i]);
        }

        if (is_sorted) {
            StreamingAggregation(input);
        } else {
            std::sort(input.begin(), input.end());
            StreamingAggregation(input);
        }
    }
};
```

### Exploiting Index Scans
```cpp
// Use B-tree index to produce sorted stream
class IndexSortedAggregation {
    BTreeIndex* index;

    void AggregateFromIndex(const GroupByColumn& col) {
        // Scan index in sorted order
        auto iterator = index->CreateOrderedIterator(col);

        GroupKey current_group;
        AggregateState state;
        bool first = true;

        while (iterator->HasNext()) {
            auto tuple = iterator->Next();

            if (first || tuple.group_key != current_group) {
                if (!first) EmitResult(current_group, state);
                current_group = tuple.group_key;
                state = InitializeAggregate(tuple);
                first = false;
            } else {
                UpdateAggregate(state, tuple);
            }
        }

        if (!first) EmitResult(current_group, state);
    }
};
```

### Early Termination with LIMIT
```cpp
// Stop early when LIMIT is satisfied
void SortedAggregationWithLimit(const std::vector<Tuple>& sorted_input,
                                 size_t limit) {
    size_t emitted = 0;
    GroupKey current_group = sorted_input[0].group_key;
    AggregateState state = InitializeAggregate(sorted_input[0]);

    for (size_t i = 1; i < sorted_input.size(); i++) {
        if (sorted_input[i].group_key != current_group) {
            EmitResult(current_group, FinalizeAggregate(state));
            emitted++;

            if (emitted >= limit) return;  // Early termination

            current_group = sorted_input[i].group_key;
            state = InitializeAggregate(sorted_input[i]);
        } else {
            UpdateAggregate(state, sorted_input[i]);
        }
    }

    if (emitted < limit) {
        EmitResult(current_group, FinalizeAggregate(state));
    }
}
```

### Multi-Column Grouping
```cpp
// Handle composite group keys efficiently
struct CompositeKey {
    int col1;
    int col2;

    bool operator!=(const CompositeKey& other) const {
        return col1 != other.col1 || col2 != other.col2;
    }

    bool operator<(const CompositeKey& other) const {
        if (col1 != other.col1) return col1 < other.col1;
        return col2 < other.col2;
    }
};

// Optimize comparison: check most significant column first
bool GroupBoundary(const CompositeKey& a, const CompositeKey& b) {
    // Fast path: most selective column differs
    if (a.col1 != b.col1) return true;
    return a.col2 != b.col2;
}
```

## Performance Characteristics
- O(n) time for sorted input, O(n log n) if sorting required
- O(1) memory overhead: single aggregate state per group key
- Throughput: 200-800M rows/sec/core (no hash table overhead)
- Cache-friendly: sequential access pattern, 95%+ cache hit rate
- Beats hash aggregation when:
  - Input already sorted (no sort cost)
  - Very high cardinality (millions of groups, hash table doesn't fit)
  - Query has ORDER BY on group key (combine operations)
- Sorting cost: 100-200 CPU cycles per tuple, dominates when input unsorted

## Real-World Examples
- **PostgreSQL**: Uses sorted aggregation when input from index scan or has ORDER BY
- **DuckDB**: Detects sorted inputs, uses streaming aggregation with early termination
- **SQL Server**: Stream aggregation for ordered inputs, combines with sort for ORDER BY
- **MonetDB**: Exploits column store ordering, sorted aggregation as default strategy

## Pitfalls
- **Sorting unsorted data**: If input not sorted, sort cost dominates, hash aggregation usually better
- **Ignoring existing order**: Fails to detect index scans or prior sorts, redundantly sorts
- **No spill strategy**: If must sort, external sort required for large inputs exceeding memory
- **Large group runs**: Many duplicates in single group still require O(n) scan, no early skip
- **Multi-column keys**: Comparison overhead grows with key width, consider key encoding
- **Assuming sortedness**: Validate input is sorted, unsorted input produces wrong results
- **No vectorization**: Process 4-8 tuples per loop iteration for SIMD aggregate updates
