# Partial Aggregation

## What It Is
Partial aggregation performs aggregation in multiple phases: local pre-aggregation on partitioned data, followed by global aggregation merging partial results. Reduces data volume early, enables parallelism, and improves cache locality in distributed and multi-threaded environments.

## When To Use
- Parallel aggregation across multiple threads or cores
- Distributed aggregation across network nodes (reduce shuffle volume)
- High reduction factor: pre-aggregation eliminates 80%+ of rows
- Decomposable aggregates (SUM, COUNT, MIN, MAX, AVG) that support partial/merge
- GROUP BY on moderate cardinality (1K-1M groups) where local aggregation effective
- OLAP queries processing billions of rows where early reduction critical

## Key Implementation Ideas

### Two-Phase Aggregation Pattern
```cpp
// Phase 1: Local aggregation (per thread/partition)
struct PartialAggregateResult {
    std::unordered_map<GroupKey, AggregateState> local_aggregates;
};

PartialAggregateResult LocalAggregate(const std::vector<Tuple>& input) {
    PartialAggregateResult result;

    for (const auto& tuple : input) {
        auto& state = result.local_aggregates[tuple.group_key];
        UpdateAggregate(state, tuple);
    }

    return result;
}

// Phase 2: Global aggregation (merge partial results)
std::unordered_map<GroupKey, AggregateState> GlobalAggregate(
    const std::vector<PartialAggregateResult>& partial_results) {

    std::unordered_map<GroupKey, AggregateState> global;

    for (const auto& partial : partial_results) {
        for (const auto& [key, state] : partial.local_aggregates) {
            MergeAggregate(global[key], state);
        }
    }

    return global;
}
```

### Thread-Local Aggregation
```cpp
// Each thread maintains its own hash table, avoiding synchronization
class ParallelAggregation {
    std::vector<std::unordered_map<GroupKey, AggregateState>> thread_local_agg;
    size_t num_threads;

    void ParallelAggregate(const std::vector<Tuple>& input) {
        thread_local_agg.resize(num_threads);

        // Phase 1: Parallel local aggregation
        #pragma omp parallel for
        for (size_t tid = 0; tid < num_threads; tid++) {
            size_t start = tid * input.size() / num_threads;
            size_t end = (tid + 1) * input.size() / num_threads;

            for (size_t i = start; i < end; i++) {
                auto& state = thread_local_agg[tid][input[i].group_key];
                UpdateAggregate(state, input[i]);
            }
        }

        // Phase 2: Merge thread-local results
        std::unordered_map<GroupKey, AggregateState> final_result;
        for (const auto& local : thread_local_agg) {
            for (const auto& [key, state] : local) {
                MergeAggregate(final_result[key], state);
            }
        }
    }
};
```

### Combiner Pattern (MapReduce-style)
```cpp
// Decomposable aggregate functions
struct AggregateCombiner {
    // Initialize from single tuple
    static AggregateState Init(const Tuple& t) {
        return {t.value, 1};  // sum, count
    }

    // Update with another tuple
    static void Update(AggregateState& state, const Tuple& t) {
        state.sum += t.value;
        state.count++;
    }

    // Merge two partial aggregate states
    static void Combine(AggregateState& dest, const AggregateState& src) {
        dest.sum += src.sum;
        dest.count += src.count;
    }

    // Finalize to produce result
    static double Finalize(const AggregateState& state) {
        return static_cast<double>(state.sum) / state.count;  // AVG
    }
};
```

### Adaptive Partial Aggregation
```cpp
// Dynamically decide whether partial aggregation is beneficial
class AdaptivePartialAggregation {
    size_t input_rows = 0;
    size_t output_rows = 0;
    double reduction_factor = 1.0;
    bool enable_partial = true;

    void ProcessBatch(const std::vector<Tuple>& batch) {
        input_rows += batch.size();

        auto partial_result = LocalAggregate(batch);
        output_rows += partial_result.local_aggregates.size();

        // Compute reduction factor
        reduction_factor = static_cast<double>(output_rows) / input_rows;

        // Disable partial aggregation if not effective (< 50% reduction)
        if (reduction_factor > 0.5) {
            enable_partial = false;
            // Fall back to direct global aggregation
        }
    }
};
```

### Partitioned Parallel Aggregation
```cpp
// Partition data by hash to avoid contention in merge phase
class PartitionedAggregation {
    static constexpr size_t NUM_PARTITIONS = 256;

    void PartitionedParallelAggregate(const std::vector<Tuple>& input) {
        // Phase 1: Partition input by hash
        std::vector<std::vector<Tuple>> partitions(NUM_PARTITIONS);
        for (const auto& tuple : input) {
            size_t partition = Hash(tuple.group_key) % NUM_PARTITIONS;
            partitions[partition].push_back(tuple);
        }

        // Phase 2: Aggregate each partition independently (fully parallel)
        std::vector<std::unordered_map<GroupKey, AggregateState>> results(NUM_PARTITIONS);

        #pragma omp parallel for
        for (size_t i = 0; i < NUM_PARTITIONS; i++) {
            for (const auto& tuple : partitions[i]) {
                auto& state = results[i][tuple.group_key];
                UpdateAggregate(state, tuple);
            }
        }

        // Phase 3: Merge partitions (no contention, disjoint keys per partition)
        std::unordered_map<GroupKey, AggregateState> final;
        for (const auto& partition_result : results) {
            final.insert(partition_result.begin(), partition_result.end());
        }
    }
};
```

### Distributed Aggregation
```cpp
// Network-aware partial aggregation
class DistributedAggregation {
    void DistributedAggregate(const std::vector<Tuple>& local_input) {
        // Phase 1: Local aggregation (reduce network transfer)
        auto local_result = LocalAggregate(local_input);

        // Serialize partial results (much smaller than raw data)
        auto serialized = Serialize(local_result);

        // Phase 2: Shuffle partial results to coordinator
        auto all_partials = GatherPartialResults(serialized);

        // Phase 3: Final aggregation at coordinator
        auto final_result = GlobalAggregate(all_partials);
    }
};
```

## Performance Characteristics
- Reduction factor: 10-1000x for low-to-medium cardinality, <2x for high cardinality
- Parallelism: Linear scaling to 8-16 threads with thread-local aggregation
- Network savings: 80-99% reduction in shuffle volume for distributed queries
- Overhead: 20-30% from merge phase when reduction factor < 2x
- Best case: 10x speedup with 95%+ reduction factor and 16-way parallelism
- Worst case: 1.3x slowdown when cardinality equals input size (no reduction)

## Real-World Examples
- **DuckDB**: Thread-local hash tables with lock-free merge, adaptive partial aggregation
- **ClickHouse**: Two-level aggregation with thread-local + global merge, pre-aggregation in storage
- **Spark**: Combiner functions for map-side aggregation, reduces shuffle by 10-100x
- **Presto**: Partial aggregation on workers, coordinator merges final results

## Pitfalls
- **High cardinality**: When groups ≈ rows, partial aggregation adds overhead without benefit
- **No reduction check**: Always doing partial aggregation even when ineffective, monitor reduction factor
- **Contention in merge**: Global hash table becomes bottleneck, use partitioned merge
- **Over-partitioning**: Too many partitions increase coordination overhead, balance at 8-256 partitions
- **Non-decomposable aggregates**: MEDIAN, PERCENTILE cannot be partially aggregated, require full data
- **Ignoring data skew**: Hot keys cause load imbalance, use fine-grained partitioning or sampling
- **Memory explosion**: Thread-local tables multiply memory footprint, spill when necessary
