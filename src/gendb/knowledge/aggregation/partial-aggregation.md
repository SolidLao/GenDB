# Partial Aggregation

## What It Is
Partial aggregation performs aggregation in multiple phases: local pre-aggregation on partitioned data, followed by global aggregation merging partial results. Reduces data volume early, enables parallelism, and improves cache locality in distributed and multi-threaded environments.

## Key Implementation Ideas
- **Two-phase aggregation pattern**: Phase 1 aggregates locally per thread/partition; Phase 2 merges partial results into a global result
- **Thread-local hash tables**: Each thread maintains its own hash table, avoiding synchronization overhead during the local phase
- **Combiner pattern (MapReduce-style)**: Define init/update/combine/finalize operations so aggregates can be partially computed and merged
- **Adaptive partial aggregation**: Monitor reduction factor at runtime; disable partial aggregation if reduction < 50% (overhead exceeds benefit)
- **Partitioned parallel aggregation**: Hash-partition data so each partition has disjoint keys, enabling contention-free parallel merge
- **Distributed aggregation with local pre-aggregation**: Aggregate locally before network shuffle to reduce transfer volume by 80-99%
- **Serialization of partial states**: Compact partial aggregate states for efficient network transfer in distributed settings
- **Data-skew-aware partitioning**: Use fine-grained partitioning or sampling to handle hot keys and avoid load imbalance
- **Non-decomposable aggregate detection**: Identify aggregates like MEDIAN/PERCENTILE that cannot be partially aggregated and require full data
- **Memory-aware spilling**: Thread-local tables multiply memory footprint; spill partitions to disk when necessary
