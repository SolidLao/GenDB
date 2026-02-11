# Zone Maps

## What It Is
Zone maps (also called min/max indexes or small materialized aggregates) store per-block statistics (min, max, count, null count) to skip entire data blocks that cannot match query predicates. They are extremely lightweight metadata structures.

## When To Use
- Range predicates on sorted or clustered data (`WHERE date BETWEEN ... AND ...`)
- Column stores with block-level compression (Parquet, ORC, columnar databases)
- Data warehouses with time-series or partitioned data
- Avoiding expensive scans on cold storage (S3, SSD)
- Multi-column filtering when data is co-clustered

## Key Implementation Ideas
- **Basic Zone Map**: Store min, max, null count, and row count per block; skip block if predicate range does not overlap zone range
- **Multi-Level Zone Maps**: Hierarchical structure (segment -> block -> sub-block) checks coarse filter first, then fine-grained
- **Multi-Column Zone Maps**: Maintain separate min/max per column per zone; combine skip decisions across columns with AND/OR logic
- **Bloom Filter Augmentation**: Pair zone maps with bloom filters for string or categorical columns where min/max is less useful
- **Zone Map Maintenance on Insert**: Update min/max incrementally in O(1); widen zone range as new values arrive
- **Lazy Rebuild on Delete**: Deleting a min/max value marks zone for recomputation rather than scanning immediately
- **Periodic Zone Rebuild**: Rebuilt zones that have become too wide (low selectivity) to restore pruning effectiveness
- **Vectorized Zone Filtering**: Use SIMD to evaluate multiple zone skip conditions in parallel
- **Data Clustering / Sorting**: Zone map effectiveness depends heavily on physical data ordering; sort data by filter columns for best pruning

## Performance Characteristics
- 10-1000x speedup on selective queries (skip 90-99% of blocks on sorted/clustered data)
- Memory overhead: <0.1% (16 bytes per block of 8K-1M rows)
- Pruning efficiency: 95%+ on time-series, 50-80% on clustered data, <20% on random data

## Pitfalls
- Random/unsorted data makes zone maps ineffective (wide min-max ranges cover everything)
- Updates and inserts widen zone ranges over time, reducing selectivity; periodic rebuild needed
- String min/max comparisons are expensive; prefer dictionary encoding with integer zone maps
