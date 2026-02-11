# Data Partitioning

## What It Is

Splitting data into disjoint subsets based on a key (hash or range) to enable parallel processing without coordination. Each partition can be processed independently by different threads or cores.

## When To Use

- Hash joins: partition both inputs on join key, then perform local joins per partition
- GROUP BY aggregations: partition on group key, aggregate locally without locks
- Parallel sort: partition into ranges, sort each partition independently
- Reducing lock contention: each partition has its own isolated data structure

## Key Implementation Ideas

- **Hash partitioning**: assign rows to partitions via hash(key) % num_partitions, enabling coordination-free parallel processing of each partition

- **Pre-allocation with slack**: reserve ~120% of expected partition size to avoid reallocation during the scatter phase

- **Radix partitioning (multi-pass)**: use low N bits of hash for cache-friendly partitioning; two-pass approach (count then scatter) avoids random writes

- **Prefix-sum offset computation**: first pass counts per-partition sizes, then exclusive scan computes write offsets for a scatter pass

- **Partition-wise join**: hash-partition both join inputs on the join key so each partition pair can be joined independently in parallel

- **Range partitioning for sort**: sample data to determine range boundaries, partition by range, sort each partition, then concatenate for a globally sorted result

- **Power-of-2 partition counts**: enable fast modulo via bitwise AND instead of expensive division

- **Partition size targeting**: aim for 100KB-1MB per partition to fit in L2/L3 cache

## Performance Characteristics

- **Scalability**: linear speedup with partition count up to core count
- **Overhead**: partitioning pass adds ~5-10% overhead, pays off with >4 cores
- **Cache**: target 100KB-1MB per partition to fit in L2/L3

## Pitfalls

- **Skewed data**: hash partitioning fails on skewed keys; use radix or hybrid approaches
- **Too many partitions**: >256 partitions thrash cache; sweet spot is 8-64 for joins
- **Small data**: partitioning overhead dominates for <100K rows
