# GenDB Knowledge Base Index

Read this index first. Only read individual files if you need specific implementation details for a technique you plan to use.

## Parallelism (CRITICAL - Check First)

Modern CPUs have 8+ cores. Single-threaded execution wastes 87.5%+ of resources and is the #1 performance bottleneck.

**Hardware Detection (do this first):**
- CPU cores: `nproc` → Use for thread count
- Cache: `lscpu | grep cache` → Use for morsel sizing
- SIMD: `lscpu | grep Flags` → Check for avx2, sse4_2

**When to use**: Almost always for scans, joins, aggregations on tables >1M rows

**Thread count**: Use `std::thread::hardware_concurrency()` (typically 8-16 cores)

**Morsel size**: Target L3_cache_size / num_threads / num_columns (typically 10K-100K rows)

**Expected speedup**: Near-linear for scans/joins/aggregations (8 cores = ~7-8x speedup)

| File | Technique | When to Use |
|------|-----------|-------------|
| `parallelism/thread-parallelism.md` | Thread Parallelism | Large scans (>100K rows), parallel joins/aggregations. Morsel-driven approach. **USE THIS FIRST** |
| `parallelism/simd.md` | SIMD (SSE/AVX2) | Filtering, aggregation, hash computation. Process 4-8 values per instruction. |
| `parallelism/data-partitioning.md` | Data Partitioning | Hash or range partitioning for parallel joins, aggregations, sorts. |

## Storage

| File | Technique | When to Use |
|------|-----------|-------------|
| `storage/columnar-vs-row.md` | Columnar vs Row Storage | Analytical queries scanning few columns over many rows. Default for OLAP. |
| `storage/compression.md` | Lightweight Compression | Dictionary encoding for strings, delta/RLE for sorted data, bit packing for small integers. |
| `storage/memory-layout.md` | Memory Layout Optimization | Hot loops, scans, joins — align to cache lines (64B), use SOA pattern. |
| `storage/string-optimization.md` | String Optimization | Low-cardinality strings (e.g., flags, segments), join keys, GROUP BY keys. Interning/dictionary encoding. |
| `storage/persistent-storage.md` | Persistent Binary Storage | Repeated query execution over large datasets. Binary column files + mmap for zero-copy access. Eliminates text parsing. |

## Indexing

| File | Technique | When to Use |
|------|-----------|-------------|
| `indexing/hash-indexes.md` | Hash Indexes | Point lookups, hash joins, GROUP BY. O(1) avg. NOT for range queries. |
| `indexing/sorted-indexes.md` | Sorted Indexes | Range queries, ordered scans, prefix matches. B-trees or sorted arrays. |
| `indexing/bloom-filters.md` | Bloom Filters | Semi-join reduction, existence checks. Probabilistic — no false negatives. |
| `indexing/zone-maps.md` | Zone Maps (Min/Max) | Range predicates on sorted/clustered data. Skip entire blocks. |

## Query Execution

| File | Technique | When to Use |
|------|-----------|-------------|
| `query-execution/vectorized-execution.md` | Vectorized Execution | Process batches of 1024-2048 tuples. Better cache use, enables SIMD. |
| `query-execution/operator-fusion.md` | Operator Fusion | Fuse scan+filter+project into single loop. Reduces materialization. |
| `query-execution/compiled-queries.md` | Compiled Queries | Template specialization for query-specific tight loops. Eliminates interpretation overhead. |
| `query-execution/pipeline-breakers.md` | Pipeline Breakers | Understanding materialization points (hash joins, sorts). Memory budget planning. |

## Joins

| File | Technique | When to Use |
|------|-----------|-------------|
| `joins/hash-join-variants.md` | Hash Join Variants | Default join method. Build on smaller side, probe with larger. Partitioned variant for large data. |
| `joins/sort-merge-join.md` | Sort-Merge Join | Pre-sorted inputs, band/range joins, memory-constrained. |
| `joins/join-ordering.md` | Join Ordering | Multi-way joins. Minimize intermediate results. Greedy heuristic: smallest first. |

## Aggregation

| File | Technique | When to Use |
|------|-----------|-------------|
| `aggregation/hash-aggregation.md` | Hash Aggregation | Medium-high cardinality GROUP BY (100-10M groups). Pre-size hash table. |
| `aggregation/sorted-aggregation.md` | Sorted Aggregation | Pre-sorted input or very low cardinality. O(n) time, O(1) memory. |
| `aggregation/partial-aggregation.md` | Partial Aggregation | Two-phase: local pre-agg + global merge. Reduces data volume for parallel exec. |

## Data Structures

| File | Technique | When to Use |
|------|-----------|-------------|
| `data-structures/compact-hash-tables.md` | Compact Hash Tables | Joins, GROUP BY. Robin Hood or Swiss Tables — 2-5x faster than std::unordered_map. |
| `data-structures/arena-allocation.md` | Arena Allocation | Temporary query structures. 10-100x faster than malloc/free. |
| `data-structures/flat-structures.md` | Flat Data Structures | SOA patterns, contiguous arrays. Better cache, enables SIMD. |

## External Libraries

| File | Technique | When to Use |
|------|-----------|-------------|
| `external-libs/abseil-folly.md` | Abseil/Folly Hash Maps | Drop-in replacement for std::unordered_map. 2-5x speedup. SIMD-accelerated lookups. |
| `external-libs/jemalloc-tcmalloc.md` | jemalloc/tcmalloc | Replace system malloc when allocation is >5% of CPU. Multi-threaded workloads. |
| `external-libs/io-libraries.md` | I/O Libraries (mmap, io_uring) | Loading large datasets (>1GB). Avoid syscall overhead. |
