# GenDB Knowledge Base Index

Read this index first. Then read `query-execution/query-planning.md` and `techniques/beating-general-purpose-engines.md` before writing any code. Only read other files if you need specific implementation details for a technique you plan to use.

## Query Planning (READ FIRST — before writing or optimizing any query code)

Wrong operation ordering causes 10-100x slowdowns even with perfect operators. Plan first, code second.

| File | Technique | When to Use |
|------|-----------|-------------|
| `query-execution/query-planning.md` | **Logical → Physical → Code Pipeline** | **ALWAYS. Covers filter pushdown, join ordering, subquery decorrelation, physical operator selection, data structure choice.** |

## Performance Techniques

Concrete implementation patterns for key optimization techniques. Read `beating-general-purpose-engines.md` first for the philosophical foundation.

| File | Technique | When to Use |
|------|-----------|-------------|
| `techniques/beating-general-purpose-engines.md` | **Specialization Philosophy (READ FIRST)** | **Always. Generated code must exploit specialization to outperform general engines.** |
| `techniques/date-operations.md` | O(1) Date Extraction | Any query extracting year/month/day from epoch-day integers. Replaces loop-based extraction. |
| `techniques/semi-join-patterns.md` | Hash Semi-Join / Anti-Join | EXISTS, NOT EXISTS, IN (SELECT ...) subqueries. Pre-compute inner result into hash set. |
| `techniques/direct-array-lookup.md` | Direct Array Lookup | Join/lookup key with <256 distinct values (small-domain dimension keys, flags). |
| `techniques/bloom-filter-join.md` | Bloom Filter for Joins | Hash joins where build side is much smaller than probe side and many probes have no match. |
| `techniques/late-materialization.md` | Late Materialization | Queries filtering on integers but outputting strings. Load strings only for qualifying rows. |

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
| `storage/encoding-handling.md` | **Storage Encoding Handling** | **CRITICAL: Read FIRST if any column uses dictionary, delta, RLE, or other encoding. Incorrect decoding causes wrong results.** |
| `storage/columnar-vs-row.md` | Columnar vs Row Storage | Analytical queries scanning few columns over many rows. Default for OLAP. |
| `storage/compression.md` | Lightweight Compression | Dictionary encoding for strings, delta/RLE for sorted data, bit packing for small integers. |
| `storage/memory-layout.md` | Memory Layout Optimization | Hot loops, scans, joins — align to cache lines (64B), use SOA pattern. |
| `storage/string-optimization.md` | String Optimization | Low-cardinality strings (e.g., flags, segments), join keys, GROUP BY keys. Interning/dictionary encoding. |
| `storage/persistent-storage.md` | Persistent Binary Storage | Repeated query execution over large datasets. Binary column files + mmap for zero-copy access. Eliminates text parsing. |
| `storage/data-loading-optimization.md` | Data Loading Optimization | Cold-start I/O: data_loading >30% of total, zone maps for selective loading, compression to reduce I/O volume. |

## Indexing

Choose indexes based on the access pattern — each type has a distinct sweet spot:

| File | Technique | When to Use |
|------|-----------|-------------|
| `indexing/zone-maps.md` | Zone Maps (Min/Max) | Range predicates — cheapest index, block-level skip. Almost always worth building on filter columns. |
| `indexing/hash-indexes.md` | Hash Indexes (Multi-Value) | Equi-joins and point lookups. O(1) avg. Multi-value design for join columns with duplicates. NOT for range queries. |
| `indexing/sorted-indexes.md` | B+ Trees & Sorted Indexes | Selective range queries (date BETWEEN X AND Y), ordered scans, merge joins. O(log N) lookup + leaf-level range scan. |
| `indexing/bloom-filters.md` | Bloom Filters | Semi-join reduction, existence checks. Probabilistic — no false negatives. |

## Query Execution

| File | Technique | When to Use |
|------|-----------|-------------|
| `query-execution/vectorized-execution.md` | Vectorized Execution | Process batches of 1024-2048 tuples. Better cache use, enables SIMD. |
| `query-execution/operator-fusion.md` | Operator Fusion | Fuse scan+filter+project into single loop. Reduces materialization. |
| `query-execution/compiled-queries.md` | Compiled Queries | Template specialization for query-specific tight loops. Eliminates interpretation overhead. |
| `query-execution/pipeline-breakers.md` | Pipeline Breakers | Understanding materialization points (hash joins, sorts). Memory budget planning. |
| `query-execution/scan-filter-optimization.md` | Scan & Filter Optimization | Predicate ordering, branch-free filtering, vectorized filtering, predicate pushdown, string optimization. |
| `query-execution/sort-topk.md` | Sort & Top-K Optimization | Partial sort for Top-K, radix sort, sort elimination, parallel merge sort. |
| `query-execution/subquery-optimization.md` | Subquery Optimization | Correlated subquery decorrelation, EXISTS→semi-join, IN→hash semi-join, window functions. |

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

## Performance Patterns

| File | Technique | When to Use |
|------|-----------|-------------|
| `patterns/parallel-hash-join.md` | Parallel Hash Join + Open Addressing | Join operations, especially multi-table joins. Never use std::unordered_map for join hash tables. |
| `patterns/zone-map-pruning.md` | Zone Map Pruning Patterns | Range/comparison predicates on zone-mapped columns. Correct skip logic. **Note: exact binary formats vary per run — rely on per-query storage guides for authoritative layouts.** |
