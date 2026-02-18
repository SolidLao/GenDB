# GenDB Knowledge Base

Optimization knowledge for GenDB agents. Each file covers one technique with actionable C++ patterns.

## How Agents Should Use This

1. Read files relevant to your current task (not all files)
2. Use the knowledge to inform technique selection, not as rigid instructions
3. You may propose approaches NOT in this knowledge base — novel ideas are encouraged
4. Each file has: What It Is, When To Use, Key Implementation Ideas, Performance Characteristics, Pitfalls
5. Check `experience.md` for known correctness and performance issues
6. Use the utility library (`src/gendb/utils/`) for date operations, hash tables, mmap, and timing

## Experience Base
- `experience.md` — Known correctness issues (C1-C4) and performance anti-patterns (P1-P5). Checked by Code Inspector, extended by DBA.

## Index

### techniques/
- `beating-general-purpose-engines.md` — Specialization philosophy, hand-written code advantages
- `date-operations.md` — O(1) date extraction via lookup tables. **Use date_utils.h**
- `semi-join-patterns.md` — Hash semi-join/anti-join for EXISTS/NOT EXISTS/IN. Combined EXISTS+NOT EXISTS pattern
- `direct-array-lookup.md` — Direct array indexing for small-domain keys
- `bloom-filter-join.md` — Bloom filter pre-filtering for joins
- `late-materialization.md` — Defer string loading until qualifying rows identified

### storage/
- `encoding-handling.md` — Dictionary, delta, RLE, date, decimal encoding
- `columnar-vs-row.md` — When columnar wins, layout strategies
- `compression.md` — Dictionary encoding, RLE, bitpacking, delta encoding
- `memory-layout.md` — Cache-line alignment, struct-of-arrays, prefetching
- `string-optimization.md` — String interning, dictionary-coded strings
- `persistent-storage.md` — Binary column files, mmap zero-copy. **Use mmap_utils.h**

### indexing/
- `hash-indexes.md` — Robin hood, open addressing, multi-value design
- `sorted-indexes.md` — Binary search, B-trees, interpolation search
- `zone-maps.md` — Min/max indexes, block-level filtering
- `bloom-filters.md` — Probabilistic filtering, optimal sizing

### query-execution/
- `query-planning.md` — Logical -> Physical -> Code pipeline (MANDATORY before all code)
- `vectorized-execution.md` — Batch processing, vector-at-a-time
- `operator-fusion.md` — Fusing scan+filter+project
- `compiled-queries.md` — Code generation, template specialization
- `pipeline-breakers.md` — Materialization points, pipeline design
- `scan-filter-optimization.md` — Predicate ordering, branch-free filtering
- `sort-topk.md` — Partial sort, radix sort, Top-K
- `subquery-optimization.md` — Decorrelation, EXISTS->semi-join, self-referencing aggregate subquery

### joins/
- `hash-join-variants.md` — Partitioned, grace, build/probe optimization
- `sort-merge-join.md` — When it beats hash join, pre-sorted exploitation
- `join-ordering.md` — Cardinality-based ordering, sampling-based order

### aggregation/
- `hash-aggregation.md` — Open addressing, pre-sizing, cache-friendly
- `sorted-aggregation.md` — Exploiting sorted input, early termination
- `partial-aggregation.md` — Two-phase aggregation, combiner patterns

### parallelism/
- `thread-parallelism.md` — Morsel-driven parallelism, OpenMP
- `simd.md` — AVX2/SSE for filtering, aggregation, comparisons
- `data-partitioning.md` — Hash partitioning, range partitioning

### data-structures/
- `compact-hash-tables.md` — Robin hood, open addressing. **Use hash_utils.h**
- `arena-allocation.md` — Bump allocators, memory pools
- `flat-structures.md` — Flat arrays, SOA patterns

### external-libs/
- `abseil-folly.md` — Google/Facebook hash maps
- `jemalloc-tcmalloc.md` — High-performance memory allocators
- `io-libraries.md` — mmap, io_uring

### patterns/
- `parallel-hash-join.md` — Parallel hash join + open addressing
- `zone-map-pruning.md` — Range predicate skip logic with zone maps
