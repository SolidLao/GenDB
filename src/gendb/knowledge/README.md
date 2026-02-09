# GenDB Knowledge Base

Optimization knowledge for GenDB agents. Each file covers one technique with actionable C++ patterns.

## How Agents Should Use This

1. Read files relevant to your current task (not all files)
2. Use the knowledge to inform technique selection, not as rigid instructions
3. You may propose approaches NOT in this knowledge base — novel ideas are encouraged
4. Each file has: What It Is, When To Use, Key Implementation Ideas, Performance Characteristics, Real-World Examples, Pitfalls

## Index

### storage/
- `columnar-vs-row.md` — When columnar wins, layout strategies, late materialization
- `compression.md` — Dictionary encoding, RLE, bitpacking, delta encoding
- `memory-layout.md` — Cache-line alignment, struct-of-arrays, prefetching
- `string-optimization.md` — String interning, dictionary-coded strings, fixed-width

### indexing/
- `hash-indexes.md` — Robin hood, cuckoo, open addressing, Swiss Tables
- `sorted-indexes.md` — Binary search, B-trees, interpolation search, SIMD search
- `zone-maps.md` — Min/max indexes, skip zones, block-level filtering
- `bloom-filters.md` — Probabilistic filtering, optimal sizing, blocked bloom filters

### query-execution/
- `vectorized-execution.md` — Batch processing, vector-at-a-time, selection vectors
- `operator-fusion.md` — Fusing scan+filter+project, loop fusion
- `compiled-queries.md` — Code generation vs interpretation, template specialization
- `pipeline-breakers.md` — Materialization points, pipeline design

### joins/
- `hash-join-variants.md` — Partitioned, grace, build/probe optimization
- `sort-merge-join.md` — When it beats hash join, pre-sorted exploitation
- `join-ordering.md` — Cardinality-based ordering, bushy vs left-deep

### aggregation/
- `hash-aggregation.md` — Open addressing, pre-sizing, cache-friendly
- `sorted-aggregation.md` — Exploiting sorted input, early termination
- `partial-aggregation.md` — Two-phase aggregation, combiner patterns

### parallelism/
- `simd.md` — AVX2/SSE for filtering, aggregation, comparisons
- `thread-parallelism.md` — Morsel-driven parallelism, partition-based
- `data-partitioning.md` — Hash partitioning, range partitioning

### data-structures/
- `compact-hash-tables.md` — Robin hood, swiss tables, cache-friendly hashing
- `arena-allocation.md` — Bump allocators, memory pools
- `flat-structures.md` — Flat arrays vs pointer-heavy, SOA patterns

### external-libs/
- `jemalloc-tcmalloc.md` — High-performance memory allocators
- `abseil-folly.md` — Google/Facebook hash maps (flat_hash_map, F14)
- `io-libraries.md` — mmap, io_uring, memory-mapped I/O
