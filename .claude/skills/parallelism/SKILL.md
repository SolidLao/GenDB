---
name: parallelism
description: Parallelism patterns for OLAP query execution. Load when implementing or optimizing multi-threaded query code. Covers morsel-driven parallelism, OpenMP, thread-local aggregation, SIMD, parallel hash joins, contention analysis.
user-invocable: false
---

# Skill: Parallelism

## When to Load
Almost always — multi-core execution is critical for OLAP performance.

## Key Principles
- Morsel-driven parallelism: partition work into cache-friendly chunks (10K-100K rows)
- Thread count: std::thread::hardware_concurrency() (typically 8-16 cores)
- Near-linear speedup for scans, joins, aggregations when table size significantly exceeds per-core cache
- Single-threaded execution wastes the majority of available cores

## Contention Analysis
If shared data structure shows significant CAS retry rates (visible in profiling as wasted cycles), switch to thread-local + merge pattern. For aggregation, thread-local is almost always faster (see experience P20).

**Detection:** High CAS retry rates in hot path, or throughput that doesn't scale with thread count despite available parallelism.

**Fix:** Switch to thread-local copies + merge. This is the preferred approach for aggregation. A single shared aggregation map may be acceptable only for very low group counts where atomic updates are trivially cheap.

## Morsel Size Calculation
Target morsel ≈ per-core cache / (num_active_columns × sizeof(T)).
- The goal is that each morsel's working set fits in the per-core cache (L2 or L3 share depending on architecture).
- Wider rows (more columns, larger types) → smaller morsels. Narrow scans → larger morsels.
- The optimal morsel size also varies by operation: pure scans can use larger morsels; hash-probe-heavy work benefits from smaller morsels that keep the hash table partition cache-resident.
- Use `schedule(dynamic, morsel_size)` so fast-finishing threads steal work from slow ones.

## Parallelism Patterns
- **Parallel scan + filter**: `#pragma omp parallel for` over row ranges. Thread-local selection vectors.
- **Parallel hash join probe**: partition probe side, each thread probes shared hash table (read-only after build).
- **Parallel hash table build**: either (a) partitioned build (each thread owns partition) or (b) concurrent build with CAS.
- **Thread-local aggregation**: each thread maintains local hash table → merge at end.
- **Parallel madvise**: prefetch multiple columns concurrently for cold-start I/O.

## GenDB-Specific
- Use OpenMP for parallelism: `#pragma omp parallel for schedule(dynamic, morsel_size)`
- Global thread pool only — no nested parallelism
- Thread-local hash tables: size for FULL cardinality per C9 (not cardinality/nthreads)
  BUT: compute total_memory = nthreads × cap × slot_size FIRST and compare against hardware LLC.
  If total_memory exceeds LLC: evaluate aggregation-optimization skill: Memory Budget Gate for strategy selection.
- Merge: thread-local aggregation is the default (P20). For merge strategy selection (tree-merge vs partitioned merge), see aggregation-optimization skill.

## SIMD (SSE/AVX2)
- Available on most x86-64: check `lscpu | grep Flags` for avx2, sse4_2
- Compiler auto-vectorization with -O3 -march=native handles most cases
- Use explicit intrinsics ONLY when: (a) compiler report shows loop not vectorized, (b) profiling identifies the loop as a bottleneck, (c) operation is simple integer comparison or arithmetic
- SIMD-friendly layout: SOA (Struct-of-Arrays), contiguous typed arrays

## Technique Keywords
morsel-driven, thread-local, work-stealing, OpenMP, SIMD, AVX2, SSE4.2,
partitioned hash join, parallel aggregation, atomic CAS

## Reference Papers
- Leis et al. 2014 — "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age"
- Balkesen et al. 2013 — "Main-Memory Hash Joins on Multi-Core CPUs: Tuning to the Underlying Hardware"
- Polychroniou et al. 2015 — "Rethinking SIMD Vectorization for In-Memory Databases"

## Common Pitfalls
→ See experience skill: C9 (thread-local hash table sizing), P15 (sequential merge cost), P22 (large allocation page faults)
