# Vectorized Execution

## What It Is
Vectorized execution processes data in batches (vectors) of 1024-2048 tuples at a time, rather than tuple-at-a-time, enabling better CPU cache utilization, SIMD vectorization, and reduced function call overhead.

## When To Use
- Analytical workloads with large scans and aggregations
- When CPU efficiency matters more than minimal latency
- Columnar storage layouts (aligned with columnar access patterns)
- Tight loops that benefit from compiler auto-vectorization
- When avoiding virtual function calls per tuple is critical

## Key Implementation Ideas
- **DataChunk abstraction:** Fixed-size batch of column vectors (typically 2048 tuples), each vector is a contiguous array of primitives with a null bitmask
- **Selection vectors:** Filter operations produce index arrays of qualifying tuples instead of copying data, avoiding materialization
- **Pull-based operator interface:** Each operator implements GetChunk() to produce the next batch on demand
- **Templated type-specialized loops:** Use C++ templates to generate type-specific tight loops that the compiler can auto-vectorize (SSE, AVX2, AVX-512)
- **Batch-level function dispatch:** Virtual function calls happen once per vector (not per tuple), amortizing dispatch overhead
- **Null handling via bitmasks:** Separate boolean arrays track NULLs, keeping the hot data path branch-free
- **Columnar in-memory layout:** Each column stored as a contiguous array for sequential access and prefetcher-friendly patterns
- **Constant vectors:** Special representation for constant/literal values to avoid redundant storage across the batch
- **String handling via indirection:** Variable-length data uses offset/pointer arrays with a separate heap, preserving vectorization on fixed-width columns

## Performance Characteristics
- **Speedup:** 3-10x vs tuple-at-a-time for scan-heavy workloads
- **Memory overhead:** Fixed-size buffers (8-32 KB per vector), predictable allocation
- **SIMD utilization:** Compiler can auto-vectorize tight loops (SSE, AVX2, AVX-512)

## Pitfalls
- Small result sets (<100 tuples): vectorization overhead not worthwhile
- Variable-length data (strings/BLOBs) requires indirection, reducing vectorization benefits
- Over-large vectors (>4KB) thrash L1 cache; sweet spot is 2048 tuples for 64-bit types
