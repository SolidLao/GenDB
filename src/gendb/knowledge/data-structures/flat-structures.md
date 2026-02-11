# Flat Structures

## What It Is
Data structures using contiguous arrays instead of pointer-based layouts (linked lists, trees). Includes Struct-of-Arrays (SOA) patterns that separate fields into parallel arrays for cache efficiency and SIMD vectorization.

## When To Use
- Columnar storage for analytical queries (scan entire columns)
- Hot loops that access a subset of fields (avoid loading unused data)
- SIMD operations on homogeneous data
- Reducing cache misses and pointer-chasing overhead

## Key Implementation Ideas
- **Struct-of-Arrays (SOA) over Array-of-Structs (AOS)**: Store each column in its own contiguous array so scans touch only the needed fields
- **Flat vectors with validity bitmaps**: Store column data in a dense array with a separate 1-bit-per-value NULL bitmap
- **SIMD-friendly columnar scans**: Contiguous same-type arrays enable vectorized filter/aggregate operations (e.g., AVX2 compare 8 int32s at once)
- **Selection vectors (avoid data copying)**: Store indices of qualifying rows instead of materializing filtered copies; apply to other columns via gather
- **Flat hash tables (open addressing)**: Replace pointer-heavy std::unordered_map with parallel key/value/occupied arrays for cache-friendly hashing
- **Flat sorted arrays with binary search**: Replace std::map (red-black tree) with sorted parallel arrays and binary search for read-heavy workloads
- **Contiguous string pools**: Concatenate all strings into one buffer with offset/length arrays, eliminating per-string heap allocations
- **Prefetching for scattered access**: When gathering via selection vectors, prefetch target indices ahead of time to hide memory latency

## Performance Characteristics
- **Cache efficiency**: 3-10x speedup from reduced cache misses (columnar vs row-wise)
- **Memory bandwidth**: Scan only needed columns, saving 5-10x bandwidth
- **SIMD enablement**: SOA layout enables auto-vectorization; AOS layout does not

## Pitfalls
- **Random access via gather** is slow for scattered indices; use prefetching to mitigate
- **Insertions/deletions in flat sorted arrays** are O(n); use hash tables for mutable data
- **Small data (<1000 rows)** does not benefit enough to justify the overhead of separate vectors
