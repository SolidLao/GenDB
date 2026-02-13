# Flat Structures

## What It Is
Data structures using contiguous arrays instead of pointer-based layouts (linked lists, trees). Includes Struct-of-Arrays (SOA) patterns that separate fields into parallel arrays for cache efficiency and SIMD vectorization.

## Key Implementation Ideas
- **Struct-of-Arrays (SOA) over Array-of-Structs (AOS)**: Store each column in its own contiguous array so scans touch only the needed fields
- **Flat vectors with validity bitmaps**: Store column data in a dense array with a separate 1-bit-per-value NULL bitmap
- **SIMD-friendly columnar scans**: Contiguous same-type arrays enable vectorized filter/aggregate operations (e.g., AVX2 compare 8 int32s at once)
- **Selection vectors (avoid data copying)**: Store indices of qualifying rows instead of materializing filtered copies; apply to other columns via gather
- **Flat hash tables (open addressing)**: Replace pointer-heavy std::unordered_map with parallel key/value/occupied arrays for cache-friendly hashing
- **Flat sorted arrays with binary search**: Replace std::map (red-black tree) with sorted parallel arrays and binary search for read-heavy workloads
- **Contiguous string pools**: Concatenate all strings into one buffer with offset/length arrays, eliminating per-string heap allocations
- **Prefetching for scattered access**: When gathering via selection vectors, prefetch target indices ahead of time to hide memory latency
