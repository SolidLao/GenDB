# String Optimization for Analytical Databases

## What It Is
Techniques to reduce memory overhead and speed up string operations in analytical workloads. Strings are expensive: variable-length, pointer-based, poor cache locality, and slow comparisons.

## Key Implementation Ideas
- **String interning (deduplication)**: Store each unique string once in a pool; use pointer equality for O(1) comparisons
- **Dictionary-coded strings**: Map strings to integer codes; filters, joins, and GROUP BY operate on 4-byte ints instead of variable-length strings
- **Inline / short string optimization (SSO)**: Store strings up to ~15 bytes directly in the struct, avoiding heap allocation (~40 cycle overhead)
- **Fixed-width string types**: Use fixed-size char arrays (e.g., 16 bytes) for known-length data like country codes or UUIDs
- **Pointer-free offset-based storage**: Concatenate all strings in a contiguous buffer and use an offset array; eliminates pointer chasing and improves cache locality
- **Hash-based string comparison**: Precompute 8-byte hashes for fast equality checks; only do full memcmp on hash collisions
- **SIMD-accelerated string matching**: Use SIMD instructions for prefix matching and bulk character comparison
- **Arena allocation for string pools**: Allocate strings from large contiguous arenas to reduce per-string allocation overhead and improve locality
- **Length-prefixed (Pascal) strings**: Store length before data instead of null-terminating; faster length lookups and supports embedded nulls
- **String view (zero-copy references)**: Use non-owning views into existing buffers to avoid copies during query processing
