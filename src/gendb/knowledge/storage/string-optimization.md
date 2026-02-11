# String Optimization for Analytical Databases

## What It Is
Techniques to reduce memory overhead and speed up string operations in analytical workloads. Strings are expensive: variable-length, pointer-based, poor cache locality, and slow comparisons.

## When To Use
- **Low Cardinality**: String interning, dictionary encoding (countries, categories, status codes)
- **Fixed-Length**: Inline storage for short strings (UUIDs, codes, abbreviations)
- **High Comparison Volume**: Hash-based comparisons, dictionary-coded comparisons
- **Join Keys**: Dictionary-coded strings avoid expensive string comparisons
- **String Aggregations**: GROUP BY on strings benefits from interning/dictionaries

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

## Performance Characteristics
- **Dictionary encoding**: 10-50x compression, 100x faster comparisons (4-byte int vs full string)
- **Inline storage**: Avoids ~40 cycle heap allocation overhead per string
- **Pointer-free storage**: 2-3x better cache hit rate vs pointer-based string arrays

## Pitfalls
- **High-cardinality dictionaries**: Dictionary grows unbounded and can exceed original data size
- **Hash collisions**: Hash-based equality must always verify with full string comparison
- **Interning memory leaks**: Interned strings never freed; use arena allocators with bounded lifetime
