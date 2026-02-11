# Abseil and Folly Hash Tables

## What It Is
High-performance hash table implementations from Google (Abseil flat_hash_map/flat_hash_set) and Facebook (Folly F14FastMap/F14NodeMap). Both use Swiss Tables design with SIMD-accelerated lookups and open addressing.

## When To Use
- Hash joins and GROUP BY aggregations in query execution
- Symbol tables, metadata caches, deduplication
- Replacing std::unordered_map for 2-5x speedup
- When profiling shows hash table operations as bottleneck
- Large working sets where memory overhead of std::unordered_map is unacceptable

## Key Implementation Ideas
- **Drop-in replacement**: absl::flat_hash_map and folly::F14FastMap are API-compatible with std::unordered_map
- **Swiss Tables internals**: Uses 16-byte control byte groups; each byte stores empty/deleted/hash-prefix metadata
- **SIMD parallel probe**: Compares 16 control bytes simultaneously via SSE2/NEON, finding candidate slots in one instruction
- **Open addressing layout**: Keys and values stored in contiguous arrays (not chained buckets), maximizing cache locality
- **Folly F14 variants**: F14FastMap (small keys), F14ValueMap (inline storage), F14NodeMap (pointer-stable like std::unordered_map)
- **Heterogeneous lookup**: Define is_transparent hash/eq functors to look up by string_view without allocating a std::string
- **Pre-reserve capacity**: Call reserve() before bulk inserts to avoid expensive rehashing pauses
- **Parallel aggregation pattern**: Use thread-local hash maps for per-thread aggregation, then merge into a global map
- **Fibonacci hashing**: Multiply keys by golden ratio constant for better hash distribution with integer keys
- **Load factor tuning**: Swiss Tables operate optimally at 87.5% load factor (7/8 full) before automatic resize
- **Custom hash functions**: Use absl::Hash or XXH3 instead of identity/poor hashes to maintain performance

## Performance Characteristics
- **Lookup speed**: 2-5x faster than std::unordered_map; SIMD adds ~15-25% on top
- **Memory overhead**: ~12-15% vs ~100-200% for std::unordered_map (chaining + per-node allocation)
- **Load factor**: Optimal at 87.5%; automatic resizing doubles capacity

## Pitfalls
- **Iterator invalidation**: Insertions invalidate all iterators (unlike std::unordered_map)
- **Pointer stability**: Use F14NodeMap or absl::node_hash_map if stable pointers/references are required
- **Hash function quality**: Poor hash functions (e.g., identity) kill performance; always use a good hash
