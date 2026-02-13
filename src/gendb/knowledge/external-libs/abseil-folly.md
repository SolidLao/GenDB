# Abseil and Folly Hash Tables

## What It Is
High-performance hash table implementations from Google (Abseil flat_hash_map/flat_hash_set) and Facebook (Folly F14FastMap/F14NodeMap). Both use Swiss Tables design with SIMD-accelerated lookups and open addressing.

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
