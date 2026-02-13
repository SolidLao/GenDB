# Hash Aggregation

## What It Is
Hash aggregation builds a hash table mapping group keys to running aggregate states, updating in-place as tuples stream through. Combines grouping and aggregation in a single pass, avoiding materialization and sort overhead.

## Key Implementation Ideas
- **Pre-sized open addressing hash table**: Pre-allocate based on cardinality estimate at 75% load factor, power-of-2 sizing for fast modulo
- **Linear probing with cached hashes**: Store hash values alongside entries to speed up comparison during probe sequences
- **Cache-friendly layout**: Pack aggregate state compactly; separate keys from values for better locality during lookup
- **Combined hash and aggregate**: Single-pass design that applies filters, hashes, and updates aggregates inline without intermediate materialization
- **Perfect hashing for low cardinality**: For small domains (dates, enums), use direct array indexing instead of hashing
- **Adaptive aggregation**: Monitor observed cardinality at runtime and switch strategy (perfect hash → hash table → sort-based) accordingly
- **SIMD-vectorized aggregate updates**: Process 4-8 aggregates per loop iteration for 2-3x throughput improvement
- **Disk spill strategy**: When hash table exceeds memory, partition and spill to disk rather than OOM crash
- **High-quality hash functions**: Use XXHash or MurmurHash to minimize collisions and avoid O(n^2) degradation
- **Key encoding for compound keys**: Normalize multi-column or string keys into fixed-width representations to reduce memory and comparison cost
