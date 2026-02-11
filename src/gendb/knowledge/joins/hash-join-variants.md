# Hash Join Variants

## What It Is
Hash join builds a hash table on the smaller relation (build side) and probes it with tuples from the larger relation (probe side). Variants optimize for different data sizes, cardinalities, and memory constraints through partitioning, table design, and execution strategies.

## When To Use
- Unordered data with no useful indexes
- High-cardinality joins where nested loops are prohibitive
- Build side fits in L3 cache (simple hash join) or memory (partitioned variants)
- Equi-joins only (hash joins require equality predicates)
- Grace hash join when build side exceeds memory: partition both sides to disk, join partition pairs

## Key Implementation Ideas
- **Build side selection**: Always choose the smaller relation as the build side based on cardinality estimates
- **Linear probing hash table**: Open addressing with linear probing is cache-friendly; store the hash value in each entry to avoid key recomputation
- **Load factor sizing**: Pre-size the hash table to ~75% load factor (size = cardinality / 0.75, rounded to power of two) to avoid expensive rehashing
- **Chained vs open addressing**: Linear probing is 10-20% faster at low collision rates; chaining degrades more gracefully under high load
- **Radix partitioning**: Partition both relations by hash bits so each partition pair fits in L3 cache, improving locality
- **Partition count tuning**: Choose partition count so each partition fits in cache (typically 64-512 partitions); over-partitioning increases coordination overhead
- **Multi-pass partitioning**: For very large data, apply radix partitioning in multiple passes with different bit ranges
- **Pre-filtering probe side**: Apply local filters on probe tuples before hashing to reduce unnecessary probe work
- **Bloom filter on build keys**: Build a bloom filter during the build phase to skip 80-90% of non-matching probes cheaply
- **Null key handling**: Hash nulls to a separate bucket; nulls never match in standard SQL join semantics
- **SIMD-optimized probing**: Vectorize hash computation and key comparison for throughput on modern CPUs
- **Adaptive hash join**: Monitor build-side size during execution and switch from simple to partitioned strategy if memory is exceeded
- **Perfect hashing for low-cardinality keys**: When key domain is small and known, use direct-mapped arrays instead of hash tables

## Performance Characteristics
- Simple hash join: 2-3x faster than sort-merge when build side fits in L3 cache
- Partitioned hash join: linear scaling to 100M+ rows, ~1.5x overhead from partitioning
- Memory overhead: 1.3-2x build relation size (hash table + overflow chains)

## Pitfalls
- **Wrong build side**: Building on the large side wastes memory and causes cache thrashing
- **Poor hash function**: Collisions degrade lookup to O(n); use MurmurHash3 or XXHash
- **Under-sizing table**: Triggers expensive rehashing mid-build; always pre-size with cardinality estimates
