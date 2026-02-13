# Hash Indexes

## What It Is
Hash indexes provide O(1) average-case lookups by mapping keys to buckets using a hash function. Modern implementations use open addressing with optimized probe sequences and power-of-2 sizing for fast modulo operations.

## Key Implementation Ideas
- **Robin Hood Hashing**: Open addressing where entries "steal" from neighbors with shorter probe distances, minimizing variance in probe lengths
- **Swiss Table (Google Abseil)**: Uses SIMD to check 16 slots in parallel via control bytes storing top 7 bits of hash
- **Cuckoo Hashing**: Two hash functions with two tables; displaces existing entries on collision for guaranteed O(1) worst-case lookup
- **Power-of-2 Sizing**: Replace expensive modulo with bitwise AND (`hash & (capacity - 1)`)
- **Linear Probing with Prefetch**: Prefetch next probe position to hide memory latency in open addressing
- **Vectorized Probing**: Process multiple probe sequences in parallel using SIMD for batch lookups
- **Hash Function Selection**: Use MurmurHash, XXHash, or hardware CRC32 to minimize clustering
- **Tombstone Management**: Open addressing requires tombstone markers for deletions; periodic compaction avoids degradation
- **NUMA-Aware Partitioning**: Partition hash table across NUMA nodes for parallel join builds
