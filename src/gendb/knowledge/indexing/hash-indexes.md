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
- **Hash Function Selection**: Use multiply-shift (`(uint64_t)key * 0x9E3779B97F4A7C15ULL >> shift`), MurmurHash, XXHash, or hardware CRC32 to minimize clustering. **NEVER use `std::hash<int32_t>` — it is often the identity function on integers, causing severe clustering and long probe chains.**
- **Tombstone Management**: Open addressing requires tombstone markers for deletions; periodic compaction avoids degradation
- **NUMA-Aware Partitioning**: Partition hash table across NUMA nodes for parallel join builds

## Multi-Value Hash Indexes for Join Columns

Join columns often have **duplicate keys** (e.g., `l_orderkey` in lineitem — ~4 rows per order). A naive one-slot-per-row design wastes space and scatters related positions. Use a **two-array design**:

1. **Positions array**: All row positions grouped by key (contiguous per key)
2. **Hash table**: Maps key → `{offset_into_positions_array, count}`
3. **Lookup**: Hash key → find `(offset, count)` → read `count` contiguous positions

**Benefits**: One hash entry per unique key (not per row), cache-friendly multi-value reads, smaller hash table (lower load factor pressure).

**Construction**: Group positions by key (parallel histogram + scatter), then build hash table on unique keys only.

## Construction Efficiency

- **mmap** for reading binary column files (zero-copy, no ifstream overhead)
- **OpenMP** for parallel construction (histogram, scatter, zone map building)
- **Compile with**: `-O3 -march=native -fopenmp` for build_indexes.cpp
- **Load factor 0.5–0.6** for multi-value hash tables (small because one entry per unique key)
