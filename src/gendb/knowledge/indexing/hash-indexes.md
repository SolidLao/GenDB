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

Join columns often have **duplicate keys** (e.g., foreign key column in fact table — multiple rows per dimension key). A naive one-slot-per-row design wastes space and scatters related positions. Use a **two-array design**:

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

## Construction Anti-Patterns

**NEVER use `std::unordered_map<K, std::vector<uint32_t>>` for histogram building.** This pattern allocates a dynamic vector per unique key, causing millions of heap allocations and pointer-chasing on large columns. For 60M rows with 15M unique keys, this can take 30+ minutes single-threaded vs <2 minutes with sort-based grouping.

**Preferred construction pattern (sort-based grouping):**
1. Create position array `[0, 1, 2, ..., N-1]`
2. Sort positions by key value (`std::sort` with key comparator)
3. Linear scan sorted positions to find group boundaries → `(key, offset, count)` per unique key
4. Insert into open-addressing hash table with multiply-shift hash

**Benefits**: O(N log N) but cache-friendly, no dynamic allocation, positions array is the final output (no copy needed).

**Index selectivity — skip tiny tables:** Do NOT build hash indexes on tables with fewer than 10,000 rows (nation, region, supplier). These fit in L1 cache; a linear scan is faster than an index lookup. Only build indexes on columns that are actually join keys or heavily-filtered in the workload queries.
