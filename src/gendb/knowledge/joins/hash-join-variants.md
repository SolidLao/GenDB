# Hash Join Variants

**When to use**: Default join method for equi-joins. Build hash table on smaller side, probe with larger side.
**Impact**: Correct build-side selection alone can yield 10-100x improvement over building on the wrong side.

## Principle
- Always choose the smaller (filtered) relation as the build side
- Open-addressing hash tables are 2-5x faster than `std::unordered_map` (no pointer chasing)
- Pre-size hash table to ~75% load factor (power-of-2 sizing)
- For 1:N joins, use multi-value design: positions array + hash table mapping key → (offset, count)
- Apply Bloom filter on build keys to skip 80-90% of non-matching probes cheaply
- Parallelize the probe phase with OpenMP; build phase is typically sequential

## Pattern
```cpp
// Build phase: hash table on smaller (filtered) side
struct BuildEntry { int32_t key; int32_t payload; };
CompactHashTable<int32_t, BuildEntry> ht(filtered_build_size);
for (int64_t i = 0; i < build_rows; i++) {
    if (build_predicate(i)) {
        ht.insert(build_key[i], {build_key[i], build_payload[i]});
    }
}

// Probe phase: scan larger side, look up in hash table
#pragma omp parallel for
for (int64_t i = 0; i < probe_rows; i++) {
    if (probe_predicate(i)) {
        auto* match = ht.find(probe_key[i]);
        if (match) {
            // Process join result
            emit(match->payload, probe_payload[i]);
        }
    }
}

// 1:N join with multi-value lookup
struct MultiValueHT {
    std::vector<uint32_t> positions;  // all positions, grouped by key
    CompactHashTable<int32_t, std::pair<uint32_t, uint32_t>> index; // key → (offset, count)
};
// Lookup: auto [offset, count] = index.find(key); iterate positions[offset..offset+count]
```

## Pitfalls
- Building hash table on the larger side is the #1 cause of slow joins
- `std::unordered_map` for joins with >10K entries wastes 2-5x performance
- Forgetting to apply single-table predicates before join → building on unfiltered data
- Pre-built hash indexes (from Query Guide) can eliminate build time entirely — check before building from scratch
- See `patterns/parallel-hash-join.md` for a complete open-addressing implementation template
