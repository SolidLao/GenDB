# Q4 Iter 1 → Iter 2: Detailed Optimization Paths

## Current State Summary
- **Execution Time**: 645.26 ms
- **Bottleneck**: semi_join_build at 632.68 ms (98.05% of total)
- **Root Cause**: Processing all 59.986M lineitem rows + 13.753M insertions
- **Data Path**: Scan → Filter → Insert → Probe

---

## Path A: Pre-built Index Strategy (Highest Expected ROI)

### The Opportunity
A pre-built hash index exists at:
```
/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/indexes/idx_lineitem_orderkey_hash.bin
```

**Size**: 613 MB (vs ~720 MB of raw lineitem columns)

**Layout** (from Q4_storage_guide.md):
```
Offset 0: [uint32_t num_unique]        - Number of distinct l_orderkey values
Offset 4: [uint32_t table_size]        - Hash table slot count
Offset 8: Hash table slots             - Each: [int32_t key, uint32_t offset, uint32_t count]
...      : Position lists              - For each slot: [uint32_t pos_count][uint32_t positions...]
```

### Current Implementation Gap
Iter 1 code has a comment in q4.cpp:
```cpp
/* Pre-built hash index: idx_lineitem_orderkey_hash.bin provides O(1) lookup
 *   Layout: [uint32_t num_unique][uint32_t table_size] then hash table entries
 *   [int32_t key, uint32_t offset, uint32_t count],
 *   then position data
 *   Eliminates 1.5s hash table build time
 */
```

But never actually uses it. Current code does:
```cpp
FastHashSet lineitem_keys(num_lineitem / 4);
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        lineitem_keys.insert(l_orderkey[i]);  // ← Rebuilds index from scratch
    }
}
```

### Proposed Implementation for Iter 2

#### Option A1: Direct Index Mapping (Simplest)
```cpp
// Step 0: Load pre-built index
size_t index_size;
const uint8_t* index_data = (const uint8_t*)mmap_file(
    (gendb_dir + "/indexes/idx_lineitem_orderkey_hash.bin").c_str(),
    index_size);

// Parse header
const uint32_t* header = (const uint32_t*)index_data;
uint32_t num_unique = header[0];        // ~10.7M unique keys
uint32_t table_size = header[1];        // Hash table size

// Hash table starts at offset 8
struct IndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};
const IndexEntry* hash_table = (const IndexEntry*)(index_data + 8);

// Position data follows immediately after hash table
const uint32_t* position_data = (const uint32_t*)(index_data + 8 + table_size * 12);

// Step 1: Build filtered set from pre-built index
FastHashSet lineitem_keys(num_unique);

for (uint32_t slot = 0; slot < table_size; slot++) {
    const IndexEntry& entry = hash_table[slot];
    if (entry.key == 0) continue;  // Empty slot

    // Read position list for this key
    uint32_t pos_count = position_data[entry.offset];
    const uint32_t* positions = &position_data[entry.offset + 1];

    // Check if ANY position satisfies l_commitdate < l_receiptdate
    bool has_valid_position = false;
    for (uint32_t i = 0; i < pos_count; i++) {
        uint32_t row = positions[i];
        if (l_commitdate[row] < l_receiptdate[row]) {
            has_valid_position = true;
            break;  // Found one match, no need to check others
        }
    }

    // Insert key only if at least one position is valid
    if (has_valid_position) {
        lineitem_keys.insert(entry.key);
    }
}
```

**Estimated Timing**:
- mmap index: ~5 ms (OS paging)
- Iterate slots: ~100 ms (60M positions to check)
- Insert valid keys: ~50 ms (only 13.7M insertions, not 59.9M)
- **Total**: ~155 ms (vs current 632.68 ms)
- **Gain**: ~477 ms (75% reduction)

**Risks**:
- Must correctly parse binary layout (offset calculations crucial)
- Position list layout assumption might be wrong
- Scattered reads of commitdate/receiptdate (cache unfriendly)

#### Option A2: Build Filtered Index from Scratch (Safer)
If parsing the pre-built index is too risky, but we want to avoid scanning 59.9M rows:

```cpp
// Still scan all lineitem, but use simpler counter structure
// Count valid (orderkey, row) pairs
std::unordered_map<int32_t, std::vector<uint32_t>> orderkey_to_rows;

// First pass: collect valid rows
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        orderkey_to_rows[l_orderkey[i]].push_back(i);
    }
}

// Second pass: extract unique keys (already filtered)
FastHashSet lineitem_keys(orderkey_to_rows.size());
for (const auto& [key, rows] : orderkey_to_rows) {
    lineitem_keys.insert(key);
}
```

**Estimated Timing**:
- Same as current ~632 ms (still scans all 59.9M rows)
- No gain, but potentially useful for other optimizations

---

## Path B: SIMD-Vectorized Filter (Medium ROI)

### The Opportunity
Current filter loop is hard to vectorize due to conditional insertion:
```cpp
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        lineitem_keys.insert(l_orderkey[i]);  // ← Can't batch easily
    }
}
```

### Vectorization Strategy

#### Option B1: AVX-512 SIMD Filter (if available)
```cpp
#include <immintrin.h>

// Process 8 elements at a time with AVX-512
for (uint32_t i = 0; i < (num_lineitem & ~7); i += 8) {
    // Load 8 pairs of dates
    __m256i commitdate_vec = _mm256_loadu_si256((__m256i*)&l_commitdate[i]);
    __m256i receiptdate_vec = _mm256_loadu_si256((__m256i*)&l_receiptdate[i]);

    // Vectorized comparison: commitdate < receiptdate
    __m256i mask = _mm256_cmpgt_epi32(receiptdate_vec, commitdate_vec);

    // Extract mask and process matches
    int match_mask = _mm256_movemask_epi8(mask);
    for (int j = 0; j < 8; j++) {
        if (match_mask & (0xF << (j * 4))) {
            lineitem_keys.insert(l_orderkey[i + j]);
        }
    }
}

// Handle remainder
for (uint32_t i = (num_lineitem & ~7); i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        lineitem_keys.insert(l_orderkey[i]);
    }
}
```

**Estimated Timing**:
- SIMD comparison: 4x-8x speedup on filter logic
- Still limited by insertion: maybe 20% overall reduction
- **Total**: ~500 ms (vs 632.68 ms)
- **Gain**: ~132 ms (20% reduction)

**Compiler Help**:
With `-O3 -march=native -ffast-math`, GCC/Clang might auto-vectorize.
Try first without manual intrinsics.

#### Option B2: Scalar Filter Optimization
Even simpler: optimize for CPU cache and branch prediction
```cpp
// Prefetch upcoming data
for (uint32_t i = 0; i < num_lineitem; i += 16) {
    __builtin_prefetch(&l_commitdate[i + 32], 0, 3);
    __builtin_prefetch(&l_receiptdate[i + 32], 0, 3);
    __builtin_prefetch(&l_orderkey[i + 32], 0, 3);

    // Process current batch
    for (int j = 0; j < 16; j++) {
        if (l_commitdate[i + j] < l_receiptdate[i + j]) {
            lineitem_keys.insert(l_orderkey[i + j]);
        }
    }
}
```

**Estimated Timing**:
- Better prefetching: 5-10% improvement
- **Total**: ~580 ms
- **Gain**: ~52 ms (8% reduction)

---

## Path C: Dual-Buffer Insertion Strategy (Low-Medium ROI)

### The Opportunity
Currently, for each filtered row:
1. Compute hash
2. Probe table
3. Write memory

This creates cache conflicts. Instead, buffer matches and insert in batches:

### Implementation

```cpp
// Thread-local buffers for match collection
const int BUFFER_SIZE = 100000;
int num_threads = omp_get_max_threads();
std::vector<std::vector<int32_t>> thread_buffers(num_threads);

// Phase 1: Filter and buffer matches (separate from insertion)
#pragma omp parallel for schedule(dynamic, 1000000)
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        int tid = omp_get_thread_num();
        thread_buffers[tid].push_back(l_orderkey[i]);
    }
}

// Merge and deduplicate buffers
std::vector<int32_t> all_matches;
for (int tid = 0; tid < num_threads; tid++) {
    all_matches.insert(all_matches.end(),
                       thread_buffers[tid].begin(),
                       thread_buffers[tid].end());
}

// Phase 2: Batch insert into hash set
FastHashSet lineitem_keys(all_matches.size());
for (int32_t key : all_matches) {
    lineitem_keys.insert(key);
}
```

**Estimated Timing**:
- Better cache behavior (L1 stays hot during filter phase)
- Batch insertion is more prefetch-friendly
- Trade-off: Intermediate buffer memory
- **Total**: ~550 ms (vs 632.68 ms)
- **Gain**: ~82 ms (13% reduction)

**Risk**: Duplicate keys in buffers need deduplication, adding overhead

---

## Path D: Predicate Push-Down with Zone Maps (Low ROI)

### The Opportunity
If zone maps existed on l_commitdate and l_receiptdate:
```
idx_lineitem_commitdate_zmap.bin
idx_lineitem_receiptdate_zmap.bin
```

Could skip entire blocks where min(commitdate) >= max(receiptdate).

### Implementation
```cpp
// Load zone maps (if available)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Parse zone maps...
std::vector<ZoneMapEntry> commitdate_zones = load_zone_map(...);
std::vector<ZoneMapEntry> receiptdate_zones = load_zone_map(...);

// Skip blocks where predicate can't possibly be true
uint32_t block_size = 100000;  // from storage_design.json
uint32_t num_blocks = (num_lineitem + block_size - 1) / block_size;

std::vector<bool> skip_block(num_blocks, false);
for (uint32_t b = 0; b < num_blocks; b++) {
    if (commitdate_zones[b].min_val >= receiptdate_zones[b].max_val) {
        skip_block[b] = true;
    }
}

// Scan only non-skipped blocks
FastHashSet lineitem_keys(...);
for (uint32_t b = 0; b < num_blocks; b++) {
    if (skip_block[b]) continue;

    uint32_t start = b * block_size;
    uint32_t end = std::min(start + block_size, num_lineitem);

    for (uint32_t i = start; i < end; i++) {
        if (l_commitdate[i] < l_receiptdate[i]) {
            lineitem_keys.insert(l_orderkey[i]);
        }
    }
}
```

**Estimated Timing**:
- Zone map load: ~20 ms
- Block skipping: ~10 ms
- Scan non-skipped blocks: ~500 ms (estimate 20% of blocks can be skipped)
- **Total**: ~530 ms
- **Gain**: ~102 ms (16% reduction)

**Problem**: Zone maps don't currently exist in storage design
Would need to be pre-built during schema creation

---

## Path E: Hybrid: Pre-built Index + SIMD (Best Case)

### The Combo
1. Load pre-built index (5 ms)
2. For each key in index, check filtered positions with SIMD
3. Insert valid keys

```cpp
// (Code combines A1 + B1)
// ...

for (uint32_t slot = 0; slot < table_size; slot += 8) {
    // SIMD-vectorized comparison of position dates
    // ... complex, but potential for 3-4x on this phase
}

// ...
```

**Estimated Timing**:
- If Index Loading Works: ~100 ms (from A1)
- SIMD Filter on positions: ~50 ms
- **Total**: ~150 ms
- **Gain**: ~482 ms (76% reduction)

**This would bring us down to 165 ms total, competitive with DuckDB!**

---

## Comparative Analysis of Paths

| Path | Approach | Est. Gain | Total Time | Complexity | Risk | Implementation Effort |
|------|----------|-----------|-----------|-----------|------|----------------------|
| **A1** | Pre-built index (direct) | 477 ms | 168 ms | Medium | Medium | 100 lines |
| **A2** | Filtered scan (safe) | 0 ms | 632 ms | Low | Very Low | 50 lines |
| **B1** | SIMD vectorization | 132 ms | 501 ms | High | Low | 50 lines |
| **B2** | Scalar prefetch | 52 ms | 581 ms | Low | Very Low | 20 lines |
| **C** | Dual-buffer insertion | 82 ms | 551 ms | Medium | Medium | 80 lines |
| **D** | Zone map skip | 102 ms | 544 ms | Medium | High | 60 lines (pre-build) |
| **A1 + B1** | Hybrid (Index + SIMD) | 482 ms | 163 ms | High | High | 150 lines |

---

## Recommended Sequence for Iter 2

### Conservative (Low Risk)
1. **B2**: Add scalar prefetching (5 min, 52 ms gain)
2. **C**: Try dual-buffer insertion (30 min, 82 ms gain)
3. If successful, **A1**: Attempt pre-built index (60 min, potential 477 ms)

**Conservative target**: 500-550 ms (down from 645 ms)

### Aggressive (Higher Risk, Higher Reward)
1. **A1**: Implement pre-built index usage (90 min, 477 ms gain)
2. **B1**: Add SIMD filter if index approach fails (60 min, 132 ms gain)

**Aggressive target**: 150-200 ms (down from 645 ms) - could be Top 5 competitive

### Pragmatic (Recommended)
1. **A1**: Attempt pre-built index with thorough testing
   - Gain: 477 ms
   - If succeeds: Done, move to other queries
   - If fails: Fall back to B2+C

2. If A1 doesn't work, use B2+C for incremental 130 ms gain

**Pragmatic target**: 150 ms if A1 works, 500 ms if not

---

## Key Implementation Notes

### For Path A1 (Pre-built Index)
- Must verify binary layout is correct before parsing
- Add sanity checks: entry.key != 0, entry.offset < data_size
- Consider endianness (likely little-endian on x86)
- Test with small dataset first (print first few entries)

### For Path B1 (SIMD)
- Requires `-mavx512f` compiler flag or fallback to AVX2
- Use `#ifdef` guards for portability
- Benchmark to ensure SIMD isn't slower due to overhead

### For Path C (Dual-Buffer)
- Watch out for memory allocation in parallel region
- Pre-allocate buffers with reserve() to avoid reallocations
- Deduplication could become bottleneck (consider std::sort + unique)

---

## Success Metrics for Iter 2

| Metric | Target | Rationale |
|--------|--------|-----------|
| Total Query Time | < 450 ms | 30% improvement over Iter 1 |
| semi_join_build | < 400 ms | Remains dominant but significantly reduced |
| Correctness | 5 rows output | Must match expected results |
| Validation | Pass | No data corruption |

---

## Fallback Plan
If Iter 2 optimization doesn't yield expected gains:
1. Profile with `perf` to understand CPU cycles vs memory stalls
2. Consider completely different approach (e.g., sorting-based semi-join)
3. Evaluate if query should use different execution model (e.g., external sorting)
4. Look at system-level issues (NUMA effects, TLB misses)
