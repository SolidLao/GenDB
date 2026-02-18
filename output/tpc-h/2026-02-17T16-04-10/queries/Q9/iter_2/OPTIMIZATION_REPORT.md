# Q9 Optimization Report - Iteration 2

## Execution Profile Analysis

### Previous Iteration (Iter 1)
- **Total Time**: 10,500.85 ms
- **Dominant Bottleneck**: scan_filter_aggregate (5,782 ms / 55%)
- **Secondary Bottleneck**: build_joins (4,406 ms / 42%)
- **Filtered Rows**: 3,261,613
- **Gap to Best-in-Class**: 52.4x slower than Umbra (200ms)

### Root Causes Identified

1. **Single-threaded execution**: 64 CPU cores, 0% utilized (1 thread)
2. **Inefficient hash table implementation**: `std::unordered_map` is 2-5x slower than open-addressing hash tables due to:
   - Pointer chasing (cache misses)
   - Bucket overhead per entry
   - Poor memory locality
3. **Redundant year computation**: O(n) loop-based epoch_days → year conversion, executed 3.2M times
4. **Join build time dominance**: Building 5 large hash tables (8M + 15M entries) before any filtering
5. **No pre-filtering**: 60M lineitem rows scanned with per-row 4-5 hash table probes

## Optimization Strategy

### Optimization 1: Open-Addressing Hash Tables (2-3x speedup)

**Technique**: Replaced `std::unordered_map` with Robin Hood open-addressing hash table

**Implementation Details**:
- Single contiguous array (better cache locality)
- Power-of-2 sizing: `capacity = next_pow2(expected * 4/3)` → 75% load factor
- Displaced distance tracking (Robin Hood property)
- Fast index computation: `hash & (capacity - 1)` vs modulo
- Better hash function: MurmurHash-style mixing for int32_t keys

**Applied to**:
- `supplier_nation_map` (100K entries)
- `orders_map` (15M entries)
- Kept `unordered_map` for complex keys (partsupp_key) due to struct complexity

**Expected Impact**:
- supplier_nation_map probe: ~200ns → ~80ns (2.5x faster)
- orders_map probe: ~250ns → ~100ns (2.5x faster)
- Aggregate reduction: 3.2M probes × 150ns = 480ms saved

---

### Optimization 2: Parallel Lineitem Scan (8-16x speedup on 64 cores)

**Technique**: OpenMP parallel for with thread-local aggregation

**Implementation Pattern**:
```cpp
// Thread-local phase: each thread maintains own aggregation map
#pragma omp parallel for schedule(dynamic, 10000)
for (size_t i = 0; i < lineitem_count; i++) {
    int thread_id = omp_get_thread_num();
    // ... process row i, aggregate into thread_agg[thread_id]
}

// Merge phase: combine thread-local results
for (int t = 0; t < num_threads; t++) {
    for (const auto& kv : thread_agg[t]) {
        aggregation[kv.first].add(kv.second.value());
    }
}
```

**Benefits**:
- No lock contention during parallel phase (each thread owns its aggregation map)
- Dynamic scheduling with 10K row morsels (load balancing + cache locality)
- 64 cores → ~50-55x raw parallelism potential on 60M rows
- Practical speedup: 8-16x after accounting for merge overhead

**Expected Impact**: 5,782ms / 12 ≈ 480ms (conservative estimate for 12 cores, 8x after merge costs)

---

### Optimization 3: O(1) Year Extraction (100ms saved)

**Technique**: Pre-computed lookup table instead of loop-based epoch_days → year

**Implementation**:
- Build `year_table[epoch_days]` at startup (O(n) once)
- Lookup is O(1): `int32_t year = year_table[epoch_days]`
- Covers days 0-11000 (1970-2000+)

**Previous Approach**:
```cpp
int32_t year = 1970;
while (days_left >= days_in_year) {  // Loop O(n) per call
    days_left -= days_in_year;
    year++;
}
```
- Cost: 3.2M calls × ~1-30 microseconds each = 100-500ms

**New Approach**:
```cpp
int32_t year = year_table[epoch_days];  // O(1) array lookup
```
- Cost: 3.2M calls × ~10ns each = 32ms

**Expected Impact**: 100-200ms saved

---

### Optimization 4: Direct Array Lookup for Nation Names

**Technique**: Replace hash map with flat array for 25-entry dimension

**Code**:
```cpp
// Old: nation_map[nationkey] with unordered_map lookup
// New: nation_map[nationkey] with direct array access
std::vector<std::string> nation_map(26);  // Index by nationkey
nation_map[n_nationkey[i]] = n_names[i];
```

**Reasoning**:
- Only 25 nations in TPC-H
- Array has O(1) lookup with cache-friendly access pattern
- Eliminates hash computation and collision resolution

**Expected Impact**: 32K lookups × 150ns saved = ~5ms

---

## Code Changes Summary

### Files Modified
- `/home/jl4492/GenDB/output/tpc-h/2026-02-17T16-04-10/queries/Q9/iter_2/q9.cpp`

### Key Code Sections

#### 1. CompactHashTable Implementation (Lines ~70-140)
- Open-addressing hash table template
- Robin Hood hashing with distance tracking
- Safe probe sequence termination

#### 2. YearLookup Class (Lines ~180-215)
- Pre-computed lookup table for epoch_days → year
- Safe bounds checking with fallback

#### 3. Build Join Structures (Lines ~245-310)
- Replaced `unordered_map` with `CompactHashTable` for supplier_nation_map and orders_map
- Changed nation_map to `std::vector<std::string>` for direct indexing
- Pre-sizing for hash tables (load factor 75%)

#### 4. Parallel Scan Phase (Lines ~320-380)
- `#pragma omp parallel for` with dynamic scheduling
- Thread-local aggregation vectors
- Merge phase at end

#### 5. Year Extraction (Line ~365)
- Changed from `epoch_days_to_year(orderdate)` to `g_year_lookup.get_year(orderdate)`

---

## Expected Performance Gains

### Conservative Estimate (8-15x Total Speedup)

| Optimization | Impact | Mechanism |
|--------------|--------|-----------|
| Parallelism (64 cores, 8x) | -7,300 ms | 60M rows / 8 cores |
| Compact hash tables (2.5x) | -300 ms | 3.2M probes × 150ns saved |
| O(1) year extraction | -150 ms | 3.2M conversions |
| Direct array nation lookup | -5 ms | 32K lookups |
| Merge overhead | +100 ms | Thread aggregation merge |
| **Predicted New Total** | **~700 ms** | 10,500 → 700 (15x) |

### Target Comparison
- Umbra: 200ms
- DuckDB: 254ms
- MonetDB: 360ms
- **GenDB Iter 1**: 10,500ms
- **GenDB Iter 2 (predicted)**: ~700ms

**Gap to Umbra**: 52.4x → 3.5x (significant improvement)

---

## Implementation Notes

### Correctness Preservation
1. ✅ All `[TIMING]` blocks preserved inside `#ifdef GENDB_PROFILE`
2. ✅ Encoding logic unchanged (decimal unscaling, date handling)
3. ✅ Kahan summation for floating-point aggregation preserved
4. ✅ CSV output format unchanged
5. ✅ Validation: same 175 rows expected, same calculations

### Parallelism Strategy
- OpenMP handles thread management and task distribution
- Dynamic scheduling (10K row chunks) balances load
- Thread-local aggregation avoids lock contention
- Final merge is sequential but negligible (only 175 groups)

### Memory Considerations
- Compact hash tables use ~2.5MB per table (8M entries × 32 bytes)
- Thread-local aggregation: 64 threads × 175 groups max = minimal overhead
- Year lookup table: 11K entries × 4 bytes = 44KB

---

## Compilation

```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE \
    -o /home/jl4492/GenDB/output/tpc-h/2026-02-17T16-04-10/queries/Q9/iter_2/q9 \
    /home/jl4492/GenDB/output/tpc-h/2026-02-17T16-04-10/queries/Q9/iter_2/q9.cpp
```

**Status**: ✅ Compilation successful (71K binary)

---

## Next Steps for Iteration 3 (if needed)

1. **SIMD vectorization**: Process multiple rows per CPU cycle with AVX2/SSE4.2
2. **Bloom filter semi-join**: Pre-filter lineitem by green_parts without per-row hash lookups
3. **Faster partition-based aggregation**: Pre-partition by (nation, year) hash to reduce merge overhead
4. **Index-based filtering**: Load pre-built zone maps or bloom filters for l_partkey column
5. **Cache-oblivious sorting**: Radix sort for final results (currently negligible)

---

## Testing Checklist

- [x] Code compiles without errors
- [x] All TIMING blocks preserved
- [x] Encoding logic unchanged
- [x] Parallel directives correct
- [x] Memory-safe (bounds checking on year_table, nation_map)
- [ ] Execution validation (Executor will run)
- [ ] Performance measurement (Executor will time)
