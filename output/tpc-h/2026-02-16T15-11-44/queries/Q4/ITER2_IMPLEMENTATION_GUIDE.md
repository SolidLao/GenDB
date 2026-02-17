# Iter 2 Implementation Guide: Concrete Code Examples

## Overview
This document provides ready-to-use code snippets and implementation guidance for optimizing Q4 in Iteration 2.

---

## Path A: Pre-built Index Usage (Primary Focus)

### Step 1: Verify Binary Format

First, write a test program to inspect the index:

```cpp
#include <cstdio>
#include <cstdint>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int main() {
    // Load index file
    const char* path = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/indexes/idx_lineitem_orderkey_hash.bin";
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        perror("open");
        return 1;
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        perror("mmap");
        return 1;
    }

    // Parse header
    const uint32_t* header = (const uint32_t*)ptr;
    uint32_t num_unique = header[0];
    uint32_t table_size = header[1];

    printf("File size: %lu bytes\n", file_size);
    printf("num_unique: %u\n", num_unique);
    printf("table_size: %u\n", table_size);
    printf("Expected hash table size: %u bytes\n", table_size * 12);
    printf("Position data starts at: %u bytes\n", 8 + table_size * 12);

    // Parse first 10 hash entries
    struct IndexEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    const IndexEntry* hash_table = (const IndexEntry*)(((uint8_t*)ptr) + 8);

    printf("\nFirst 10 hash entries:\n");
    int shown = 0;
    for (uint32_t slot = 0; slot < table_size && shown < 10; slot++) {
        if (hash_table[slot].key != 0) {
            printf("  Slot %u: key=%d, offset=%u, count=%u\n",
                   slot, hash_table[slot].key, hash_table[slot].offset, hash_table[slot].count);
            shown++;
        }
    }

    munmap(ptr, file_size);
    return 0;
}
```

**Run this to verify**:
- num_unique is approximately 10.7M (matches expectation)
- table_size is power of 2
- Offset values are reasonable (< file_size)

---

### Step 2: Implement Index-Based Semi-Join Build

Once verified, modify q4.cpp:

```cpp
// Add new helper function after mmap_file()

struct IndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

bool load_pre_built_index(const std::string& index_path,
                          const uint8_t*& out_data,
                          size_t& out_size,
                          uint32_t& out_num_unique,
                          uint32_t& out_table_size,
                          const IndexEntry*& out_hash_table,
                          const uint32_t*& out_position_data) {
    int fd = open(index_path.c_str(), O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "Failed to open index: %s\n", index_path.c_str());
        return false;
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        perror("fstat");
        close(fd);
        return false;
    }

    out_size = st.st_size;
    out_data = (const uint8_t*)mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (out_data == MAP_FAILED) {
        perror("mmap");
        return false;
    }

    // Parse header
    const uint32_t* header = (const uint32_t*)out_data;
    out_num_unique = header[0];
    out_table_size = header[1];

    // Pointer to hash table (starts at offset 8)
    out_hash_table = (const IndexEntry*)(out_data + 8);

    // Pointer to position data
    out_position_data = (const uint32_t*)(out_data + 8 + out_table_size * 12);

    // Sanity checks
    if (out_num_unique > 100000000 || out_table_size > 100000000) {
        fprintf(stderr, "Index header looks corrupted\n");
        return false;
    }

    return true;
}

// New semi-join build using pre-built index
void build_semi_join_from_index(
        const std::string& gendb_dir,
        const std::string& index_path,
        const int32_t* l_commitdate,
        const int32_t* l_receiptdate,
        uint32_t num_lineitem,
        FastHashSet& lineitem_keys) {

    #ifdef GENDB_PROFILE
    auto t_index_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load pre-built index
    const uint8_t* index_data;
    size_t index_size;
    uint32_t num_unique;
    uint32_t table_size;
    const IndexEntry* hash_table;
    const uint32_t* position_data;

    if (!load_pre_built_index(index_path, index_data, index_size,
                             num_unique, table_size, hash_table, position_data)) {
        fprintf(stderr, "Failed to load pre-built index, falling back to scan\n");
        // Fallback: use old implementation
        for (uint32_t i = 0; i < num_lineitem; i++) {
            if (l_commitdate[i] < l_receiptdate[i]) {
                lineitem_keys.insert(l_orderkey[i]);
            }
        }
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_index_loaded = std::chrono::high_resolution_clock::now();
    double index_load_ms = std::chrono::duration<double, std::milli>(t_index_loaded - t_index_start).count();
    printf("[TIMING] index_load: %.2f ms\n", index_load_ms);
    #endif

    // Iterate through hash table and check positions
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    uint32_t keys_checked = 0;
    uint32_t keys_added = 0;
    uint32_t positions_filtered = 0;

    for (uint32_t slot = 0; slot < table_size; slot++) {
        const IndexEntry& entry = hash_table[slot];

        // Empty slot
        if (entry.key == 0) continue;

        keys_checked++;

        // Check if ANY position for this key satisfies the filter
        // entry.offset points to position data: [uint32_t pos_count][uint32_t positions...]
        uint32_t pos_count = position_data[entry.offset];

        bool has_valid_position = false;
        for (uint32_t i = 0; i < pos_count; i++) {
            uint32_t row = position_data[entry.offset + 1 + i];

            // Sanity check
            if (row >= num_lineitem) {
                fprintf(stderr, "WARNING: Invalid row index %u >= %u\n", row, num_lineitem);
                continue;
            }

            positions_filtered++;

            if (l_commitdate[row] < l_receiptdate[row]) {
                has_valid_position = true;
                break;  // Found one match, no need to check others
            }
        }

        if (has_valid_position) {
            lineitem_keys.insert(entry.key);
            keys_added++;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] index_filter: %.2f ms\n", filter_ms);
    printf("[DEBUG] index_keys_checked: %u\n", keys_checked);
    printf("[DEBUG] index_keys_added: %u\n", keys_added);
    printf("[DEBUG] index_positions_filtered: %u\n", positions_filtered);
    #endif

    munmap((void*)index_data, index_size);
}
```

### Step 3: Update run_q4() to Use Index

Replace the semi_join_build section:

```cpp
// OLD CODE:
/*
// Step 1: Build semi-join set from lineitem (l_commitdate < l_receiptdate)
#ifdef GENDB_PROFILE
auto t_sj_build_start = std::chrono::high_resolution_clock::now();
#endif

FastHashSet lineitem_keys(num_lineitem / 4);
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        lineitem_keys.insert(l_orderkey[i]);
    }
}

#ifdef GENDB_PROFILE
auto t_sj_build_end = std::chrono::high_resolution_clock::now();
double sj_build_ms = std::chrono::duration<double, std::milli>(t_sj_build_end - t_sj_build_start).count();
printf("[TIMING] semi_join_build: %.2f ms\n", sj_build_ms);
printf("[DEBUG] semi_join_set_size: %zu\n", lineitem_keys.get_size());
#endif
*/

// NEW CODE:
#ifdef GENDB_PROFILE
auto t_sj_build_start = std::chrono::high_resolution_clock::now();
#endif

FastHashSet lineitem_keys(num_lineitem / 4);

// Try to use pre-built index, fall back to scan if not available
std::string index_path = gendb_dir + "/indexes/idx_lineitem_orderkey_hash.bin";
build_semi_join_from_index(gendb_dir, index_path, l_commitdate, l_receiptdate, num_lineitem, lineitem_keys);

#ifdef GENDB_PROFILE
auto t_sj_build_end = std::chrono::high_resolution_clock::now();
double sj_build_ms = std::chrono::duration<double, std::milli>(t_sj_build_end - t_sj_build_start).count();
printf("[TIMING] semi_join_build: %.2f ms\n", sj_build_ms);
printf("[DEBUG] semi_join_set_size: %zu\n", lineitem_keys.get_size());
#endif
```

---

## Path B: SIMD Vectorization (Alternative/Supplement)

### Option B1: Rely on Compiler Auto-Vectorization (Simplest)

Just compile with the right flags:
```bash
g++ -O3 -march=native -ffast-math -fopenmp -DGENDB_PROFILE q4.cpp -o q4
```

The compiler might auto-vectorize the filter loop. To help it:

```cpp
// Make loop more vectorization-friendly
#pragma omp simd collapse(1) aligned(l_commitdate:64) aligned(l_receiptdate:64)
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        lineitem_keys.insert(l_orderkey[i]);
    }
}
```

**Expected improvement**: 5-10% (modest, since insertion still isn't vectorized)

### Option B2: Manual AVX-512 (Advanced)

Only if auto-vectorization doesn't help:

```cpp
#include <immintrin.h>

void filter_with_simd(const int32_t* l_orderkey,
                      const int32_t* l_commitdate,
                      const int32_t* l_receiptdate,
                      uint32_t num_lineitem,
                      FastHashSet& lineitem_keys) {

    // Ensure availability with runtime check
    if (!__builtin_cpu_supports("avx512f")) {
        fprintf(stderr, "AVX-512 not supported, using scalar fallback\n");
        for (uint32_t i = 0; i < num_lineitem; i++) {
            if (l_commitdate[i] < l_receiptdate[i]) {
                lineitem_keys.insert(l_orderkey[i]);
            }
        }
        return;
    }

    // Process 16 elements at a time with AVX-512
    const int VWIDTH = 16;
    uint32_t i = 0;

    for (; i + VWIDTH <= num_lineitem; i += VWIDTH) {
        // Load 16 pairs of dates
        __m512i commitdate = _mm512_loadu_si512((__m512i*)&l_commitdate[i]);
        __m512i receiptdate = _mm512_loadu_si512((__m512i*)&l_receiptdate[i]);
        __m512i orderkey = _mm512_loadu_si512((__m512i*)&l_orderkey[i]);

        // Vectorized comparison: commitdate < receiptdate
        __mmask16 mask = _mm512_cmplt_epi32_mask(commitdate, receiptdate);

        // Extract matching keys and insert them
        for (int j = 0; j < VWIDTH; j++) {
            if (mask & (1U << j)) {
                int32_t key = _mm512_extract_epi32(orderkey, j);
                lineitem_keys.insert(key);
            }
        }
    }

    // Handle remainder with scalar code
    for (; i < num_lineitem; i++) {
        if (l_commitdate[i] < l_receiptdate[i]) {
            lineitem_keys.insert(l_orderkey[i]);
        }
    }
}
```

Compile with: `g++ -O3 -march=skylake-avx512 q4.cpp -o q4`

**Expected improvement**: 20-30% (still limited by insertion bottleneck)

---

## Path C: Dual-Buffer Insertion

### Implementation

Replace the simple insertion loop:

```cpp
// OLD: Direct insertion
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        lineitem_keys.insert(l_orderkey[i]);
    }
}

// NEW: Buffer-based insertion
const int BUFFER_SIZE = 1000000;
std::vector<int32_t> match_buffer;
match_buffer.reserve(BUFFER_SIZE);

// Phase 1: Collect matches into buffer (separate from insertion)
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        match_buffer.push_back(l_orderkey[i]);

        // Flush buffer periodically to avoid excessive memory use
        if (match_buffer.size() >= BUFFER_SIZE) {
            // Process buffer
            std::sort(match_buffer.begin(), match_buffer.end());
            auto last = std::unique(match_buffer.begin(), match_buffer.end());
            for (auto it = match_buffer.begin(); it != last; ++it) {
                lineitem_keys.insert(*it);
            }
            match_buffer.clear();
        }
    }
}

// Flush remaining buffer
if (!match_buffer.empty()) {
    std::sort(match_buffer.begin(), match_buffer.end());
    auto last = std::unique(match_buffer.begin(), match_buffer.end());
    for (auto it = match_buffer.begin(); it != last; ++it) {
        lineitem_keys.insert(*it);
    }
}
```

**Why this helps**:
- Filter phase: L1 cache stays hot (sequential read of 3 columns, write to buffer)
- Insertion phase: Sorted data has better cache behavior in hash table
- Deduplication before insertion reduces actual insertions

**Expected improvement**: 10-15%

---

## Path D: Scalar Loop Optimization (Quick Win)

### With Prefetching

```cpp
// Prefetch-aware filtering
const int PREFETCH_DISTANCE = 32;  // Adjust based on CPU

for (uint32_t i = 0; i < num_lineitem; i++) {
    // Prefetch data that will be needed in ~32 iterations
    if (i + PREFETCH_DISTANCE < num_lineitem) {
        __builtin_prefetch(&l_commitdate[i + PREFETCH_DISTANCE], 0, 3);
        __builtin_prefetch(&l_receiptdate[i + PREFETCH_DISTANCE], 0, 3);
        __builtin_prefetch(&l_orderkey[i + PREFETCH_DISTANCE], 0, 3);
    }

    if (l_commitdate[i] < l_receiptdate[i]) {
        lineitem_keys.insert(l_orderkey[i]);
    }
}
```

**Expected improvement**: 3-5% (modest but safe)

---

## Testing & Validation

### Before Committing Changes

```cpp
// Add this validation code
printf("[DEBUG] Starting validation...\n");

// Check a few random keys from the lineitem
std::unordered_set<int32_t> full_set;
for (uint32_t i = 0; i < num_lineitem; i++) {
    if (l_commitdate[i] < l_receiptdate[i]) {
        full_set.insert(l_orderkey[i]);
    }
}

printf("[DEBUG] Full scan set size: %zu\n", full_set.size());
printf("[DEBUG] Index-based set size: %zu\n", lineitem_keys.get_size());

if (full_set.size() != lineitem_keys.get_size()) {
    fprintf(stderr, "ERROR: Set sizes don't match!\n");
    // Detailed comparison...
    for (int32_t key : full_set) {
        if (!lineitem_keys.contains(key)) {
            fprintf(stderr, "  Missing key: %d\n", key);
        }
    }
    return;
}

printf("[DEBUG] Validation passed!\n");
```

---

## Compilation Recommendations

### Conservative (Maximum Compatibility)
```bash
g++ -O3 -march=native -fopenmp -DGENDB_PROFILE q4.cpp -o q4
```

### Aggressive (Maximize Performance)
```bash
g++ -O3 -march=native -ffast-math -fopenmp \
    -fno-math-errno -funsafe-math-optimizations \
    -DGENDB_PROFILE q4.cpp -o q4
```

### With Intel Compiler (if available)
```bash
icpc -O3 -march=native -qopenmp -DGENDB_PROFILE q4.cpp -o q4
```

---

## Expected Performance Outcomes

### Scenario 1: Pre-built Index Works
```
Current: 645.26 ms
├─ Index load:  5 ms
├─ Index filter: 100 ms
├─ Insertion: 50 ms
└─ Total semi_join_build: 155 ms
```
**New total: ~170 ms (74% improvement)**

### Scenario 2: Index Doesn't Work, Use SIMD+Buffer
```
Current: 645.26 ms
├─ SIMD filter + buffer: 500 ms (20% gain)
├─ Batch insertion: 80 ms (20% gain)
└─ Total semi_join_build: 580 ms
```
**New total: ~595 ms (8% improvement)**

### Scenario 3: Conservative Approach
```
Current: 645.26 ms
├─ Prefetch: 610 ms
├─ SIMD (if auto-vectorized): 550 ms
└─ Both combined: 500 ms
```
**New total: ~515 ms (20% improvement)**

---

## Debugging Tips

If the index-based approach is slower than expected:

1. **Profile with perf**:
   ```bash
   perf record -g ./q4 /path/to/gendb
   perf report
   ```

2. **Check cache behavior**:
   ```bash
   perf stat -e cache-references,cache-misses ./q4 /path/to/gendb
   ```

3. **Verify index correctness**:
   - Print first 100 entries of index
   - Verify position lists are within bounds
   - Check for null keys (should be 0)

4. **Add detailed timing**:
   - Time index load separately
   - Time position list iteration separately
   - Time insertion separately

---

## Summary of Implementation Path

**Recommended order**:

1. **Verify index format** (30 min) → Test program
2. **Implement index-based build** (60 min) → 75% gain expected
3. **If fails, add prefetching** (10 min) → 5% gain
4. **If still need more, add SIMD** (60 min) → 20% gain

**Expected result**: 150-300 ms (if index works), 500-600 ms (if not)
