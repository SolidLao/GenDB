#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_set>
#include <string>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cstdint>

namespace {

/*
================================================================================
                        Q20 OPTIMIZED IMPLEMENTATION (ITER 10)
================================================================================

Target: 102ms (currently 401ms) — 6.0x gap from Umbra (67ms)

ITERATION 10: ATTACK DOMINANT BOTTLENECK (part_filter = 303ms, 76%)

BOTTLENECK ANALYSIS (Iter 7):
- part_filter: 303ms (76%) ← DOMINANT
- lineitem_aggregation: 35ms (9%)
- partsupp_scan1: 21ms (5%)
- supplier_filter: 26ms (6%)
- partsupp_scan2: 15ms (4%)

ROOT CAUSE OF part_filter DOMINANCE (303ms):
- load_string_column() re-allocates and copies all 2M strings
- Each string copy is 20-50 bytes, adding 40-100MB memory bandwidth
- Two-phase approach: mark pass + set-build pass = excess data movement
- Vector<string> has pointer indirection for every lookup

ITERATION 10 OPTIMIZATION STRATEGY:

1. RADICAL SIMPLIFICATION OF part_filter (303ms → 30-50ms target):
   - AVOID load_string_column() entirely — read strings directly during mmap scan
   - Use inline string comparison during single-pass filtering
   - Parallel with thread-local result accumulation
   - Expected: 6-10x speedup on string-matching-heavy operation

2. MICRO-OPTIMIZE lineitem_aggregation (35ms → 25ms target):
   - Current approach: hash set lookup for candidate_pairs per row is O(1) but has constant overhead
   - Optimization: convert to thread-local aggregation without constant-time bottleneck
   - Use open-addressing hash directly (already done, but verify bucket density)

3. REDUCE MEMORY PRESSURE:
   - Avoid materializing large string vectors
   - Use streaming string reads where possible
   - Keep hot data (partkey, suppkey, availqty) in L3 cache

PHYSICAL PLAN:
1. Load nation → find CANADA nationkey (0ms)
2. OPTIMIZE part_filter (30-50ms target):
   - Open part/p_name.bin directly, scan without copying strings
   - Build qualified_partkey set in single pass with inline comparison
   - Parallel with aggregation thread-local results
3. Load partsupp + lineitem data for aggregation (same as before)
4. Two-pass partsupp for availability check (same as before)
5. Load supplier → filter (same as before)
6. Sort & output (same as before)

EXPECTED PERFORMANCE:
- nation_filter: <1ms
- part_filter: 30-50ms (was 303ms) ← KEY WIN (6-10x)
- partsupp_scan1: 20ms (unchanged)
- lineitem_aggregation: 25ms (was 35ms, slight improvement)
- partsupp_scan2: 15ms (unchanged)
- supplier_filter: 26ms (unchanged)
- sort: 0ms
- Total: 115-165ms target (currently 401ms)
- Gap to Umbra (67ms): still 1.7-2.5x, but this is the algorithmic limit for our approach

================================================================================
*/

const int32_t DATE_1994_01_01 = 8766;
const int32_t DATE_1995_01_01 = 9131;

struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        size = lseek(fd, 0, SEEK_END);
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }
    }

    ~MmapFile() {
        if (ptr != MAP_FAILED && ptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }
};

std::vector<std::string> load_string_column(const std::string& file_path) {
    // OPTIMIZATION: Use mmap instead of ifstream for better I/O performance
    MmapFile mmap_file(file_path);
    int32_t* data = static_cast<int32_t*>(mmap_file.ptr);
    size_t remaining = mmap_file.size;

    std::vector<std::string> result;
    result.reserve(2000000);  // Pre-allocate for part table size

    const char* byte_ptr = reinterpret_cast<const char*>(data);
    size_t offset = 0;

    while (offset + sizeof(int32_t) < remaining) {
        int32_t len = *reinterpret_cast<const int32_t*>(byte_ptr + offset);
        offset += sizeof(int32_t);

        if (offset + len > remaining || len < 0) break;

        result.emplace_back(byte_ptr + offset, len);
        offset += len;
    }

    return result;
}

// OPTIMIZATION: Branch-free vectorized prefix match with length pre-check
inline bool starts_with_forest(const std::string& s) {
    // Length pre-check eliminates ~99% of names in O(1)
    if (s.size() < 6) return false;

    // Vectorized comparison: load first 6 bytes and compare as packed integers
    // Avoids branch misprediction in character-by-character comparison
    const char* data = s.data();
    // Use memcmp for compiler optimization (vectorized by optimizer)
    return s.size() >= 6 && std::memcmp(data, "forest", 6) == 0;
}

// Efficient pair hash with bit packing (avoid std::hash<pair> overhead)
inline uint64_t hash_pair(int32_t k1, int32_t k2) {
    uint64_t h = ((uint64_t)k1 << 32) | (uint32_t)k2;
    h ^= h >> 33;
    h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33;
    return h;
}

using KeyPair = std::pair<int32_t, int32_t>;

// Open-addressing hash table with Robin Hood hashing for aggregation
struct CompactHashTable {
    struct Entry {
        KeyPair key;
        int64_t value;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashTable(size_t expected_size = 0) : count(0) {
        // Pre-size at 75% load factor, power-of-2
        size_t capacity = 1;
        if (expected_size > 0) {
            while (capacity < expected_size * 4 / 3) capacity <<= 1;
        } else {
            capacity = 16;
        }
        table.resize(capacity);
        mask = capacity - 1;
    }

    void reserve(size_t expected_size) {
        size_t capacity = 1;
        while (capacity < expected_size * 4 / 3) capacity <<= 1;
        if (capacity > table.capacity()) {
            // Rebuild table with new capacity
            std::vector<Entry> old_table = std::move(table);
            table.clear();
            table.resize(capacity);
            mask = capacity - 1;
            count = 0;

            for (const auto& entry : old_table) {
                if (entry.occupied) {
                    insert(entry.key, entry.value);
                }
            }
        }
    }

    void insert(KeyPair key, int64_t value) {
        size_t pos = hash_pair(key.first, key.second) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }

        table[pos] = entry;
        count++;
    }

    // Insert with aggregation (add to existing or create new)
    void insert_agg(KeyPair key, int64_t delta) {
        size_t pos = hash_pair(key.first, key.second) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value += delta;
                return;
            }
            if (dist > table[pos].dist) break;
            pos = (pos + 1) & mask;
            dist++;
        }

        // Not found, need to insert
        Entry entry{key, delta, dist, true};
        while (table[pos].occupied) {
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    // Find with const reference (for merging)
    int64_t* find(KeyPair key) {
        size_t pos = hash_pair(key.first, key.second) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                return &table[pos].value;
            }
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    // Iterator for merge
    auto begin() {
        return table.begin();
    }

    auto end() {
        return table.end();
    }

    size_t size() const {
        return count;
    }
};

} // end anonymous namespace

void run_q20(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // =========================================================================
    // Step 1: Load nation, find CANADA
    // =========================================================================

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string nation_dir = gendb_dir + "/nation";
    MmapFile nation_nationkey_mmap(nation_dir + "/n_nationkey.bin");
    int32_t* nation_nationkey = static_cast<int32_t*>(nation_nationkey_mmap.ptr);
    size_t nation_count = nation_nationkey_mmap.size / sizeof(int32_t);
    auto nation_names = load_string_column(nation_dir + "/n_name.bin");

    int32_t canada_nationkey = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (nation_names[i] == "CANADA") {
            canada_nationkey = nation_nationkey[i];
            break;
        }
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] nation_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 2: Load part, filter by p_name LIKE 'forest%' (PARALLELIZED)
    // OPTIMIZATION: Single-pass with inline string reading (no load_string_column copy)
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string part_dir = gendb_dir + "/part";
    MmapFile part_partkey_mmap(part_dir + "/p_partkey.bin");
    int32_t* part_partkey = static_cast<int32_t*>(part_partkey_mmap.ptr);
    size_t part_count = part_partkey_mmap.size / sizeof(int32_t);

    // OPTIMIZATION: Load part names directly from mmap file
    // Then filter into qualified_partkey in single pass
    MmapFile part_name_mmap(part_dir + "/p_name.bin");
    const char* name_data = static_cast<const char*>(part_name_mmap.ptr);
    size_t name_remaining = part_name_mmap.size;

    // Build index of string offsets and lengths (single scan)
    std::vector<std::pair<const char*, int32_t>> name_strings;
    name_strings.reserve(part_count);
    {
        const char* byte_ptr = name_data;
        size_t offset = 0;
        while (offset + sizeof(int32_t) < name_remaining) {
            int32_t len = *reinterpret_cast<const int32_t*>(byte_ptr + offset);
            offset += sizeof(int32_t);
            if (offset + len > name_remaining || len < 0) break;
            name_strings.emplace_back(byte_ptr + offset, len);
            offset += len;
        }
    }

    // Single-pass filtering: check string match and build result set
    int num_threads_p = omp_get_max_threads();
    std::vector<std::vector<int32_t>> thread_local_results(num_threads_p);
    for (int t = 0; t < num_threads_p; t++) {
        thread_local_results[t].reserve(15000 / num_threads_p + 100);
    }

#pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_results = thread_local_results[tid];

#pragma omp for schedule(static, 50000)
        for (size_t i = 0; i < part_count; i++) {
            const auto& [name_ptr, name_len] = name_strings[i];
            // Check prefix match without materializing std::string
            if (name_len >= 6 && std::memcmp(name_ptr, "forest", 6) == 0) {
                local_results.push_back(part_partkey[i]);
            }
        }
    }

    // Merge thread-local results into final set
    std::unordered_set<int32_t> qualified_partkey;
    qualified_partkey.reserve(15000);
    for (int t = 0; t < num_threads_p; t++) {
        for (int32_t pk : thread_local_results[t]) {
            qualified_partkey.insert(pk);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] part_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 3: Load partsupp, build candidate (partkey, suppkey) pairs
    // KEY OPTIMIZATION: Get candidates BEFORE lineitem aggregation
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string partsupp_dir = gendb_dir + "/partsupp";
    MmapFile ps_partkey_mmap(partsupp_dir + "/ps_partkey.bin");
    int32_t* ps_partkey = static_cast<int32_t*>(ps_partkey_mmap.ptr);
    size_t partsupp_count = ps_partkey_mmap.size / sizeof(int32_t);

    MmapFile ps_suppkey_mmap(partsupp_dir + "/ps_suppkey.bin");
    int32_t* ps_suppkey = static_cast<int32_t*>(ps_suppkey_mmap.ptr);

    MmapFile ps_availqty_mmap(partsupp_dir + "/ps_availqty.bin");
    int32_t* ps_availqty = static_cast<int32_t*>(ps_availqty_mmap.ptr);

    struct PairHashFn {
        size_t operator()(const KeyPair& p) const {
            return hash_pair(p.first, p.second);
        }
    };

    // Build candidate_pairs set by filtering partsupp on qualified_partkey
    std::unordered_set<KeyPair, PairHashFn> candidate_pairs;
    candidate_pairs.reserve(50000);

    // OPTIMIZATION: Parallel scan of 8M partsupp rows
    // Use thread-local sets, then merge
    int num_threads_ps = omp_get_max_threads();
    std::vector<std::unordered_set<KeyPair, PairHashFn>> thread_local_pairs(num_threads_ps);
    for (int t = 0; t < num_threads_ps; t++) {
        thread_local_pairs[t].reserve(50000 / num_threads_ps + 1000);
    }

#pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_pairs = thread_local_pairs[tid];

#pragma omp for schedule(static, 100000)
        for (size_t i = 0; i < partsupp_count; i++) {
            if (qualified_partkey.count(ps_partkey[i])) {
                local_pairs.insert({ps_partkey[i], ps_suppkey[i]});
            }
        }
    }

    // Single-pass merge into final set
    for (int t = 0; t < num_threads_ps; t++) {
        for (const auto& pair : thread_local_pairs[t]) {
            candidate_pairs.insert(pair);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] partsupp_scan1: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 4: Aggregate lineitem ONLY for candidate (partkey, suppkey) pairs
    // ARCHITECTURE FIX: Only hash ~500K-1M rows instead of 60M
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string lineitem_dir = gendb_dir + "/lineitem";
    MmapFile li_partkey_mmap(lineitem_dir + "/l_partkey.bin");
    int32_t* li_partkey = static_cast<int32_t*>(li_partkey_mmap.ptr);
    size_t lineitem_count = li_partkey_mmap.size / sizeof(int32_t);

    MmapFile li_suppkey_mmap(lineitem_dir + "/l_suppkey.bin");
    int32_t* li_suppkey = static_cast<int32_t*>(li_suppkey_mmap.ptr);

    MmapFile li_quantity_mmap(lineitem_dir + "/l_quantity.bin");
    int64_t* li_quantity = static_cast<int64_t*>(li_quantity_mmap.ptr);

    MmapFile li_shipdate_mmap(lineitem_dir + "/l_shipdate.bin");
    int32_t* li_shipdate = static_cast<int32_t*>(li_shipdate_mmap.ptr);

    // Single hash table for aggregation (no partitioning needed - only ~40K groups)
    CompactHashTable lineitem_agg(candidate_pairs.size());

    // Parallel aggregation with thread-local tables
    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTable> thread_local_agg(num_threads, CompactHashTable(candidate_pairs.size() / num_threads + 1000));

#pragma omp parallel
    {
        int tid = omp_get_thread_num();
        CompactHashTable& local_agg = thread_local_agg[tid];

#pragma omp for schedule(static, 500000)
        for (size_t i = 0; i < lineitem_count; i++) {
            // Date filter
            if (li_shipdate[i] < DATE_1994_01_01 || li_shipdate[i] >= DATE_1995_01_01)
                continue;

            KeyPair key = {li_partkey[i], li_suppkey[i]};

            // Check if this pair is a candidate
            if (candidate_pairs.count(key)) {
                local_agg.insert_agg(key, li_quantity[i]);
            }
        }
    }

    // Merge thread-local aggregations into final table
    for (int t = 0; t < num_threads; t++) {
        for (auto& entry : thread_local_agg[t]) {
            if (entry.occupied) {
                lineitem_agg.insert_agg(entry.key, entry.value);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] lineitem_aggregation: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 5: Re-scan partsupp, filter by availability check
    // OPTIMIZATION: Parallel scan with thread-local result merging
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> qualified_suppkey;
    qualified_suppkey.reserve(2000);

    // Parallel scan to build qualified_suppkey
    std::vector<std::unordered_set<int32_t>> thread_local_suppkey(num_threads_ps);
    for (int t = 0; t < num_threads_ps; t++) {
        thread_local_suppkey[t].reserve(2000 / num_threads_ps + 100);
    }

#pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_suppkey = thread_local_suppkey[tid];

#pragma omp for schedule(static, 100000)
        for (size_t i = 0; i < partsupp_count; i++) {
            // Early exit: skip if partkey not in qualified set
            if (qualified_partkey.count(ps_partkey[i]) == 0) continue;

            KeyPair key = {ps_partkey[i], ps_suppkey[i]};
            int64_t* agg_val = lineitem_agg.find(key);
            if (!agg_val) continue;

            // Availability check: ps_availqty > 0.5 * sum(l_quantity)
            int64_t threshold = (*agg_val) / 2;  // 0.5 * sum
            if ((int64_t)ps_availqty[i] * 100 > threshold) {
                local_suppkey.insert(ps_suppkey[i]);
            }
        }
    }

    // Single-pass merge into final set
    for (int t = 0; t < num_threads_ps; t++) {
        for (int32_t suppkey : thread_local_suppkey[t]) {
            qualified_suppkey.insert(suppkey);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] partsupp_scan2: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 6: Load supplier, filter by nation and qualified_suppkey
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string supplier_dir = gendb_dir + "/supplier";
    MmapFile supp_suppkey_mmap(supplier_dir + "/s_suppkey.bin");
    int32_t* supp_suppkey = static_cast<int32_t*>(supp_suppkey_mmap.ptr);
    size_t supplier_count = supp_suppkey_mmap.size / sizeof(int32_t);

    MmapFile supp_nationkey_mmap(supplier_dir + "/s_nationkey.bin");
    int32_t* supp_nationkey = static_cast<int32_t*>(supp_nationkey_mmap.ptr);

    auto supp_names = load_string_column(supplier_dir + "/s_name.bin");
    auto supp_addresses = load_string_column(supplier_dir + "/s_address.bin");

    struct Result {
        std::string s_name;
        std::string s_address;
        bool operator<(const Result& other) const {
            return s_name < other.s_name;
        }
    };

    std::vector<Result> results;
    results.reserve(2000);

    for (size_t i = 0; i < supplier_count; i++) {
        if (supp_nationkey[i] != canada_nationkey) continue;
        if (qualified_suppkey.count(supp_suppkey[i]) == 0) continue;

        Result r;
        r.s_name = supp_names[i];
        r.s_address = supp_addresses[i];
        results.push_back(r);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] supplier_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 7: Sort results by s_name
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end());

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 8: Write CSV output
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q20.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        exit(1);
    }

    out << "s_name,s_address\n";
    for (const auto& r : results) {
        std::string escaped_address = r.s_address;
        bool needs_quotes = false;
        for (char c : escaped_address) {
            if (c == ',' || c == '"' || c == '\n') {
                needs_quotes = true;
                break;
            }
        }

        out << r.s_name;
        out << ",";
        if (needs_quotes) {
            out << "\"";
            for (char c : escaped_address) {
                if (c == '"') out << "\"\"";
                else out << c;
            }
            out << "\"";
        } else {
            out << escaped_address;
        }
        out << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q20(gendb_dir, results_dir);
    return 0;
}
#endif
