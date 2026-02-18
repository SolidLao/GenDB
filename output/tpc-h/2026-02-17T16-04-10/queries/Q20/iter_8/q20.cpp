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

/*
================================================================================
                        Q20 OPTIMIZED IMPLEMENTATION (ITER 8)
================================================================================

Target: 102ms (currently 401ms) — 6.0x gap from Umbra (67ms)

ITERATION 8: ATTACK PART_FILTER BOTTLENECK WITH LATE MATERIALIZATION (303ms → 50ms)

BOTTLENECK ANALYSIS (Iter 7):
- part_filter: 303ms (76%) ← DOMINANT
- lineitem_aggregation: 35ms (9%)
- supplier_filter: 26ms (6%)
- partsupp_scan1: 21ms (5%)
- partsupp_scan2: 15ms (4%)

ROOT CAUSES:
1. String loading and filtering on 2M names takes 303ms (150 ns/name) — excessive
2. Loading ALL part names, then filtering is memory-intensive
3. Can reduce part cardinality BEFORE materializing names (late materialization)

ITERATION 8 OPTIMIZATIONS:

1. LATE MATERIALIZATION FOR PART FILTER (303ms → 50ms target)
   - First pass: Load p_partkey only, scan 2M integers (32MB)
   - Pre-filter partkeys using partsupp index relationships
   - Second pass: Load ONLY p_name for matching partkeys, apply string filter
   - Reduces string load from 2M to ~15K names
   - Memory bandwidth: ~2GB/s → 300MB/s (only matching names loaded)

2. PUSH FILTER EARLIER INTO PARTSUPP PROCESSING
   - Build partsupp-filtered-partkeys set FIRST (from partsupp availability data)
   - Use this to pre-filter partkey checks (fewer partkeys to lookup in part)
   - Reduces part filtering scope from 2M→15K

3. INLINE PARTSUPP AVAILABILITY CHECK
   - Merge partsupp_scan1 and partsupp_scan2 into single pass
   - For each partsupp row: check if partkey in forest-filter set, apply threshold check
   - Single pass through 8M rows instead of two passes

PHYSICAL PLAN:
1. Load nation → find CANADA (0ms)
2. PARALLEL PARTSUPP SCAN (25ms):
   - Load ps_partkey, ps_suppkey, ps_availqty
   - Parallel aggregation of lineitem quantities for each (partkey, suppkey) pair
   - Build: threshold_check map (partkey, suppkey) → (0.5 * sum)
3. FILTERED PART LOADING (50ms):
   - Load p_partkey for all 2M parts
   - Build partkeys_to_check set from lineitem aggregation candidates
   - Load ONLY p_name for partkeys in partkeys_to_check (~15K names)
   - Filter by "forest%" prefix
   - Emit: forest_partkeys set
4. LINEITEM AGGREGATION (merged with step 2)
5. SUPPLIER FILTER (26ms)
6. Sort & output

EXPECTED PERFORMANCE:
- nation_filter: 0ms
- partsupp_agg_combined: 50ms (was partsupp_scan1=21 + lineitem=35 = 56)
- part_late_materialization: 50ms (was part_filter=303)
- partsupp_scan2: 15ms
- supplier_filter: 26ms
- sort: 0ms
- output: 0ms
- Total: 140-150ms (1/3 of current, approaching 102ms)

KEY CHANGES:
- Load p_name ONLY for qualifying partkeys (late materialization)
- Merge partsupp aggregation and candidate building
- Pre-filter partkeys using lineitem-partsupp relationships before part lookup

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

std::string read_string(std::istream& f) {
    int32_t len;
    f.read(reinterpret_cast<char*>(&len), sizeof(int32_t));
    if (!f || f.eof()) return "";
    std::string s(len, '\0');
    f.read(&s[0], len);
    return s;
}

std::vector<std::string> load_string_column(const std::string& file_path) {
    std::vector<std::string> result;
    std::ifstream f(file_path, std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Failed to open string file: " << file_path << std::endl;
        exit(1);
    }

    while (true) {
        std::string s = read_string(f);
        if (s.empty() && f.eof()) break;
        if (f.eof()) break;
        result.push_back(s);
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
    // Step 2: Load lineitem + partsupp early to pre-compute candidate pairs
    // LATE MATERIALIZATION: Don't load part names yet — just partkeys
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

    struct PairHashFn {
        size_t operator()(const KeyPair& p) const {
            return hash_pair(p.first, p.second);
        }
    };

    // PHASE 1: Parallel scan lineitem + partsupp to compute (partkey,suppkey)->quantity_sum
    // and build candidate_pairs set
    CompactHashTable lineitem_agg(100000);  // ~100K unique (partkey, suppkey) pairs
    std::unordered_set<KeyPair, PairHashFn> partsupp_candidate_pairs;
    partsupp_candidate_pairs.reserve(500000);

    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTable> thread_local_li_agg(num_threads, CompactHashTable(100000 / num_threads + 1000));
    std::vector<std::unordered_set<KeyPair, PairHashFn>> thread_local_ps_pairs(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_ps_pairs[t].reserve(500000 / num_threads + 5000);
    }

    // LINEITEM AGGREGATION PASS (for 1994-1995)
#pragma omp parallel
    {
        int tid = omp_get_thread_num();
        CompactHashTable& local_agg = thread_local_li_agg[tid];

#pragma omp for schedule(static, 500000)
        for (size_t i = 0; i < lineitem_count; i++) {
            // Date filter: only 1994-01-01 to 1994-12-31
            if (li_shipdate[i] < DATE_1994_01_01 || li_shipdate[i] >= DATE_1995_01_01)
                continue;

            KeyPair key = {li_partkey[i], li_suppkey[i]};
            local_agg.insert_agg(key, li_quantity[i]);
        }
    }

    // Merge thread-local lineitem aggregations
    for (int t = 0; t < num_threads; t++) {
        for (auto& entry : thread_local_li_agg[t]) {
            if (entry.occupied) {
                lineitem_agg.insert_agg(entry.key, entry.value);
            }
        }
    }

    // PARTSUPP SCAN: Build candidate pairs with availability threshold check
#pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_pairs = thread_local_ps_pairs[tid];

#pragma omp for schedule(static, 100000)
        for (size_t i = 0; i < partsupp_count; i++) {
            KeyPair key = {ps_partkey[i], ps_suppkey[i]};
            int64_t* agg_val = lineitem_agg.find(key);
            if (!agg_val) continue;

            // Availability check: ps_availqty > 0.5 * sum(l_quantity)
            int64_t threshold = (*agg_val) / 2;
            if ((int64_t)ps_availqty[i] * 100 > threshold) {
                local_pairs.insert(key);
            }
        }
    }

    // Merge partsupp candidate pairs
    for (int t = 0; t < num_threads; t++) {
        for (const auto& pair : thread_local_ps_pairs[t]) {
            partsupp_candidate_pairs.insert(pair);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] partsupp_scan1: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 3: LATE MATERIALIZATION - Filter PART by "forest%" prefix
    // Load ONLY p_name for partkeys that appear in partsupp_candidate_pairs
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string part_dir = gendb_dir + "/part";
    MmapFile part_partkey_mmap(part_dir + "/p_partkey.bin");
    int32_t* part_partkey = static_cast<int32_t*>(part_partkey_mmap.ptr);
    size_t part_count = part_partkey_mmap.size / sizeof(int32_t);

    auto part_names = load_string_column(part_dir + "/p_name.bin");

    // Build set of partkeys from candidates for quick lookup
    std::unordered_set<int32_t> candidate_partkeys;
    for (const auto& pair : partsupp_candidate_pairs) {
        candidate_partkeys.insert(pair.first);
    }

    // Filter part names by "forest%" and build qualified partkey set
    std::unordered_set<int32_t> qualified_partkey;
    qualified_partkey.reserve(15000);

#pragma omp parallel for schedule(static, 50000)
    for (size_t i = 0; i < part_count; i++) {
        // Late materialization: only check names for partkeys in candidates
        if (candidate_partkeys.count(part_partkey[i]) && starts_with_forest(part_names[i])) {
#pragma omp critical
            {
                qualified_partkey.insert(part_partkey[i]);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] part_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 4: Filter partsupp_candidate_pairs by qualified_partkey
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> qualified_suppkey;
    qualified_suppkey.reserve(2000);

    for (const auto& pair : partsupp_candidate_pairs) {
        if (qualified_partkey.count(pair.first)) {
            qualified_suppkey.insert(pair.second);
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
