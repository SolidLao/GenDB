#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <mutex>

/*
 * Q16 Implementation Plan (Iteration 4 - FUNDAMENTAL REWRITE):
 *
 * Architecture-Level Failures Identified:
 * 1. std::unordered_map for join hash table (200K entries) → 2-5x slower than open-addressing
 * 2. std::unordered_map for aggregation (27,840 groups) → 2-5x slower than open-addressing
 * 3. std::set<int32_t> for distinct counting → Poor cache locality
 * 4. Thread-local sets merged sequentially → 27,840 groups × 64 threads = merge bottleneck
 *
 * NEW Physical Plan:
 * 1. Anti-join: Keep unordered_set (only 100K entries, not the bottleneck)
 * 2. Part filter: Sequential scan with predicates (315ms is acceptable)
 * 3. Join: Open-addressing CompactHashTable (build=filtered part ~200K)
 * 4. Aggregation: Open-addressing hash table with composite key → vector<int32_t> for suppkeys
 * 5. Distinct count: Sort+unique per group (or bitset if suppkey domain is small)
 * 6. Parallelism: Parallel probe with lock-free aggregation (atomic append or partitioning)
 *
 * Data structures:
 * - CompactHashTable for part join (200K entries)
 * - CompactHashTable for aggregation (27,840 groups)
 * - vector<int32_t> per group for suppkeys (sort+unique at end for distinct count)
 */

// Helper: Load dictionary from file
std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary: " << path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(file, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Helper: mmap a binary column
template<typename T>
T* mmap_column(const std::string& path, size_t expected_rows, size_t& actual_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    fstat(fd, &sb);
    actual_rows = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed: " << path << std::endl;
        return nullptr;
    }
    return static_cast<T*>(addr);
}

// Helper: Load variable-length string column (length-prefixed)
std::vector<std::string> load_string_column(const std::string& path, size_t expected_rows) {
    std::vector<std::string> result;
    result.reserve(expected_rows);

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return result;
    }

    struct stat sb;
    fstat(fd, &sb);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed: " << path << std::endl;
        return result;
    }

    const char* data = static_cast<const char*>(addr);
    size_t offset = 0;

    while (offset < sb.st_size) {
        uint32_t len;
        memcpy(&len, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        if (offset + len > sb.st_size) break;

        result.emplace_back(data + offset, len);
        offset += len;
    }

    munmap(addr, sb.st_size);
    return result;
}

// Helper: Check if string contains substring
bool contains_substring(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

// Open-addressing hash table for joins (replaces std::unordered_map)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };
    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, const V& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Part row data
struct PartRow {
    int32_t brand;
    std::string type;
    int32_t size;
};

// Composite key for GROUP BY (brand, type, size)
struct GroupKey {
    int32_t brand;
    std::string type;
    int32_t size;

    bool operator==(const GroupKey& other) const {
        return brand == other.brand && type == other.type && size == other.size;
    }
};

// Custom hash for composite key (avoid std::hash)
struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        size_t h1 = (size_t)k.brand * 0x9E3779B97F4A7C15ULL;
        size_t h2 = std::hash<std::string>()(k.type);
        size_t h3 = (size_t)k.size * 0x85EBCA6B;
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Open-addressing hash table for aggregation with composite key
struct AggHashTable {
    struct Entry {
        GroupKey key;
        std::vector<int32_t> suppkeys;
        bool occupied = false;
    };
    std::vector<Entry> table;
    size_t mask;
    size_t count = 0;

    AggHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(const GroupKey& key) const {
        GroupKeyHash hasher;
        return hasher(key);
    }

    std::vector<int32_t>* find_or_insert(const GroupKey& key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].suppkeys;
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].occupied = true;
        count++;
        return &table[idx].suppkeys;
    }

    Entry* begin() { return table.data(); }
    Entry* end() { return table.data() + table.size(); }
};

void run_q16(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Step 1: Load supplier and build anti-join set
    #ifdef GENDB_PROFILE
    auto t_anti_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t s_rows = 0;
    int32_t* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", 100000, s_rows);
    auto s_comment = load_string_column(gendb_dir + "/supplier/s_comment.bin", 100000);

    std::unordered_set<int32_t> bad_suppliers;
    for (size_t i = 0; i < s_rows; i++) {
        if (contains_substring(s_comment[i], "Customer") &&
            contains_substring(s_comment[i], "Complaints")) {
            bad_suppliers.insert(s_suppkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_anti_end = std::chrono::high_resolution_clock::now();
    double ms_anti = std::chrono::duration<double, std::milli>(t_anti_end - t_anti_start).count();
    printf("[TIMING] anti_join_build: %.2f ms\n", ms_anti);
    #endif

    // Step 2: Load part table and filter
    #ifdef GENDB_PROFILE
    auto t_part_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t p_rows = 0;
    int32_t* p_partkey = mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", 2000000, p_rows);
    int32_t* p_brand_code = mmap_column<int32_t>(gendb_dir + "/part/p_brand.bin", 2000000, p_rows);
    auto p_type = load_string_column(gendb_dir + "/part/p_type.bin", 2000000);
    int32_t* p_size = mmap_column<int32_t>(gendb_dir + "/part/p_size.bin", 2000000, p_rows);

    auto p_brand_dict = load_dictionary(gendb_dir + "/part/p_brand_dict.txt");

    // Find 'Brand#45' code
    int32_t brand45_code = -1;
    for (size_t i = 0; i < p_brand_dict.size(); i++) {
        if (p_brand_dict[i] == "Brand#45") {
            brand45_code = i;
            break;
        }
    }

    // Valid sizes
    std::unordered_set<int32_t> valid_sizes = {49, 14, 23, 45, 19, 3, 36, 9};

    // Build hash table on filtered part (OPEN-ADDRESSING)
    CompactHashTable<int32_t, PartRow> part_ht(200000);

    for (size_t i = 0; i < p_rows; i++) {
        // Filter predicates
        if (p_brand_code[i] == brand45_code) continue;
        if (p_type[i].substr(0, 15) == "MEDIUM POLISHED") continue;
        if (valid_sizes.find(p_size[i]) == valid_sizes.end()) continue;

        part_ht.insert(p_partkey[i], {p_brand_code[i], p_type[i], p_size[i]});
    }

    #ifdef GENDB_PROFILE
    auto t_part_end = std::chrono::high_resolution_clock::now();
    double ms_part = std::chrono::duration<double, std::milli>(t_part_end - t_part_start).count();
    printf("[TIMING] scan_filter_part: %.2f ms\n", ms_part);
    #endif

    // Step 3: Load partsupp and probe
    #ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t ps_rows = 0;
    int32_t* ps_partkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", 8000000, ps_rows);
    int32_t* ps_suppkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", 8000000, ps_rows);

    // Aggregation: (brand, type, size) -> vector of suppkeys (OPEN-ADDRESSING)
    // Use thread-local aggregation with open-addressing hash tables
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;

    std::vector<AggHashTable> thread_aggs;
    thread_aggs.reserve(num_threads);
    for (unsigned int t = 0; t < num_threads; t++) {
        thread_aggs.emplace_back(40000);  // ~30K groups, pre-size to avoid resizing
    }

    std::vector<std::thread> threads;
    size_t chunk_size = (ps_rows + num_threads - 1) / num_threads;

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, ps_rows);

            for (size_t i = start; i < end; i++) {
                // Anti-join filter
                if (bad_suppliers.count(ps_suppkey[i])) continue;

                // Join with part
                auto* part_row = part_ht.find(ps_partkey[i]);
                if (!part_row) continue;

                // Add to group
                GroupKey gk = {part_row->brand, part_row->type, part_row->size};
                auto* suppkeys = thread_aggs[t].find_or_insert(gk);
                suppkeys->push_back(ps_suppkey[i]);
            }
        });
    }

    for (auto& th : threads) th.join();

    // Merge thread-local aggregations into global hash table
    AggHashTable global_agg(40000);
    for (auto& local_agg : thread_aggs) {
        for (auto* entry = local_agg.begin(); entry != local_agg.end(); ++entry) {
            if (entry->occupied) {
                auto* global_suppkeys = global_agg.find_or_insert(entry->key);
                global_suppkeys->insert(global_suppkeys->end(),
                                       entry->suppkeys.begin(),
                                       entry->suppkeys.end());
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_join);
    #endif

    // Step 4: Sort results
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    struct ResultRow {
        std::string brand;
        std::string type;
        int32_t size;
        int32_t supplier_cnt;
    };

    std::vector<ResultRow> results;
    results.reserve(global_agg.count);

    // Compute distinct counts using sort+unique per group
    for (auto* entry = global_agg.begin(); entry != global_agg.end(); ++entry) {
        if (entry->occupied) {
            // Sort and count unique suppkeys
            std::sort(entry->suppkeys.begin(), entry->suppkeys.end());
            auto last = std::unique(entry->suppkeys.begin(), entry->suppkeys.end());
            int32_t distinct_cnt = std::distance(entry->suppkeys.begin(), last);

            results.push_back({
                p_brand_dict[entry->key.brand],
                entry->key.type,
                entry->key.size,
                distinct_cnt
            });
        }
    }

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.supplier_cnt != b.supplier_cnt) return a.supplier_cnt > b.supplier_cnt;
        if (a.brand != b.brand) return a.brand < b.brand;
        if (a.type != b.type) return a.type < b.type;
        return a.size < b.size;
    });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif

    // Step 5: Write output
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q16.csv");
    out << "p_brand,p_type,p_size,supplier_cnt\n";

    for (const auto& row : results) {
        out << row.brand << "," << row.type << "," << row.size << "," << row.supplier_cnt << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    std::cout << "Q16 complete. Results: " << results.size() << " rows\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q16(gendb_dir, results_dir);
    return 0;
}
#endif
