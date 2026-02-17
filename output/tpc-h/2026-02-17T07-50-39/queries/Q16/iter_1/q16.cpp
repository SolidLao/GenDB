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
 * Q16 Implementation Plan (Iteration 1):
 *
 * Bottleneck Analysis (iter_0):
 * - join: 671ms (66%) - DOMINANT - using std::unordered_map + std::set for distinct counting
 * - scan_filter_part: 315ms (31%) - using std::unordered_map for hash table
 * - Gap to Umbra (84ms): 12.1x
 *
 * Optimization Strategy:
 * 1. Replace std::unordered_map with open-addressing hash table (2-5x faster)
 * 2. Use bitset for distinct suppkey counting (suppkey range 1-100K, O(1) insert/check)
 * 3. Reduce string copies: store p_type indices instead of full strings
 * 4. Optimize parallel aggregation: atomic bitset operations to avoid merge overhead
 *
 * Logical Plan:
 * 1. Anti-join: Build hash set of supplier keys with 'Customer%Complaints%' in s_comment
 * 2. Filter part: p_brand <> 'Brand#45', p_type NOT LIKE 'MEDIUM POLISHED%', p_size IN (49,14,23,45,19,3,36,9)
 * 3. Build open-addressing hash table on filtered part (p_partkey → {brand_code, type_idx, size})
 * 4. Parallel probe partsupp: join on ps_partkey, filter bad_suppliers, aggregate with bitsets
 * 5. Sort results
 *
 * Physical Plan:
 * - Anti-join: bitset[100001] for O(1) lookup
 * - Part hash table: Open-addressing with linear probing
 * - Aggregation: Hash map with bitset[100001] for distinct suppkey counting
 * - Parallelism: Thread-local aggregation with final merge
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

// Open-addressing hash table for part join
template<typename V>
struct CompactHashTable {
    struct Entry {
        int32_t key;
        V value;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz, {0, {}, false});
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, const V& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Compact bitset for distinct suppkey counting (suppkey range: 1-100000)
struct SupplierBitset {
    static const size_t MAX_SUPPKEY = 100001;
    static const size_t NUM_WORDS = (MAX_SUPPKEY + 63) / 64;
    uint64_t bits[NUM_WORDS];

    SupplierBitset() {
        memset(bits, 0, sizeof(bits));
    }

    void insert(int32_t suppkey) {
        if (suppkey >= 0 && suppkey < MAX_SUPPKEY) {
            bits[suppkey / 64] |= (1ULL << (suppkey % 64));
        }
    }

    bool contains(int32_t suppkey) const {
        if (suppkey < 0 || suppkey >= MAX_SUPPKEY) return false;
        return (bits[suppkey / 64] & (1ULL << (suppkey % 64))) != 0;
    }

    void merge(const SupplierBitset& other) {
        for (size_t i = 0; i < NUM_WORDS; i++) {
            bits[i] |= other.bits[i];
        }
    }

    int32_t count() const {
        int32_t cnt = 0;
        for (size_t i = 0; i < NUM_WORDS; i++) {
            cnt += __builtin_popcountll(bits[i]);
        }
        return cnt;
    }
};

// Composite key for GROUP BY (brand, type_idx, size)
struct GroupKey {
    int32_t brand;
    uint32_t type_idx;  // index into p_type vector
    int32_t size;

    bool operator==(const GroupKey& other) const {
        return brand == other.brand && type_idx == other.type_idx && size == other.size;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        size_t h = (size_t)k.brand;
        h ^= (size_t)k.type_idx << 16;
        h ^= (size_t)k.size << 8;
        return h * 0x9E3779B97F4A7C15ULL;
    }
};

struct PartRow {
    int32_t brand;
    uint32_t type_idx;
    int32_t size;
};

void run_q16(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Step 1: Load supplier and build anti-join bitset
    #ifdef GENDB_PROFILE
    auto t_anti_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t s_rows = 0;
    int32_t* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", 100000, s_rows);
    auto s_comment = load_string_column(gendb_dir + "/supplier/s_comment.bin", 100000);

    // Use bitset for O(1) lookup
    SupplierBitset bad_suppliers;
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

    // Valid sizes - use array for O(1) lookup
    bool valid_size_lookup[51] = {};
    int valid_sizes[] = {49, 14, 23, 45, 19, 3, 36, 9};
    for (int sz : valid_sizes) {
        if (sz < 51) valid_size_lookup[sz] = true;
    }

    // Build open-addressing hash table on filtered part
    CompactHashTable<PartRow> part_ht(200000);

    for (size_t i = 0; i < p_rows; i++) {
        // Filter predicates
        if (p_brand_code[i] == brand45_code) continue;
        if (p_type[i].size() >= 15 && p_type[i].substr(0, 15) == "MEDIUM POLISHED") continue;
        if (p_size[i] < 0 || p_size[i] >= 51 || !valid_size_lookup[p_size[i]]) continue;

        part_ht.insert(p_partkey[i], {p_brand_code[i], (uint32_t)i, p_size[i]});
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

    // Parallel probe with thread-local aggregation using bitsets
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;

    std::vector<std::unordered_map<GroupKey, SupplierBitset, GroupKeyHash>> thread_aggs(num_threads);
    std::vector<std::thread> threads;

    size_t chunk_size = (ps_rows + num_threads - 1) / num_threads;

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, ps_rows);

            for (size_t i = start; i < end; i++) {
                // Anti-join filter
                if (bad_suppliers.contains(ps_suppkey[i])) continue;

                // Join with part
                auto* part_row = part_ht.find(ps_partkey[i]);
                if (part_row == nullptr) continue;

                // Add to group - bitset insert is O(1)
                GroupKey gk = {part_row->brand, part_row->type_idx, part_row->size};
                thread_aggs[t][gk].insert(ps_suppkey[i]);
            }
        });
    }

    for (auto& th : threads) th.join();

    // Merge thread-local aggregations using bitset merge
    std::unordered_map<GroupKey, SupplierBitset, GroupKeyHash> agg;
    for (auto& local_agg : thread_aggs) {
        for (auto& [key, bitset] : local_agg) {
            agg[key].merge(bitset);
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
    results.reserve(agg.size());

    for (const auto& [key, bitset] : agg) {
        results.push_back({
            p_brand_dict[key.brand],
            p_type[key.type_idx],
            key.size,
            bitset.count()
        });
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
