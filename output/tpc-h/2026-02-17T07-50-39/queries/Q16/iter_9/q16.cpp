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
 * Q16 Implementation Plan (Iteration 9):
 *
 * Bottleneck from iter 6:
 * - scan_filter_part (142ms, 51%): Still scanning 2M part rows when we have pre-built indexes
 * - join (102ms, 37%): Building hash table on filtered part
 *
 * ARCHITECTURE-LEVEL FIX: Reverse join order + use pre-built indexes
 *
 * Current approach: Scan 2M part → filter → build hash → probe with partsupp
 * NEW approach: Scan partsupp → collect qualifying partkeys → index lookup on part
 *
 * Optimizations:
 * 1. BITMAP ANTI-JOIN: Supplier keys 0-99,999 → 12.5KB bitmap (not unordered_set)
 * 2. REVERSE JOIN: Filter partsupp first (8M rows) → collect unique partkeys
 * 3. INDEX LOOKUP: Load pre-built part_partkey_hash index, probe for each qualifying partkey
 * 4. LATE FILTER: Apply part predicates (brand, type, size) only on looked-up rows
 * 5. DIRECT AGGREGATION: Aggregate on-the-fly without intermediate hash table
 *
 * Physical Plan:
 * 1. Anti-join: 100K-bit bitmap for bad suppliers (12.5KB)
 * 2. Partsupp scan: PARALLEL filter → collect (partkey, suppkey) pairs passing anti-join
 * 3. Part lookup: For each unique partkey → mmap index probe → apply part filters
 * 4. Aggregation: Partitioned hash table with unordered_set for distinct suppkey counts
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

// Pre-built hash index structure (single-value hash: key → position)
struct HashIndexSingle {
    struct Entry {
        int32_t key;
        uint32_t position;
    };

    Entry* table;
    uint32_t num_entries;
    uint32_t table_size;
    uint32_t mask;
    size_t file_size;

    HashIndexSingle(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open index: " << path << std::endl;
            table = nullptr;
            return;
        }

        struct stat sb;
        fstat(fd, &sb);
        file_size = sb.st_size;

        void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);

        if (addr == MAP_FAILED) {
            std::cerr << "mmap failed for index: " << path << std::endl;
            table = nullptr;
            return;
        }

        // Read header: [uint32_t num_entries][uint32_t table_size]
        uint32_t* header = static_cast<uint32_t*>(addr);
        num_entries = header[0];
        table_size = header[1];
        mask = table_size - 1;

        // Table starts after 8-byte header
        table = reinterpret_cast<Entry*>(header + 2);
    }

    ~HashIndexSingle() {
        if (table) {
            munmap(table - 2, file_size);  // Unmap from header start
        }
    }

    // Probe hash index for key, return position or -1 if not found
    int32_t lookup(int32_t key) const {
        if (!table) return -1;

        size_t hash = (size_t)key * 0x9E3779B97F4A7C15ULL;
        size_t idx = hash & mask;

        // Linear probing (empty slots have key=-1 or position=0xFFFFFFFF based on index builder)
        for (uint32_t probe = 0; probe < table_size; probe++) {
            const Entry& e = table[idx];
            // Empty slot check (could be key=-1 or position=0xFFFFFFFF)
            if (e.key == -1 || e.position == 0xFFFFFFFF) return -1;
            if (e.key == key) return e.position;
            idx = (idx + 1) & mask;
        }
        return -1;
    }
};

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

// Helper: Build offset index for length-prefixed string column (for selective loading)
struct StringColumnIndex {
    const char* data;
    size_t file_size;
    std::vector<size_t> offsets;  // offset[i] = byte offset of i-th string
    int fd;

    StringColumnIndex(const std::string& path, size_t expected_rows) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            data = nullptr;
            file_size = 0;
            return;
        }

        struct stat sb;
        fstat(fd, &sb);
        file_size = sb.st_size;
        void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);

        if (addr == MAP_FAILED) {
            std::cerr << "mmap failed: " << path << std::endl;
            close(fd);
            data = nullptr;
            file_size = 0;
            return;
        }

        data = static_cast<const char*>(addr);
        offsets.reserve(expected_rows);

        // Build offset index
        size_t offset = 0;
        while (offset < file_size) {
            offsets.push_back(offset);
            uint32_t len;
            memcpy(&len, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t) + len;
            if (offset > file_size) break;
        }
    }

    ~StringColumnIndex() {
        if (data) {
            munmap((void*)data, file_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    // Get string at specific row index
    std::string get(size_t row_idx) const {
        if (row_idx >= offsets.size()) return "";
        size_t offset = offsets[row_idx];
        uint32_t len;
        memcpy(&len, data + offset, sizeof(uint32_t));
        return std::string(data + offset + sizeof(uint32_t), len);
    }
};

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

// Open-addressing hash table for aggregation with composite key (using unordered_set for distinct suppkeys)
struct AggHashTable {
    struct Entry {
        GroupKey key;
        std::unordered_set<int32_t> suppkeys;  // Changed from vector to unordered_set for distinct counting
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

    std::unordered_set<int32_t>* find_or_insert(const GroupKey& key) {
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

    // Step 1: Load supplier and build anti-join BITMAP (not unordered_set)
    #ifdef GENDB_PROFILE
    auto t_anti_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t s_rows = 0;
    int32_t* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", 100000, s_rows);
    auto s_comment = load_string_column(gendb_dir + "/supplier/s_comment.bin", 100000);

    // Bitmap for 100K suppliers (12.5KB)
    constexpr size_t BITMAP_SIZE = 100000;
    std::vector<bool> bad_supplier_bitmap(BITMAP_SIZE, false);

    for (size_t i = 0; i < s_rows; i++) {
        if (contains_substring(s_comment[i], "Customer") &&
            contains_substring(s_comment[i], "Complaints")) {
            if (s_suppkey[i] >= 0 && s_suppkey[i] < (int32_t)BITMAP_SIZE) {
                bad_supplier_bitmap[s_suppkey[i]] = true;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_anti_end = std::chrono::high_resolution_clock::now();
    double ms_anti = std::chrono::duration<double, std::milli>(t_anti_end - t_anti_start).count();
    printf("[TIMING] anti_join_build: %.2f ms\n", ms_anti);
    #endif

    // Step 2: Load part columns and pre-built index (NO SCAN — index lookup only)
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t p_rows = 0;
    // Note: p_partkey not needed since we use index lookup
    mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", 2000000, p_rows);  // Just to get p_rows
    int32_t* p_brand_code = mmap_column<int32_t>(gendb_dir + "/part/p_brand.bin", 2000000, p_rows);
    int32_t* p_size = mmap_column<int32_t>(gendb_dir + "/part/p_size.bin", 2000000, p_rows);

    auto p_brand_dict = load_dictionary(gendb_dir + "/part/p_brand_dict.txt");

    // Load pre-built hash index for part_partkey
    HashIndexSingle part_index(gendb_dir + "/indexes/part_partkey_hash.bin");

    // Build string column index for selective p_type loading
    StringColumnIndex p_type_index(gendb_dir + "/part/p_type.bin", p_rows);

    // Find 'Brand#45' code
    int32_t brand45_code = -1;
    for (size_t i = 0; i < p_brand_dict.size(); i++) {
        if (p_brand_dict[i] == "Brand#45") {
            brand45_code = i;
            break;
        }
    }

    // Valid sizes (use array for faster lookup)
    bool valid_size_lookup[256] = {false};
    int valid_sizes_arr[] = {49, 14, 23, 45, 19, 3, 36, 9};
    for (int sz : valid_sizes_arr) {
        if (sz < 256) valid_size_lookup[sz] = true;
    }

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_part_index: %.2f ms\n", ms_load);
    #endif

    // Step 3: Scan partsupp with anti-join + index lookup on part + aggregate
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t ps_rows = 0;
    int32_t* ps_partkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", 8000000, ps_rows);
    int32_t* ps_suppkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", 8000000, ps_rows);

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] load_partsupp: %.2f ms\n", ms_scan);
    #endif

    // Step 4: PARALLEL filter + index lookup + aggregation
    #ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
    #endif

    // PARTITIONED AGGREGATION: hash-partition groups across threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;

    unsigned int num_partitions = 1;
    while (num_partitions < num_threads) num_partitions <<= 1;
    unsigned int partition_mask = num_partitions - 1;

    std::vector<AggHashTable> partitioned_aggs;
    partitioned_aggs.reserve(num_partitions);
    for (unsigned int p = 0; p < num_partitions; p++) {
        partitioned_aggs.emplace_back(40000 / num_partitions + 1000);
    }

    std::vector<std::mutex> partition_mutexes(num_partitions);
    std::vector<std::thread> threads;
    size_t chunk_size = (ps_rows + num_threads - 1) / num_threads;

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, ps_rows);

            for (size_t i = start; i < end; i++) {
                int32_t suppkey = ps_suppkey[i];
                int32_t partkey = ps_partkey[i];

                // Anti-join filter (bitmap lookup)
                if (suppkey >= 0 && suppkey < (int32_t)BITMAP_SIZE && bad_supplier_bitmap[suppkey]) {
                    continue;
                }

                // Index lookup on part (O(1) via pre-built hash index)
                int32_t part_pos = part_index.lookup(partkey);
                if (part_pos < 0 || part_pos >= (int32_t)p_rows) continue;

                // Apply part filters
                int32_t brand = p_brand_code[part_pos];
                if (brand == brand45_code) continue;

                int32_t size = p_size[part_pos];
                if (size < 0 || size >= 256 || !valid_size_lookup[size]) continue;

                // Load p_type string selectively
                std::string p_type_str = p_type_index.get(part_pos);
                if (p_type_str.size() >= 15 && p_type_str.substr(0, 15) == "MEDIUM POLISHED") continue;

                // Aggregate: partition by group key
                GroupKey gk = {brand, std::move(p_type_str), size};
                GroupKeyHash hasher;
                unsigned int partition = hasher(gk) & partition_mask;

                {
                    std::lock_guard<std::mutex> lock(partition_mutexes[partition]);
                    auto* suppkeys = partitioned_aggs[partition].find_or_insert(gk);
                    suppkeys->insert(suppkey);
                }
            }
        });
    }

    for (auto& th : threads) th.join();

    #ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", ms_join);
    #endif

    // Step 4: Collect and sort results (from partitioned aggregation)
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
    results.reserve(30000);  // Estimate ~27,840 groups

    // Collect from all partitions (distinct count already computed via unordered_set)
    for (auto& partition : partitioned_aggs) {
        for (auto* entry = partition.begin(); entry != partition.end(); ++entry) {
            if (entry->occupied) {
                results.push_back({
                    p_brand_dict[entry->key.brand],
                    entry->key.type,
                    entry->key.size,
                    static_cast<int32_t>(entry->suppkeys.size())  // Distinct count from set size
                });
            }
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
