#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>

// Memory-mapped file utilities
struct MmapedFile {
    int fd;
    void* ptr;
    size_t size;

    MmapedFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return;
        }
        size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "mmap failed for: " << path << std::endl;
            close(fd);
            fd = -1;
            ptr = nullptr;
            size = 0;
        }
    }

    ~MmapedFile() {
        if (ptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }

    bool is_valid() const { return ptr != nullptr; }
    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }
};

// Helper to load dictionary for dictionary-encoded columns
std::unordered_map<int8_t, char> load_dict(const std::string& path) {
    std::unordered_map<int8_t, char> dict;
    std::ifstream f(path);
    if (!f) {
        std::cerr << "Failed to open dict: " << path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        dict[std::stoi(line.substr(0, eq))] = line[eq + 1];
    }
    return dict;
}

// Helper to extract variable-length string from offset-based storage
std::string extract_varstring(const uint8_t* data, const uint32_t* offsets, uint32_t idx, uint32_t total_rows) {
    uint32_t start = offsets[idx];
    uint32_t end = (idx + 1 < total_rows) ? offsets[idx + 1] : offsets[0]; // Last offset in header
    if (idx == 0) {
        // Need to get the actual end from first offset value
        end = offsets[0]; // First offset value IS the start of strings
        if (total_rows > 1) {
            start = offsets[0];
            end = offsets[1];
        }
    } else {
        start = offsets[idx];
        end = (idx + 1 < total_rows) ? offsets[idx + 1] : offsets[total_rows];
    }
    return std::string((const char*)(data + start), end - start);
}

// Proper extraction of variable-length strings with correct offset handling
std::string extract_varstring_correct(const uint8_t* full_data, uint32_t idx, uint32_t /* total_rows */) {
    // First uint32_t is the offset table size
    const uint32_t* offsets = (const uint32_t*)full_data;
    uint32_t offset_count = offsets[0];  // Total number of offsets

    uint32_t start = offsets[idx + 1];  // +1 because offsets[0] is the count
    uint32_t end;
    if (idx + 1 < offset_count) {
        end = offsets[idx + 2];  // Next offset
    } else {
        // Last string goes to end of file
        end = offset_count; // Use offset_count as end position (byte position)
    }

    // The actual string data starts after the offset table
    const uint8_t* str_data = (const uint8_t*)(offsets + (offset_count + 1));
    return std::string((const char*)(str_data + start), end - start);
}

// Compact open-addressing hash table for 1:N joins
template<typename K>
struct MultiValueHashIndex {
    struct Entry {
        K key;
        uint32_t offset;
        uint32_t count;
        bool occupied;
    };

    std::vector<Entry> table;
    std::vector<uint32_t> values;
    size_t num_entries;
    static constexpr double LOAD_FACTOR = 0.75;

    MultiValueHashIndex() : num_entries(0) {}

    inline uint64_t hash_fn(K key) const {
        // Fibonacci hashing for good distribution
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, uint32_t row_idx) {
        if (table.empty()) {
            table.resize(1024);
            for (auto& e : table) e.occupied = false;
        }

        // Expand if needed
        if (num_entries >= table.size() * LOAD_FACTOR) {
            rehash();
        }

        uint64_t h = hash_fn(key);
        size_t idx = h % table.size();

        while (table[idx].occupied && table[idx].key != key) {
            idx = (idx + 1) % table.size();
        }

        if (!table[idx].occupied) {
            table[idx].key = key;
            table[idx].offset = values.size();
            table[idx].count = 0;
            table[idx].occupied = true;
            num_entries++;
        }

        values.push_back(row_idx);
        table[idx].count++;
    }

    void rehash() {
        std::vector<Entry> old_table = std::move(table);
        std::vector<uint32_t> old_values = std::move(values);

        table.clear();
        table.resize(old_table.size() * 2);
        for (auto& e : table) e.occupied = false;
        values.clear();
        num_entries = 0;

        for (const auto& e : old_table) {
            if (e.occupied) {
                for (uint32_t i = 0; i < e.count; i++) {
                    insert(e.key, old_values[e.offset + i]);
                }
            }
        }
    }

    struct ValueRange {
        const uint32_t* data;
        uint32_t count;

        auto begin() const { return data; }
        auto end() const { return data + count; }
    };

    const ValueRange* find(K key) const {
        if (table.empty()) return nullptr;

        uint64_t h = hash_fn(key);
        size_t idx = h % table.size();

        while (table[idx].occupied && table[idx].key != key) {
            idx = (idx + 1) % table.size();
        }

        if (!table[idx].occupied) return nullptr;

        static thread_local ValueRange range;
        range.data = &values[table[idx].offset];
        range.count = table[idx].count;
        return &range;
    }

    size_t size() const { return num_entries; }

    void reserve(size_t cap) {
        size_t table_size = static_cast<size_t>(cap / LOAD_FACTOR);
        // Round up to nearest power of 2
        size_t p = 1;
        while (p < table_size) p *= 2;
        table.resize(p);
        for (auto& e : table) e.occupied = false;
        values.reserve(cap);
    }
};

// Compact open-addressing hash table for 1:1 joins (single value per key)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t num_entries = 0;

    CompactHashTable() = default;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    inline uint64_t hash_fn(K key) const {
        // Fibonacci hashing
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        if (table.empty()) {
            table.resize(256);
            mask = 255;
        }

        size_t idx = hash_fn(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
        num_entries++;
    }

    V* find(K key) {
        if (table.empty()) return nullptr;
        size_t idx = hash_fn(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    size_t size() const { return num_entries; }

    void reserve(size_t cap) {
        // Pre-size to next power of 2
        size_t sz = 1;
        while (sz < cap * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }
};

void run_Q21(const std::string& gendb_dir, const std::string& results_dir) {
    using std::chrono::high_resolution_clock;

#ifdef GENDB_PROFILE
    auto total_start = high_resolution_clock::now();
    auto t_start = high_resolution_clock::now();
    (void)total_start;  // Suppress unused warning in non-profile paths
    (void)t_start;
#endif

    // === Load Data ===

    // Load supplier columns
    MmapedFile supplier_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
    MmapedFile supplier_nationkey(gendb_dir + "/supplier/s_nationkey.bin");
    MmapedFile supplier_name_raw(gendb_dir + "/supplier/s_name.bin");

    // Load lineitem columns
    MmapedFile lineitem_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
    MmapedFile lineitem_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapedFile lineitem_receiptdate(gendb_dir + "/lineitem/l_receiptdate.bin");
    MmapedFile lineitem_commitdate(gendb_dir + "/lineitem/l_commitdate.bin");

    // Load orders columns
    MmapedFile orders_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MmapedFile orders_orderstatus(gendb_dir + "/orders/o_orderstatus.bin");

    // Load nation columns
    MmapedFile nation_nationkey(gendb_dir + "/nation/n_nationkey.bin");
    MmapedFile nation_name_raw(gendb_dir + "/nation/n_name.bin");

    // Verify all files loaded
    if (!supplier_suppkey.is_valid() || !supplier_nationkey.is_valid() || !supplier_name_raw.is_valid() ||
        !lineitem_suppkey.is_valid() || !lineitem_orderkey.is_valid() || !lineitem_receiptdate.is_valid() || !lineitem_commitdate.is_valid() ||
        !orders_orderkey.is_valid() || !orders_orderstatus.is_valid() ||
        !nation_nationkey.is_valid() || !nation_name_raw.is_valid()) {
        std::cerr << "Failed to load required data files" << std::endl;
        return;
    }

    // Load dictionaries
    auto orders_status_dict = load_dict(gendb_dir + "/orders/o_orderstatus_dict.txt");

    // Get row counts
    uint32_t supplier_rows = supplier_suppkey.size / sizeof(int32_t);
    uint32_t lineitem_rows = lineitem_suppkey.size / sizeof(int32_t);
    uint32_t orders_rows = orders_orderkey.size / sizeof(int32_t);
    uint32_t nation_rows = nation_nationkey.size / sizeof(int32_t);

    // Cast pointers
    const int32_t* s_suppkey = supplier_suppkey.as<int32_t>();
    const int32_t* s_nationkey = supplier_nationkey.as<int32_t>();

    const int32_t* l_suppkey = lineitem_suppkey.as<int32_t>();
    const int32_t* l_orderkey = lineitem_orderkey.as<int32_t>();
    const int32_t* l_receiptdate = lineitem_receiptdate.as<int32_t>();
    const int32_t* l_commitdate = lineitem_commitdate.as<int32_t>();

    const int32_t* o_orderkey = orders_orderkey.as<int32_t>();
    const int8_t* o_orderstatus = orders_orderstatus.as<int8_t>();

    const int32_t* n_nationkey = nation_nationkey.as<int32_t>();

    // Helper to extract supplier name
    auto get_supplier_name = [&](uint32_t idx) -> std::string {
        const uint8_t* data = supplier_name_raw.as<uint8_t>();
        const uint32_t* offsets = (const uint32_t*)data;
        uint32_t offset_count = offsets[0];  // First uint32_t is total offset count

        // String data starts after the offset array
        const uint8_t* str_data = data + (offset_count + 1) * sizeof(uint32_t);

        uint32_t start = offsets[idx + 1];  // offsets[idx + 1] is start of idx-th string
        uint32_t end;
        if (idx + 1 < offset_count) {
            end = offsets[idx + 2];  // offsets[idx + 2] is start of (idx+1)-th string
        } else {
            // Last string: use file size as end
            end = supplier_name_raw.size - (offset_count + 1) * sizeof(uint32_t);
        }

        return std::string((const char*)(str_data + start), end - start);
    };

    // Helper to extract nation name
    auto get_nation_name = [&](uint32_t idx) -> std::string {
        const uint8_t* data = nation_name_raw.as<uint8_t>();
        const uint32_t* offsets = (const uint32_t*)data;
        uint32_t offset_count = offsets[0];

        // String data starts after the offset array
        const uint8_t* str_data = data + (offset_count + 1) * sizeof(uint32_t);

        uint32_t start = offsets[idx + 1];  // offsets[idx + 1] is start of idx-th string
        uint32_t end;
        if (idx + 1 < offset_count) {
            end = offsets[idx + 2];  // offsets[idx + 2] is start of (idx+1)-th string
        } else {
            // Last string: use file size as end
            end = nation_name_raw.size - (offset_count + 1) * sizeof(uint32_t);
        }

        return std::string((const char*)(str_data + start), end - start);
    };

    // Find SAUDI ARABIA nation key
    int32_t saudi_arabia_key = -1;
    for (uint32_t i = 0; i < nation_rows; i++) {
        if (get_nation_name(i) == "SAUDI ARABIA") {
            saudi_arabia_key = n_nationkey[i];
            break;
        }
    }

    if (saudi_arabia_key == -1) {
        std::cerr << "SAUDI ARABIA not found in nation table" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_load = high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load - t_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    // === Build Indexes ===
#ifdef GENDB_PROFILE
    t_start = high_resolution_clock::now();
#endif

    // Pre-cache supplier names indexed by s_suppkey
    // This avoids expensive string extraction in the hot loop
    std::unordered_map<int32_t, std::string> suppkey_to_name;
    for (uint32_t i = 0; i < supplier_rows; i++) {
        suppkey_to_name[s_suppkey[i]] = get_supplier_name(i);
    }

    // Build hash index: orderkey -> list of lineitem indices
    MultiValueHashIndex<int32_t> l_orderkey_idx;
    l_orderkey_idx.reserve(lineitem_rows);  // ~60M lineitem rows
    for (uint32_t i = 0; i < lineitem_rows; i++) {
        l_orderkey_idx.insert(l_orderkey[i], i);
    }

    // Build hash index: suppkey -> list of lineitem indices
    MultiValueHashIndex<int32_t> l_suppkey_idx;
    l_suppkey_idx.reserve(lineitem_rows);  // ~60M lineitem rows
    for (uint32_t i = 0; i < lineitem_rows; i++) {
        l_suppkey_idx.insert(l_suppkey[i], i);
    }

    // Build hash index: suppkey -> supplier index (use compact hash table)
    CompactHashTable<int32_t, uint32_t> supplier_idx(100000);  // ~100K suppliers
    for (uint32_t i = 0; i < supplier_rows; i++) {
        supplier_idx.insert(s_suppkey[i], i);
    }

    // Build hash index: orderkey -> order index (use compact hash table)
    CompactHashTable<int32_t, uint32_t> order_idx(15000000);  // ~15M unique orders
    for (uint32_t i = 0; i < orders_rows; i++) {
        order_idx.insert(o_orderkey[i], i);
    }

#ifdef GENDB_PROFILE
    t_start = high_resolution_clock::now();
    t_load = high_resolution_clock::now();
    ms_load = std::chrono::duration<double, std::milli>(t_load - t_start).count();
    printf("[TIMING] build_indexes: %.2f ms\n", ms_load);
    t_start = high_resolution_clock::now();
#endif

    // === Execute Query ===
    // For each lineitem l1:
    // 1. Check l_receiptdate > l_commitdate
    // 2. Join to orders (o_orderstatus = 'F')
    // 3. Join to supplier
    // 4. Check supplier.s_nationkey = SAUDI_ARABIA
    // 5. Check EXISTS (l2 with different suppkey)
    // 6. Check NOT EXISTS (l3 with different suppkey and l_receiptdate > l_commitdate)

    // Find the code for 'F' in orders status
    int8_t status_F = -1;
    for (const auto& [code, val] : orders_status_dict) {
        if (val == 'F') {
            status_F = code;
            break;
        }
    }

    if (status_F == -1) {
        std::cerr << "Status 'F' not found in dictionary" << std::endl;
        return;
    }

    // Pre-filter: build list of valid lineitem indices (saudi arabia suppliers + receipt > commit)
    std::vector<uint32_t> valid_lineitems;
    valid_lineitems.reserve(lineitem_rows / 100);  // Heuristic: ~1% valid

    // Single-threaded pre-filter pass with optimized predicate order
    for (uint32_t l1_idx = 0; l1_idx < lineitem_rows; l1_idx++) {
        int32_t l1_suppkey = l_suppkey[l1_idx];
        int32_t l1_receiptdate = l_receiptdate[l1_idx];
        int32_t l1_commitdate = l_commitdate[l1_idx];

        // Filter 1: l_receiptdate > l_commitdate (cheap, ~5% selectivity)
        if (l1_receiptdate <= l1_commitdate) continue;

        // Filter 2: Check supplier nation (more expensive, but now on filtered set)
        uint32_t* supp_idx_val = supplier_idx.find(l1_suppkey);
        if (!supp_idx_val) continue;

        if (s_nationkey[*supp_idx_val] != saudi_arabia_key) continue;

        valid_lineitems.push_back(l1_idx);
    }

    std::unordered_map<std::string, uint64_t> group_counts;  // supplier name -> count
    uint64_t result_rows = 0;

    // Parallel execution over pre-filtered valid lineitem indices
    const int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<std::string, uint64_t>> thread_group_counts(num_threads);

    #pragma omp parallel for schedule(dynamic, 1000) reduction(+:result_rows)
    for (size_t i = 0; i < valid_lineitems.size(); i++) {
        uint32_t l1_idx = valid_lineitems[i];
        int32_t l1_suppkey = l_suppkey[l1_idx];
        int32_t l1_orderkey = l_orderkey[l1_idx];

        // Filter 3: Join to order and check o_orderstatus = 'F'
        uint32_t* order_idx_val_ptr = order_idx.find(l1_orderkey);
        if (!order_idx_val_ptr) continue;
        uint32_t order_idx_val = *order_idx_val_ptr;

        if (o_orderstatus[order_idx_val] != status_F) continue;

        // Filter 4: EXISTS (l2 with l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
        const auto* l2_candidates = l_orderkey_idx.find(l1_orderkey);
        if (!l2_candidates) continue;

        bool exists_l2 = false;
        for (uint32_t l2_idx : *l2_candidates) {
            if (l_suppkey[l2_idx] != l1_suppkey) {
                exists_l2 = true;
                break;
            }
        }
        if (!exists_l2) continue;

        // Filter 5: NOT EXISTS (l3 with l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
        const auto* l3_candidates = l_orderkey_idx.find(l1_orderkey);
        bool not_exists_l3 = true;
        if (l3_candidates) {
            for (uint32_t l3_idx : *l3_candidates) {
                if (l_suppkey[l3_idx] != l1_suppkey && l_receiptdate[l3_idx] > l_commitdate[l3_idx]) {
                    not_exists_l3 = false;
                    break;
                }
            }
        }
        if (!not_exists_l3) continue;

        // All filters passed - lookup cached supplier name
        const auto& supp_name_it = suppkey_to_name.find(l1_suppkey);
        if (supp_name_it != suppkey_to_name.end()) {
            thread_group_counts[omp_get_thread_num()][supp_name_it->second]++;
            result_rows++;
        }
    }

    // Merge thread-local aggregations into global map
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [name, count] : thread_group_counts[t]) {
            group_counts[name] += count;
        }
    }

#ifdef GENDB_PROFILE
    auto t_exec = high_resolution_clock::now();
    double ms_exec = std::chrono::duration<double, std::milli>(t_exec - t_start).count();
    printf("[TIMING] execute: %.2f ms\n", ms_exec);
    t_start = high_resolution_clock::now();
#endif

    // === Sort Results ===
    std::vector<std::pair<std::string, uint64_t>> sorted_results;
    for (const auto& [name, count] : group_counts) {
        sorted_results.push_back({name, count});
    }

    // Sort by count DESC, then by name ASC
    std::sort(sorted_results.begin(), sorted_results.end(),
        [](const auto& a, const auto& b) {
            if (a.second != b.second) return a.second > b.second;
            return a.first < b.first;
        });

    // Take top 100
    if (sorted_results.size() > 100) {
        sorted_results.resize(100);
    }

#ifdef GENDB_PROFILE
    auto t_sort = high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
    t_start = high_resolution_clock::now();
#endif

    // === Write Results ===
    std::ofstream out(results_dir + "/Q21.csv");
    if (!out) {
        std::cerr << "Failed to open output file" << std::endl;
        return;
    }

    // Write header
    out << "s_name,numwait\n";

    // Write data
    for (const auto& [name, count] : sorted_results) {
        out << name << "," << count << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output = high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);

    auto total_end = high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
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
    run_Q21(gendb_dir, results_dir);
    return 0;
}
#endif
