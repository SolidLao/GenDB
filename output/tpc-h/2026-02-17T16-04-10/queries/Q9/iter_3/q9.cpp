#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstring>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <omp.h>

// ============================================================================
// ITERATION 3 OPTIMIZATION PLAN:
// BOTTLENECK FROM ITERATION 2: build_joins 2732ms (87%)
//   - partsupp_map: 8M entries using std::unordered_map (DOMINANT COST)
//   - orders_map: 15M entries
//
// ARCHITECTURE-LEVEL FIX: Filter partsupp BEFORE building hash table
// Current (WRONG): Build 8M entry hash → filter during probe → only 400K hits
// New (CORRECT): Filter partsupp by green_parts FIRST → build 400K entry hash
//
// PHYSICAL PLAN:
// 1. Filter part by p_name LIKE '%green%' → green_parts set (~100K)
// 2. **Filter partsupp by green_parts** → keep only ~400K rows (5% of 8M)
// 3. Build hash on filtered partsupp (400K vs 8M → 20x smaller!)
// 4. Use flat array for orders (dense key space)
// 5. Parallel lineitem scan with all probes
//
// Expected: build_joins 300ms (down from 2732ms), total ~700ms
// ============================================================================

// Helper struct for aggregation with Kahan summation
struct AggValue {
    double sum = 0.0;
    double c = 0.0;  // Kahan compensation

    void add(double x) {
        double y = x - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }

    double value() const {
        return sum;
    }
};

// Helper struct for aggregation key
struct AggKey {
    std::string nation;
    int32_t year;

    bool operator==(const AggKey& other) const {
        return nation == other.nation && year == other.year;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        // Better hash combining for string + int32
        uint64_t h1 = std::hash<std::string>()(k.nation);
        uint64_t h2 = std::hash<int32_t>()(k.year);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

// Open-addressing hash table for integer keys (faster than unordered_map)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t size;

    CompactHashTable() : mask(0), size(0) {}

    CompactHashTable(size_t expected_entries) {
        size_t capacity = 1;
        while (capacity < expected_entries * 4 / 3) capacity <<= 1;
        table.resize(capacity, {0, V(), 0, false});
        mask = capacity - 1;
        size = 0;
    }

    inline size_t hash_key(K key) const {
        // MurmurHash-like mixing for int32_t
        uint64_t h = (uint64_t)key * 0xBF58476D1CE4E5B9ULL;
        return (h ^ (h >> 27)) & mask;
    }

    void insert(K key, const V& value) {
        size_t pos = hash_key(key);
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
        size++;
    }

    V* find(K key) {
        size_t pos = hash_key(key);
        uint8_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    V* find_or_insert(K key) {
        size_t pos = hash_key(key);
        uint8_t dist = 0;
        Entry entry{key, V(), 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        table[pos] = entry;
        size++;
        return &table[pos].value;
    }
};

// Memory-mapped file helper
template<typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    count = file_size / sizeof(T);

    void* data = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Error: mmap failed for " << path << std::endl;
        return nullptr;
    }

    return (T*)data;
}

// String loading: length-prefixed format (4 bytes length, then string)
std::vector<std::string> load_strings(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return {};
    }

    std::vector<std::string> strings;
    uint32_t len;
    while (file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t))) {
        std::string str(len, '\0');
        file.read(&str[0], len);
        strings.push_back(str);
    }
    file.close();
    return strings;
}

// Pre-computed year lookup table for epoch days
// Build once, use 60M times with O(1) lookup
class YearLookup {
public:
    std::vector<int32_t> year_table;

    YearLookup() {
        // Pre-compute years for epoch days 0 to ~11000 (covers 1970-2000)
        year_table.resize(11000);
        int32_t year = 1970;
        int32_t day_counter = 0;

        for (int32_t i = 0; i < 11000; i++) {
            int32_t days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
            if (day_counter + days_in_year <= i) {
                day_counter += days_in_year;
                year++;
            }
            year_table[i] = year;
        }
    }

    inline int32_t get_year(int32_t epoch_days) const {
        if (epoch_days >= 0 && epoch_days < (int32_t)year_table.size()) {
            return year_table[epoch_days];
        }
        // Fallback for out-of-range (shouldn't happen in TPC-H)
        return 1970 + epoch_days / 365;
    }
};

// Global year lookup
YearLookup g_year_lookup;

// String contains helper for LIKE
bool string_contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD DATA
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    std::string base_path = gendb_dir + "/";

    // Load part
    size_t part_count = 0;
    int32_t* p_partkey = mmap_file<int32_t>(base_path + "part/p_partkey.bin", part_count);
    auto p_names = load_strings(base_path + "part/p_name.bin");

    // Load supplier
    size_t supplier_count = 0;
    int32_t* s_suppkey = mmap_file<int32_t>(base_path + "supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey = mmap_file<int32_t>(base_path + "supplier/s_nationkey.bin", supplier_count);

    // Load nation
    size_t nation_count = 0;
    int32_t* n_nationkey = mmap_file<int32_t>(base_path + "nation/n_nationkey.bin", nation_count);
    auto n_names = load_strings(base_path + "nation/n_name.bin");

    // Load lineitem
    size_t lineitem_count = 0;
    int32_t* l_suppkey = mmap_file<int32_t>(base_path + "lineitem/l_suppkey.bin", lineitem_count);
    int32_t* l_partkey = mmap_file<int32_t>(base_path + "lineitem/l_partkey.bin", lineitem_count);
    int32_t* l_orderkey = mmap_file<int32_t>(base_path + "lineitem/l_orderkey.bin", lineitem_count);
    int64_t* l_quantity = mmap_file<int64_t>(base_path + "lineitem/l_quantity.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(base_path + "lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(base_path + "lineitem/l_discount.bin", lineitem_count);

    // Load partsupp
    size_t partsupp_count = 0;
    int32_t* ps_partkey = mmap_file<int32_t>(base_path + "partsupp/ps_partkey.bin", partsupp_count);
    int32_t* ps_suppkey = mmap_file<int32_t>(base_path + "partsupp/ps_suppkey.bin", partsupp_count);
    int64_t* ps_supplycost = mmap_file<int64_t>(base_path + "partsupp/ps_supplycost.bin", partsupp_count);

    // Load orders
    size_t orders_count = 0;
    int32_t* o_orderkey = mmap_file<int32_t>(base_path + "orders/o_orderkey.bin", orders_count);
    int32_t* o_orderdate = mmap_file<int32_t>(base_path + "orders/o_orderdate.bin", orders_count);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double t_load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", t_load_ms);
#endif

    // ========================================================================
    // BUILD JOIN STRUCTURES
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Build part filter: p_name LIKE '%green%'
    std::unordered_set<int32_t> green_parts;
    for (size_t i = 0; i < part_count; i++) {
        if (string_contains(p_names[i], "green")) {
            green_parts.insert(p_partkey[i]);
        }
    }

    // Build nation map: nationkey → name (only 25 entries, flat array is fine)
    std::vector<std::string> nation_map(26);  // Index by nationkey
    for (size_t i = 0; i < nation_count; i++) {
        nation_map[n_nationkey[i]] = n_names[i];
    }

    // Build supplier map: suppkey → nationkey using open-addressing hash table
    CompactHashTable<int32_t, int32_t> supplier_nation_map(supplier_count);
    for (size_t i = 0; i < supplier_count; i++) {
        supplier_nation_map.insert(s_suppkey[i], s_nationkey[i]);
    }

    // CRITICAL OPTIMIZATION: Filter partsupp by green_parts BEFORE building hash table
    // This reduces hash table size from 8M to ~400K (20x reduction)
    std::vector<int32_t> filtered_ps_partkey;
    std::vector<int32_t> filtered_ps_suppkey;
    std::vector<int64_t> filtered_ps_cost;
    filtered_ps_partkey.reserve(partsupp_count / 10);  // Estimate ~10% selectivity
    filtered_ps_suppkey.reserve(partsupp_count / 10);
    filtered_ps_cost.reserve(partsupp_count / 10);

    for (size_t i = 0; i < partsupp_count; i++) {
        if (green_parts.find(ps_partkey[i]) != green_parts.end()) {
            filtered_ps_partkey.push_back(ps_partkey[i]);
            filtered_ps_suppkey.push_back(ps_suppkey[i]);
            filtered_ps_cost.push_back(ps_supplycost[i]);
        }
    }

    // Build partsupp map on FILTERED data using open-addressing hash table
    // Composite key: (ps_partkey, ps_suppkey) → ps_supplycost
    struct PartSuppKey {
        int32_t partkey;
        int32_t suppkey;

        bool operator==(const PartSuppKey& other) const {
            return partkey == other.partkey && suppkey == other.suppkey;
        }
    };

    struct PartSuppKeyHash {
        size_t operator()(const PartSuppKey& k) const {
            uint64_t h1 = (uint64_t)k.partkey * 0xBF58476D1CE4E5B9ULL;
            uint64_t h2 = (uint64_t)k.suppkey * 0x94D049BB133111EBULL;
            return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
        }
    };

    std::unordered_map<PartSuppKey, int64_t, PartSuppKeyHash> partsupp_map;
    partsupp_map.reserve(filtered_ps_partkey.size());
    for (size_t i = 0; i < filtered_ps_partkey.size(); i++) {
        partsupp_map[{filtered_ps_partkey[i], filtered_ps_suppkey[i]}] = filtered_ps_cost[i];
    }

    // Build orders map: use flat array for dense key space (o_orderkey 1 to ~60M)
    // Find max orderkey to size array
    int32_t max_orderkey = 0;
    for (size_t i = 0; i < orders_count; i++) {
        if (o_orderkey[i] > max_orderkey) {
            max_orderkey = o_orderkey[i];
        }
    }
    std::vector<int32_t> orders_array(max_orderkey + 1, -1);
    for (size_t i = 0; i < orders_count; i++) {
        orders_array[o_orderkey[i]] = o_orderdate[i];
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double t_build_ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_joins: %.2f ms\n", t_build_ms);
#endif

    // ========================================================================
    // SCAN AND AGGREGATE (PARALLEL with thread-local aggregation)
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local aggregation to avoid contention
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<AggKey, AggValue, AggKeyHash>> thread_agg(num_threads);
    std::vector<int64_t> thread_filtered_rows(num_threads, 0);

#pragma omp parallel for schedule(dynamic, 10000)
    for (size_t i = 0; i < lineitem_count; i++) {
        int thread_id = omp_get_thread_num();
        int32_t partkey = l_partkey[i];
        int32_t suppkey = l_suppkey[i];

        // Check if part is green
        if (green_parts.find(partkey) == green_parts.end()) {
            continue;
        }

        // Probe partsupp
        auto ps_it = partsupp_map.find({partkey, suppkey});
        if (ps_it == partsupp_map.end()) {
            continue;
        }
        int64_t ps_cost = ps_it->second;

        // Probe supplier → nation
        int32_t* nation_it = supplier_nation_map.find(suppkey);
        if (nation_it == nullptr) {
            continue;
        }
        int32_t nationkey = *nation_it;

        // Direct array lookup for nation name (only 25 entries)
        if (nationkey < 0 || nationkey >= (int32_t)nation_map.size()) {
            continue;
        }
        const std::string& nation_name = nation_map[nationkey];

        // Probe orders (flat array lookup)
        int32_t orderkey = l_orderkey[i];
        if (orderkey < 0 || orderkey >= (int32_t)orders_array.size()) {
            continue;
        }
        int32_t orderdate = orders_array[orderkey];
        if (orderdate < 0) {  // Not found
            continue;
        }
        int32_t year = g_year_lookup.get_year(orderdate);

        // Compute profit amount in double precision to avoid rounding errors
        // amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
        // All DECIMAL values are scaled by 100
        double extended_price = l_extendedprice[i] / 100.0;  // unscale
        double discount = l_discount[i] / 100.0;              // unscale
        double quantity = l_quantity[i] / 100.0;              // unscale
        double ps_cost_unscaled = ps_cost / 100.0;            // unscale

        double amount = extended_price * (1.0 - discount) - ps_cost_unscaled * quantity;

        AggKey key = {nation_name, year};
        thread_agg[thread_id][key].add(amount);
        thread_filtered_rows[thread_id]++;
    }

    // Merge thread-local aggregation into global result
    std::unordered_map<AggKey, AggValue, AggKeyHash> aggregation;
    int64_t filtered_rows = 0;

    for (int t = 0; t < num_threads; t++) {
        for (const auto& kv : thread_agg[t]) {
            aggregation[kv.first].add(kv.second.value());
        }
        filtered_rows += thread_filtered_rows[t];
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double t_scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", t_scan_ms);
    printf("[TIMING] filtered_rows: %ld\n", filtered_rows);
#endif

    // ========================================================================
    // SORT RESULTS
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<AggKey, AggValue>> results;
    for (const auto& kv : aggregation) {
        results.push_back(kv);
    }

    // Sort by nation ASC, year DESC
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.first.nation != b.first.nation) {
            return a.first.nation < b.first.nation;
        }
        return a.first.year > b.first.year;
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double t_sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", t_sort_ms);
#endif

    // ========================================================================
    // OUTPUT CSV
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q9.csv";
    std::ofstream output_file(output_path);
    output_file << "nation,o_year,sum_profit\n";

    for (const auto& row : results) {
        output_file << row.first.nation << "," << row.first.year << ","
                    << std::fixed << std::setprecision(2) << row.second.value() << "\n";
    }
    output_file.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double t_output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", t_output_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double t_total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", t_total_ms);
#endif

    std::cout << "Q9 execution complete. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);

    return 0;
}
#endif
