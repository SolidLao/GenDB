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

// ============================================================================
// PLAN:
// 1. Load part, partsupp, supplier, nation, lineitem, orders via mmap
// 2. Filter part by p_name LIKE '%green%' → build part_dict: partkey → row_idx
// 3. Build join structures:
//    - partsupp_map: (ps_partkey, ps_suppkey) → ps_supplycost
//    - supplier_nation_map: s_suppkey → (s_nationkey, nation_name)
// 4. Single-pass lineitem scan:
//    - For each lineitem row:
//      - Probe partsupp_map with (l_partkey, l_suppkey) to get ps_supplycost
//      - If found, probe supplier_nation_map with l_suppkey to get nation_name
//      - Probe orders with o_orderkey to get o_orderdate → extract year
//      - Compute profit: l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
//      - Aggregate to hashmap: (nation, year) → sum_profit
// 5. Sort results by (nation, year DESC) and output CSV
// ============================================================================

// Helper struct for nation name lookup
struct NationInfo {
    int32_t nationkey;
    std::string nation_name;
};

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

// Helper struct for aggregation
struct AggKey {
    std::string nation;
    int32_t year;

    bool operator==(const AggKey& other) const {
        return nation == other.nation && year == other.year;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        return std::hash<std::string>()(k.nation) ^ (std::hash<int32_t>()(k.year) << 1);
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

// Extract year from epoch days (days since 1970-01-01)
int32_t epoch_days_to_year(int32_t epoch_days) {
    // Days since 1970-01-01
    int32_t year = 1970;
    int32_t days_left = epoch_days;

    while (true) {
        int32_t days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days_left < days_in_year) break;
        days_left -= days_in_year;
        year++;
    }

    return year;
}

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

    // Build nation map: nationkey → name
    std::unordered_map<int32_t, std::string> nation_map;
    for (size_t i = 0; i < nation_count; i++) {
        nation_map[n_nationkey[i]] = n_names[i];
    }

    // Build supplier map: suppkey → nationkey
    std::unordered_map<int32_t, int32_t> supplier_nation_map;
    for (size_t i = 0; i < supplier_count; i++) {
        supplier_nation_map[s_suppkey[i]] = s_nationkey[i];
    }

    // Build partsupp map: (ps_partkey, ps_suppkey) → ps_supplycost
    struct PartSuppKey {
        int32_t partkey;
        int32_t suppkey;

        bool operator==(const PartSuppKey& other) const {
            return partkey == other.partkey && suppkey == other.suppkey;
        }
    };

    struct PartSuppKeyHash {
        size_t operator()(const PartSuppKey& k) const {
            return std::hash<int32_t>()(k.partkey) ^ (std::hash<int32_t>()(k.suppkey) << 1);
        }
    };

    std::unordered_map<PartSuppKey, int64_t, PartSuppKeyHash> partsupp_map;
    for (size_t i = 0; i < partsupp_count; i++) {
        partsupp_map[{ps_partkey[i], ps_suppkey[i]}] = ps_supplycost[i];
    }

    // Build orders map: orderkey → orderdate
    std::unordered_map<int32_t, int32_t> orders_map;
    for (size_t i = 0; i < orders_count; i++) {
        orders_map[o_orderkey[i]] = o_orderdate[i];
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double t_build_ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_joins: %.2f ms\n", t_build_ms);
#endif

    // ========================================================================
    // SCAN AND AGGREGATE
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<AggKey, AggValue, AggKeyHash> aggregation;
    int64_t filtered_rows = 0;

    for (size_t i = 0; i < lineitem_count; i++) {
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
        auto nation_it = supplier_nation_map.find(suppkey);
        if (nation_it == supplier_nation_map.end()) {
            continue;
        }
        int32_t nationkey = nation_it->second;

        auto nation_name_it = nation_map.find(nationkey);
        if (nation_name_it == nation_map.end()) {
            continue;
        }
        std::string nation_name = nation_name_it->second;

        // Probe orders
        int32_t orderkey = l_orderkey[i];
        auto order_it = orders_map.find(orderkey);
        if (order_it == orders_map.end()) {
            continue;
        }
        int32_t orderdate = order_it->second;
        int32_t year = epoch_days_to_year(orderdate);

        // Compute profit amount in double precision to avoid rounding errors
        // amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
        // All DECIMAL values are scaled by 100
        double extended_price = l_extendedprice[i] / 100.0;  // unscale
        double discount = l_discount[i] / 100.0;              // unscale
        double quantity = l_quantity[i] / 100.0;              // unscale
        double ps_cost_unscaled = ps_cost / 100.0;            // unscale

        double amount = extended_price * (1.0 - discount) - ps_cost_unscaled * quantity;

        AggKey key = {nation_name, year};
        aggregation[key].add(amount);
        filtered_rows++;
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
