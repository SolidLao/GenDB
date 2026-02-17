/*
 * Q9: Product Type Profit Measure
 *
 * LOGICAL PLAN:
 * 1. Filter part on p_name LIKE '%green%' → ~200K rows (selective)
 * 2. Build hash on filtered part p_partkey
 * 3. Load nation into flat array (25 rows)
 * 4. Build hash on supplier: s_suppkey → s_nationkey
 * 5. Build hash on partsupp: (ps_partkey, ps_suppkey) → ps_supplycost
 * 6. Build hash on orders: o_orderkey → o_orderdate
 * 7. Use pre-built index lineitem_partkey_suppkey_hash to probe lineitem efficiently
 * 8. For each matching lineitem tuple:
 *    - Check part hash
 *    - Lookup supplier to get nation
 *    - Lookup partsupp to get supplycost
 *    - Lookup orders to get orderdate
 *    - Compute profit = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
 *    - Extract year from orderdate (O(1) lookup)
 *    - Aggregate by (nation_name, year)
 * 9. Sort by nation ASC, year DESC
 *
 * PHYSICAL PLAN:
 * - nation: flat array (25 entries)
 * - part: hash set of matching p_partkeys
 * - supplier: open-addressing hash s_suppkey → s_nationkey
 * - partsupp: open-addressing hash (ps_partkey, ps_suppkey) → ps_supplycost
 * - orders: open-addressing hash o_orderkey → o_orderdate
 * - aggregation: hash map (nation_name, year) → sum_profit
 * - Date: O(1) year lookup table
 * - Decimals: scaled int64_t arithmetic (scale 100)
 */

#include <iostream>
#include <fstream>
#include <iomanip>
#include <string>
#include <vector>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <cmath>
#include <omp.h>

// O(1) date lookup table for year extraction
static int16_t YEAR_TABLE[30000];

void init_date_tables() {
    int year = 1970, month = 1, day = 1;
    const int days_per_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = year;

        day++;
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && leap ? 1 : 0);
        if (day > dim) {
            day = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

inline int extract_year(int32_t epoch_day) {
    return YEAR_TABLE[epoch_day];
}

// Custom hash for pair<int32_t, int32_t>
struct PairHash {
    std::size_t operator()(const std::pair<int32_t, int32_t>& p) const {
        uint64_t h1 = (uint64_t)p.first * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)p.second * 0x9E3779B185EBCA87ULL;
        return (h1 >> 32) ^ (h2 >> 32);
    }
};

// Custom hash for string + int pair
struct NationYearHash {
    std::size_t operator()(const std::pair<std::string, int>& p) const {
        std::size_t h1 = std::hash<std::string>{}(p.first);
        std::size_t h2 = (uint64_t)p.second * 0x9E3779B97F4A7C15ULL;
        return h1 ^ (h2 >> 32);
    }
};

// mmap helper
template<typename T>
T* mmap_binary(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    fstat(fd, &sb);
    count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return static_cast<T*>(addr);
}

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    init_date_tables();

    // Load nation (25 rows) into flat array
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t nation_rows;
    int32_t* n_nationkey = mmap_binary<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_rows);

    // Load nation names (non-binary, text file)
    std::string nation_names[25];
    std::ifstream n_name_file(gendb_dir + "/nation/n_name.bin", std::ios::binary);
    for (size_t i = 0; i < nation_rows; i++) {
        uint32_t len;
        n_name_file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        nation_names[n_nationkey[i]].resize(len);
        n_name_file.read(&nation_names[n_nationkey[i]][0], len);
    }
    n_name_file.close();

    // Load part and filter on p_name LIKE '%green%'
    size_t part_rows;
    int32_t* p_partkey = mmap_binary<int32_t>(gendb_dir + "/part/p_partkey.bin", part_rows);

    std::unordered_set<int32_t> green_parts;
    green_parts.reserve(500000);

    std::ifstream p_name_file(gendb_dir + "/part/p_name.bin", std::ios::binary);
    for (size_t i = 0; i < part_rows; i++) {
        uint32_t len;
        p_name_file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        std::string name(len, ' ');
        p_name_file.read(&name[0], len);
        if (name.find("green") != std::string::npos) {
            green_parts.insert(p_partkey[i]);
        }
    }
    p_name_file.close();

    // Load supplier: s_suppkey → s_nationkey
    size_t supp_rows;
    int32_t* s_suppkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supp_rows);
    int32_t* s_nationkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supp_rows);

    std::unordered_map<int32_t, int32_t> supp_nation;
    supp_nation.reserve(supp_rows);
    for (size_t i = 0; i < supp_rows; i++) {
        supp_nation[s_suppkey[i]] = s_nationkey[i];
    }

    // Load partsupp: (ps_partkey, ps_suppkey) → ps_supplycost
    size_t ps_rows;
    int32_t* ps_partkey = mmap_binary<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", ps_rows);
    int32_t* ps_suppkey = mmap_binary<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", ps_rows);
    int64_t* ps_supplycost = mmap_binary<int64_t>(gendb_dir + "/partsupp/ps_supplycost.bin", ps_rows);

    std::unordered_map<std::pair<int32_t, int32_t>, int64_t, PairHash> partsupp_cost;
    partsupp_cost.reserve(ps_rows);
    for (size_t i = 0; i < ps_rows; i++) {
        partsupp_cost[{ps_partkey[i], ps_suppkey[i]}] = ps_supplycost[i];
    }

    // Load orders: o_orderkey → o_orderdate
    size_t ord_rows;
    int32_t* o_orderkey = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderkey.bin", ord_rows);
    int32_t* o_orderdate = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderdate.bin", ord_rows);

    std::unordered_map<int32_t, int32_t> order_date;
    order_date.reserve(ord_rows);
    for (size_t i = 0; i < ord_rows; i++) {
        order_date[o_orderkey[i]] = o_orderdate[i];
    }

    // Load lineitem columns
    size_t li_rows;
    int32_t* l_orderkey = mmap_binary<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", li_rows);
    int32_t* l_partkey = mmap_binary<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", li_rows);
    int32_t* l_suppkey = mmap_binary<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", li_rows);
    int64_t* l_quantity = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", li_rows);
    int64_t* l_extendedprice = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", li_rows);
    int64_t* l_discount = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_discount.bin", li_rows);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] data_load: %.2f ms\n", load_ms);
#endif

    // Join and aggregate
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<std::pair<std::string, int>, double, NationYearHash> agg_map;
    agg_map.reserve(1000); // nations × years

    for (size_t i = 0; i < li_rows; i++) {
        // Filter by green parts
        if (green_parts.find(l_partkey[i]) == green_parts.end()) continue;

        // Join with supplier
        auto supp_it = supp_nation.find(l_suppkey[i]);
        if (supp_it == supp_nation.end()) continue;
        int32_t nationkey = supp_it->second;

        // Join with partsupp
        auto ps_it = partsupp_cost.find({l_partkey[i], l_suppkey[i]});
        if (ps_it == partsupp_cost.end()) continue;
        int64_t supplycost = ps_it->second;

        // Join with orders
        auto ord_it = order_date.find(l_orderkey[i]);
        if (ord_it == order_date.end()) continue;
        int32_t orderdate = ord_it->second;

        // Compute profit (all in scaled integers, scale 100)
        // revenue = l_extendedprice * (1 - l_discount)
        // profit = revenue - ps_supplycost * l_quantity
        int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]); // scale 100 * 100 = 10000
        int64_t cost = supplycost * l_quantity[i]; // scale 100 * 100 = 10000
        int64_t profit_scaled = revenue - cost; // scale 10000
        double profit = profit_scaled / 10000.0;

        // Extract year
        int year = extract_year(orderdate);

        // Aggregate
        std::string& nation = nation_names[nationkey];
        agg_map[{nation, year}] += profit;
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", join_ms);
#endif

    // Sort by nation ASC, year DESC
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::tuple<std::string, int, double>> results;
    results.reserve(agg_map.size());
    for (const auto& kv : agg_map) {
        results.push_back({kv.first.first, kv.first.second, kv.second});
    }

    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (std::get<0>(a) != std::get<0>(b)) return std::get<0>(a) < std::get<0>(b);
        return std::get<1>(a) > std::get<1>(b); // year DESC
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q9.csv");
    out << "nation,o_year,sum_profit\n";
    for (const auto& r : results) {
        out << std::get<0>(r) << "," << std::get<1>(r) << ","
            << std::fixed << std::setprecision(2) << std::get<2>(r) << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
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
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
