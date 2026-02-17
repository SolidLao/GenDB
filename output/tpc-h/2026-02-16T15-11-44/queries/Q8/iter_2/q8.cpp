/*
================================================================================
  Q8: National Market Share
================================================================================

LOGICAL PLAN:
  1. Filter region by r_name = 'AMERICA'
     - Region is tiny (5 rows), sequential scan
     - Get regionkey

  2. Filter nation (n1) by n_regionkey IN (regionkeys from AMERICA)
     - Estimated: ~5 nations (out of 25)

  3. Filter orders by o_orderdate BETWEEN 1995-01-01 AND 1996-12-31
     - Full scan with date range predicate
     - Estimated: ~1.6M rows (from ~15M total)

  4. Filter part by p_type = 'ECONOMY ANODIZED STEEL'
     - Estimated: ~13.3K rows (from ~2M total, ~150 distinct types)

  5. Filter lineitem by l_shipdate in valid range (any date within order period + buffer)
     - Estimated: ~1.6M rows (from ~60M total)

  6. Join execution order (smallest first):
     a) part ⨝ lineitem (part is filtered to ~13.3K, lineitem filtered to ~1.6M)
        Build hash table on part, probe with lineitem
        Intermediate result: ~1.6M rows

     b) orders filtered → ~1.6M rows
        Join with (part ⨝ lineitem) on l_orderkey = o_orderkey
        Build hash table on orders, probe with intermediate
        Result: ~1.6M rows

     c) customer
        Join on o_custkey = c_custkey
        Build hash table on customer, probe with intermediate
        Result: ~1.6M rows

     d) supplier
        Join on l_suppkey = s_suppkey
        Build hash table on supplier, probe with intermediate
        Result: ~1.6M rows

     e) nation n1 filtered (AMERICA region)
        Join on c_nationkey = n1.n_nationkey
        Tiny: ~5 nations, broadcast join
        Result: ~1.6M rows

     f) Nation n2
        Join on s_nationkey = n2.n_nationkey
        All 25 nations, broadcast join
        Result: ~1.6M rows

  7. Aggregation: GROUP BY o_year
     - Extract YEAR from o_orderdate
     - Expected: 2 years (1995, 1996)
     - Use flat array [0..200] indexed by (year - 1900)
     - Aggregate: SUM(volume) and SUM(CASE WHEN nation='BRAZIL' THEN volume ELSE 0)

PHYSICAL PLAN:
  - Scan strategy: Full scans with predicate pushdown (no indexes needed for this query)
  - Join method: Hash joins (build on smaller side)
  - Aggregation: Flat array (very low cardinality: 2 groups)
  - Parallelism: Scan filters in parallel (OpenMP). Join probe in parallel with thread-local aggregation.
  - Data structures:
    * Part hash table: std::unordered_map<int32_t, vector<size_t>> (p_partkey → lineitem indices)
    * Orders hash table: std::unordered_map<int32_t, vector<size_t>> (o_orderkey → order indices)
    * Customer hash table: std::unordered_map<int32_t, int32_t> (c_custkey → c_nationkey)
    * Supplier hash table: std::unordered_map<int32_t, int32_t> (s_suppkey → s_nationkey)
    * Nation n1 set: std::unordered_set<int32_t> (valid AMERICA nations)
    * Nation n2 map: std::unordered_map<int32_t, std::string> (n_nationkey → n_name)
    * Aggregation: flat arrays indexed by year

CRITICAL DETAILS:
  - Dates: o_orderdate and l_shipdate are int32_t epoch days
    * 1995-01-01 = 9131 days
    * 1996-12-31 = 9496 days
  - Decimals: l_extendedprice and l_discount are int64_t with scale_factor=100
    * volume = (l_extendedprice * (1 - l_discount / 100)) / 100
    * Note: l_discount is already scaled, so 5 = 0.05
  - Dictionary strings: p_type=101, r_name=1 for AMERICA, n_name varies
  - BRAZIL = code 2 (from nation dictionary)
  - Year extraction: (epoch_days / 365.25) + 1970 or use calendar computation
================================================================================
*/

#include <iostream>
#include <fstream>
#include <vector>
#include <array>
#include <unordered_map>
#include <unordered_set>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <algorithm>
#include <sstream>
#include <iomanip>

// Helper function to compute epoch days from YYYY-MM-DD
// 1970-01-01 = day 0
inline int32_t date_to_epoch_days(int year, int month, int day) {
    int days = 0;
    // Add days for complete years from 1970
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add days for complete months
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) days_in_month[1] = 29;
    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }
    days += day - 1;  // day is 1-indexed, epoch day 0 = Jan 1
    return days;
}

// Helper function to extract year from epoch days
inline int32_t extract_year_from_epoch_days(int32_t days) {
    int year = 1970;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }
    return year;
}

// Helper function to convert epoch days to YYYY-MM-DD string
std::string epoch_days_to_date_string(int32_t days) {
    int year = 1970;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) days_in_month[1] = 29;
    int month = 1;
    for (int m = 0; m < 12; ++m) {
        if (days < days_in_month[m]) {
            month = m + 1;
            break;
        }
        days -= days_in_month[m];
    }
    int day = days + 1;

    char buf[16];
    snprintf(buf, 16, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Compact open-addressing hash table for joins (faster than unordered_map)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable() : mask(0) {}

    explicit CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        for (auto& e : table) e.occupied = false;
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
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

// MMAP helper
template<typename T>
std::vector<T> load_column(const std::string& path, int32_t num_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open " << path << std::endl;
        exit(1);
    }
    size_t size = static_cast<size_t>(num_rows) * sizeof(T);
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "ERROR: mmap failed for " << path << std::endl;
        exit(1);
    }
    const T* data = static_cast<const T*>(ptr);
    std::vector<T> result(data, data + num_rows);
    munmap((void*)ptr, size);
    return result;
}

// Load dictionary and return code for a target string
int32_t get_dictionary_code(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            std::string value = line.substr(eq + 1);
            if (value == target) {
                return std::stoi(line.substr(0, eq));
            }
        }
    }
    std::cerr << "ERROR: Dictionary code not found for '" << target << "' in " << dict_path << std::endl;
    exit(1);
}

// Load dictionary and return map of code -> string
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> result;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            result[code] = value;
        }
    }
    return result;
}

void run_q8(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // ========== Load Data ==========
    const int32_t n_rows = 25;
    const int32_t s_rows = 100000;
    const int32_t p_rows = 2000000;
    const int32_t r_rows = 5;
    const int32_t c_rows = 1500000;
    const int32_t o_rows = 15000000;
    const int32_t l_rows = 59986052;

    // Load all columns
    auto r_regionkey = load_column<int32_t>(gendb_dir + "/region/r_regionkey.bin", r_rows);
    auto r_name = load_column<int32_t>(gendb_dir + "/region/r_name.bin", r_rows);

    auto n_nationkey = load_column<int32_t>(gendb_dir + "/nation/n_nationkey.bin", n_rows);
    auto n_name = load_column<int32_t>(gendb_dir + "/nation/n_name.bin", n_rows);
    auto n_regionkey = load_column<int32_t>(gendb_dir + "/nation/n_regionkey.bin", n_rows);

    auto s_suppkey = load_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", s_rows);
    auto s_nationkey = load_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", s_rows);

    auto p_partkey = load_column<int32_t>(gendb_dir + "/part/p_partkey.bin", p_rows);
    auto p_type = load_column<int32_t>(gendb_dir + "/part/p_type.bin", p_rows);

    auto c_custkey = load_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", c_rows);
    auto c_nationkey = load_column<int32_t>(gendb_dir + "/customer/c_nationkey.bin", c_rows);

    auto o_orderkey = load_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", o_rows);
    auto o_custkey = load_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", o_rows);
    auto o_orderdate = load_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", o_rows);

    auto l_orderkey = load_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", l_rows);
    auto l_partkey = load_column<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", l_rows);
    auto l_suppkey = load_column<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", l_rows);
    auto l_extendedprice = load_column<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", l_rows);
    auto l_discount = load_column<int64_t>(gendb_dir + "/lineitem/l_discount.bin", l_rows);

    // Load dictionaries
    auto r_name_dict = load_dictionary(gendb_dir + "/region/r_name_dict.txt");
    auto n_name_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");
    auto p_type_dict = load_dictionary(gendb_dir + "/part/p_type_dict.txt");

    // Get dictionary codes for filters
    int32_t region_america_code = get_dictionary_code(gendb_dir + "/region/r_name_dict.txt", "AMERICA");
    int32_t part_type_code = get_dictionary_code(gendb_dir + "/part/p_type_dict.txt", "ECONOMY ANODIZED STEEL");
    int32_t nation_brazil_code = get_dictionary_code(gendb_dir + "/nation/n_name_dict.txt", "BRAZIL");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // ========== Step 1: Filter region by r_name = 'AMERICA' ==========
#ifdef GENDB_PROFILE
    auto t_region_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> america_regions;
    for (int32_t i = 0; i < r_rows; ++i) {
        if (r_name[i] == region_america_code) {
            america_regions.insert(r_regionkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_region_end = std::chrono::high_resolution_clock::now();
    double region_ms = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
    printf("[TIMING] filter_region: %.2f ms\n", region_ms);
#endif

    // ========== Step 2: Filter nation by regionkey in AMERICA ==========
#ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> america_nations;
    for (int32_t i = 0; i < n_rows; ++i) {
        if (america_regions.count(n_regionkey[i]) > 0) {
            america_nations.insert(n_nationkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    double nation_ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] filter_nation: %.2f ms\n", nation_ms);
#endif

    // ========== Step 3: Filter part by p_type = 'ECONOMY ANODIZED STEEL' ==========
#ifdef GENDB_PROFILE
    auto t_part_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> valid_parts;
    #pragma omp parallel for
    for (int32_t i = 0; i < p_rows; ++i) {
        if (p_type[i] == part_type_code) {
            #pragma omp critical
            valid_parts.insert(p_partkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_part_end = std::chrono::high_resolution_clock::now();
    double part_ms = std::chrono::duration<double, std::milli>(t_part_end - t_part_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", part_ms);
#endif

    // ========== Step 4: Filter orders by o_orderdate BETWEEN 1995-01-01 AND 1996-12-31 ==========
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t date_1995_01_01 = date_to_epoch_days(1995, 1, 1);
    int32_t date_1996_12_31 = date_to_epoch_days(1996, 12, 31);

    std::vector<int32_t> valid_order_indices;
    #pragma omp parallel
    {
        std::vector<int32_t> local_indices;
        #pragma omp for
        for (int32_t i = 0; i < o_rows; ++i) {
            if (o_orderdate[i] >= date_1995_01_01 && o_orderdate[i] <= date_1996_12_31) {
                local_indices.push_back(i);
            }
        }
        #pragma omp critical
        valid_order_indices.insert(valid_order_indices.end(), local_indices.begin(), local_indices.end());
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms\n", orders_ms);
#endif

    // ========== Step 5: Filter lineitem and build part-lineitem join ==========
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash table: orderkey -> list of lineitem indices (that have valid parts)
    std::unordered_map<int32_t, std::vector<int32_t>> lineitem_by_orderkey;

    #pragma omp parallel
    {
        std::unordered_map<int32_t, std::vector<int32_t>> local_map;
        #pragma omp for
        for (int32_t i = 0; i < l_rows; ++i) {
            if (valid_parts.count(l_partkey[i]) > 0) {
                local_map[l_orderkey[i]].push_back(i);
            }
        }
        #pragma omp critical
        {
            for (auto& [key, vec] : local_map) {
                lineitem_by_orderkey[key].insert(lineitem_by_orderkey[key].end(), vec.begin(), vec.end());
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] filter_lineitem: %.2f ms\n", lineitem_ms);
#endif

    // ========== Step 6: Build supplier hash table using CompactHashTable ==========
#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif

    CompactHashTable<int32_t, int32_t> supplier_map(s_rows); // s_suppkey -> s_nationkey
    for (int32_t i = 0; i < s_rows; ++i) {
        supplier_map.insert(s_suppkey[i], s_nationkey[i]);
    }

#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double supplier_ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] build_supplier: %.2f ms\n", supplier_ms);
#endif

    // ========== Step 7: Build customer hash table using CompactHashTable ==========
#ifdef GENDB_PROFILE
    auto t_customer_start = std::chrono::high_resolution_clock::now();
#endif

    CompactHashTable<int32_t, int32_t> customer_map(c_rows); // c_custkey -> c_nationkey
    for (int32_t i = 0; i < c_rows; ++i) {
        customer_map.insert(c_custkey[i], c_nationkey[i]);
    }

#ifdef GENDB_PROFILE
    auto t_customer_end = std::chrono::high_resolution_clock::now();
    double customer_ms = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    printf("[TIMING] build_customer: %.2f ms\n", customer_ms);
#endif

    // ========== Step 8: Aggregate ==========
#ifdef GENDB_PROFILE
    auto t_aggregate_start = std::chrono::high_resolution_clock::now();
#endif

    // Aggregate: GROUP BY o_year
    // Use array for known small cardinality (1995, 1996)
    // Array indexed: [year - 1995] for years 0-200 range
    std::array<std::pair<int64_t, int64_t>, 200> year_agg_array;
    for (auto& p : year_agg_array) {
        p = {0, 0};
    }

    #pragma omp parallel
    {
        std::array<std::pair<int64_t, int64_t>, 200> local_agg_array;
        for (auto& p : local_agg_array) {
            p = {0, 0};
        }

        #pragma omp for
        for (size_t o_idx = 0; o_idx < valid_order_indices.size(); ++o_idx) {
            int32_t order_idx = valid_order_indices[o_idx];
            int32_t orderkey = o_orderkey[order_idx];
            int32_t custkey = o_custkey[order_idx];
            int32_t order_date = o_orderdate[order_idx];
            int32_t order_year = extract_year_from_epoch_days(order_date);

            // Find customer's nation (n1)
            auto cust_nation_ptr = customer_map.find(custkey);
            if (!cust_nation_ptr) continue;
            int32_t cust_nation = *cust_nation_ptr;

            // Check if customer is from AMERICA
            if (america_nations.count(cust_nation) == 0) continue;

            // Find all lineitems for this order
            auto it_li = lineitem_by_orderkey.find(orderkey);
            if (it_li == lineitem_by_orderkey.end()) continue;

            for (int32_t li_idx : it_li->second) {
                int32_t suppkey = l_suppkey[li_idx];

                // Find supplier's nation (n2)
                auto supp_nation_ptr = supplier_map.find(suppkey);
                if (!supp_nation_ptr) continue;
                int32_t supp_nation = *supp_nation_ptr;

                // Compute volume = l_extendedprice * (1 - l_discount / 100)
                // l_extendedprice and l_discount are scaled by 100
                // So: volume = l_extendedprice * (100 - l_discount) / 100
                int64_t volume_scaled = (l_extendedprice[li_idx] * (100 - l_discount[li_idx])) / 100;

                // Aggregate using array indexing (much faster than hashmap)
                int32_t arr_idx = order_year - 1900;
                if (arr_idx >= 0 && arr_idx < 200) {
                    local_agg_array[arr_idx].first += volume_scaled;
                    if (supp_nation == nation_brazil_code) {
                        local_agg_array[arr_idx].second += volume_scaled;
                    }
                }
            }
        }

        #pragma omp critical
        {
            for (int i = 0; i < 200; ++i) {
                year_agg_array[i].first += local_agg_array[i].first;
                year_agg_array[i].second += local_agg_array[i].second;
            }
        }
    }

    // Convert array back to map for sorting
    std::unordered_map<int32_t, std::pair<int64_t, int64_t>> year_agg;
    for (int i = 0; i < 200; ++i) {
        if (year_agg_array[i].first > 0) {
            year_agg[1900 + i] = year_agg_array[i];
        }
    }

#ifdef GENDB_PROFILE
    auto t_aggregate_end = std::chrono::high_resolution_clock::now();
    double aggregate_ms = std::chrono::duration<double, std::milli>(t_aggregate_end - t_aggregate_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", aggregate_ms);
#endif

    // ========== Step 9: Output ==========
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    // Sort by year
    std::vector<std::pair<int32_t, std::pair<int64_t, int64_t>>> result_vec(year_agg.begin(), year_agg.end());
    std::sort(result_vec.begin(), result_vec.end());

    // Write CSV
    std::string output_path = results_dir + "/Q8.csv";
    std::ofstream out(output_path);
    out << "o_year,mkt_share\n";

    for (auto& [year, val] : result_vec) {
        int64_t total_volume = val.first;
        int64_t brazil_volume = val.second;

        if (total_volume == 0) {
            out << year << ",0.00\n";
        } else {
            double mkt_share = static_cast<double>(brazil_volume) / static_cast<double>(total_volume);
            out << year << "," << std::fixed << std::setprecision(2) << mkt_share << "\n";
        }
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Query results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q8(gendb_dir, results_dir);
    return 0;
}
#endif
