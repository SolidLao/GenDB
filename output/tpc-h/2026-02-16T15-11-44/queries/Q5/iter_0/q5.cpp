/*
QUERY PLAN - Q5: Local Supplier Volume

LOGICAL PLAN:
  Tables: region, nation, supplier, customer, orders, lineitem

  Single-Table Filters:
    1. region: r_name = 'ASIA' → 1 row (cardinality: 1)
    2. orders: o_orderdate >= 1994-01-01 AND o_orderdate < 1995-01-01 → ~1.5M rows
    3. nation: n_regionkey matches ASIA region → 5 nations
    4. lineitem: no single-table predicates → 60M rows

  Join Ordering (Smallest First):
    1. region (1 row) → Filter on r_name = 'ASIA'
    2. nation (5 rows after join) → Join region on r_regionkey
    3. supplier (20K rows) → Join nation on s_nationkey = n_nationkey
    4. customer (300K rows) → Join supplier on c_nationkey = s_nationkey
    5. orders (1.5M rows) → Join customer on o_custkey = c_custkey (date filtered)
    6. lineitem (60M rows) → Join orders on l_orderkey = o_orderkey

  Aggregation:
    - GROUP BY n_name (5 distinct groups after filtering)
    - SUM(l_extendedprice * (1 - l_discount)) as revenue
    - ORDER BY revenue DESC

PHYSICAL PLAN:
  1. Scan region, find ASIA (dictionary decode r_name)
  2. Hash join: nation on n_regionkey (1:1 on dimension)
  3. Hash join: supplier on s_nationkey (1:N, ~20K rows)
  4. Hash join: customer on c_nationkey (1:N, ~300K rows)
  5. Filter and hash join: orders on o_custkey (1:N, ~1.5M, date filtered)
  6. Hash join: lineitem on l_orderkey (1:N, ~60M)
  7. Hash aggregation: GROUP BY n_name_code, SUM(revenue)
  8. Sort by revenue DESC
  9. Output to CSV

Data Structures:
  - Open-addressing hash tables for joins (faster than unordered_map)
  - Flat array for nation aggregation (25 distinct values, but filtered to ~5)
  - unordered_map for intermediate results if needed for simplicity in iter_0

Parallelism:
  - Lineitem join probe parallelized with OpenMP (60M rows)
  - Aggregation uses thread-local buffers with merge at end
*/

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <cmath>
#include <iomanip>

// Helper: Parse dictionary file and find code for value
std::unordered_map<std::string, int32_t> load_dictionary(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos == std::string::npos) continue;
        int32_t code = std::stoi(line.substr(0, eq_pos));
        std::string value = line.substr(eq_pos + 1);
        dict[value] = code;
    }
    return dict;
}

// Reverse lookup: code -> value
std::unordered_map<int32_t, std::string> load_dictionary_reverse(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos == std::string::npos) continue;
        int32_t code = std::stoi(line.substr(0, eq_pos));
        std::string value = line.substr(eq_pos + 1);
        dict[code] = value;
    }
    return dict;
}

// Memory map file
template <typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    count = file_size / sizeof(T);
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for: " << path << std::endl;
        return nullptr;
    }
    return (T*)ptr;
}

// Date constant computation (epoch days since 1970-01-01)
// 1994-01-01 = ?
// Years 1970-1993: 24 years, with leap years in 1972, 1976, 1980, 1984, 1988, 1992
// days = 24*365 + 6 = 8766
int32_t date_1994_01_01() {
    int32_t days = 0;
    for (int y = 1970; y < 1994; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    return days;
}

// 1995-01-01
int32_t date_1995_01_01() {
    int32_t days = 0;
    for (int y = 1970; y < 1995; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    return days;
}

// Convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    int year = 1970;
    while (days >= (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 366 : 365)) {
        days -= (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        year++;
    }
    int daysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) daysInMonth[1] = 29;

    int month = 0;
    while (month < 12 && days >= daysInMonth[month]) {
        days -= daysInMonth[month];
        month++;
    }
    int day = days + 1;

    char buf[32];
    snprintf(buf, 32, "%04d-%02d-%02d", year, month + 1, day);
    return std::string(buf);
}

void run_Q5(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Constants for date filtering
    int32_t date_start = date_1994_01_01();
    int32_t date_end = date_1995_01_01();


    // Step 1: Load and filter REGION (find ASIA)
#ifdef GENDB_PROFILE
    auto t_region_start = std::chrono::high_resolution_clock::now();
#endif

    size_t region_count = 0;
    int32_t* region_regionkey = mmap_file<int32_t>(gendb_dir + "/region/r_regionkey.bin", region_count);
    int32_t* region_name = mmap_file<int32_t>(gendb_dir + "/region/r_name.bin", region_count);

    auto region_name_dict = load_dictionary(gendb_dir + "/region/r_name_dict.txt");
    int32_t asia_code = region_name_dict["ASIA"];

    int32_t asia_regionkey = -1;
    for (size_t i = 0; i < region_count; i++) {
        if (region_name[i] == asia_code) {
            asia_regionkey = region_regionkey[i];
            break;
        }
    }

#ifdef GENDB_PROFILE
    auto t_region_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
    printf("[TIMING] scan_region_filter: %.2f ms\n", ms);
#endif

    // Step 2: Load and filter NATION by regionkey
#ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif

    size_t nation_count = 0;
    int32_t* nation_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    int32_t* nation_name = mmap_file<int32_t>(gendb_dir + "/nation/n_name.bin", nation_count);
    int32_t* nation_regionkey = mmap_file<int32_t>(gendb_dir + "/nation/n_regionkey.bin", nation_count);

    auto nation_name_dict = load_dictionary_reverse(gendb_dir + "/nation/n_name_dict.txt");

    std::vector<int32_t> asia_nations;
    for (size_t i = 0; i < nation_count; i++) {
        if (nation_regionkey[i] == asia_regionkey) {
            asia_nations.push_back(nation_nationkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] scan_nation_filter: %.2f ms (found %zu nations)\n", ms, asia_nations.size());
#endif

    // Step 3: Load SUPPLIER and filter by nation
#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif

    size_t supplier_count = 0;
    int32_t* supplier_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* supplier_nationkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Hash table: nationkey -> vector of suppkeys
    std::unordered_map<int32_t, std::vector<uint32_t>> supplier_by_nation;
    for (int32_t nkey : asia_nations) {
        supplier_by_nation[nkey] = std::vector<uint32_t>();
    }
    for (size_t i = 0; i < supplier_count; i++) {
        if (supplier_by_nation.find(supplier_nationkey[i]) != supplier_by_nation.end()) {
            supplier_by_nation[supplier_nationkey[i]].push_back(i);
        }
    }

#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    size_t total_suppliers = 0;
    for (auto& p : supplier_by_nation) total_suppliers += p.second.size();
    printf("[TIMING] scan_supplier_filter: %.2f ms (found %zu suppliers)\n", ms, total_suppliers);
#endif

    // Step 4: Load CUSTOMER and filter by nation
#ifdef GENDB_PROFILE
    auto t_customer_start = std::chrono::high_resolution_clock::now();
#endif

    size_t customer_count = 0;
    int32_t* customer_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* customer_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Hash table: nationkey -> vector of custkeys
    std::unordered_map<int32_t, std::vector<uint32_t>> customer_by_nation;
    for (int32_t nkey : asia_nations) {
        customer_by_nation[nkey] = std::vector<uint32_t>();
    }
    for (size_t i = 0; i < customer_count; i++) {
        if (customer_by_nation.find(customer_nationkey[i]) != customer_by_nation.end()) {
            customer_by_nation[customer_nationkey[i]].push_back(i);
        }
    }

#ifdef GENDB_PROFILE
    auto t_customer_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    size_t total_customers = 0;
    for (auto& p : customer_by_nation) total_customers += p.second.size();
    printf("[TIMING] scan_customer_filter: %.2f ms (found %zu customers)\n", ms, total_customers);
#endif

    // Step 5: Load ORDERS and filter by date and custkey
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    size_t orders_count = 0;
    int32_t* orders_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* orders_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);
    int32_t* orders_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_count);

    // Build set of customer keys in Asia
    std::unordered_map<int32_t, bool> asia_custkeys;
    for (auto& p : customer_by_nation) {
        for (uint32_t idx : p.second) {
            asia_custkeys[customer_custkey[idx]] = true;
        }
    }

    // Hash table: custkey -> vector of orderkeys (filtered by date and custkey)
    std::unordered_map<int32_t, std::vector<uint32_t>> orders_by_custkey;
    std::vector<uint32_t> filtered_order_indices;

    for (size_t i = 0; i < orders_count; i++) {
        if (orders_orderdate[i] >= date_start && orders_orderdate[i] < date_end &&
            asia_custkeys.find(orders_custkey[i]) != asia_custkeys.end()) {
            orders_by_custkey[orders_custkey[i]].push_back(i);
            filtered_order_indices.push_back(i);
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] scan_filter_orders: %.2f ms (found %zu orders)\n", ms, filtered_order_indices.size());
#endif

    // Step 6: Load LINEITEM and join with filtered orders
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    size_t lineitem_count = 0;
    int32_t* lineitem_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    int32_t* lineitem_suppkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    int64_t* lineitem_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* lineitem_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] load_lineitem: %.2f ms (%zu rows)\n", ms, lineitem_count);
#endif

    // Step 7: Build hash table for supplier lookup: suppkey -> nationkey
#ifdef GENDB_PROFILE
    auto t_supplier_ht_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> suppkey_to_nationkey;
    std::unordered_map<int32_t, int32_t> custkey_to_nationkey;
    for (size_t i = 0; i < supplier_count; i++) {
        suppkey_to_nationkey[supplier_suppkey[i]] = supplier_nationkey[i];
    }
    for (size_t i = 0; i < customer_count; i++) {
        custkey_to_nationkey[customer_custkey[i]] = customer_nationkey[i];
    }

#ifdef GENDB_PROFILE
    auto t_supplier_ht_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_supplier_ht_end - t_supplier_ht_start).count();
    printf("[TIMING] build_supplier_ht: %.2f ms\n", ms);
#endif

    // Step 8: Build hash table for orderkey lookup: orderkey -> custkey
#ifdef GENDB_PROFILE
    auto t_orderkey_ht_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> orderkey_to_custkey;
    for (uint32_t idx : filtered_order_indices) {
        orderkey_to_custkey[orders_orderkey[idx]] = orders_custkey[idx];
    }

#ifdef GENDB_PROFILE
    auto t_orderkey_ht_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_orderkey_ht_end - t_orderkey_ht_start).count();
    printf("[TIMING] build_orderkey_ht: %.2f ms\n", ms);
#endif

    // Step 9: Build hash table for nation_name lookup: nationkey -> code
#ifdef GENDB_PROFILE
    auto t_nation_ht_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> nationkey_to_name_code;
    for (size_t i = 0; i < nation_count; i++) {
        nationkey_to_name_code[nation_nationkey[i]] = nation_name[i];
    }

#ifdef GENDB_PROFILE
    auto t_nation_ht_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_nation_ht_end - t_nation_ht_start).count();
    printf("[TIMING] build_nation_ht: %.2f ms\n", ms);
#endif

    // Step 10: Process lineitem with parallel aggregation
#ifdef GENDB_PROFILE
    auto t_join_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // Result aggregation: nationkey -> (name_code, revenue)
    std::unordered_map<int32_t, int64_t> revenue_by_nation;
    for (int32_t nkey : asia_nations) {
        revenue_by_nation[nkey] = 0;
    }

    // Parallel loop over lineitem with thread-local aggregation (using double for precision)
    std::vector<std::unordered_map<int32_t, double>> thread_revenues(omp_get_max_threads());
    size_t matched_lineitem_count = 0;
    for (int t = 0; t < omp_get_max_threads(); t++) {
        for (int32_t nkey : asia_nations) {
            thread_revenues[t][nkey] = 0.0;
        }
    }

    #pragma omp parallel for schedule(dynamic, 100000) reduction(+: matched_lineitem_count)
    for (size_t i = 0; i < lineitem_count; i++) {
        int32_t order_key = lineitem_orderkey[i];

        // Check if orderkey matches filtered orders
        auto it_order = orderkey_to_custkey.find(order_key);
        if (it_order == orderkey_to_custkey.end()) continue;

        int32_t cust_key = it_order->second;

        // Get customer's nation
        auto it_cust = custkey_to_nationkey.find(cust_key);
        if (it_cust == custkey_to_nationkey.end()) continue;
        int32_t cust_nation = it_cust->second;

        int32_t supp_key = lineitem_suppkey[i];

        // Check if suppkey exists
        auto it_supp = suppkey_to_nationkey.find(supp_key);
        if (it_supp == suppkey_to_nationkey.end()) continue;

        int32_t supp_nation = it_supp->second;

        // CRITICAL: Customer and supplier must be from the SAME nation
        if (cust_nation != supp_nation) continue;

        // Check if nation is in Asia
        if (revenue_by_nation.find(supp_nation) == revenue_by_nation.end()) continue;

        matched_lineitem_count++;

        // Calculate revenue: l_extendedprice * (1 - l_discount)
        // Both are scaled by 100, convert to double for full precision
        double price = static_cast<double>(lineitem_extendedprice[i]) / 100.0;
        double discount = static_cast<double>(lineitem_discount[i]) / 100.0;

        // revenue = price * (1 - discount)
        double revenue = price * (1.0 - discount);

        int thread_id = omp_get_thread_num();
        thread_revenues[thread_id][supp_nation] += revenue;
    }


    // Merge thread-local results and convert back to int64
    std::unordered_map<int32_t, double> revenue_by_nation_double;
    for (int32_t nkey : asia_nations) {
        revenue_by_nation_double[nkey] = 0.0;
    }
    for (int t = 0; t < omp_get_max_threads(); t++) {
        for (auto& p : thread_revenues[t]) {
            revenue_by_nation_double[p.first] += p.second;
        }
    }

    // Convert back to int64 for storage (will be scaled back to double in output)
    for (auto& p : revenue_by_nation_double) {
        revenue_by_nation[p.first] = static_cast<int64_t>(p.second * 100.0);
    }

#ifdef GENDB_PROFILE
    auto t_join_agg_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_join_agg_end - t_join_agg_start).count();
    printf("[TIMING] join_and_aggregation: %.2f ms\n", ms);
#endif

    // Step 11: Build result vector for sorting
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    struct ResultRow {
        std::string nation_name;
        int64_t revenue;
        bool operator<(const ResultRow& other) const {
            return revenue > other.revenue; // Descending
        }
    };

    std::vector<ResultRow> results;
    for (auto& p : revenue_by_nation) {
        int32_t nation_key = p.first;
        int64_t revenue = p.second;

        auto it_name = nationkey_to_name_code.find(nation_key);
        if (it_name == nationkey_to_name_code.end()) continue;

        auto it_dict = nation_name_dict.find(it_name->second);
        if (it_dict == nation_name_dict.end()) continue;

        results.push_back({it_dict->second, revenue});
    }

    std::sort(results.begin(), results.end());

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // Step 12: Write results to CSV
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q5.csv";
    std::ofstream out(output_path);
    out << "n_name,revenue\n";

    for (const auto& row : results) {
        // Convert revenue from scaled (scale_factor=100) to decimal with 4 places
        double revenue_val = static_cast<double>(row.revenue) / 100.0;
        out << row.nation_name << "," << std::fixed << std::setprecision(4) << revenue_val << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
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
    run_Q5(gendb_dir, results_dir);
    return 0;
}
#endif
