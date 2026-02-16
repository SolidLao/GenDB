#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <stdint.h>
#include <cassert>
#include <iomanip>

// ============================================================================
// Helper Structs and Functions
// ============================================================================

struct Region {
    int32_t r_regionkey;
    std::string r_name;
};

struct Nation {
    int32_t n_nationkey;
    int32_t n_regionkey;
    std::string n_name;
};

struct Supplier {
    int32_t s_suppkey;
    int32_t s_nationkey;
};

struct Part {
    int32_t p_partkey;
    int16_t p_type_code;
};

struct Customer {
    int32_t c_custkey;
    int32_t c_nationkey;
};

struct Order {
    int32_t o_orderkey;
    int32_t o_custkey;
    int32_t o_orderdate;
};

struct LineItem {
    int32_t l_orderkey;
    int32_t l_partkey;
    int32_t l_suppkey;
    int64_t l_extendedprice;
    int64_t l_discount;
};

// ============================================================================
// mmap helpers
// ============================================================================

template<typename T>
T* mmap_column(const std::string& path, int32_t& num_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    off_t size = lseek(fd, 0, SEEK_END);
    num_rows = size / sizeof(T);
    T* data = (T*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }
    return data;
}

// ============================================================================
// String helpers
// ============================================================================

std::string read_string(const std::string& path) {
    std::ifstream f(path);
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    return content;
}

// ============================================================================
// Dictionary helpers
// ============================================================================

int16_t lookup_dict_code(const std::unordered_map<std::string, int16_t>& dict, const std::string& value) {
    auto it = dict.find(value);
    if (it == dict.end()) return -999;  // Not found sentinel
    return it->second;
}

std::unordered_map<std::string, int16_t> load_string_dict(const std::string& dict_path) {
    std::unordered_map<std::string, int16_t> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int16_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[value] = code;
        }
    }
    return dict;
}

// ============================================================================
// Date helper (epoch days since 1970-01-01)
// ============================================================================

int32_t compute_epoch_date(int year, int month, int day) {
    // Days from 1970-01-01 to year-01-01
    int32_t days = 0;
    for (int y = 1970; y < year; y++) {
        bool is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += is_leap ? 366 : 365;
    }

    // Days for complete months in the given year
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) days_in_month[2] = 29;

    for (int m = 1; m < month; m++) {
        days += days_in_month[m];
    }

    // Days in the current month (1-indexed -> 0-indexed)
    days += (day - 1);

    return days;
}

std::string epoch_to_date_string(int32_t epoch_days) {
    // Approximate year
    int year = 1970;
    int days_left = epoch_days;

    while (true) {
        bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_year = is_leap ? 366 : 365;
        if (days_left < days_in_year) break;
        days_left -= days_in_year;
        year++;
    }

    // Month and day
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) days_in_month[2] = 29;

    int month = 1;
    while (month <= 12 && days_left >= days_in_month[month]) {
        days_left -= days_in_month[month];
        month++;
    }

    int day = days_left + 1;

    char buf[16];
    snprintf(buf, 16, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// ============================================================================
// Variable-length string handling
// ============================================================================

std::vector<std::string> read_offset_strings(const std::string& path) {
    std::vector<std::string> result;
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        std::cerr << "Failed to open " << path << std::endl;
        return result;
    }

    // Read row count
    int32_t num_rows = 0;
    f.read((char*)&num_rows, 4);

    // Read offset table (4 bytes per row)
    std::vector<int32_t> offsets(num_rows);
    f.read((char*)offsets.data(), num_rows * 4);

    // Determine string data size
    std::streamoff current_pos = f.tellg();
    f.seekg(0, std::ios::end);
    std::streamoff end_pos = f.tellg();
    int32_t data_size = end_pos - current_pos;

    // Read all string data
    f.seekg(current_pos);
    std::vector<char> string_data(data_size);
    f.read(string_data.data(), data_size);

    // Extract strings using offsets
    for (int32_t i = 0; i < num_rows; i++) {
        int32_t start = offsets[i];
        int32_t end = (i + 1 < num_rows) ? offsets[i + 1] : data_size;
        int32_t len = end - start;
        result.push_back(std::string(&string_data[start], len));
    }

    return result;
}

// ============================================================================
// Main Query Implementation
// ============================================================================

void run_q8(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ========================================================================
    // 1. Load Dictionary for p_type
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<std::string, int16_t> p_type_dict =
        load_string_dict(gendb_dir + "/part/p_type_dict.txt");
    int16_t economy_anodized_steel_code = lookup_dict_code(p_type_dict, "ECONOMY ANODIZED STEEL");

    #ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] dict_load: %.2f ms\n", dict_ms);
    #endif

    // ========================================================================
    // 2. Load Region table
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_region_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t region_rows = 5;  // Region table has 5 rows
    int32_t* r_regionkey = mmap_column<int32_t>(gendb_dir + "/region/r_regionkey.bin", region_rows);

    // Load region names (offset-based format)
    std::vector<std::string> r_names = read_offset_strings(gendb_dir + "/region/r_name.bin");
    if (r_names.size() != (size_t)region_rows) {
        std::cerr << "ERROR: region_rows mismatch. Expected " << region_rows << ", got " << r_names.size() << std::endl;
        return;
    }

    // Find AMERICA region key
    int32_t america_regionkey = -1;
    for (int32_t i = 0; i < region_rows; i++) {
        if (r_names[i] == "AMERICA") {
            america_regionkey = r_regionkey[i];
            break;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_region_end = std::chrono::high_resolution_clock::now();
    double region_ms = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
    printf("[TIMING] load_region: %.2f ms\n", region_ms);
    #endif

    // ========================================================================
    // 3. Load Nation table
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t nation_rows = 25;  // Nation table has 25 rows
    int32_t* n_nationkey = mmap_column<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_rows);
    int32_t* n_regionkey = mmap_column<int32_t>(gendb_dir + "/nation/n_regionkey.bin", nation_rows);

    // Load nation names (offset-based format)
    std::vector<std::string> n_names = read_offset_strings(gendb_dir + "/nation/n_name.bin");
    if (n_names.size() != (size_t)nation_rows) {
        std::cerr << "ERROR: nation_rows mismatch. Expected " << nation_rows << ", got " << n_names.size() << std::endl;
        return;
    }

    // Build hash map: n_nationkey -> n_regionkey (for filter by regionkey=AMERICA)
    std::unordered_map<int32_t, int32_t> nation_regionkey_map;
    for (int32_t i = 0; i < nation_rows; i++) {
        nation_regionkey_map[n_nationkey[i]] = n_regionkey[i];
    }

    // Build hash map: n_nationkey -> n_name (to get name by key)
    std::unordered_map<int32_t, std::string> nation_name_map;
    for (int32_t i = 0; i < nation_rows; i++) {
        nation_name_map[n_nationkey[i]] = n_names[i];
    }

    #ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    double nation_ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] load_nation: %.2f ms\n", nation_ms);
    #endif

    // ========================================================================
    // 4. Load Part table and filter
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_part_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t part_rows = 0;
    int32_t* p_partkey = mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", part_rows);
    int16_t* p_type = mmap_column<int16_t>(gendb_dir + "/part/p_type.bin", part_rows);

    // Build hash set of parts with type = ECONOMY ANODIZED STEEL
    std::unordered_map<int32_t, bool> part_filter;
    for (int32_t i = 0; i < part_rows; i++) {
        if (p_type[i] == economy_anodized_steel_code) {
            part_filter[p_partkey[i]] = true;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_part_end = std::chrono::high_resolution_clock::now();
    double part_ms = std::chrono::duration<double, std::milli>(t_part_end - t_part_start).count();
    printf("[TIMING] load_part: %.2f ms, filtered_count=%zu\n", part_ms, part_filter.size());
    #endif

    // ========================================================================
    // 5. Load Supplier table
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t supplier_rows = 0;
    int32_t* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_rows);
    int32_t* s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_rows);

    // Build hash map: s_suppkey -> s_nationkey
    std::unordered_map<int32_t, int32_t> supplier_nationkey_map;
    for (int32_t i = 0; i < supplier_rows; i++) {
        supplier_nationkey_map[s_suppkey[i]] = s_nationkey[i];
    }

    #ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double supplier_ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] load_supplier: %.2f ms\n", supplier_ms);
    #endif

    // ========================================================================
    // 6. Load Customer table
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_customer_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t customer_rows = 0;
    int32_t* c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_rows);
    int32_t* c_nationkey = mmap_column<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_rows);

    // Build hash map: c_custkey -> c_nationkey
    std::unordered_map<int32_t, int32_t> customer_nationkey_map;
    for (int32_t i = 0; i < customer_rows; i++) {
        customer_nationkey_map[c_custkey[i]] = c_nationkey[i];
    }

    #ifdef GENDB_PROFILE
    auto t_customer_end = std::chrono::high_resolution_clock::now();
    double customer_ms = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", customer_ms);
    #endif

    // ========================================================================
    // 7. Load Orders table
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t orders_rows = 0;
    int32_t* o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_rows);
    int32_t* o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_rows);
    int32_t* o_orderdate = mmap_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_rows);

    // Build hash map: o_orderkey -> (o_custkey, o_orderdate)
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_map;
    for (int32_t i = 0; i < orders_rows; i++) {
        order_map[o_orderkey[i]] = {o_custkey[i], o_orderdate[i]};
    }

    // Date filter: 1995-01-01 to 1996-12-31
    int32_t date_1995_01_01 = compute_epoch_date(1995, 1, 1);
    int32_t date_1996_12_31 = compute_epoch_date(1996, 12, 31);

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] load_orders: %.2f ms, date_range=[%d, %d]\n", orders_ms, date_1995_01_01, date_1996_12_31);
    #endif

    // ========================================================================
    // 8. Load LineItem table and process
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
    #endif

    int32_t lineitem_rows = 0;
    int32_t* l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_rows);
    int32_t* l_partkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", lineitem_rows);
    int32_t* l_suppkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_rows);
    int64_t* l_extendedprice = mmap_column<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_rows);
    int64_t* l_discount = mmap_column<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_rows);

    #ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_load_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", lineitem_load_ms);
    #endif

    // ========================================================================
    // 9. Main join and aggregation loop
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_join_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    // Aggregation structure: year -> (sum_volume_brazil, sum_volume_total)
    std::unordered_map<int32_t, std::pair<int64_t, int64_t>> year_aggregates;

    // Find BRAZIL's nationkey for filtering
    // (not strictly needed since we check nation_name later, but kept for clarity)

    // Process lineitem rows
    #pragma omp parallel for schedule(dynamic, 10000)
    for (int32_t i = 0; i < lineitem_rows; i++) {
        int32_t orderkey = l_orderkey[i];
        int32_t partkey = l_partkey[i];
        int32_t suppkey = l_suppkey[i];
        int64_t extendedprice = l_extendedprice[i];
        int64_t discount = l_discount[i];

        // Filter 1: Part must be ECONOMY ANODIZED STEEL
        if (part_filter.find(partkey) == part_filter.end()) continue;

        // Join Orders: get o_custkey and o_orderdate
        auto order_it = order_map.find(orderkey);
        if (order_it == order_map.end()) continue;

        int32_t custkey = order_it->second.first;
        int32_t orderdate = order_it->second.second;

        // Filter 2: o_orderdate between 1995-01-01 and 1996-12-31
        if (orderdate < date_1995_01_01 || orderdate > date_1996_12_31) continue;

        // Join Customer: get c_nationkey
        auto cust_it = customer_nationkey_map.find(custkey);
        if (cust_it == customer_nationkey_map.end()) continue;

        int32_t c_nkey = cust_it->second;

        // Join Nation (n1): filter by region = AMERICA
        auto nation_region_it = nation_regionkey_map.find(c_nkey);
        if (nation_region_it == nation_regionkey_map.end()) continue;
        if (nation_region_it->second != america_regionkey) continue;

        // Join Supplier: get s_nationkey
        auto supp_it = supplier_nationkey_map.find(suppkey);
        if (supp_it == supplier_nationkey_map.end()) continue;

        int32_t s_nkey = supp_it->second;

        // Get s_nation name (n2)
        auto s_nation_it = nation_name_map.find(s_nkey);
        if (s_nation_it == nation_name_map.end()) continue;

        std::string nation_name = s_nation_it->second;

        // Compute volume: l_extendedprice * (1 - l_discount)
        // Both are scaled by 100, so: volume_scaled = extendedprice * (100 - discount) / 100
        // To preserve precision, accumulate at higher scale
        int64_t volume_scaled = extendedprice * (100 - discount);  // scale^2 = 10000

        // Extract year from orderdate
        int32_t year = 1970;
        int32_t days_left = orderdate;
        while (true) {
            bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            int32_t days_in_year = is_leap ? 366 : 365;
            if (days_left < days_in_year) break;
            days_left -= days_in_year;
            year++;
        }

        // Thread-local aggregation
        #pragma omp critical
        {
            if (nation_name == "BRAZIL") {
                year_aggregates[year].first += volume_scaled;
            }
            year_aggregates[year].second += volume_scaled;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_agg_end = std::chrono::high_resolution_clock::now();
    double join_agg_ms = std::chrono::duration<double, std::milli>(t_join_agg_end - t_join_agg_start).count();
    printf("[TIMING] join_aggregation: %.2f ms\n", join_agg_ms);
    #endif

    // ========================================================================
    // 10. Sort results by year
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<int32_t> years;
    for (auto& [year, _] : year_aggregates) {
        years.push_back(year);
    }
    std::sort(years.begin(), years.end());

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
    #endif

    // ========================================================================
    // 11. Write output
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q8.csv";
    std::ofstream out(output_path);

    out << "o_year,mkt_share\n";

    for (int32_t year : years) {
        auto& [sum_brazil, sum_total] = year_aggregates[year];

        // Descale: both are at scale 10000, so divide by 10000 to get scale 1
        // mkt_share = sum_brazil / sum_total
        double mkt_share = 0.0;
        if (sum_total > 0) {
            mkt_share = (double)sum_brazil / (double)sum_total;
        }

        out << year << "," << std::fixed << std::setprecision(2) << mkt_share << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    // ========================================================================
    // 12. Print summary
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    printf("Output written to: %s\n", output_path.c_str());
    printf("Result rows: %zu\n", year_aggregates.size());
    #endif
}

// ============================================================================
// Main Entry Point
// ============================================================================

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
