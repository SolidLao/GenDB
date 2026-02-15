#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <iomanip>
#include <sstream>

// ============================================================================
// Helper structures and utilities
// ============================================================================

// Result row for GROUP BY aggregation
struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue_scaled;  // scaled by 100
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const ResultRow& other) const {
        // Sort by revenue DESC, then o_orderdate ASC
        if (revenue_scaled != other.revenue_scaled) {
            return revenue_scaled > other.revenue_scaled;
        }
        return o_orderdate < other.o_orderdate;
    }
};

// Utility to convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t epoch_days) {
    // Days since 1970-01-01
    // NOTE: gendb data is stored with dates offset by +1 (1995-03-02 stored as 9192 not 9191)
    // So we subtract 1 here for correct display
    epoch_days = epoch_days - 1;

    // Handle leap years and convert to year/month/day
    const int DAYS_PER_4YEARS = 1461;  // 365*4 + 1
    const int DAYS_PER_100YEARS = 36524;  // 365*100 + 24
    const int DAYS_PER_400YEARS = 146097;  // 365*400 + 97

    int year = 1970;
    int days = epoch_days;

    // Fast forward through 400-year cycles
    int cycles_400 = days / DAYS_PER_400YEARS;
    year += cycles_400 * 400;
    days -= cycles_400 * DAYS_PER_400YEARS;

    // Handle remaining 100-year cycles
    if (days >= DAYS_PER_100YEARS) {
        int cycles_100 = (days - DAYS_PER_100YEARS) / DAYS_PER_100YEARS + 1;
        if (cycles_100 > 3) cycles_100 = 3;
        year += cycles_100 * 100;
        days -= cycles_100 * DAYS_PER_100YEARS;
    }

    // Handle remaining 4-year cycles
    while (days >= DAYS_PER_4YEARS) {
        year += 4;
        days -= DAYS_PER_4YEARS;
    }

    // Handle remaining single years
    int days_per_year = 365;
    while (days >= days_per_year) {
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
            days_per_year = 366;
        } else {
            days_per_year = 365;
        }
        if (days >= days_per_year) {
            days -= days_per_year;
            year++;
        } else {
            break;
        }
    }

    // Now find month and day
    const int month_days_normal[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const int month_days_leap[] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    const int* month_days = is_leap ? month_days_leap : month_days_normal;

    int month = 1;
    for (int m = 0; m < 12; m++) {
        if (days < month_days[m]) {
            month = m + 1;
            break;
        }
        days -= month_days[m];
    }
    int day = days + 1;

    char buffer[12];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month, day);
    return std::string(buffer);
}

// Utility to load dictionary file
std::unordered_map<uint8_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, std::string> dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(file, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            uint8_t code = static_cast<uint8_t>(std::stoi(line.substr(0, eq_pos)));
            std::string value = line.substr(eq_pos + 1);
            dict[code] = value;
        }
    }
    file.close();
    return dict;
}

// Utility to mmap a binary file
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < 0) {
        close(fd);
        return nullptr;
    }
    size = (size_t)file_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for: " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// ============================================================================
// Q3 Query Implementation
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    // Timer for total execution
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    std::string base_path = gendb_dir + "/tables/";

    // ========================================================================
    // Load customer table
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_customer_start = std::chrono::high_resolution_clock::now();
#endif

    size_t customer_size = 0;
    void* customer_custkey_ptr = mmap_file(base_path + "customer/c_custkey.bin", customer_size);
    size_t num_customers = customer_size / sizeof(int32_t);
    int32_t* customer_custkey = (int32_t*)customer_custkey_ptr;

    // Load c_mktsegment (dictionary-encoded)
    size_t mktseg_size = 0;
    void* customer_mktseg_ptr = mmap_file(base_path + "customer/c_mktsegment.bin", mktseg_size);
    uint8_t* customer_mktsegment = (uint8_t*)customer_mktseg_ptr;

    // Load dictionary for c_mktsegment
    auto mktseg_dict = load_dictionary(base_path + "customer/c_mktsegment_dict.txt");

    // Find code for "BUILDING"
    uint8_t building_code = 255;  // Invalid sentinel
    for (const auto& [code, value] : mktseg_dict) {
        if (value == "BUILDING") {
            building_code = code;
            break;
        }
    }

    if (building_code == 255) {
        std::cerr << "Could not find BUILDING in c_mktsegment dictionary" << std::endl;
        return;
    }

    // Build set of qualifying customer keys (c_mktsegment = 'BUILDING')
    std::unordered_map<int32_t, bool> qualifying_customers;
    for (size_t i = 0; i < num_customers; i++) {
        if (customer_mktsegment[i] == building_code) {
            qualifying_customers[customer_custkey[i]] = true;
        }
    }

#ifdef GENDB_PROFILE
    auto t_customer_end = std::chrono::high_resolution_clock::now();
    double customer_ms = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    printf("[TIMING] customer_filter: %.2f ms\n", customer_ms);
#endif

    // ========================================================================
    // Load orders table
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    size_t orders_size = 0;
    void* orders_orderkey_ptr = mmap_file(base_path + "orders/o_orderkey.bin", orders_size);
    size_t num_orders = orders_size / sizeof(int32_t);
    int32_t* orders_orderkey = (int32_t*)orders_orderkey_ptr;

    size_t orders_custkey_size = 0;
    void* orders_custkey_ptr = mmap_file(base_path + "orders/o_custkey.bin", orders_custkey_size);
    int32_t* orders_custkey = (int32_t*)orders_custkey_ptr;

    size_t orders_orderdate_size = 0;
    void* orders_orderdate_ptr = mmap_file(base_path + "orders/o_orderdate.bin", orders_orderdate_size);
    int32_t* orders_orderdate = (int32_t*)orders_orderdate_ptr;

    size_t orders_shippriority_size = 0;
    void* orders_shippriority_ptr = mmap_file(base_path + "orders/o_shippriority.bin", orders_shippriority_size);
    int32_t* orders_shippriority = (int32_t*)orders_shippriority_ptr;

    // Build hash table of qualifying orders
    // Key: o_orderkey, Value: {o_custkey, o_orderdate, o_shippriority}
    struct OrderData {
        int32_t o_custkey;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::unordered_map<int32_t, std::vector<OrderData>> orders_map;
    orders_map.reserve(num_orders / 10);  // Estimate

    const int32_t orderdate_threshold = 9205;  // 1995-03-15 in epoch days (data is off by +1)

    for (size_t i = 0; i < num_orders; i++) {
        int32_t custkey = orders_custkey[i];
        int32_t orderdate = orders_orderdate[i];

        // Filter: c_custkey = o_custkey AND o_orderdate < 1995-03-15
        if (qualifying_customers.count(custkey) && orderdate < orderdate_threshold) {
            OrderData od;
            od.o_custkey = custkey;
            od.o_orderdate = orderdate;
            od.o_shippriority = orders_shippriority[i];
            orders_map[orders_orderkey[i]].push_back(od);
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_filter: %.2f ms\n", orders_ms);
#endif

    // ========================================================================
    // Load lineitem table and join with orders
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    size_t lineitem_size = 0;
    void* lineitem_orderkey_ptr = mmap_file(base_path + "lineitem/l_orderkey.bin", lineitem_size);
    size_t num_lineitem = lineitem_size / sizeof(int32_t);
    int32_t* lineitem_orderkey = (int32_t*)lineitem_orderkey_ptr;

    size_t lineitem_shipdate_size = 0;
    void* lineitem_shipdate_ptr = mmap_file(base_path + "lineitem/l_shipdate.bin", lineitem_shipdate_size);
    int32_t* lineitem_shipdate = (int32_t*)lineitem_shipdate_ptr;

    size_t lineitem_extendedprice_size = 0;
    void* lineitem_extendedprice_ptr = mmap_file(base_path + "lineitem/l_extendedprice.bin", lineitem_extendedprice_size);
    int64_t* lineitem_extendedprice = (int64_t*)lineitem_extendedprice_ptr;

    size_t lineitem_discount_size = 0;
    void* lineitem_discount_ptr = mmap_file(base_path + "lineitem/l_discount.bin", lineitem_discount_size);
    int64_t* lineitem_discount = (int64_t*)lineitem_discount_ptr;

    // GROUP BY aggregation: l_orderkey, o_orderdate, o_shippriority
    // Aggregate: SUM(l_extendedprice * (1 - l_discount))
    struct GroupKey {
        int32_t l_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;

        bool operator==(const GroupKey& other) const {
            return l_orderkey == other.l_orderkey &&
                   o_orderdate == other.o_orderdate &&
                   o_shippriority == other.o_shippriority;
        }
    };

    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const {
            return std::hash<int32_t>()(k.l_orderkey) ^
                   (std::hash<int32_t>()(k.o_orderdate) << 1) ^
                   (std::hash<int32_t>()(k.o_shippriority) << 2);
        }
    };

    std::unordered_map<GroupKey, int64_t, GroupKeyHash> group_aggregates;
    group_aggregates.reserve(num_lineitem / 100);  // Estimate

    const int32_t shipdate_threshold = 9205;  // 1995-03-15 in epoch days (data is off by +1)

    for (size_t i = 0; i < num_lineitem; i++) {
        int32_t l_orderkey = lineitem_orderkey[i];
        int32_t l_shipdate = lineitem_shipdate[i];

        // Filter: l_shipdate > 1995-03-15
        if (l_shipdate <= shipdate_threshold) continue;

        // Join with orders
        auto it = orders_map.find(l_orderkey);
        if (it == orders_map.end()) continue;

        // For each matching order
        for (const auto& od : it->second) {
            // Compute revenue = l_extendedprice * (1 - l_discount)
            int64_t ext_price = lineitem_extendedprice[i];  // scaled by 100
            int64_t discount = lineitem_discount[i];        // scaled by 100
            // (1 - l_discount) where discount is scaled
            // To preserve precision: store numerator without dividing by 100
            // This keeps the fractional part for later aggregation
            int64_t revenue = ext_price * (100 - discount);  // Don't divide yet!

            GroupKey gk;
            gk.l_orderkey = l_orderkey;
            gk.o_orderdate = od.o_orderdate;
            gk.o_shippriority = od.o_shippriority;

            group_aggregates[gk] += revenue;
        }
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] lineitem_join_aggregate: %.2f ms\n", lineitem_ms);
#endif

    // ========================================================================
    // Convert aggregation results to result rows and sort
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    results.reserve(group_aggregates.size());

    for (const auto& [gk, revenue] : group_aggregates) {
        ResultRow row;
        row.l_orderkey = gk.l_orderkey;
        row.revenue_scaled = revenue;
        row.o_orderdate = gk.o_orderdate;
        row.o_shippriority = gk.o_shippriority;
        results.push_back(row);
    }

    // Sort: revenue DESC, o_orderdate ASC
    std::sort(results.begin(), results.end());

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

    // ========================================================================
    // Write CSV output (LIMIT 10)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream output_file(output_path);
    if (!output_file.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    output_file << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Write up to 10 rows
    size_t limit = std::min(results.size(), size_t(10));
    for (size_t i = 0; i < limit; i++) {
        const auto& row = results[i];
        // revenue_scaled is the numerator (ext_price * (100 - discount))
        // Divide by 10000 at output time to preserve fractional cents
        double revenue_actual = static_cast<double>(row.revenue_scaled) / 10000.0;
        std::string date_str = format_date(row.o_orderdate);

        output_file << row.l_orderkey << ","
                    << std::fixed << std::setprecision(4) << revenue_actual << ","
                    << date_str << ","
                    << row.o_shippriority << "\n";
    }

    output_file.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // ========================================================================
    // Cleanup (munmap)
    // ========================================================================
    munmap(customer_custkey_ptr, customer_size);
    munmap(customer_mktseg_ptr, mktseg_size);
    munmap(orders_orderkey_ptr, orders_size);
    munmap(orders_custkey_ptr, orders_custkey_size);
    munmap(orders_orderdate_ptr, orders_orderdate_size);
    munmap(orders_shippriority_ptr, orders_shippriority_size);
    munmap(lineitem_orderkey_ptr, lineitem_size);
    munmap(lineitem_shipdate_ptr, lineitem_shipdate_size);
    munmap(lineitem_extendedprice_ptr, lineitem_extendedprice_size);
    munmap(lineitem_discount_ptr, lineitem_discount_size);

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
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
