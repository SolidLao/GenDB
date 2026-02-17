// Q18: Large Volume Customer
// Correctness-focused implementation based on reference solution
// with performance optimizations

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>

// Helper: mmap a binary column file
template<typename T>
T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        exit(1);
    }
    return static_cast<T*>(addr);
}

// Load reverse dictionary (code -> string)
std::vector<std::string> load_dict_reverse(const std::string& dict_path) {
    std::vector<std::string> rev;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        return rev;
    }
    std::string line;
    while (std::getline(file, line)) {
        rev.push_back(line);
    }
    return rev;
}

// Convert epoch days to YYYY-MM-DD
std::string epoch_to_date(int32_t epoch_day) {
    int year = 1970;
    int days_remaining = epoch_day;

    // Find year
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days_remaining < days_in_year) break;
        days_remaining -= days_in_year;
        year++;
    }

    // Find month and day
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[2] = 29;
    }

    int month = 1;
    for (month = 1; month <= 12; month++) {
        if (days_remaining < days_in_month[month]) {
            break;
        }
        days_remaining -= days_in_month[month];
    }

    int day_of_month = days_remaining + 1;  // 1-indexed

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day_of_month);
    return std::string(buf);
}

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Step 1: Load lineitem columns
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t li_orderkey_count, li_qty_count;
    int32_t* li_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", li_orderkey_count);
    int64_t* li_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", li_qty_count);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", ms_load);
    #endif

    // Step 2: Subquery - aggregate lineitem by orderkey, find qualifying orderkeys
    // GROUP BY l_orderkey HAVING SUM(l_quantity) > 300 (scaled: > 30000)
    #ifdef GENDB_PROFILE
    auto t_subq_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int64_t> lineitem_agg;
    lineitem_agg.reserve(15000000);

    for (size_t i = 0; i < li_orderkey_count; i++) {
        lineitem_agg[li_orderkey[i]] += li_quantity[i];
    }

    std::unordered_set<int32_t> qualifying_orderkeys;
    qualifying_orderkeys.reserve(1000);

    for (const auto& entry : lineitem_agg) {
        if (entry.second > 30000) {  // 300 * 100
            qualifying_orderkeys.insert(entry.first);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_subq_end = std::chrono::high_resolution_clock::now();
    double ms_subq = std::chrono::duration<double, std::milli>(t_subq_end - t_subq_start).count();
    printf("[TIMING] subquery_aggregation: %.2f ms (found %zu qualifying orderkeys)\n",
           ms_subq, qualifying_orderkeys.size());
    #endif

    // Step 3: Load orders and filter by qualifying orderkeys
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t ord_count;
    int32_t* ord_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", ord_count);
    int32_t* ord_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", ord_count);
    int32_t* ord_orderdate = mmap_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", ord_count);
    int64_t* ord_totalprice = mmap_column<int64_t>(gendb_dir + "/orders/o_totalprice.bin", ord_count);

    // Build hash map of qualifying orders
    struct OrderInfo {
        int32_t o_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
    };
    std::unordered_map<int32_t, OrderInfo> orders_map;
    orders_map.reserve(qualifying_orderkeys.size());

    for (size_t i = 0; i < ord_count; i++) {
        int32_t ok = ord_orderkey[i];
        if (qualifying_orderkeys.count(ok)) {
            orders_map[ok] = {ord_custkey[i], ord_orderdate[i], ord_totalprice[i]};
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms (found %zu qualifying orders)\n",
           ms_orders, orders_map.size());
    #endif

    // Step 4: Load customer data
    #ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t cust_count;
    int32_t* cust_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", cust_count);
    int32_t* cust_name_code = mmap_column<int32_t>(gendb_dir + "/customer/c_name.bin", cust_count);

    // Load reverse dictionary for customer names
    std::vector<std::string> c_name_dict = load_dict_reverse(gendb_dir + "/customer/c_name_dict.txt");

    // Build customer hash table
    std::unordered_map<int32_t, int32_t> customer_hash;
    customer_hash.reserve(cust_count);
    for (size_t i = 0; i < cust_count; i++) {
        customer_hash[cust_custkey[i]] = cust_name_code[i];
    }

    #ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double ms_cust = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms_cust);
    #endif

    // Step 5: Join lineitem with filtered orders and customer, then aggregate
    #ifdef GENDB_PROFILE
    auto t_join_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    // Final aggregation key
    struct FinalAggKey {
        int32_t custkey;
        int32_t orderkey;
        int32_t orderdate;
        int64_t totalprice;
        int32_t name_code;

        bool operator==(const FinalAggKey& other) const {
            return custkey == other.custkey && orderkey == other.orderkey &&
                   orderdate == other.orderdate && totalprice == other.totalprice &&
                   name_code == other.name_code;
        }
    };

    struct FinalAggKeyHash {
        size_t operator()(const FinalAggKey& k) const {
            size_t h = 0;
            h ^= std::hash<int32_t>()(k.custkey) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<int32_t>()(k.orderkey) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<int32_t>()(k.orderdate) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<int64_t>()(k.totalprice) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<int32_t>()(k.name_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };

    std::unordered_map<FinalAggKey, int64_t, FinalAggKeyHash> final_agg;
    final_agg.reserve(qualifying_orderkeys.size());

    for (size_t i = 0; i < li_orderkey_count; i++) {
        int32_t orderkey = li_orderkey[i];

        // Check if orderkey qualifies
        if (!qualifying_orderkeys.count(orderkey)) continue;

        // Lookup order data
        auto order_it = orders_map.find(orderkey);
        if (order_it == orders_map.end()) continue;

        int32_t custkey = order_it->second.o_custkey;

        // Lookup customer name code
        auto customer_it = customer_hash.find(custkey);
        if (customer_it == customer_hash.end()) continue;

        int32_t name_code = customer_it->second;

        // Insert into final aggregation
        FinalAggKey agg_key = {
            custkey,
            orderkey,
            order_it->second.o_orderdate,
            order_it->second.o_totalprice,
            name_code
        };

        final_agg[agg_key] += li_quantity[i];
    }

    #ifdef GENDB_PROFILE
    auto t_join_agg_end = std::chrono::high_resolution_clock::now();
    double ms_join_agg = std::chrono::duration<double, std::milli>(t_join_agg_end - t_join_agg_start).count();
    printf("[TIMING] join_aggregation: %.2f ms (aggregated %zu groups)\n",
           ms_join_agg, final_agg.size());
    #endif

    // Step 6: Sort and limit to top 100
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    struct ResultRow {
        std::string c_name;
        int32_t c_custkey;
        int32_t o_orderkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
        int64_t sum_qty;
    };

    std::vector<ResultRow> results;
    results.reserve(final_agg.size());

    for (const auto& entry : final_agg) {
        std::string name = (entry.first.name_code < (int32_t)c_name_dict.size())
            ? c_name_dict[entry.first.name_code]
            : "UNKNOWN";
        results.push_back({
            name,
            entry.first.custkey,
            entry.first.orderkey,
            entry.first.orderdate,
            entry.first.totalprice,
            entry.second
        });
    }

    // Sort: ORDER BY o_totalprice DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.o_totalprice != b.o_totalprice)
            return a.o_totalprice > b.o_totalprice; // DESC
        return a.o_orderdate < b.o_orderdate; // ASC
    });

    // Apply LIMIT 100
    if (results.size() > 100) {
        results.resize(100);
    }

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

    // Step 7: Write output
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q18.csv");
    out << std::fixed << std::setprecision(2);
    out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";

    for (const auto& row : results) {
        out << row.c_name << ","
            << row.c_custkey << ","
            << row.o_orderkey << ","
            << epoch_to_date(row.o_orderdate) << ","
            << (row.o_totalprice / 100.0) << ","
            << (row.sum_qty / 100.0) << "\n";
    }

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
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
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
