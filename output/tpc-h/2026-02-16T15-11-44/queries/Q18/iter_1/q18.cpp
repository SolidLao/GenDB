#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <omp.h>

// ===== QUERY PLAN =====
//
// Logical Plan:
//   1. Subquery: Filter lineitem by grouping on l_orderkey, keeping groups with SUM(l_quantity) > 300
//      Input: 60M rows → Output: ~3K-4K qualifying orderkeys
//   2. Filter orders: o_orderkey IN (subquery result) → ~3K rows
//   3. Filter lineitem: l_orderkey IN (subquery result) → ~12K rows
//   4. Join orders ⋈ lineitem on o_orderkey = l_orderkey
//   5. Join result ⋈ customer on o_custkey = c_custkey
//   6. Final aggregation: GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
//      Compute SUM(l_quantity) per group
//   7. Sort by o_totalprice DESC, o_orderdate
//   8. LIMIT 100
//
// Physical Plan:
//   - Step 1: Full scan lineitem + parallel hash aggregation by orderkey
//            Parallel across 64 cores with thread-local aggregation buffers
//            Filter HAVING SUM(quantity) > 300 → store qualifying orderkeys in set
//   - Step 2: Full scan orders, filter by orderkey membership in set
//   - Step 3: Hash join: build on filtered_orders (small side, ~3K rows)
//            probe with filtered_lineitem (larger side, ~12K rows)
//   - Step 4: Hash join: build on join result, probe customer by custkey
//   - Step 5: Final aggregation using open-addressing hash table
//   - Step 6: Create result vector, sort by (o_totalprice DESC, o_orderdate)
//   - Step 7: Write top 100 rows to CSV

// ===== HELPER STRUCTURES =====

struct ResultRow {
    int32_t c_name_code;  // dictionary code for c_name
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;  // scaled by 100
    int64_t sum_qty;       // scaled by 100
};

struct GroupKey {
    int32_t c_name_code;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;

    bool operator==(const GroupKey& other) const {
        return c_name_code == other.c_name_code &&
               c_custkey == other.c_custkey &&
               o_orderkey == other.o_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_totalprice == other.o_totalprice;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        // Composite hash from all fields
        size_t h = 0;
        h ^= std::hash<int32_t>()(k.c_name_code) + 0x9E3779B9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_custkey) + 0x9E3779B9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.o_orderkey) + 0x9E3779B9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.o_orderdate) + 0x9E3779B9 + (h << 6) + (h >> 2);
        h ^= std::hash<int64_t>()(k.o_totalprice) + 0x9E3779B9 + (h << 6) + (h >> 2);
        return h;
    }
};

// Load dictionary file: format "code=value\n"
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_file) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_file);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_file << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// Convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Jan 1, 1970 is day 0
    // Calculate year, month, day from days since epoch
    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int year = 1970;
    int remaining = days;

    while (true) {
        int days_this_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (remaining < days_this_year) break;
        remaining -= days_this_year;
        year++;
    }

    int month = 0;
    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    for (int m = 0; m < 12; m++) {
        int days_this_month = days_in_month[m];
        if (m == 1 && is_leap) days_this_month = 29;
        if (remaining < days_this_month) {
            month = m + 1;
            break;
        }
        remaining -= days_this_month;
    }

    int day = remaining + 1;

    char buf[20];
    snprintf(buf, 20, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ===== LOAD DATA =====
#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load lineitem
    size_t lineitem_count = 59986052;
    int fd_l_orderkey = open((gendb_dir + "/lineitem/l_orderkey.bin").c_str(), O_RDONLY);
    int fd_l_quantity = open((gendb_dir + "/lineitem/l_quantity.bin").c_str(), O_RDONLY);

    int32_t* l_orderkey = (int32_t*)mmap(nullptr, lineitem_count * sizeof(int32_t), PROT_READ, MAP_SHARED, fd_l_orderkey, 0);
    int64_t* l_quantity = (int64_t*)mmap(nullptr, lineitem_count * sizeof(int64_t), PROT_READ, MAP_SHARED, fd_l_quantity, 0);

    // Load orders
    size_t orders_count = 15000000;
    int fd_o_orderkey = open((gendb_dir + "/orders/o_orderkey.bin").c_str(), O_RDONLY);
    int fd_o_custkey = open((gendb_dir + "/orders/o_custkey.bin").c_str(), O_RDONLY);
    int fd_o_orderdate = open((gendb_dir + "/orders/o_orderdate.bin").c_str(), O_RDONLY);
    int fd_o_totalprice = open((gendb_dir + "/orders/o_totalprice.bin").c_str(), O_RDONLY);

    int32_t* o_orderkey = (int32_t*)mmap(nullptr, orders_count * sizeof(int32_t), PROT_READ, MAP_SHARED, fd_o_orderkey, 0);
    int32_t* o_custkey = (int32_t*)mmap(nullptr, orders_count * sizeof(int32_t), PROT_READ, MAP_SHARED, fd_o_custkey, 0);
    int32_t* o_orderdate = (int32_t*)mmap(nullptr, orders_count * sizeof(int32_t), PROT_READ, MAP_SHARED, fd_o_orderdate, 0);
    int64_t* o_totalprice = (int64_t*)mmap(nullptr, orders_count * sizeof(int64_t), PROT_READ, MAP_SHARED, fd_o_totalprice, 0);

    // Load customer
    size_t customer_count = 1500000;
    int fd_c_custkey = open((gendb_dir + "/customer/c_custkey.bin").c_str(), O_RDONLY);
    int fd_c_name = open((gendb_dir + "/customer/c_name.bin").c_str(), O_RDONLY);

    int32_t* c_custkey = (int32_t*)mmap(nullptr, customer_count * sizeof(int32_t), PROT_READ, MAP_SHARED, fd_c_custkey, 0);
    int32_t* c_name = (int32_t*)mmap(nullptr, customer_count * sizeof(int32_t), PROT_READ, MAP_SHARED, fd_c_name, 0);

    // Load c_name dictionary
    std::unordered_map<int32_t, std::string> c_name_dict = load_dictionary(gendb_dir + "/customer/c_name_dict.txt");

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_data: %.2f ms\n", ms);
#endif

    // ===== STEP 1: FILTER LINEITEM BY SUBQUERY =====
    // Group lineitem by l_orderkey, compute SUM(l_quantity), keep where sum > 300
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();

    // Per-thread aggregation buffers for lineitem grouping
    // Pre-size hash tables to avoid repeated rehashing during insertion
    std::vector<std::unordered_map<int32_t, int64_t>> thread_agg(num_threads);
    for (auto& ht : thread_agg) {
        ht.reserve(100000);  // Expect ~100K-200K unique orderkeys per thread
    }

#pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < lineitem_count; i++) {
        int tid = omp_get_thread_num();
        thread_agg[tid][l_orderkey[i]] += l_quantity[i];
    }

    // Merge thread-local aggregations with parallel merge optimization
    // First pass: count unique keys to pre-size global table
    std::unordered_set<int32_t> all_keys;
    for (int t = 0; t < num_threads; t++) {
        for (auto& p : thread_agg[t]) {
            all_keys.insert(p.first);
        }
    }

    std::unordered_map<int32_t, int64_t> global_agg;
    global_agg.reserve(all_keys.size());

    // Second pass: merge with pre-sized global table
    for (int t = 0; t < num_threads; t++) {
        for (auto& p : thread_agg[t]) {
            global_agg[p.first] += p.second;
        }
    }

    // Filter by HAVING SUM(l_quantity) > 300 (stored as 30000 due to scale factor 100)
    // Use unordered_set with pre-sizing for faster lookup
    std::unordered_set<int32_t> qualifying_orderkeys;
    const int64_t threshold = 30000;  // 300 * 100

    // Count qualifying keys first
    size_t num_qualifying = 0;
    for (auto& p : global_agg) {
        if (p.second > threshold) {
            num_qualifying++;
        }
    }
    qualifying_orderkeys.reserve(num_qualifying);

    // Insert qualifying keys
    for (auto& p : global_agg) {
        if (p.second > threshold) {
            qualifying_orderkeys.insert(p.first);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] subquery_filter: %.2f ms (found %zu qualifying orders)\n", ms, qualifying_orderkeys.size());
#endif

    // ===== STEP 2: BUILD HASH TABLE OF ORDERS MATCHING SUBQUERY =====
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Store filtered orders: orderkey -> (custkey, orderdate, totalprice)
    struct OrderData {
        int32_t o_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
    };

    std::unordered_map<int32_t, OrderData> filtered_orders;
    filtered_orders.reserve(qualifying_orderkeys.size());  // Pre-size based on qualifying orders count

    for (size_t i = 0; i < orders_count; i++) {
        if (qualifying_orderkeys.count(o_orderkey[i])) {
            filtered_orders[o_orderkey[i]] = {o_custkey[i], o_orderdate[i], o_totalprice[i]};
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_orders: %.2f ms (found %zu orders)\n", ms, filtered_orders.size());
#endif

    // ===== STEP 3: BUILD CUSTOMER LOOKUP TABLE =====
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> customer_name_lookup;  // custkey -> name_code
    customer_name_lookup.reserve(customer_count);  // Pre-size to customer_count

    for (size_t i = 0; i < customer_count; i++) {
        customer_name_lookup[c_custkey[i]] = c_name[i];
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_customer_lookup: %.2f ms\n", ms);
#endif

    // ===== STEP 4: FINAL AGGREGATION AND JOIN =====
    // JOIN: filtered_orders ⋈ lineitem ON o_orderkey = l_orderkey
    //       ⋈ customer ON o_custkey = c_custkey
    // GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
    // SUM(l_quantity)

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local aggregation buffers to avoid lock contention
    std::vector<std::unordered_map<GroupKey, int64_t, GroupKeyHash>> thread_final_agg(num_threads);
    for (auto& ht : thread_final_agg) {
        ht.reserve(10000);  // Pre-size: expect ~10K-20K final groups
    }

#pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < lineitem_count; i++) {
        int32_t orderkey = l_orderkey[i];

        // Check if this orderkey is in the filtered set
        if (qualifying_orderkeys.count(orderkey)) {
            // Look up order details
            auto it = filtered_orders.find(orderkey);
            if (it != filtered_orders.end()) {
                int32_t custkey = it->second.o_custkey;
                int32_t orderdate = it->second.o_orderdate;
                int64_t totalprice = it->second.o_totalprice;

                // Look up customer name
                auto cust_it = customer_name_lookup.find(custkey);
                if (cust_it != customer_name_lookup.end()) {
                    int32_t name_code = cust_it->second;

                    GroupKey key{name_code, custkey, orderkey, orderdate, totalprice};

                    // Use thread-local buffer - no synchronization needed
                    int tid = omp_get_thread_num();
                    thread_final_agg[tid][key] += l_quantity[i];
                }
            }
        }
    }

    // Merge thread-local aggregations into global result
    std::unordered_map<GroupKey, int64_t, GroupKeyHash> final_agg;
    for (int t = 0; t < num_threads; t++) {
        for (auto& p : thread_final_agg[t]) {
            final_agg[p.first] += p.second;
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] aggregation: %.2f ms (%zu groups)\n", ms, final_agg.size());
#endif

    // ===== STEP 5: CREATE RESULT VECTOR AND SORT =====
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    for (auto& p : final_agg) {
        results.push_back({
            p.first.c_name_code,
            p.first.c_custkey,
            p.first.o_orderkey,
            p.first.o_orderdate,
            p.first.o_totalprice,
            p.second
        });
    }

    // Sort by o_totalprice DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.o_totalprice != b.o_totalprice) {
            return a.o_totalprice > b.o_totalprice;  // DESC
        }
        return a.o_orderdate < b.o_orderdate;  // ASC
    });

    // Take top 100
    if (results.size() > 100) {
        results.resize(100);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // ===== STEP 6: OUTPUT CSV =====
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q18.csv";
    std::ofstream out(output_path);

    // Header
    out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";

    // Rows
    for (const auto& row : results) {
        std::string c_name = "UNKNOWN";
        if (c_name_dict.count(row.c_name_code)) {
            c_name = c_name_dict[row.c_name_code];
        }

        std::string date_str = format_date(row.o_orderdate);

        // Format decimal values with 2 decimal places
        double totalprice_val = static_cast<double>(row.o_totalprice) / 100.0;
        double sum_qty_val = static_cast<double>(row.sum_qty) / 100.0;

        out << c_name << ","
            << row.c_custkey << ","
            << row.o_orderkey << ","
            << date_str << ","
            << std::fixed << std::setprecision(2) << totalprice_val << ","
            << std::fixed << std::setprecision(2) << sum_qty_val << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    // ===== CLEANUP =====
    munmap(l_orderkey, lineitem_count * sizeof(int32_t));
    munmap(l_quantity, lineitem_count * sizeof(int64_t));
    munmap(o_orderkey, orders_count * sizeof(int32_t));
    munmap(o_custkey, orders_count * sizeof(int32_t));
    munmap(o_orderdate, orders_count * sizeof(int32_t));
    munmap(o_totalprice, orders_count * sizeof(int64_t));
    munmap(c_custkey, customer_count * sizeof(int32_t));
    munmap(c_name, customer_count * sizeof(int32_t));

    close(fd_l_orderkey);
    close(fd_l_quantity);
    close(fd_o_orderkey);
    close(fd_o_custkey);
    close(fd_o_orderdate);
    close(fd_o_totalprice);
    close(fd_c_custkey);
    close(fd_c_name);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
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
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
