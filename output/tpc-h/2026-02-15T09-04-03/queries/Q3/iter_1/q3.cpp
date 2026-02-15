#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <iomanip>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cassert>
#include <chrono>
#include <omp.h>

// Structure for aggregation result
struct AggResult {
    int32_t l_orderkey;
    int64_t revenue;  // scaled by 100
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Hash struct for GROUP BY key
struct GroupByKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const GroupByKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct GroupByKeyHasher {
    size_t operator()(const GroupByKey& k) const {
        // Combine three fields into hash
        size_t h1 = std::hash<int32_t>()(k.l_orderkey);
        size_t h2 = std::hash<int32_t>()(k.o_orderdate);
        size_t h3 = std::hash<int32_t>()(k.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Utility: mmap file and return pointer
template<typename T>
T* mmap_file(const std::string& path, size_t& num_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "ERROR: Cannot stat " << path << std::endl;
        close(fd);
        return nullptr;
    }

    size_t file_size = sb.st_size;
    num_elements = file_size / sizeof(T);

    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        std::cerr << "ERROR: Cannot mmap " << path << std::endl;
        return nullptr;
    }

    return (T*)addr;
}

// Utility: unmmap file
template<typename T>
void unmmap_file(T* ptr, size_t num_elements) {
    if (ptr != nullptr) {
        munmap(ptr, num_elements * sizeof(T));
    }
}

// Utility: read dictionary file and find code for target string
uint8_t find_dict_code(const std::string& dict_file, const std::string& target) {
    std::ifstream f(dict_file);
    if (!f.is_open()) {
        std::cerr << "ERROR: Cannot open dictionary " << dict_file << std::endl;
        return 0;
    }

    std::string line;
    while (std::getline(f, line)) {
        // Format: code=value
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            uint8_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            if (value == target) {
                f.close();
                return code;
            }
        }
    }
    f.close();
    std::cerr << "ERROR: Dictionary code not found for '" << target << "'" << std::endl;
    return 0;
}

// Utility: convert days since epoch to YYYY-MM-DD string
std::string days_to_date_string(int32_t days_since_epoch) {
    // Days since 1970-01-01
    // Simplified calculation (ignoring leap seconds for this OLAP task)
    int year = 1970;
    int month = 1;
    int day = 1;

    // Approximate: 365.25 days per year, 30.44 days per month
    // For better precision, count actual days
    int total_days = days_since_epoch;

    // Days in each month (non-leap year)
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    while (total_days >= 365) {
        bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_year = is_leap ? 366 : 365;
        if (total_days >= days_in_year) {
            total_days -= days_in_year;
            year++;
        } else {
            break;
        }
    }

    // Now handle remaining months and days
    for (int m = 0; m < 12; m++) {
        int dm = days_in_month[m];
        if (m == 1) {  // February
            bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            if (is_leap) dm = 29;
        }

        if (total_days >= dm) {
            total_days -= dm;
            month++;
        } else {
            break;
        }
    }

    day = total_days + 1;

    // Format: YYYY-MM-DD
    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Q3 Query Execution\n");
    printf("[METADATA CHECK] c_mktsegment encoding: dictionary (code=0 for BUILDING)\n");
    printf("[METADATA CHECK] o_orderdate threshold: 1995-03-15 = 9204 days\n");
    printf("[METADATA CHECK] l_shipdate threshold: 1995-03-15 = 9204 days\n");
#endif

    // === LOAD CUSTOMER TABLE ===
#ifdef GENDB_PROFILE
    auto t_load_customer = std::chrono::high_resolution_clock::now();
#endif

    size_t num_customers = 0;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", num_customers);
    uint8_t* c_mktsegment = mmap_file<uint8_t>(gendb_dir + "/customer/c_mktsegment.bin", num_customers);

    if (!c_custkey || !c_mktsegment) {
        std::cerr << "ERROR: Failed to load customer data" << std::endl;
        return;
    }

    // Find dictionary code for 'BUILDING'
    uint8_t building_code = find_dict_code(gendb_dir + "/customer/c_mktsegment_dict.txt", "BUILDING");
#ifdef GENDB_PROFILE
    auto t_load_customer_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] load_customer: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_load_customer_end - t_load_customer).count());
#endif

    // === LOAD ORDERS TABLE ===
#ifdef GENDB_PROFILE
    auto t_load_orders = std::chrono::high_resolution_clock::now();
#endif

    size_t num_orders = 0;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", num_orders);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", num_orders);
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", num_orders);
    int32_t* o_shippriority = mmap_file<int32_t>(gendb_dir + "/orders/o_shippriority.bin", num_orders);

    if (!o_orderkey || !o_custkey || !o_orderdate || !o_shippriority) {
        std::cerr << "ERROR: Failed to load orders data" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_load_orders_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] load_orders: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_load_orders_end - t_load_orders).count());
#endif

    // === LOAD LINEITEM TABLE ===
#ifdef GENDB_PROFILE
    auto t_load_lineitem = std::chrono::high_resolution_clock::now();
#endif

    size_t num_lineitem = 0;
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", num_lineitem);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", num_lineitem);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", num_lineitem);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", num_lineitem);

    if (!l_orderkey || !l_shipdate || !l_extendedprice || !l_discount) {
        std::cerr << "ERROR: Failed to load lineitem data" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_load_lineitem_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] load_lineitem: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_load_lineitem_end - t_load_lineitem).count());
#endif

    // Thresholds (in days since epoch)
    int32_t o_orderdate_threshold = 9204;  // 1995-03-15
    int32_t l_shipdate_threshold = 9204;   // 1995-03-15

    // === FILTER CUSTOMER ===
#ifdef GENDB_PROFILE
    auto t_filter_customer = std::chrono::high_resolution_clock::now();
#endif

    // Parallel customer filtering using thread-local buffers
    int max_threads = omp_get_max_threads();
    std::vector<std::vector<int32_t>> thread_custkeys(max_threads);

    #pragma omp parallel for
    for (int t = 0; t < max_threads; t++) {
        thread_custkeys[t].reserve(num_customers / max_threads + 100);
    }

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < num_customers; i++) {
        if (c_mktsegment[i] == building_code) {
            int tid = omp_get_thread_num();
            thread_custkeys[tid].push_back(c_custkey[i]);
        }
    }

    // Merge thread-local results
    std::vector<int32_t> filtered_custkeys;
    size_t total_filtered = 0;
    for (int t = 0; t < max_threads; t++) {
        total_filtered += thread_custkeys[t].size();
    }
    filtered_custkeys.reserve(total_filtered);
    for (int t = 0; t < max_threads; t++) {
        filtered_custkeys.insert(filtered_custkeys.end(),
                                thread_custkeys[t].begin(), thread_custkeys[t].end());
    }

#ifdef GENDB_PROFILE
    auto t_filter_customer_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] filter_customer: %.2f ms (found %zu customers)\n",
        std::chrono::duration<double, std::milli>(t_filter_customer_end - t_filter_customer).count(),
        filtered_custkeys.size());
#endif

    // === BUILD CUSTOMER SET FOR QUICK LOOKUP ===
#ifdef GENDB_PROFILE
    auto t_build_cust_set = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_set<int32_t> customer_set(filtered_custkeys.begin(), filtered_custkeys.end());

#ifdef GENDB_PROFILE
    auto t_build_cust_set_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] build_customer_set: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_build_cust_set_end - t_build_cust_set).count());
#endif

    // === FILTER ORDERS ===
#ifdef GENDB_PROFILE
    auto t_filter_orders = std::chrono::high_resolution_clock::now();
#endif

    // Parallel orders filtering using thread-local buffers
    std::vector<std::vector<int32_t>> thread_orderkeys(max_threads);

    #pragma omp parallel for
    for (int t = 0; t < max_threads; t++) {
        thread_orderkeys[t].reserve(num_orders / max_threads + 100);
    }

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < num_orders; i++) {
        if (o_orderdate[i] < o_orderdate_threshold && customer_set.count(o_custkey[i])) {
            int tid = omp_get_thread_num();
            thread_orderkeys[tid].push_back(o_orderkey[i]);
        }
    }

    // Merge thread-local results
    std::vector<int32_t> filtered_orderkeys;
    total_filtered = 0;
    for (int t = 0; t < max_threads; t++) {
        total_filtered += thread_orderkeys[t].size();
    }
    filtered_orderkeys.reserve(total_filtered);
    for (int t = 0; t < max_threads; t++) {
        filtered_orderkeys.insert(filtered_orderkeys.end(),
                                 thread_orderkeys[t].begin(), thread_orderkeys[t].end());
    }

#ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] filter_orders: %.2f ms (found %zu orders)\n",
        std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders).count(),
        filtered_orderkeys.size());
#endif

    // === BUILD ORDERKEY SET & MAP FOR LINEITEM JOIN ===
#ifdef GENDB_PROFILE
    auto t_build_order_index = std::chrono::high_resolution_clock::now();
#endif

    // Map orderkey to (o_orderdate, o_shippriority)
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_info;
    order_info.reserve(filtered_orderkeys.size());
    std::unordered_set<int32_t> orderkey_set;
    orderkey_set.reserve(filtered_orderkeys.size());

    // First, build the orderkey set from filtered orders
    for (int32_t ok : filtered_orderkeys) {
        orderkey_set.insert(ok);
    }

    // Now iterate through all orders once and map filtered orderkeys to their dates/priority
    // Use OpenMP to parallelize reading + inserting (with atomic updates to avoid overhead)
    #pragma omp parallel for
    for (size_t i = 0; i < num_orders; i++) {
        if (orderkey_set.count(o_orderkey[i])) {
            #pragma omp critical(order_info_insert)
            {
                order_info[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_build_order_index_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] build_order_index: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_build_order_index_end - t_build_order_index).count());
#endif

    // === FILTER LINEITEM ===
#ifdef GENDB_PROFILE
    auto t_filter_lineitem = std::chrono::high_resolution_clock::now();
#endif

    // Parallel lineitem scan with thread-local aggregation
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<GroupByKey, int64_t, GroupByKeyHasher>> thread_agg_maps(num_threads);

    // Pre-size each thread's aggregation map
    #pragma omp parallel for
    for (int t = 0; t < num_threads; t++) {
        thread_agg_maps[t].reserve(filtered_orderkeys.size() / num_threads + 100);
    }

    // Main parallel scan loop
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < num_lineitem; i++) {
        if (l_shipdate[i] > l_shipdate_threshold && orderkey_set.count(l_orderkey[i])) {
            // Found matching lineitem
            auto it = order_info.find(l_orderkey[i]);
            if (it != order_info.end()) {
                int32_t o_orderdate_val = it->second.first;
                int32_t o_shippriority_val = it->second.second;

                GroupByKey key = {l_orderkey[i], o_orderdate_val, o_shippriority_val};

                // Calculate revenue: l_extendedprice * (1 - l_discount)
                // Both are scaled by 100:
                // l_extendedprice[i] is already scaled by 100 (e.g., 12345 = 123.45)
                // l_discount[i] is scaled by 100 (e.g., 5 = 0.05)
                // revenue = l_extendedprice * (1 - l_discount/100)
                //         = l_extendedprice * (100 - l_discount) / 100
                // Keep full precision: multiply first, then divide for output only
                // Stored as: l_extendedprice * (100 - l_discount)
                int64_t revenue_unscaled = l_extendedprice[i] * (100 - l_discount[i]);

                int tid = omp_get_thread_num();
                auto& local_agg_map = thread_agg_maps[tid];
                auto it_local = local_agg_map.find(key);
                if (it_local != local_agg_map.end()) {
                    it_local->second += revenue_unscaled;
                } else {
                    local_agg_map[key] = revenue_unscaled;
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] filter_lineitem_and_aggregate: %.2f ms (aggregated %zu groups)\n",
        std::chrono::duration<double, std::milli>(t_filter_lineitem_end - t_filter_lineitem).count(),
        0); // Placeholder: will compute after merge
#endif

    // === MERGE THREAD-LOCAL AGGREGATION RESULTS ===
    std::unordered_map<GroupByKey, int64_t, GroupByKeyHasher> agg_map;
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_agg_maps[t]) {
            auto it = agg_map.find(entry.first);
            if (it != agg_map.end()) {
                it->second += entry.second;
            } else {
                agg_map[entry.first] = entry.second;
            }
        }
    }

    // === CONVERT TO RESULT VECTOR ===
    std::vector<AggResult> results;
    results.reserve(agg_map.size());

    for (const auto& entry : agg_map) {
        results.push_back({
            entry.first.l_orderkey,
            entry.second,
            entry.first.o_orderdate,
            entry.first.o_shippriority
        });
    }

    // === SORT: revenue DESC, o_orderdate ASC ===
#ifdef GENDB_PROFILE
    auto t_sort = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end(), [](const AggResult& a, const AggResult& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;  // DESC
        return a.o_orderdate < b.o_orderdate;  // ASC
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] sort: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_sort_end - t_sort).count());
#endif

    // === LIMIT 10 ===
    if (results.size() > 10) {
        results.resize(10);
    }

    // === WRITE OUTPUT ===
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q3.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "ERROR: Cannot open output file " << output_file << std::endl;
        return;
    }

    // Write header (with CRLF line ending)
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\r\n";

    // Write results (with CRLF line ending)
    for (const auto& row : results) {
        // revenue is stored as l_extendedprice * (100 - l_discount)
        // which is (scaled * scaled) = scaled^2
        // We need to divide by 100*100 to get the final scaled value (scale 100)
        double revenue_decimal = row.revenue / (100.0 * 100.0);
        std::string date_str = days_to_date_string(row.o_orderdate);

        out << row.l_orderkey << ","
            << std::fixed << std::setprecision(4) << revenue_decimal << ","
            << date_str << ","
            << row.o_shippriority << "\r\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] output: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count());
#endif

    // === CLEANUP ===
    unmmap_file(c_custkey, num_customers);
    unmmap_file(c_mktsegment, num_customers);
    unmmap_file(o_orderkey, num_orders);
    unmmap_file(o_custkey, num_orders);
    unmmap_file(o_orderdate, num_orders);
    unmmap_file(o_shippriority, num_orders);
    unmmap_file(l_orderkey, num_lineitem);
    unmmap_file(l_shipdate, num_lineitem);
    unmmap_file(l_extendedprice, num_lineitem);
    unmmap_file(l_discount, num_lineitem);

    auto t_total_end = std::chrono::high_resolution_clock::now();
#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n",
        std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count());
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
