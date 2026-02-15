#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <numeric>
#include <map>
#include <iomanip>
#include <omp.h>
#include <cmath>
#include <thread>

namespace {

// Constants for date conversion
constexpr int32_t DATE_1995_03_15 = 9204;  // 1995-03-15 epoch days
constexpr int32_t DATE_1992_01_01 = 8035;  // 1992-01-01 epoch days for reference

// Convert epoch days to YYYY-MM-DD string
// Algorithm from Wikipedia: "Julian day number"
std::string epoch_days_to_date(int32_t days) {
    // days since 1970-01-01
    // Reference: 1970-01-01 is day 0
    int32_t jdn = days + 2440588;  // Convert to Julian Day Number
    int32_t l = jdn + 68569;
    int32_t n = (4 * l) / 146097;
    l = l - (146097 * n + 3) / 4;
    int32_t i = (4000 * (l + 1)) / 1461001;
    l = l - (1461 * i) / 4 + 31;
    int32_t j = (80 * l) / 2447;
    int32_t day = l - (2447 * j) / 80;
    l = j / 11;
    int32_t month = j + 2 - (12 * l);
    int32_t year = 100 * (n - 49) + i + l;

    char buf[32];
    snprintf(buf, 32, "%04d-%02d-%02d", (int)year, (int)month, (int)day);
    return std::string(buf);
}

// Helper to parse dictionary from text file
// Format: "0=BUILDING", "1=AUTOMOBILE", etc.
std::vector<std::string> load_dictionary(const std::string& dict_path) {
    std::vector<std::string> result;
    std::ifstream f(dict_path);
    if (!f) return result;
    std::string line;
    while (std::getline(f, line)) {
        // Parse "code=value" format
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            std::string value = line.substr(eq_pos + 1);
            result.push_back(value);
        } else {
            result.push_back(line);  // fallback for old format
        }
    }
    return result;
}

// Safe mmap helper
void* safe_mmap(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    fstat(fd, &st);
    out_size = st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return ptr;
}

// Group key: (l_orderkey, o_orderdate, o_shippriority)
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
        size_t h = std::hash<int32_t>{}(k.l_orderkey);
        h ^= std::hash<int32_t>{}(k.o_orderdate) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>{}(k.o_shippriority) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

struct AggResult {
    double revenue;
};

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Load customer columns
    size_t n_customer = 0;
    auto t_load_start = std::chrono::high_resolution_clock::now();

    size_t c_custkey_size = 0, c_mktsegment_size = 0;
    int32_t* c_custkey = (int32_t*)safe_mmap(gendb_dir + "/customer/c_custkey.bin", c_custkey_size);
    uint8_t* c_mktsegment_codes = (uint8_t*)safe_mmap(gendb_dir + "/customer/c_mktsegment.bin", c_mktsegment_size);
    n_customer = c_custkey_size / sizeof(int32_t);

    auto c_mktsegment_dict = load_dictionary(gendb_dir + "/customer/c_mktsegment_dict.txt");

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms_load);

    // Load orders columns
    size_t n_orders = 0;
    auto t_load_orders_start = std::chrono::high_resolution_clock::now();

    size_t o_orderkey_size = 0, o_custkey_size = 0, o_orderdate_size = 0, o_shippriority_size = 0;
    int32_t* o_orderkey = (int32_t*)safe_mmap(gendb_dir + "/orders/o_orderkey.bin", o_orderkey_size);
    int32_t* o_custkey = (int32_t*)safe_mmap(gendb_dir + "/orders/o_custkey.bin", o_custkey_size);
    int32_t* o_orderdate = (int32_t*)safe_mmap(gendb_dir + "/orders/o_orderdate.bin", o_orderdate_size);
    int32_t* o_shippriority = (int32_t*)safe_mmap(gendb_dir + "/orders/o_shippriority.bin", o_shippriority_size);
    n_orders = o_orderkey_size / sizeof(int32_t);

    auto t_load_orders_end = std::chrono::high_resolution_clock::now();
    double ms_load_orders = std::chrono::duration<double, std::milli>(t_load_orders_end - t_load_orders_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms_load_orders);

    // Load lineitem columns
    size_t n_lineitem = 0;
    auto t_load_lineitem_start = std::chrono::high_resolution_clock::now();

    size_t l_orderkey_size = 0, l_extendedprice_size = 0, l_discount_size = 0, l_shipdate_size = 0;
    int32_t* l_orderkey = (int32_t*)safe_mmap(gendb_dir + "/lineitem/l_orderkey.bin", l_orderkey_size);
    int64_t* l_extendedprice = (int64_t*)safe_mmap(gendb_dir + "/lineitem/l_extendedprice.bin", l_extendedprice_size);
    int64_t* l_discount = (int64_t*)safe_mmap(gendb_dir + "/lineitem/l_discount.bin", l_discount_size);
    int32_t* l_shipdate = (int32_t*)safe_mmap(gendb_dir + "/lineitem/l_shipdate.bin", l_shipdate_size);
    n_lineitem = l_orderkey_size / sizeof(int32_t);

    auto t_load_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_load_lineitem = std::chrono::duration<double, std::milli>(t_load_lineitem_end - t_load_lineitem_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms_load_lineitem);

    // Find dictionary code for 'BUILDING'
    int building_code = -1;
    for (size_t i = 0; i < c_mktsegment_dict.size(); i++) {
        if (c_mktsegment_dict[i] == "BUILDING") {
            building_code = (int)i;
            break;
        }
    }

    // Phase 1: Filter customer by c_mktsegment = 'BUILDING'
    auto t_filter_customer_start = std::chrono::high_resolution_clock::now();
    std::vector<int32_t> customer_ids;
    customer_ids.reserve(n_customer / 5);  // ~22% selectivity
    for (size_t i = 0; i < n_customer; i++) {
        if ((int)c_mktsegment_codes[i] == building_code) {
            customer_ids.push_back(c_custkey[i]);
        }
    }
    auto t_filter_customer_end = std::chrono::high_resolution_clock::now();
    double ms_filter_customer = std::chrono::duration<double, std::milli>(t_filter_customer_end - t_filter_customer_start).count();
    printf("[TIMING] filter_customer: %.2f ms (matched %zu rows)\n", ms_filter_customer, customer_ids.size());

    // Phase 2: Build hash map of filtered customer IDs
    auto t_build_cust_hash_start = std::chrono::high_resolution_clock::now();
    std::unordered_set<int32_t> cust_set(customer_ids.begin(), customer_ids.end());
    auto t_build_cust_hash_end = std::chrono::high_resolution_clock::now();
    double ms_build_cust_hash = std::chrono::duration<double, std::milli>(t_build_cust_hash_end - t_build_cust_hash_start).count();
    printf("[TIMING] build_customer_hash: %.2f ms\n", ms_build_cust_hash);

    // Phase 3-5: Filter orders and build orderkey_info in single pass to avoid double-scan
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();

    // Build mapping of orderkey -> (orderdate, shippriority) directly in one scan
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orderkey_info;
    orderkey_info.reserve(n_orders / 3);

    for (size_t i = 0; i < n_orders; i++) {
        if (o_orderdate[i] < DATE_1995_03_15 && cust_set.count(o_custkey[i])) {
            orderkey_info[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
        }
    }

    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double ms_filter_orders = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms (matched %zu rows)\n", ms_filter_orders, orderkey_info.size());

    // Phase 4: Build hash set for quick existence check
    auto t_build_order_hash_start = std::chrono::high_resolution_clock::now();
    std::unordered_set<int32_t> order_key_set;
    order_key_set.reserve(orderkey_info.size());
    for (const auto& kv : orderkey_info) {
        order_key_set.insert(kv.first);
    }
    auto t_build_order_hash_end = std::chrono::high_resolution_clock::now();
    double ms_build_order_hash = std::chrono::duration<double, std::milli>(t_build_order_hash_end - t_build_order_hash_start).count();
    printf("[TIMING] build_order_hash: %.2f ms\n", ms_build_order_hash);

    // Phase 6: Filter lineitem and aggregate with parallel execution
    auto t_lineitem_agg_start = std::chrono::high_resolution_clock::now();

    // HDD: use 4 threads max to avoid seek storms
    int num_threads = std::min(4u, std::thread::hardware_concurrency());
    std::vector<std::unordered_map<GroupKey, AggResult, GroupKeyHash>> thread_agg_maps(num_threads);

    // Reserve space in each thread's map
    for (int t = 0; t < num_threads; t++) {
        thread_agg_maps[t].reserve(n_lineitem / (10 * num_threads) + 10000);
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t chunk_size = (n_lineitem + num_threads - 1) / num_threads;
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, n_lineitem);

            auto& local_agg = thread_agg_maps[t];

            for (size_t i = start; i < end; i++) {
                if (l_shipdate[i] <= DATE_1995_03_15) continue;  // l_shipdate > DATE_1995_03_15

                int32_t ok = l_orderkey[i];
                auto it = orderkey_info.find(ok);
                if (it == orderkey_info.end()) continue;

                int32_t od = it->second.first;
                int32_t sp = it->second.second;
                GroupKey key = {ok, od, sp};

                // Calculate revenue: l_extendedprice * (1 - l_discount)
                // Both are scaled by 100
                double ext_price = (double)l_extendedprice[i] / 100.0;
                double discount_factor = 1.0 - ((double)l_discount[i] / 100.0);
                double revenue_contribution = ext_price * discount_factor;

                auto it_agg = local_agg.find(key);
                if (it_agg == local_agg.end()) {
                    local_agg[key] = {revenue_contribution};
                } else {
                    it_agg->second.revenue += revenue_contribution;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Merge all thread-local aggregations
    std::unordered_map<GroupKey, AggResult, GroupKeyHash> agg_map;
    agg_map.reserve(n_lineitem / 10);

    for (int t = 0; t < num_threads; t++) {
        for (auto& kv : thread_agg_maps[t]) {
            auto it = agg_map.find(kv.first);
            if (it == agg_map.end()) {
                agg_map[kv.first] = kv.second;
            } else {
                it->second.revenue += kv.second.revenue;
            }
        }
    }

    auto t_lineitem_agg_end = std::chrono::high_resolution_clock::now();
    double ms_lineitem_agg = std::chrono::duration<double, std::milli>(t_lineitem_agg_end - t_lineitem_agg_start).count();
    printf("[TIMING] filter_and_aggregate_lineitem: %.2f ms (groups: %zu)\n", ms_lineitem_agg, agg_map.size());

    // Phase 7: Convert to result vector and sort
    auto t_sort_start = std::chrono::high_resolution_clock::now();

    struct Result {
        int32_t l_orderkey;
        double revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<Result> results;
    results.reserve(agg_map.size());
    for (auto& kv : agg_map) {
        results.push_back({
            kv.first.l_orderkey,
            kv.second.revenue,
            kv.first.o_orderdate,
            kv.first.o_shippriority
        });
    }

    // Sort by revenue DESC, then by o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    // Keep only top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_and_limit: %.2f ms\n", ms_sort);

    // Phase 8: Output results
    auto t_output_start = std::chrono::high_resolution_clock::now();

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (const auto& res : results) {
        out << res.l_orderkey << ","
            << std::fixed << std::setprecision(4) << res.revenue << ","
            << epoch_days_to_date(res.o_orderdate) << ","
            << res.o_shippriority << "\n";
    }
    out.close();

    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);

    // Cleanup
    munmap(c_custkey, c_custkey_size);
    munmap(c_mktsegment_codes, c_mktsegment_size);
    munmap(o_orderkey, o_orderkey_size);
    munmap(o_custkey, o_custkey_size);
    munmap(o_orderdate, o_orderdate_size);
    munmap(o_shippriority, o_shippriority_size);
    munmap(l_orderkey, l_orderkey_size);
    munmap(l_extendedprice, l_extendedprice_size);
    munmap(l_discount, l_discount_size);
    munmap(l_shipdate, l_shipdate_size);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    printf("Results written to %s\n", output_path.c_str());
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
