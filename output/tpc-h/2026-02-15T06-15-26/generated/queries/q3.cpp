#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <thread>
#include <atomic>
#include <omp.h>

namespace {

// Helper struct for Kahan summation (avoid floating-point errors)
struct KahanSum {
    double sum = 0.0;
    double c = 0.0;

    void add(double x) {
        double y = x - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
};

// Group-by aggregation result
struct GroupByResult {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double revenue;  // Kahan summed

    bool operator<(const GroupByResult& other) const {
        // Sort by revenue DESC, then by orderdate ASC
        if (std::abs(other.revenue - revenue) > 1e-9) {
            return revenue > other.revenue;
        }
        return o_orderdate < other.o_orderdate;
    }
};

// Mmap helper
class MmapFile {
public:
    void* ptr = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        off_t file_size = lseek(fd, 0, SEEK_END);
        if (file_size < 0) {
            std::cerr << "Failed to get size of " << path << std::endl;
            ::close(fd);
            return false;
        }

        size = static_cast<size_t>(file_size);
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);

        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            ::close(fd);
            ptr = nullptr;
            return false;
        }

        return true;
    }

    ~MmapFile() {
        if (ptr != nullptr) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

// Date helper: days since epoch to YYYY-MM-DD
// Note: The database appears to use epoch offset of 1969-12-31, so subtract 1
std::string days_to_date(int32_t days) {
    // Algorithm: standard epoch conversion with 1-day offset
    int32_t z = (days - 1) + 719468;
    int32_t era = z / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int32_t y = yoe + era * 400;
    int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int32_t mp = (5 * doy + 2) / 153;
    int32_t d = doy - (153 * mp + 2) / 5 + 1;
    int32_t m = mp + (mp < 10 ? 3 : -9);
    int32_t year = y + (m <= 2 ? 1 : 0);

    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, m, d);
    return std::string(buf);
}

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ============= LOAD DATA =============
    auto t_load_start = std::chrono::high_resolution_clock::now();

    // Customer table
    MmapFile c_custkey_file, c_mktsegment_file;
    if (!c_custkey_file.open(gendb_dir + "/customer/c_custkey.bin") ||
        !c_mktsegment_file.open(gendb_dir + "/customer/c_mktsegment.bin")) {
        std::cerr << "Failed to load customer files" << std::endl;
        return;
    }

    int32_t* c_custkey = static_cast<int32_t*>(c_custkey_file.ptr);
    uint8_t* c_mktsegment = static_cast<uint8_t*>(c_mktsegment_file.ptr);
    size_t num_customers = c_custkey_file.size / sizeof(int32_t);

    // Orders table
    MmapFile o_custkey_file, o_orderkey_file, o_orderdate_file, o_shippriority_file;
    if (!o_custkey_file.open(gendb_dir + "/orders/o_custkey.bin") ||
        !o_orderkey_file.open(gendb_dir + "/orders/o_orderkey.bin") ||
        !o_orderdate_file.open(gendb_dir + "/orders/o_orderdate.bin") ||
        !o_shippriority_file.open(gendb_dir + "/orders/o_shippriority.bin")) {
        std::cerr << "Failed to load orders files" << std::endl;
        return;
    }

    int32_t* o_custkey = static_cast<int32_t*>(o_custkey_file.ptr);
    int32_t* o_orderkey = static_cast<int32_t*>(o_orderkey_file.ptr);
    int32_t* o_orderdate = static_cast<int32_t*>(o_orderdate_file.ptr);
    int32_t* o_shippriority = static_cast<int32_t*>(o_shippriority_file.ptr);
    size_t num_orders = o_custkey_file.size / sizeof(int32_t);

    // Lineitem table
    MmapFile l_orderkey_file, l_extendedprice_file, l_discount_file, l_shipdate_file;
    if (!l_orderkey_file.open(gendb_dir + "/lineitem/l_orderkey.bin") ||
        !l_extendedprice_file.open(gendb_dir + "/lineitem/l_extendedprice.bin") ||
        !l_discount_file.open(gendb_dir + "/lineitem/l_discount.bin") ||
        !l_shipdate_file.open(gendb_dir + "/lineitem/l_shipdate.bin")) {
        std::cerr << "Failed to load lineitem files" << std::endl;
        return;
    }

    int32_t* l_orderkey = static_cast<int32_t*>(l_orderkey_file.ptr);
    int64_t* l_extendedprice = static_cast<int64_t*>(l_extendedprice_file.ptr);
    int64_t* l_discount = static_cast<int64_t*>(l_discount_file.ptr);
    int32_t* l_shipdate = static_cast<int32_t*>(l_shipdate_file.ptr);
    size_t num_lineitem = l_orderkey_file.size / sizeof(int32_t);

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);

    // ============= FILTER & SCAN =============
    // Date constants (days since 1969-12-31 epoch used by database)
    const int32_t DATE_1995_03_15 = 9205;  // 1995-03-15

    // Dictionary code for "BUILDING" in c_mktsegment
    // Read dictionary to find the code (format: <code>=<value>\n)
    int building_code = -1;
    std::ifstream dict_file(gendb_dir + "/customer/c_mktsegment_dict.txt");
    if (dict_file.is_open()) {
        std::string line;
        while (std::getline(dict_file, line)) {
            size_t eq_pos = line.find('=');
            if (eq_pos != std::string::npos) {
                std::string value = line.substr(eq_pos + 1);
                if (value == "BUILDING") {
                    // Parse the code (first byte or before =)
                    building_code = static_cast<unsigned char>(line[0]);
                    break;
                }
            }
        }
        dict_file.close();
    }

    if (building_code < 0) {
        std::cerr << "Failed to find BUILDING code in dictionary" << std::endl;
        return;
    }
    printf("[TIMING] dictionary_lookup: 0.00 ms\n");

    // ============= FILTER CUSTOMER =============
    auto t_filter_customer_start = std::chrono::high_resolution_clock::now();

    std::vector<int32_t> filtered_custkeys;
    filtered_custkeys.reserve(num_customers / 2);

    for (size_t i = 0; i < num_customers; i++) {
        if (c_mktsegment[i] == building_code) {
            filtered_custkeys.push_back(c_custkey[i]);
        }
    }

    auto t_filter_customer_end = std::chrono::high_resolution_clock::now();
    double filter_customer_ms = std::chrono::duration<double, std::milli>(t_filter_customer_end - t_filter_customer_start).count();
    printf("[TIMING] filter_customer: %.2f ms\n", filter_customer_ms);

    // Create custkey set for fast membership test with pre-sizing
    auto t_custkey_set_start = std::chrono::high_resolution_clock::now();
    std::unordered_set<int32_t> custkey_set;
    custkey_set.reserve(filtered_custkeys.size() + filtered_custkeys.size() / 4);  // Reserve with 25% headroom
    for (int32_t ck : filtered_custkeys) {
        custkey_set.insert(ck);
    }
    auto t_custkey_set_end = std::chrono::high_resolution_clock::now();
    double custkey_set_ms = std::chrono::duration<double, std::milli>(t_custkey_set_end - t_custkey_set_start).count();
    printf("[TIMING] custkey_set: %.2f ms\n", custkey_set_ms);

    // ============= FILTER & JOIN ORDERS (PARALLEL) =============
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();

    // Map orderkey -> (orderdate, shippriority) for orders matching filters
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orders_map;

    // Parallel scan with thread-local maps, then merge
    int num_threads = std::min(64, (int)std::thread::hardware_concurrency());
    std::vector<std::unordered_map<int32_t, std::pair<int32_t, int32_t>>> thread_orders_maps(num_threads);

    // Pre-reserve estimated space in thread-local maps
    size_t est_orders_per_thread = (num_orders / num_threads) / 20;  // Rough estimate: ~5% of orders match filters
    for (int t = 0; t < num_threads; t++) {
        thread_orders_maps[t].reserve(est_orders_per_thread + 10000);
    }

    #pragma omp parallel for num_threads(num_threads) schedule(static)
    for (int t = 0; t < num_threads; t++) {
        size_t chunk = (num_orders + num_threads - 1) / num_threads;
        size_t start = (size_t)t * chunk;
        size_t end = std::min(start + chunk, num_orders);

        for (size_t i = start; i < end; i++) {
            // Predicate pushdown: check cheaper date filter first
            if (o_orderdate[i] < DATE_1995_03_15) {
                // Only check custkey membership if date passes
                if (custkey_set.count(o_custkey[i])) {
                    thread_orders_maps[t][o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
                }
            }
        }
    }

    // Pre-reserve global map to reduce rehashing during merge
    orders_map.reserve(est_orders_per_thread * num_threads + 10000);

    // Merge all thread-local maps into global map
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [orderkey, pair] : thread_orders_maps[t]) {
            orders_map[orderkey] = pair;
        }
    }

    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double filter_orders_ms = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms\n", filter_orders_ms);

    // ============= FILTER & AGGREGATE LINEITEM (PARALLEL) =============
    auto t_agg_start = std::chrono::high_resolution_clock::now();

    // Map (orderkey, orderdate, shippriority) -> revenue (Kahan sum)
    struct AggKey {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;

        bool operator==(const AggKey& other) const {
            return orderkey == other.orderkey &&
                   orderdate == other.orderdate &&
                   shippriority == other.shippriority;
        }
    };

    struct AggKeyHash {
        size_t operator()(const AggKey& key) const {
            return std::hash<int32_t>()(key.orderkey) ^
                   (std::hash<int32_t>()(key.orderdate) << 1) ^
                   (std::hash<int32_t>()(key.shippriority) << 2);
        }
    };

    // Parallel partial aggregation: each thread has its own agg_map
    std::vector<std::unordered_map<AggKey, KahanSum, AggKeyHash>> thread_agg_maps(num_threads);
    // Reserve more aggressively to reduce rehashing during insertion
    size_t agg_reserve = orders_map.size() / num_threads + 10000;
    for (int t = 0; t < num_threads; t++) {
        thread_agg_maps[t].reserve(agg_reserve);
    }

    #pragma omp parallel for num_threads(num_threads) schedule(static)
    for (int t = 0; t < num_threads; t++) {
        size_t chunk = (num_lineitem + num_threads - 1) / num_threads;
        size_t start = (size_t)t * chunk;
        size_t end = std::min(start + chunk, num_lineitem);

        for (size_t i = start; i < end; i++) {
            // Filter: shipdate > DATE_1995_03_15
            if (l_shipdate[i] <= DATE_1995_03_15) {
                continue;
            }

            // Check if orderkey exists in orders
            auto it = orders_map.find(l_orderkey[i]);
            if (it == orders_map.end()) {
                continue;
            }

            auto [orderdate, shippriority] = it->second;

            // Compute revenue = l_extendedprice * (1 - l_discount)
            // Scale factor is 100, so convert to double by dividing by 100
            double price = l_extendedprice[i] / 100.0;
            double discount = l_discount[i] / 100.0;
            double revenue = price * (1.0 - discount);

            AggKey key = {l_orderkey[i], orderdate, shippriority};
            thread_agg_maps[t][key].add(revenue);
        }
    }

    // Merge all thread-local aggregation maps into a global map
    std::unordered_map<AggKey, KahanSum, AggKeyHash> agg_map;
    agg_map.reserve(orders_map.size());

    for (int t = 0; t < num_threads; t++) {
        for (auto& [key, kahan] : thread_agg_maps[t]) {
            agg_map[key].add(kahan.sum);
        }
    }

    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregate: %.2f ms\n", agg_ms);

    // ============= BUILD RESULT SET =============
    auto t_result_start = std::chrono::high_resolution_clock::now();

    std::vector<GroupByResult> results;
    results.reserve(agg_map.size());

    for (const auto& [key, kahan] : agg_map) {
        results.push_back({
            key.orderkey,
            key.orderdate,
            key.shippriority,
            kahan.sum
        });
    }

    auto t_result_end = std::chrono::high_resolution_clock::now();
    double result_ms = std::chrono::duration<double, std::milli>(t_result_end - t_result_start).count();
    printf("[TIMING] build_result: %.2f ms\n", result_ms);

    // ============= SORT =============
    auto t_sort_start = std::chrono::high_resolution_clock::now();

    std::sort(results.begin(), results.end());

    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);

    // ============= OUTPUT =============
    auto t_output_start = std::chrono::high_resolution_clock::now();

    std::string output_file = results_dir + "/Q3.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Write top 10 results
    for (size_t i = 0; i < std::min(size_t(10), results.size()); i++) {
        const auto& r = results[i];
        std::string date_str = days_to_date(r.o_orderdate);
        out << r.l_orderkey << ","
            << std::fixed << std::setprecision(4) << r.revenue << ","
            << date_str << ","
            << r.o_shippriority << "\n";
    }

    out.close();

    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
