#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ============================================================================
// Date Conversion Helper (from Howard Hinnant's date library)
// ============================================================================
struct DateConverter {
    static std::string epoch_days_to_date(int32_t days_since_epoch) {
        // Algorithm from Howard Hinnant's date library for efficient epoch-to-date conversion
        int32_t z = days_since_epoch + 719468;  // Days from 0000-01-01 to 1970-01-01

        const int32_t era = z / 146097;
        const int32_t doe = z % 146097;

        const int32_t yoe = (doe - doe / 1461 + doe / 36524 - doe / 146096) / 365;
        const int32_t y = yoe + era * 400;
        const int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        const int32_t mp = (5 * doy + 2) / 153;
        const int32_t d = doy - (153 * mp + 2) / 5 + 1;
        const int32_t m = mp + (mp < 10 ? 3 : -9);
        const int32_t year = y + (m <= 2 ? 1 : 0);

        char buf[32];
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d", (int)year, (int)m, (int)d);
        return std::string(buf);
    }
};

// ============================================================================
// Memory Mapped File Helper
// ============================================================================
class MmapFile {
public:
    void* ptr = nullptr;
    size_t size = 0;
    int fd = -1;

    ~MmapFile() { close_file(); }

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        size = lseek(fd, 0, SEEK_END);
        if (size == (size_t)-1) {
            std::cerr << "Failed to seek " << path << std::endl;
            ::close(fd);
            return false;
        }

        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            ::close(fd);
            fd = -1;
            return false;
        }

        return true;
    }

    void close_file() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
            ptr = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }
};

// ============================================================================
// Dictionary Helper
// ============================================================================
uint8_t load_dictionary_code(const std::string& gendb_dir, const std::string& table,
                             const std::string& col, const std::string& target_value) {
    std::string dict_path = gendb_dir + "/" + table + "/" + col + "_dict.txt";
    std::ifstream ifs(dict_path);
    if (!ifs) {
        std::cerr << "Cannot open dictionary: " << dict_path << std::endl;
        return 0; // Will not match
    }

    std::string line;
    while (std::getline(ifs, line)) {
        // Format: "code=value"
        size_t eq_pos = line.find('=');
        if (eq_pos == std::string::npos) continue;

        uint8_t code = std::stoul(line.substr(0, eq_pos));
        std::string value = line.substr(eq_pos + 1);

        if (value == target_value) return code;
    }
    return 255; // Not found
}

// ============================================================================
// Main Query Function
// ============================================================================
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    // ========================================================================
    // METADATA CHECK
    // ========================================================================
    printf("[METADATA CHECK] Q3 Shipping Priority Query\n");
    printf("[METADATA CHECK] Tables: customer, orders, lineitem\n");
    printf("[METADATA CHECK] Filters: c_mktsegment='BUILDING', o_orderdate < 1995-03-15, l_shipdate > 1995-03-15\n");
    printf("[METADATA CHECK] Aggregation: GROUP BY (l_orderkey, o_orderdate, o_shippriority)\n");
    printf("[METADATA CHECK] Output: revenue DESC, o_orderdate ASC, LIMIT 10\n");

    // Date thresholds (days since epoch)
    // NOTE: Dates in the binary format are encoded as +1 day offset
    // For o_orderdate < 1995-03-15, need to compare against 9205 (not 9204)
    // For l_shipdate > 1995-03-15, need to compare against 9205 (not 9204)
    const int32_t ORDER_DATE_THRESHOLD = 9205;  // 1995-03-15 (adjusted for +1 encoding offset)
    const int32_t SHIP_DATE_THRESHOLD = 9205;   // 1995-03-15 (adjusted for +1 encoding offset)

    // ========================================================================
    // LOAD DICTIONARIES
    // ========================================================================
    auto t_dict_start = std::chrono::high_resolution_clock::now();

    uint8_t building_code = load_dictionary_code(gendb_dir, "customer", "c_mktsegment", "BUILDING");
    printf("[METADATA CHECK] BUILDING dictionary code: %u\n", (unsigned)building_code);

    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", dict_ms);

    // ========================================================================
    // LOAD CUSTOMER DATA
    // ========================================================================
    auto t_customer_start = std::chrono::high_resolution_clock::now();

    MmapFile f_c_custkey, f_c_mktsegment;
    if (!f_c_custkey.open(gendb_dir + "/customer/c_custkey.bin") ||
        !f_c_mktsegment.open(gendb_dir + "/customer/c_mktsegment.bin")) {
        std::cerr << "Failed to load customer data" << std::endl;
        return;
    }

    const int32_t* c_custkey = (const int32_t*)f_c_custkey.ptr;
    const uint8_t* c_mktsegment = (const uint8_t*)f_c_mktsegment.ptr;
    size_t num_customers = f_c_custkey.size / sizeof(int32_t);

    auto t_customer_end = std::chrono::high_resolution_clock::now();
    double customer_ms = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", customer_ms);

    // Build hash table: custkey → true (for fast membership check)
    auto t_cust_hash_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<int32_t, bool> customer_filter_map;
    customer_filter_map.reserve(num_customers / 5); // ~20% selectivity

    #pragma omp parallel for
    for (size_t i = 0; i < num_customers; i++) {
        if (c_mktsegment[i] == building_code) {
            #pragma omp critical
            customer_filter_map[c_custkey[i]] = true;
        }
    }

    auto t_cust_hash_end = std::chrono::high_resolution_clock::now();
    double cust_hash_ms = std::chrono::duration<double, std::milli>(t_cust_hash_end - t_cust_hash_start).count();
    printf("[TIMING] customer_filter: %.2f ms (matched %zu customers)\n", cust_hash_ms, customer_filter_map.size());

    // ========================================================================
    // LOAD ORDERS DATA
    // ========================================================================
    auto t_orders_start = std::chrono::high_resolution_clock::now();

    MmapFile f_o_orderkey, f_o_custkey, f_o_orderdate, f_o_shippriority;
    if (!f_o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin") ||
        !f_o_custkey.open(gendb_dir + "/orders/o_custkey.bin") ||
        !f_o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin") ||
        !f_o_shippriority.open(gendb_dir + "/orders/o_shippriority.bin")) {
        std::cerr << "Failed to load orders data" << std::endl;
        return;
    }

    const int32_t* o_orderkey = (const int32_t*)f_o_orderkey.ptr;
    const int32_t* o_custkey = (const int32_t*)f_o_custkey.ptr;
    const int32_t* o_orderdate = (const int32_t*)f_o_orderdate.ptr;
    const int32_t* o_shippriority = (const int32_t*)f_o_shippriority.ptr;
    size_t num_orders = f_o_orderkey.size / sizeof(int32_t);

    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", orders_ms);

    // Build hash table: orderkey → (custkey, orderdate, shippriority)
    auto t_orders_hash_start = std::chrono::high_resolution_clock::now();

    struct OrderKey { int32_t custkey; int32_t orderdate; int32_t shippriority; };
    std::unordered_map<int32_t, OrderKey> orders_map;
    orders_map.reserve(num_orders);

    for (size_t i = 0; i < num_orders; i++) {
        if (o_orderdate[i] < ORDER_DATE_THRESHOLD && customer_filter_map.count(o_custkey[i])) {
            orders_map[o_orderkey[i]] = {o_custkey[i], o_orderdate[i], o_shippriority[i]};
        }
    }

    auto t_orders_hash_end = std::chrono::high_resolution_clock::now();
    double orders_hash_ms = std::chrono::duration<double, std::milli>(t_orders_hash_end - t_orders_hash_start).count();
    printf("[TIMING] orders_filter_and_join: %.2f ms (matched %zu orders)\n", orders_hash_ms, orders_map.size());

    // ========================================================================
    // LOAD LINEITEM DATA
    // ========================================================================
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();

    MmapFile f_l_orderkey, f_l_shipdate, f_l_extendedprice, f_l_discount;
    if (!f_l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin") ||
        !f_l_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin") ||
        !f_l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin") ||
        !f_l_discount.open(gendb_dir + "/lineitem/l_discount.bin")) {
        std::cerr << "Failed to load lineitem data" << std::endl;
        return;
    }

    const int32_t* l_orderkey = (const int32_t*)f_l_orderkey.ptr;
    const int32_t* l_shipdate = (const int32_t*)f_l_shipdate.ptr;
    const int64_t* l_extendedprice = (const int64_t*)f_l_extendedprice.ptr;
    const int64_t* l_discount = (const int64_t*)f_l_discount.ptr;
    size_t num_lineitems = f_l_orderkey.size / sizeof(int32_t);

    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", lineitem_ms);

    // ========================================================================
    // AGGREGATE WITH JOIN
    // ========================================================================
    auto t_agg_start = std::chrono::high_resolution_clock::now();

    struct AggKey {
        int32_t l_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;

        bool operator==(const AggKey& other) const {
            return l_orderkey == other.l_orderkey &&
                   o_orderdate == other.o_orderdate &&
                   o_shippriority == other.o_shippriority;
        }
    };

    struct AggKeyHash {
        size_t operator()(const AggKey& k) const {
            return ((uint64_t)k.l_orderkey << 32) ^ ((uint64_t)k.o_orderdate << 16) ^ k.o_shippriority;
        }
    };

    struct AggValue {
        double revenue_sum = 0.0;  // Use double for precision
    };

    std::unordered_map<AggKey, AggValue, AggKeyHash> agg_map;
    agg_map.reserve(100000); // Estimated ~100K groups

    // Single-threaded scan + aggregate (lineitem is large, but join selectivity is low)
    // Use thread-local aggregation for parallelism
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<AggKey, AggValue, AggKeyHash>> local_aggs(num_threads);
    for (int t = 0; t < num_threads; t++) {
        local_aggs[t].reserve(100000 / num_threads + 1000);
    }

    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < num_lineitems; i++) {
        if (l_shipdate[i] > SHIP_DATE_THRESHOLD) {
            auto it = orders_map.find(l_orderkey[i]);
            if (it != orders_map.end()) {
                AggKey key = {l_orderkey[i], it->second.orderdate, it->second.shippriority};

                // Calculate revenue: l_extendedprice * (1 - l_discount)
                // Both l_extendedprice and l_discount are stored as int64_t with scale 100
                // l_extendedprice: e.g., 123456 = 1234.56
                // l_discount: e.g., 4 = 0.04 (it's the actual percentage: 0-10 means 0%-10%)
                // Formula: (l_extendedprice / 100) * (1 - l_discount / 100)
                double extended_price = (double)l_extendedprice[i] / 100.0;
                double discount_rate = (double)l_discount[i] / 100.0;
                double revenue_item = extended_price * (1.0 - discount_rate);

                int thread_id = omp_get_thread_num();
                local_aggs[thread_id][key].revenue_sum += revenue_item;
            }
        }
    }

    // Merge local aggregations
    for (int t = 1; t < num_threads; t++) {
        for (auto& [key, val] : local_aggs[t]) {
            agg_map[key].revenue_sum += val.revenue_sum;
        }
    }
    for (auto& [key, val] : local_aggs[0]) {
        agg_map[key].revenue_sum += val.revenue_sum;
    }

    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms (produced %zu groups)\n", agg_ms, agg_map.size());

    // ========================================================================
    // SORT AND LIMIT
    // ========================================================================
    auto t_sort_start = std::chrono::high_resolution_clock::now();

    struct ResultRow {
        int32_t l_orderkey;
        double revenue;     // In decimal form (e.g., 421478.73)
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<ResultRow> results;
    results.reserve(agg_map.size());
    for (auto& [key, val] : agg_map) {
        results.push_back({key.l_orderkey, val.revenue_sum, key.o_orderdate, key.o_shippriority});
    }

    // Partial sort for top 10 by revenue DESC, then o_orderdate ASC
    size_t limit_size = std::min(size_t(10), results.size());
    std::partial_sort(results.begin(), results.begin() + limit_size, results.end(),
                      [](const ResultRow& a, const ResultRow& b) {
                          if (a.revenue != b.revenue) return a.revenue > b.revenue;  // DESC
                          return a.o_orderdate < b.o_orderdate;                      // ASC
                      });

    // Keep only top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_and_limit: %.2f ms\n", sort_ms);

    // ========================================================================
    // OUTPUT
    // ========================================================================
    auto t_output_start = std::chrono::high_resolution_clock::now();

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream ofs(output_path);
    if (!ofs) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    ofs << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Write results
    for (const auto& row : results) {
        // revenue is already in decimal form
        // Note: epoch days appear to be off by 1, so subtract 1
        std::string date_str = DateConverter::epoch_days_to_date(row.o_orderdate - 1);

        ofs << row.l_orderkey << ","
            << std::fixed << std::setprecision(4) << row.revenue << ","
            << date_str << ","
            << row.o_shippriority << "\n";
    }

    ofs.close();

    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    // Total timing (computation only, exclude output)
    auto t_total = agg_ms + sort_ms + orders_hash_ms + cust_hash_ms;
    printf("[TIMING] total: %.2f ms\n", t_total);

    printf("[RESULT] Q3 completed successfully: %zu rows written to %s\n", results.size(), output_path.c_str());
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
