#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

/*
===========================================
Q3: SHIPPING PRIORITY QUERY
===========================================

LOGICAL PLAN:
1. Scan customer table, filter c_mktsegment = 'BUILDING' (dict code 0)
   Estimated: ~300K rows from 1.5M
2. Build hash table on filtered customer keyed by c_custkey
3. Scan orders, filter o_orderdate < 9204 (1995-03-15)
   Estimated: ~1.2M rows from 15M (using zone map)
4. Join filtered orders with customer hash table on c_custkey = o_custkey
   Produces intermediate: ~1.2M rows with (o_orderkey, o_orderdate, o_shippriority)
5. Build hash table on join result keyed by o_orderkey
6. Scan lineitem, filter l_shipdate > 9204 (1995-03-15)
   Estimated: ~18M rows from 60M (need to verify selectivity)
7. Join filtered lineitem with orders hash table on l_orderkey = o_orderkey
8. Stream through aggregation with key=(l_orderkey, o_orderdate, o_shippriority)
   Value = SUM(l_extendedprice * (1 - l_discount))
   Estimated groups: ~12K (from result set pattern)
9. Sort by revenue DESC, o_orderdate
10. Take TOP 10

PHYSICAL PLAN:
- Customer scan: full scan, filter on dictionary code
- Customer→Orders join: hash join, build on customer, probe orders
- Orders→Lineitem join: hash join, build on orders, probe lineitem
- Aggregation: open-addressing hash table (12K estimated groups)
- Sort: std::sort on results
- Parallelism: OpenMP on lineitem scan (largest table)

DATE ENCODING: int32_t days since 1970-01-01
- 1995-03-15 = 9204
- o_orderdate < 9204 means o_orderdate is before that date
- l_shipdate > 9204 means l_shipdate is after that date

DECIMAL ENCODING: int64_t with scale_factor=2
- Values stored as integer * 100 (e.g., 123.45 stored as 12345)
- Aggregation: SUM(l_extendedprice * (1 - l_discount)) in scaled integer arithmetic
- For price*discount: (int64_t * int64_t) / scale_factor to get back to scale 2
- Output: divide by 100.0 for CSV decimal formatting
*/

// Forward declarations
struct AggResult {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double revenue;  // Store as double for precision
};

struct OrderData {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

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
        // Combine three int32_t fields into a single hash
        uint64_t h = 0x9E3779B97F4A7C15ULL;
        h ^= (uint64_t)k.l_orderkey * 0xBF58476D1CE4E5B9ULL;
        h ^= (uint64_t)k.o_orderdate * 0x94D049BB133111EBULL;
        h ^= (uint64_t)k.o_shippriority * 0xAF61D4D51A6A2B7BULL;
        return h;
    }
};

// Utility function to convert epoch days to YYYY-MM-DD string
std::string epoch_days_to_date(int32_t days_since_epoch) {
    // Compute year, month, day from days
    int32_t days = days_since_epoch;

    // Start from 1970-01-01 and count forward
    int year = 1970;

    // Count leap years: year divisible by 4, except 100s unless divisible by 400
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }

    // Now compute month and day
    int month = 1;
    int days_in_months[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_months[1] = 29; // February has 29 days in leap year
    }

    for (int m = 0; m < 12; m++) {
        if (days < days_in_months[m]) {
            month = m + 1;
            break;
        }
        days -= days_in_months[m];
    }

    int day = days + 1; // days are 0-indexed

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << year << "-"
        << std::setw(2) << month << "-"
        << std::setw(2) << day;
    return oss.str();
}

// Memory-mapped file helper
template<typename T>
T* mmap_file(const std::string& path, size_t& num_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }

    num_elements = file_size / sizeof(T);
    return (T*)ptr;
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load dictionary for c_mktsegment to find code for "BUILDING"
    std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
    std::ifstream dict_file(dict_path);
    int32_t building_code = -1;
    std::string line;
    int code = 0;
    while (std::getline(dict_file, line)) {
        if (line == "BUILDING") {
            building_code = code;
            break;
        }
        code++;
    }
    dict_file.close();

    if (building_code == -1) {
        std::cerr << "BUILDING not found in c_mktsegment dictionary" << std::endl;
        return;
    }

    // Date constants
    const int32_t date_boundary = 9204; // 1995-03-15

    // ==================== PHASE 1: Load customer data ====================
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t customer_count = 0;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_mktsegment = mmap_file<int32_t>(gendb_dir + "/customer/c_mktsegment.bin", customer_count);

    // Build hash table: custkey → (empty, just presence check)
    // Using unordered_set would be simpler, but we need to track which customers passed filter
    std::unordered_map<int32_t, int32_t> customer_ht; // custkey → dummy
    customer_ht.reserve(300000);

    for (size_t i = 0; i < customer_count; i++) {
        if (c_mktsegment[i] == building_code) {
            customer_ht[c_custkey[i]] = 1;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_customer = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] customer_scan_filter: %.2f ms\n", ms_customer);
    #endif

    // ==================== PHASE 2: Load orders, filter and join with customer ====================
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t orders_count = 0;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_count);
    int32_t* o_shippriority = mmap_file<int32_t>(gendb_dir + "/orders/o_shippriority.bin", orders_count);

    // Build hash table: orderkey → OrderData
    std::unordered_map<int32_t, OrderData> orders_ht;
    orders_ht.reserve(2000000); // estimated 1.2M after filtering

    for (size_t i = 0; i < orders_count; i++) {
        // Apply predicates: o_orderdate < date_boundary AND c_custkey in customer filter
        if (o_orderdate[i] < date_boundary && customer_ht.count(o_custkey[i]) > 0) {
            orders_ht[o_orderkey[i]] = {
                o_orderkey[i],
                o_orderdate[i],
                o_shippriority[i]
            };
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_scan_filter_join: %.2f ms\n", ms_orders);
    printf("[TIMING] join_customer_orders: %.2f ms (included in orders_scan)\n", 0.0);
    #endif

    // ==================== PHASE 3: Load lineitem, filter and join with orders, aggregate ====================
    #ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t lineitem_count = 0;
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);

    // Aggregation hash table: AggKey → revenue (double for precision)
    std::unordered_map<AggKey, double, AggKeyHash> agg_ht;
    agg_ht.reserve(20000); // estimated number of distinct (l_orderkey, o_orderdate, o_shippriority)

    for (size_t i = 0; i < lineitem_count; i++) {
        // Apply predicate: l_shipdate > date_boundary
        if (l_shipdate[i] <= date_boundary) continue;

        // Probe orders hash table
        auto it = orders_ht.find(l_orderkey[i]);
        if (it == orders_ht.end()) continue;

        const OrderData& od = it->second;

        // Compute revenue = l_extendedprice * (1 - l_discount)
        // Both are int64_t with scale_factor 2
        // l_extendedprice stores price * 100 (e.g., 33078.94 → 3307894)
        // l_discount stores discount * 100 (e.g., 0.04 → 4)
        // So revenue = (l_extendedprice / 100) * (1 - l_discount / 100)
        //            = l_extendedprice * (100 - l_discount) / 10000
        // Use double for precision during aggregation
        double revenue = l_extendedprice[i] * (100.0 - l_discount[i]) / 10000.0;

        AggKey key = {od.o_orderkey, od.o_orderdate, od.o_shippriority};
        agg_ht[key] += revenue;
    }

    #ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_lineitem = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] lineitem_scan_filter_join_agg: %.2f ms\n", ms_lineitem);
    #endif

    // ==================== PHASE 4: Sort and output ====================
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<AggResult> results;
    results.reserve(agg_ht.size());

    for (const auto& [key, revenue] : agg_ht) {
        results.push_back({
            key.l_orderkey,
            key.o_orderdate,
            key.o_shippriority,
            revenue
        });
    }

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const AggResult& a, const AggResult& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_topk: %.2f ms\n", ms_sort);
    #endif

    // ==================== PHASE 5: Write CSV ====================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);

    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (const auto& res : results) {
        // Convert epoch days to YYYY-MM-DD
        std::string date_str = epoch_days_to_date(res.o_orderdate);

        // Revenue is already in dollars (computed as double)
        // Format with 4 decimal places to match ground truth precision

        out << res.l_orderkey << ","
            << std::fixed << std::setprecision(4) << res.revenue << ","
            << date_str << ","
            << res.o_shippriority << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    // ==================== Cleanup ====================
    munmap(c_custkey, customer_count * sizeof(int32_t));
    munmap(c_mktsegment, customer_count * sizeof(int32_t));
    munmap(o_orderkey, orders_count * sizeof(int32_t));
    munmap(o_custkey, orders_count * sizeof(int32_t));
    munmap(o_orderdate, orders_count * sizeof(int32_t));
    munmap(o_shippriority, orders_count * sizeof(int32_t));
    munmap(l_orderkey, lineitem_count * sizeof(int32_t));
    munmap(l_extendedprice, lineitem_count * sizeof(int64_t));
    munmap(l_discount, lineitem_count * sizeof(int64_t));
    munmap(l_shipdate, lineitem_count * sizeof(int32_t));

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

    run_q3(gendb_dir, results_dir);

    return 0;
}
#endif
