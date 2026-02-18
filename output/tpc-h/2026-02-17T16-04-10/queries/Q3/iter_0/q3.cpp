/*
QUERY PLAN for Q3: Shipping Priority
======================================

SQL:
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND
      l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND
      l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate LIMIT 10

LOGICAL PLAN:
1. Filter customer: c_mktsegment = 'BUILDING' → ~300K rows
2. Filter orders: o_orderdate < 1995-03-15 (date = 9204 days)
3. Filter lineitem: l_shipdate > 1995-03-15 (date = 9204 days)
4. Join customer → orders on c_custkey = o_custkey
5. Join result → lineitem on o_orderkey = l_orderkey
6. GROUP BY (l_orderkey, o_orderdate, o_shippriority) with SUM(revenue)
7. ORDER BY revenue DESC, o_orderdate ASC
8. LIMIT 10

PHYSICAL PLAN:
1. Load and filter customer on c_mktsegment = 'BUILDING' (hash index lookup)
2. Load and filter orders on o_orderdate < 9204 (zone map pruning)
3. Load and filter lineitem on l_shipdate > 9204
4. Hash join customer ⋈ orders on c_custkey = o_custkey
5. Hash join result ⋈ lineitem on o_orderkey = l_orderkey
6. Hash aggregation with GROUP BY
7. Partial sort for TOP 10
8. Output to CSV

KEY OPTIMIZATIONS:
- Use unordered_map for aggregation (safe for correctness)
- Date handling: epoch days as int32_t, YYYY-MM-DD format for output
- Decimal handling: int64_t with scale 100, arithmetic: a * (100 - b) / 100
- Dictionary loading: format "code=value"
*/

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

// ============================================================================
// Helper structures
// ============================================================================

struct AggregateKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggregateKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct AggregateKeyHash {
    size_t operator()(const AggregateKey& k) const {
        return ((size_t)k.l_orderkey * 73856093) ^
               ((size_t)k.o_orderdate * 19349663) ^
               ((size_t)k.o_shippriority * 83492791);
    }
};

struct AggregateValue {
    int64_t revenue_sum;
};

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ============================================================================
// Helper functions
// ============================================================================

// Load binary file via mmap
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open file: " << path << std::endl;
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
        return nullptr;
    }

    return ptr;
}

// Load dictionary (format: "code=value\n")
std::unordered_map<std::string, int32_t> load_dict(const std::string& path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(path);
    if (!f.is_open()) {
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[value] = code;
        }
    }
    return dict;
}

// Convert epoch days (int32_t) to YYYY-MM-DD
std::string epoch_days_to_date(int32_t days) {
    // Leap year check
    auto is_leap = [](int y) { return (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0); };

    int year = 1970;
    int day = days + 1;  // 0-indexed to 1-indexed

    // Advance by years
    while (true) {
        int days_in_year = is_leap(year) ? 366 : 365;
        if (day <= days_in_year) break;
        day -= days_in_year;
        year++;
    }

    // Days in each month (non-leap)
    const int daysInMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int month = 1;
    int feb_days = is_leap(year) ? 29 : 28;

    while (true) {
        int days_in_month = (month == 2) ? feb_days : daysInMonth[month];
        if (day <= days_in_month) break;
        day -= days_in_month;
        month++;
    }

    char buf[20];
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wformat-truncation"
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    #pragma GCC diagnostic pop
    return std::string(buf);
}

// ============================================================================
// Q3 Implementation
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Date constants (epoch days)
    const int32_t DATE_1995_03_15 = 9204;

    // Load customer data
    size_t customer_size = 0;
    int32_t* c_custkey = (int32_t*)mmap_file(gendb_dir + "/customer/c_custkey.bin", customer_size);
    int32_t num_customer = customer_size / sizeof(int32_t);

    size_t c_mktseg_size = 0;
    int32_t* c_mktsegment = (int32_t*)mmap_file(gendb_dir + "/customer/c_mktsegment.bin", c_mktseg_size);

    auto c_dict = load_dict(gendb_dir + "/customer/c_mktsegment_dict.txt");
    int32_t building_code = -1;
    for (const auto& p : c_dict) {
        if (p.first == "BUILDING") {
            building_code = p.second;
            break;
        }
    }

    if (building_code == -1) {
        std::cerr << "BUILDING code not found" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Filter customer
    std::vector<int32_t> filtered_custkeys;
    for (int32_t i = 0; i < num_customer; i++) {
        if (c_mktsegment[i] == building_code) {
            filtered_custkeys.push_back(c_custkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] customer_filter: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Build customer lookup
    std::unordered_map<int32_t, bool> customer_set;
    for (int32_t key : filtered_custkeys) {
        customer_set[key] = true;
    }

    // Load orders
    size_t orders_size = 0;
    int32_t* o_custkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_custkey.bin", orders_size);
    int32_t num_orders = orders_size / sizeof(int32_t);

    size_t order_date_size = 0;
    int32_t* o_orderdate = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderdate.bin", order_date_size);

    size_t order_key_size = 0;
    int32_t* o_orderkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", order_key_size);

    size_t order_shipprio_size = 0;
    int32_t* o_shippriority = (int32_t*)mmap_file(gendb_dir + "/orders/o_shippriority.bin", order_shipprio_size);

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Filter and join orders with customer
    struct OrderData {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::vector<OrderData> filtered_orders;

    for (int32_t i = 0; i < num_orders; i++) {
        if (o_orderdate[i] < DATE_1995_03_15 && customer_set.count(o_custkey[i])) {
            OrderData od;
            od.orderkey = o_orderkey[i];
            od.orderdate = o_orderdate[i];
            od.shippriority = o_shippriority[i];
            filtered_orders.push_back(od);
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] orders_filter_join: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Build order lookup
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_lookup;  // orderkey -> (orderdate, shippriority)
    for (const auto& od : filtered_orders) {
        order_lookup[od.orderkey] = {od.orderdate, od.shippriority};
    }

    // Load lineitem
    size_t lineitem_size = 0;
    int32_t* l_orderkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_size);
    int32_t num_lineitem = lineitem_size / sizeof(int32_t);

    size_t shipdate_size = 0;
    int32_t* l_shipdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size);

    size_t extprice_size = 0;
    int64_t* l_extendedprice = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", extprice_size);

    size_t discount_size = 0;
    int64_t* l_discount = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin", discount_size);

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Filter, join, and aggregate
    std::unordered_map<AggregateKey, AggregateValue, AggregateKeyHash> agg_table;

    for (int32_t i = 0; i < num_lineitem; i++) {
        if (l_shipdate[i] > DATE_1995_03_15) {
            auto it = order_lookup.find(l_orderkey[i]);
            if (it != order_lookup.end()) {
                // Compute revenue: l_extendedprice * (1 - l_discount)
                // l_extendedprice is int64_t scaled by 100 (e.g., 3307894 means 33078.94)
                // l_discount is int64_t scaled by 100 (e.g., 4 means 0.04)
                // Revenue = (extprice/100) * (1 - discount/100)
                //         = (extprice/100) * ((100 - discount)/100)
                //         = extprice * (100 - discount) / 10000
                // We store as int64_t with no additional scaling to preserve precision
                int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);

                AggregateKey key;
                key.l_orderkey = l_orderkey[i];
                key.o_orderdate = it->second.first;
                key.o_shippriority = it->second.second;

                auto& agg_val = agg_table[key];
                agg_val.revenue_sum += revenue;
            }
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] lineitem_filter_join_agg: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Convert aggregation table to result vector
    std::vector<ResultRow> results;
    for (const auto& p : agg_table) {
        ResultRow row;
        row.l_orderkey = p.first.l_orderkey;
        row.o_orderdate = p.first.o_orderdate;
        row.o_shippriority = p.first.o_shippriority;
        row.revenue = p.second.revenue_sum;
        results.push_back(row);
    }

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) {
            return a.revenue > b.revenue;
        }
        return a.o_orderdate < b.o_orderdate;
    });

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] sort: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    #ifdef GENDB_PROFILE
    t_scan = std::chrono::high_resolution_clock::now();
    #endif

    // Write results to CSV
    std::ofstream out(results_dir + "/Q3.csv");
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (const auto& row : results) {
        // Revenue is extprice * (100 - discount), divide by 10000 to get actual value
        double revenue_decimal = (double)row.revenue / 10000.0;
        std::string date_str = epoch_days_to_date(row.o_orderdate);
        out << std::fixed << row.l_orderkey << ","
            << std::setprecision(4) << revenue_decimal << ","
            << date_str << ","
            << row.o_shippriority << "\n";
    }
    out.close();

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] output: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_end - t_scan).count());
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] total: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count());
    #endif

    // Cleanup
    if (c_custkey) munmap(c_custkey, customer_size);
    if (c_mktsegment) munmap(c_mktsegment, c_mktseg_size);
    if (o_custkey) munmap(o_custkey, orders_size);
    if (o_orderkey) munmap(o_orderkey, order_key_size);
    if (o_orderdate) munmap(o_orderdate, order_date_size);
    if (o_shippriority) munmap(o_shippriority, order_shipprio_size);
    if (l_orderkey) munmap(l_orderkey, lineitem_size);
    if (l_shipdate) munmap(l_shipdate, shipdate_size);
    if (l_extendedprice) munmap(l_extendedprice, extprice_size);
    if (l_discount) munmap(l_discount, discount_size);
}

// ============================================================================
// Main
// ============================================================================

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
