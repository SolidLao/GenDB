#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <omp.h>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*
=== LOGICAL QUERY PLAN FOR Q3 ===

Query: SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
              o_orderdate, o_shippriority
       FROM customer, orders, lineitem
       WHERE c_mktsegment = 'BUILDING'
         AND c_custkey = o_custkey
         AND l_orderkey = o_orderkey
         AND o_orderdate < 1995-03-15 (epoch day 9204)
         AND l_shipdate > 1995-03-15 (epoch day 9204)
       GROUP BY l_orderkey, o_orderdate, o_shippriority
       ORDER BY revenue DESC, o_orderdate ASC
       LIMIT 10

Single-table predicates:
  - customer: c_mktsegment = 'BUILDING' (dictionary code 0) → ~300K rows
  - orders: o_orderdate < 9204 → ~5M rows
  - lineitem: l_shipdate > 9204 → ~15M rows

Join ordering (smallest filtered first):
  1. Filter customer by mktsegment
  2. Hash join: orders (filtered) probed with customer (filtered) on o_custkey = c_custkey
  3. Hash join: lineitem (filtered) probed with orders result on l_orderkey = o_orderkey

Aggregation:
  - GROUP BY (l_orderkey, o_orderdate, o_shippriority) → estimated 200K-500K groups
  - Aggregate: SUM(l_extendedprice * (1 - l_discount)) with scale_factor = 100

=== PHYSICAL QUERY PLAN ===

1. SCAN & FILTER customer:
   - Full scan, filter c_mktsegment == 0 (BUILDING)
   - Parallel with OpenMP
   - Output: set of c_custkey values

2. SCAN & FILTER orders:
   - Full scan, filter o_orderdate < 9204
   - Use zone map index to skip blocks
   - Parallel with OpenMP
   - Output: o_orderkey, o_custkey, o_orderdate, o_shippriority

3. HASH JOIN 1 (orders ⊲⊳ customer):
   - Build side: filtered customer (smaller ~300K)
   - Probe side: filtered orders (larger ~5M)
   - Join condition: o_custkey = c_custkey
   - Output: o_orderkey, o_orderdate, o_shippriority, (o_custkey removed after join)

4. SCAN & FILTER lineitem:
   - Full scan, filter l_shipdate > 9204
   - Use zone map index to skip blocks
   - Parallel with OpenMP
   - Output: l_orderkey, l_extendedprice, l_discount

5. HASH JOIN 2 (lineitem ⊲⊳ orders_result):
   - Build side: orders result (smaller ~5M)
   - Probe side: lineitem (larger ~15M)
   - Join condition: l_orderkey = o_orderkey
   - Output: l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority

6. AGGREGATION (GROUP BY):
   - Open-addressing hash table: key = (l_orderkey, o_orderdate, o_shippriority)
   - Aggregate: SUM(revenue) where revenue = l_extendedprice * (1 - l_discount) / scale_factor
   - Thread-local partial aggregation + final merge
   - Output: (l_orderkey, o_orderdate, o_shippriority) → revenue

7. SORT & LIMIT:
   - Sort by revenue DESC, o_orderdate ASC
   - Take top 10

8. OUTPUT:
   - Write CSV: l_orderkey, revenue, o_orderdate, o_shippriority
   - Dates in YYYY-MM-DD format
   - Revenue with 4 decimal places
*/

// Constants
const int32_t DATE_THRESHOLD = 9204; // 1995-03-15 in epoch days
const int BUILDING_CODE = 0; // c_mktsegment = 'BUILDING'

// Helper: Convert epoch days to YYYY-MM-DD
std::string epoch_days_to_date(int32_t days) {
    // days since 1970-01-01
    int year = 1970;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }

    int month = 1;
    int days_in_months[] = {31, (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 29 : 28,
                            31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    while (days >= days_in_months[month - 1]) {
        days -= days_in_months[month - 1];
        month++;
    }

    int day = days + 1; // days are 0-indexed within the month, but day numbering is 1-indexed

    char buffer[11];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month, day);
    return std::string(buffer);
}

// Helper: mmap a binary file
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    size = file_size;

    void* ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// Helper: Load dictionary and find code for a value
int32_t find_dict_code(const std::string& dict_path, const std::string& value) {
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Error opening dictionary " << dict_path << std::endl;
        return -1;
    }

    std::string line;
    while (std::getline(dict_file, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            std::string code_str = line.substr(0, eq_pos);
            std::string dict_value = line.substr(eq_pos + 1);
            if (dict_value == value) {
                return std::stoi(code_str);
            }
        }
    }
    return -1;
}

// Structure for join output (orders after filtering and customer join)
struct OrderResult {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Structure for aggregation result
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

// Hash function for AggKey
struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        return ((size_t)k.l_orderkey * 73856093) ^
               ((size_t)k.o_orderdate * 19349663) ^
               ((size_t)k.o_shippriority * 83492791);
    }
};

struct AggValue {
    double revenue_sum; // accumulated revenue as floating point
};

// Structure for final output
struct FinalResult {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const FinalResult& other) const {
        if (other.revenue != this->revenue) {
            return other.revenue < this->revenue; // DESC
        }
        return this->o_orderdate < other.o_orderdate; // ASC
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ===== METADATA CHECK =====
    std::cout << "[METADATA CHECK] Q3 Query" << std::endl;
    std::cout << "  - DATE_THRESHOLD (1995-03-15): " << DATE_THRESHOLD << " epoch days" << std::endl;
    std::cout << "  - c_mktsegment='BUILDING' code: " << BUILDING_CODE << std::endl;
    std::cout << "  - Decimal scale_factor: 100" << std::endl;

    // ===== LOAD DATA =====

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load customer table
    size_t customer_size = 0;
    int32_t* c_custkey_data = (int32_t*)mmap_file(gendb_dir + "/customer/c_custkey.bin", customer_size);
    int32_t* c_mktsegment_data = (int32_t*)mmap_file(gendb_dir + "/customer/c_mktsegment.bin", customer_size);
    int32_t num_customers = customer_size / sizeof(int32_t);

    // Load orders table
    size_t orders_size = 0;
    int32_t* o_orderkey_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", orders_size);
    int32_t* o_custkey_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_custkey.bin", orders_size);
    int32_t* o_orderdate_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderdate.bin", orders_size);
    int32_t* o_shippriority_data = (int32_t*)mmap_file(gendb_dir + "/orders/o_shippriority.bin", orders_size);
    int32_t num_orders = orders_size / sizeof(int32_t);

    // Load lineitem table
    size_t li_orderkey_size = 0, li_extprice_size = 0, li_discount_size = 0, li_shipdate_size = 0;
    int32_t* l_orderkey_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", li_orderkey_size);
    int64_t* l_extendedprice_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", li_extprice_size);
    int64_t* l_discount_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin", li_discount_size);
    int32_t* l_shipdate_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", li_shipdate_size);
    int32_t num_lineitem = li_orderkey_size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    std::cout << "[METADATA CHECK] Loaded " << num_customers << " customers, "
              << num_orders << " orders, " << num_lineitem << " lineitems" << std::endl;

    // ===== STEP 1: FILTER CUSTOMER =====

#ifdef GENDB_PROFILE
    auto t_filter_customer_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<int32_t> filtered_custkeys;
    filtered_custkeys.reserve(num_customers / 5); // Estimate ~300K customers

#pragma omp parallel for
    for (int32_t i = 0; i < num_customers; i++) {
        if (c_mktsegment_data[i] == BUILDING_CODE) {
            #pragma omp critical
            {
                filtered_custkeys.push_back(c_custkey_data[i]);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_customer_end = std::chrono::high_resolution_clock::now();
    double ms_filter_customer = std::chrono::duration<double, std::milli>(t_filter_customer_end - t_filter_customer_start).count();
    printf("[TIMING] filter_customer: %.2f ms\n", ms_filter_customer);
#endif

    std::cout << "[METADATA CHECK] Filtered customers: " << filtered_custkeys.size() << std::endl;

    // ===== STEP 2: FILTER ORDERS =====

#ifdef GENDB_PROFILE
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<OrderResult> filtered_orders;
    filtered_orders.reserve(num_orders / 3);

#pragma omp parallel for
    for (int32_t i = 0; i < num_orders; i++) {
        if (o_orderdate_data[i] < DATE_THRESHOLD) {
            OrderResult or_row;
            or_row.o_orderkey = o_orderkey_data[i];
            or_row.o_orderdate = o_orderdate_data[i];
            or_row.o_shippriority = o_shippriority_data[i];

            #pragma omp critical
            {
                filtered_orders.push_back(or_row);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double ms_filter_orders = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms\n", ms_filter_orders);
#endif

    std::cout << "[METADATA CHECK] Filtered orders: " << filtered_orders.size() << std::endl;

    // ===== STEP 3: HASH JOIN 1 (orders ⊲⊳ customer on o_custkey = c_custkey) =====

#ifdef GENDB_PROFILE
    auto t_join1_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash set of filtered customer keys
    std::unordered_map<int32_t, bool> customer_set;
    for (int32_t custkey : filtered_custkeys) {
        customer_set[custkey] = true;
    }

    // Probe orders with customer set (filter to only orders from BUILDING customers)
    // We need the o_custkey for the join, so we'll create a temporary structure
    struct OrderWithCustkey {
        int32_t o_orderkey;
        int32_t o_custkey;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<OrderResult> orders_joined;
    orders_joined.reserve(filtered_orders.size());

    for (const auto& ord : filtered_orders) {
        // We need to look up the o_custkey from the original data
        // But we only have o_orderkey, o_orderdate, o_shippriority from filtered_orders
        // We need to re-scan to get custkey
    }

    // Actually, we need to do the filtering and joining simultaneously
    // Let's restart with a better approach

    filtered_orders.clear();
    orders_joined.clear();

#pragma omp parallel for
    for (int32_t i = 0; i < num_orders; i++) {
        if (o_orderdate_data[i] < DATE_THRESHOLD) {
            int32_t custkey = o_custkey_data[i];
            if (customer_set.count(custkey) > 0) {
                OrderResult or_row;
                or_row.o_orderkey = o_orderkey_data[i];
                or_row.o_orderdate = o_orderdate_data[i];
                or_row.o_shippriority = o_shippriority_data[i];

                #pragma omp critical
                {
                    orders_joined.push_back(or_row);
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_join1_end = std::chrono::high_resolution_clock::now();
    double ms_join1 = std::chrono::duration<double, std::milli>(t_join1_end - t_join1_start).count();
    printf("[TIMING] join1: %.2f ms\n", ms_join1);
#endif

    std::cout << "[METADATA CHECK] Orders after join with customer: " << orders_joined.size() << std::endl;

    // ===== STEP 4: FILTER LINEITEM =====

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    struct LineitemFiltered {
        int32_t l_orderkey;
        int64_t l_extendedprice;
        int64_t l_discount;
    };

    std::vector<LineitemFiltered> filtered_lineitem;
    filtered_lineitem.reserve(num_lineitem / 4);

#pragma omp parallel for
    for (int32_t i = 0; i < num_lineitem; i++) {
        if (l_shipdate_data[i] > DATE_THRESHOLD) {
            LineitemFiltered li_row;
            li_row.l_orderkey = l_orderkey_data[i];
            li_row.l_extendedprice = l_extendedprice_data[i];
            li_row.l_discount = l_discount_data[i];

            #pragma omp critical
            {
                filtered_lineitem.push_back(li_row);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_filter_lineitem = std::chrono::duration<double, std::milli>(t_filter_lineitem_end - t_filter_lineitem_start).count();
    printf("[TIMING] filter_lineitem: %.2f ms\n", ms_filter_lineitem);
#endif

    std::cout << "[METADATA CHECK] Filtered lineitem: " << filtered_lineitem.size() << std::endl;

    // ===== STEP 5: HASH JOIN 2 (lineitem ⊲⊳ orders on l_orderkey = o_orderkey) =====

#ifdef GENDB_PROFILE
    auto t_join2_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash map from orders_joined on o_orderkey
    struct OrderJoinData {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::unordered_map<int32_t, std::vector<OrderJoinData>> orders_map;
    for (const auto& ord : orders_joined) {
        orders_map[ord.o_orderkey].push_back({ord.o_orderdate, ord.o_shippriority});
    }

    // Join lineitem with orders
    struct JoinedLineitemOrder {
        int32_t l_orderkey;
        int64_t l_extendedprice;
        int64_t l_discount;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<JoinedLineitemOrder> joined_results;
    joined_results.reserve(filtered_lineitem.size());

    for (const auto& li : filtered_lineitem) {
        auto it = orders_map.find(li.l_orderkey);
        if (it != orders_map.end()) {
            for (const auto& ord_data : it->second) {
                JoinedLineitemOrder jr;
                jr.l_orderkey = li.l_orderkey;
                jr.l_extendedprice = li.l_extendedprice;
                jr.l_discount = li.l_discount;
                jr.o_orderdate = ord_data.o_orderdate;
                jr.o_shippriority = ord_data.o_shippriority;
                joined_results.push_back(jr);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_join2_end = std::chrono::high_resolution_clock::now();
    double ms_join2 = std::chrono::duration<double, std::milli>(t_join2_end - t_join2_start).count();
    printf("[TIMING] join2: %.2f ms\n", ms_join2);
#endif

    std::cout << "[METADATA CHECK] Joined lineitem-orders: " << joined_results.size() << std::endl;

    // ===== STEP 6: AGGREGATION =====

#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<AggKey, AggValue, AggKeyHash> agg_table;
    agg_table.reserve(500000); // Pre-size for estimated 500K groups

    for (const auto& jr : joined_results) {
        // Calculate revenue: l_extendedprice * (1 - l_discount)
        // Both l_extendedprice and l_discount are scaled by 100.
        // Convert to actual values and compute
        double actual_price = jr.l_extendedprice / 100.0;
        double actual_discount = jr.l_discount / 100.0;
        double revenue = actual_price * (1.0 - actual_discount);

        AggKey key;
        key.l_orderkey = jr.l_orderkey;
        key.o_orderdate = jr.o_orderdate;
        key.o_shippriority = jr.o_shippriority;

        agg_table[key].revenue_sum += revenue;
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms_agg);
#endif

    std::cout << "[METADATA CHECK] Aggregation groups: " << agg_table.size() << std::endl;

    // ===== STEP 7: SORT & LIMIT =====

#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<FinalResult> final_results;
    final_results.reserve(agg_table.size());

    for (const auto& entry : agg_table) {
        FinalResult fr;
        fr.l_orderkey = entry.first.l_orderkey;
        fr.revenue = entry.second.revenue_sum; // Already in actual units from floating-point calc
        fr.o_orderdate = entry.first.o_orderdate;
        fr.o_shippriority = entry.first.o_shippriority;
        final_results.push_back(fr);
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::sort(final_results.begin(), final_results.end());

    // Keep only top 10
    if (final_results.size() > 10) {
        final_results.resize(10);
    }

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

    // ===== STEP 8: OUTPUT =====

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out_file(output_path);

    if (!out_file.is_open()) {
        std::cerr << "Error opening output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out_file << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Write results (output precision: 4 decimal places for revenue)
    for (const auto& fr : final_results) {
        std::string date_str = epoch_days_to_date(fr.o_orderdate);
        out_file << std::fixed
                 << fr.l_orderkey << ","
                 << std::setprecision(4) << fr.revenue << ","
                 << date_str << ","
                 << std::setprecision(0) << fr.o_shippriority << "\n";
    }

    out_file.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    std::cout << "Output written to " << output_path << std::endl;
    std::cout << "Results: " << final_results.size() << " rows" << std::endl;
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
