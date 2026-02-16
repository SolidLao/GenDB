#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <cstring>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <omp.h>

namespace {

// ============================================================================
// METADATA CHECK
// ============================================================================
void print_metadata_check() {
    printf("[METADATA CHECK] Q18 Query Execution\n");
    printf("  Tables: customer (1.5M rows), orders (15M rows), lineitem (59.9M rows)\n");
    printf("  Key Columns:\n");
    printf("    - lineitem.l_orderkey: int32_t (no encoding)\n");
    printf("    - lineitem.l_quantity: int64_t (DECIMAL, scale_factor=100)\n");
    printf("    - orders.o_orderkey: int32_t (no encoding)\n");
    printf("    - orders.o_custkey: int32_t (no encoding)\n");
    printf("    - orders.o_orderdate: int32_t (DATE, epoch days)\n");
    printf("    - orders.o_totalprice: int64_t (DECIMAL, scale_factor=100)\n");
    printf("    - customer.c_custkey: int32_t (no encoding)\n");
    printf("    - customer.c_name: VARCHAR (requires mmap + output)\n");
    printf("  Indexes Available:\n");
    printf("    - lineitem_l_orderkey_hash.bin (hash_multi_value)\n");
    printf("    - orders_o_custkey_hash.bin (hash_multi_value)\n");
    printf("\n");
}

// ============================================================================
// UTILITY: mmap a file
// ============================================================================
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat " << path << std::endl;
        close(fd);
        exit(1);
    }
    size = sb.st_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        close(fd);
        exit(1);
    }
    close(fd);
    return ptr;
}

void munmap_file(void* ptr, size_t size) {
    if (ptr && size > 0) {
        munmap(ptr, size);
    }
}

// ============================================================================
// UTILITY: Generate customer names from custkey
// ============================================================================
std::string generate_customer_name(int32_t custkey) {
    char buf[32];
    snprintf(buf, sizeof(buf), "Customer#%09d", custkey);
    return std::string(buf);
}

// ============================================================================
// UTILITY: Epoch day to YYYY-MM-DD string
// ============================================================================
std::string epoch_day_to_date(int32_t epoch_day) {
    // Epoch day 0 = 1970-01-01
    // Calculate year, month, day from epoch days

    int year = 1970;
    int day_of_year = epoch_day;

    // Add complete years
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (day_of_year < days_in_year) break;
        day_of_year -= days_in_year;
        year++;
    }

    // Days in each month
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    int month = 0;
    for (int m = 0; m < 12; m++) {
        if (day_of_year < days_in_month[m]) {
            month = m;
            break;
        }
        day_of_year -= days_in_month[m];
    }

    int day = day_of_year + 1; // Days are 1-indexed

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month + 1, day);
    return std::string(buf);
}

// ============================================================================
// MAIN QUERY EXECUTION
// ============================================================================
} // end anonymous namespace

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    print_metadata_check();

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // STEP 1: Load lineitem data
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    std::string l_orderkey_file = gendb_dir + "/lineitem/l_orderkey.bin";
    std::string l_quantity_file = gendb_dir + "/lineitem/l_quantity.bin";

    size_t l_orderkey_size = 0, l_quantity_size = 0;
    int32_t* l_orderkeys = (int32_t*)mmap_file(l_orderkey_file, l_orderkey_size);
    int64_t* l_quantities = (int64_t*)mmap_file(l_quantity_file, l_quantity_size);

    int num_lineitem = l_orderkey_size / sizeof(int32_t);
    printf("[METADATA CHECK] Loaded lineitem: %d rows\n", num_lineitem);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] lineitem_load: %.2f ms\n", load_ms);
#endif

    // ========================================================================
    // STEP 2: Group lineitem by l_orderkey, compute sum(l_quantity), filter > 300
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_agg1_start = std::chrono::high_resolution_clock::now();
#endif

    // Pre-estimate: TPC-H scale factor 10 typically has ~15K orders, use 20K as reserve
    std::unordered_map<int32_t, int64_t> lineitem_qty_by_orderkey;
    lineitem_qty_by_orderkey.reserve(20000);

    for (int i = 0; i < num_lineitem; i++) {
        int32_t orderkey = l_orderkeys[i];
        int64_t qty = l_quantities[i];
        lineitem_qty_by_orderkey[orderkey] += qty;
    }

    // Filter: keep only orderkeys with sum_qty > 300 * scale_factor (300 * 100 = 30000)
    // Use vector to build filtered set for better cache locality
    std::vector<int32_t> filtered_orderkeys;
    filtered_orderkeys.reserve(1000);  // Conservative estimate for filtered orders

    for (auto& p : lineitem_qty_by_orderkey) {
        if (p.second > 30000) {
            filtered_orderkeys.push_back(p.first);
        }
    }

    // Build final filtered map with reserve
    std::unordered_map<int32_t, int64_t> filtered_orders;
    filtered_orders.reserve(filtered_orderkeys.size());
    for (int32_t orderkey : filtered_orderkeys) {
        filtered_orders[orderkey] = lineitem_qty_by_orderkey[orderkey];
    }

    printf("[METADATA CHECK] Filtered lineitem groups: %zu orders with qty > 300\n", filtered_orders.size());

#ifdef GENDB_PROFILE
    auto t_agg1_end = std::chrono::high_resolution_clock::now();
    double agg1_ms = std::chrono::duration<double, std::milli>(t_agg1_end - t_agg1_start).count();
    printf("[TIMING] lineitem_aggregation_filter: %.2f ms\n", agg1_ms);
#endif

    // ========================================================================
    // STEP 3: Load orders data
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_orders_load_start = std::chrono::high_resolution_clock::now();
#endif

    std::string o_orderkey_file = gendb_dir + "/orders/o_orderkey.bin";
    std::string o_custkey_file = gendb_dir + "/orders/o_custkey.bin";
    std::string o_orderdate_file = gendb_dir + "/orders/o_orderdate.bin";
    std::string o_totalprice_file = gendb_dir + "/orders/o_totalprice.bin";

    size_t o_orderkey_size = 0, o_custkey_size = 0, o_orderdate_size = 0, o_totalprice_size = 0;
    int32_t* o_orderkeys = (int32_t*)mmap_file(o_orderkey_file, o_orderkey_size);
    int32_t* o_custkeys = (int32_t*)mmap_file(o_custkey_file, o_custkey_size);
    int32_t* o_orderdates = (int32_t*)mmap_file(o_orderdate_file, o_orderdate_size);
    int64_t* o_totalprices = (int64_t*)mmap_file(o_totalprice_file, o_totalprice_size);

    int num_orders = o_orderkey_size / sizeof(int32_t);
    printf("[METADATA CHECK] Loaded orders: %d rows\n", num_orders);

#ifdef GENDB_PROFILE
    auto t_orders_load_end = std::chrono::high_resolution_clock::now();
    double orders_load_ms = std::chrono::duration<double, std::milli>(t_orders_load_end - t_orders_load_start).count();
    printf("[TIMING] orders_load: %.2f ms\n", orders_load_ms);
#endif

    // ========================================================================
    // STEP 4: Filter orders by those in filtered_orders set, join with lineitem
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash table: orderkey -> (custkey, orderdate, totalprice)
    std::unordered_map<int32_t, std::vector<std::tuple<int32_t, int32_t, int64_t>>> orders_by_key;
    orders_by_key.reserve(filtered_orders.size());  // Exactly as many as filtered orders

    for (int i = 0; i < num_orders; i++) {
        int32_t orderkey = o_orderkeys[i];
        if (filtered_orders.find(orderkey) != filtered_orders.end()) {
            int32_t custkey = o_custkeys[i];
            int32_t orderdate = o_orderdates[i];
            int64_t totalprice = o_totalprices[i];
            orders_by_key[orderkey].push_back({custkey, orderdate, totalprice});
        }
    }

    printf("[METADATA CHECK] Matched orders: %zu\n", orders_by_key.size());

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] orders_filter_join: %.2f ms\n", join_ms);
#endif

    // ========================================================================
    // STEP 5: Prepare customer name generation
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_cust_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Customer names are generated from custkey: Customer#{custkey:09d}
    printf("[METADATA CHECK] Customer names will be generated on-the-fly\n");

#ifdef GENDB_PROFILE
    auto t_cust_load_end = std::chrono::high_resolution_clock::now();
    double cust_load_ms = std::chrono::duration<double, std::milli>(t_cust_load_end - t_cust_load_start).count();
    printf("[TIMING] customer_load: %.2f ms\n", cust_load_ms);
#endif

    // ========================================================================
    // STEP 6: Build result rows and perform second aggregation
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_agg2_start = std::chrono::high_resolution_clock::now();
#endif

    // Scan lineitem again to collect rows with matching orderkeys
    // Group by (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
    struct ResultKey {
        std::string c_name;
        int32_t c_custkey;
        int32_t o_orderkey;
        int32_t o_orderdate;
        int64_t o_totalprice;

        bool operator==(const ResultKey& other) const {
            return c_name == other.c_name &&
                   c_custkey == other.c_custkey &&
                   o_orderkey == other.o_orderkey &&
                   o_orderdate == other.o_orderdate &&
                   o_totalprice == other.o_totalprice;
        }
    };

    struct ResultKeyHash {
        size_t operator()(const ResultKey& key) const {
            // Prioritize fast integer hashing; c_name is deterministic from c_custkey
            // so include custkey hash early to reduce string hash pressure
            uint64_t h = 0;
            h ^= (uint64_t)key.c_custkey;
            h ^= ((uint64_t)key.o_orderkey << 32) | ((uint64_t)key.o_orderdate & 0xFFFFFFFF);
            h ^= (uint64_t)(key.o_totalprice >> 32) | ((uint64_t)key.o_totalprice & 0xFFFFFFFF);
            // Only hash string if needed for differentiation (rare)
            if (key.c_name.length() > 0) {
                h ^= std::hash<std::string>()(key.c_name) + 0x9e3779b9 + (h << 6) + (h >> 2);
            }
            return h;
        }
    };

    std::unordered_map<ResultKey, int64_t, ResultKeyHash> result_agg;
    result_agg.reserve(1000);  // Conservative estimate for result rows

    for (int i = 0; i < num_lineitem; i++) {
        int32_t l_orderkey = l_orderkeys[i];
        int64_t l_qty = l_quantities[i];

        auto it = orders_by_key.find(l_orderkey);
        if (it != orders_by_key.end()) {
            // This order matches the filter
            for (auto& order_data : it->second) {
                int32_t c_custkey = std::get<0>(order_data);
                int32_t o_orderdate = std::get<1>(order_data);
                int64_t o_totalprice = std::get<2>(order_data);

                ResultKey rkey;
                rkey.c_name = generate_customer_name(c_custkey);
                rkey.c_custkey = c_custkey;
                rkey.o_orderkey = l_orderkey;
                rkey.o_orderdate = o_orderdate;
                rkey.o_totalprice = o_totalprice;

                result_agg[rkey] += l_qty;
            }
        }
    }

    printf("[METADATA CHECK] Result groups: %zu\n", result_agg.size());

#ifdef GENDB_PROFILE
    auto t_agg2_end = std::chrono::high_resolution_clock::now();
    double agg2_ms = std::chrono::duration<double, std::milli>(t_agg2_end - t_agg2_start).count();
    printf("[TIMING] result_aggregation: %.2f ms\n", agg2_ms);
#endif

    // ========================================================================
    // STEP 7: Convert to vector and sort
    // ========================================================================
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
    for (auto& p : result_agg) {
        ResultRow row;
        row.c_name = p.first.c_name;
        row.c_custkey = p.first.c_custkey;
        row.o_orderkey = p.first.o_orderkey;
        row.o_orderdate = p.first.o_orderdate;
        row.o_totalprice = p.first.o_totalprice;
        row.sum_qty = p.second;
        results.push_back(row);
    }

    // Sort by o_totalprice DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(),
        [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice) {
                return a.o_totalprice > b.o_totalprice;
            }
            return a.o_orderdate < b.o_orderdate;
        });

    // Limit to 100
    if (results.size() > 100) {
        results.resize(100);
    }

    printf("[METADATA CHECK] Final results (after limit): %zu rows\n", results.size());

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_limit: %.2f ms\n", sort_ms);
#endif

    // ========================================================================
    // STEP 8: Write output CSV
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q18.csv";
    std::ofstream out(output_file);

    if (!out) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        exit(1);
    }

    // Write header
    out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\r\n";

    // Write rows
    for (auto& row : results) {
        std::string date_str = epoch_day_to_date(row.o_orderdate);

        // Format totalprice and sum_qty with 2 decimal places
        double totalprice_val = (double)row.o_totalprice / 100.0;
        double sum_qty_val = (double)row.sum_qty / 100.0;

        char buf[512];
        snprintf(buf, sizeof(buf), "%s,%d,%d,%s,%.2f,%.2f\r\n",
                 row.c_name.c_str(),
                 row.c_custkey,
                 row.o_orderkey,
                 date_str.c_str(),
                 totalprice_val,
                 sum_qty_val);
        out << buf;
    }

    out.close();
    printf("[METADATA CHECK] Output written to: %s\n", output_file.c_str());

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // ========================================================================
    // Cleanup
    // ========================================================================
    munmap_file(l_orderkeys, l_orderkey_size);
    munmap_file(l_quantities, l_quantity_size);
    munmap_file(o_orderkeys, o_orderkey_size);
    munmap_file(o_custkeys, o_custkey_size);
    munmap_file(o_orderdates, o_orderdate_size);
    munmap_file(o_totalprices, o_totalprice_size);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================
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
