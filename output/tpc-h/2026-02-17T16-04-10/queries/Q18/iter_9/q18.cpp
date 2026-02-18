// Q18: Large Volume Customer (Optimized Iteration 9)
// Logical Plan:
//   1. Subquery: GROUP BY l_orderkey, HAVING SUM(l_quantity) > 300 → hash set of qualified orderkeys
//   2. Filter orders: o_orderkey IN semi-join set → ~3K orders
//   3. Pre-aggregate at order level: GROUP BY (o_orderkey, o_custkey, o_orderdate, o_totalprice), SUM(l_quantity)
//   4. Join customer on c_custkey, retrieve c_name
//   5. ORDER BY o_totalprice DESC, o_orderdate ASC, LIMIT 100
//
// Physical Plan (FUSED & LATE MATERIALIZATION):
//   - Step 1: Full scan lineitem (60M rows) → thread-local group by l_orderkey with hash map → filter HAVING > 300
//   - Step 2: Build hash set of qualified l_orderkey values (~3K entries) via atomic merge
//   - Step 3: Single second pass over lineitem with qualified orderkeys → build order-level aggregates (integer keys only)
//   - Step 4: Load customer names ONLY for the ~100 result rows (late materialization)
//   - Step 5: Top-K sort with partial materialization

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <queue>
#include <sstream>

// ==================== DATE UTILITIES ====================
inline int32_t date_to_days(const char* date_str) {
    int year, month, day;
    sscanf(date_str, "%d-%d-%d", &year, &month, &day);

    int days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;

    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += days_in_month[m];
    }
    days += day - 1;

    if (month > 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
        days += 1;
    }

    return days;
}

std::string days_to_date_str(int32_t days) {
    int year = 1970 + days / 365;
    int remaining = days - ((year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400);

    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

    int month = 1;
    int accumulated = 0;
    for (int m = 1; m <= 12; ++m) {
        int days_this_month = days_in_month[m];
        if (m == 2 && leap) days_this_month = 29;
        if (accumulated + days_this_month > remaining) {
            month = m;
            break;
        }
        accumulated += days_this_month;
    }
    int day = remaining - accumulated + 1;

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << year << "-"
        << std::setw(2) << month << "-" << std::setw(2) << day;
    return oss.str();
}

// ==================== MMAP UTILITIES ====================
template<typename T>
T* mmap_column(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    file_size = sb.st_size;
    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "Error: mmap failed for " << path << std::endl;
        return nullptr;
    }
    madvise(addr, file_size, MADV_SEQUENTIAL);
    return static_cast<T*>(addr);
}

// ==================== STRING LOADING ====================
// Load string column (stored with length-prefixed strings)
// Format: each row is [4-byte length][length-byte string] repeated
std::vector<std::string> load_string_column(const std::string& path, size_t expected_rows) {
    std::vector<std::string> col(expected_rows);
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return col;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return col;
    }

    void* mapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) {
        std::cerr << "Error: mmap failed for " << path << std::endl;
        return col;
    }

    const char* data = (const char*)mapped;
    const char* end_ptr = data + sb.st_size;

    const char* ptr = data;
    size_t idx = 0;

    while (ptr < end_ptr && idx < expected_rows) {
        // Read 4-byte length
        if (ptr + 4 > end_ptr) break;
        uint32_t len = *((const uint32_t*)ptr);
        ptr += 4;

        // Read string data
        if (ptr + len > end_ptr) break;
        col[idx] = std::string(ptr, len);
        ptr += len;
        idx++;
    }

    munmap(mapped, sb.st_size);
    return col;
}

// ==================== AGGREGATION STRUCTURES ====================
// Intermediate result: integer-only keys (no strings to avoid memory overhead)
struct OrderAgg {
    int32_t o_custkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;
};

struct ResultRow {
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;

    bool operator<(const ResultRow& other) const {
        if (o_totalprice != other.o_totalprice) {
            return o_totalprice > other.o_totalprice;
        }
        return o_orderdate < other.o_orderdate;
    }
};

// ==================== MAIN QUERY EXECUTION ====================
void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto total_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t lineitem_rows = 59986052;
    const size_t orders_rows = 15000000;
    const size_t customer_rows = 1500000;
    const unsigned num_threads = std::thread::hardware_concurrency();

    // ==================== STEP 1: SUBQUERY - Find orders with SUM(l_quantity) > 300 ====================
#ifdef GENDB_PROFILE
    auto t1_start = std::chrono::high_resolution_clock::now();
#endif

    size_t file_size;
    auto l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    auto l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", file_size);
    if (!l_orderkey || !l_quantity) return;

    // Thread-local aggregation
    std::vector<std::unordered_map<int32_t, int64_t>> quantity_by_orderkey_per_thread(num_threads);
    std::atomic<size_t> lineitem_counter(0);

    auto scan_lineitem_subquery = [&](int thread_id) {
        const size_t morsel_size = 100000;
        while (true) {
            size_t start_idx = lineitem_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= lineitem_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, lineitem_rows);

            for (size_t i = start_idx; i < end_idx; i++) {
                quantity_by_orderkey_per_thread[thread_id][l_orderkey[i]] += l_quantity[i];
            }
        }
    };

    std::vector<std::thread> threads;
    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back(scan_lineitem_subquery, t);
    }
    for (auto& th : threads) th.join();
    threads.clear();

    // Merge thread-local maps and filter HAVING SUM(l_quantity) > 300
    // l_quantity is stored as int64_t with scale factor 100, so 300 * 100 = 30000
    std::unordered_set<int32_t> qualified_orderkeys;
    for (auto& local_map : quantity_by_orderkey_per_thread) {
        for (auto& kv : local_map) {
            if (kv.second > 30000) {
                qualified_orderkeys.insert(kv.first);
            }
        }
    }
    quantity_by_orderkey_per_thread.clear();

    munmap(l_orderkey, file_size);
    munmap(l_quantity, file_size);

#ifdef GENDB_PROFILE
    auto t1_end = std::chrono::high_resolution_clock::now();
    double ms1 = std::chrono::duration<double, std::milli>(t1_end - t1_start).count();
    printf("[TIMING] subquery: %.2f ms\n", ms1);
#endif

    // ==================== STEP 2: Build qualified orders hash ====================
#ifdef GENDB_PROFILE
    auto t2_start = std::chrono::high_resolution_clock::now();
#endif

    auto o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", file_size);
    auto o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", file_size);
    auto o_orderdate = mmap_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", file_size);
    auto o_totalprice = mmap_column<int64_t>(gendb_dir + "/orders/o_totalprice.bin", file_size);
    if (!o_orderkey || !o_custkey || !o_orderdate || !o_totalprice) return;

    // Build hash map: orderkey → (custkey, orderdate, totalprice)
    struct OrderInfo {
        int32_t o_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
    };

    std::unordered_map<int32_t, OrderInfo> orders_hash;
    for (size_t i = 0; i < orders_rows; i++) {
        if (qualified_orderkeys.count(o_orderkey[i])) {
            orders_hash[o_orderkey[i]] = {o_custkey[i], o_orderdate[i], o_totalprice[i]};
        }
    }

    munmap(o_orderkey, file_size);
    munmap(o_custkey, file_size);
    munmap(o_orderdate, file_size);
    munmap(o_totalprice, file_size);

#ifdef GENDB_PROFILE
    auto t2_end = std::chrono::high_resolution_clock::now();
    double ms2 = std::chrono::duration<double, std::milli>(t2_end - t2_start).count();
    printf("[TIMING] orders_filter: %.2f ms\n", ms2);
#endif

    // ==================== STEP 3: Load customer and build hash ====================
#ifdef GENDB_PROFILE
    auto t3_start = std::chrono::high_resolution_clock::now();
#endif

    auto c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", file_size);
    if (!c_custkey) return;

    // Load customer names as vector
    auto c_name = load_string_column(gendb_dir + "/customer/c_name.bin", customer_rows);

    // Build hash map: custkey → (custkey_idx for later lookup)
    std::unordered_map<int32_t, size_t> customer_idx_hash;
    for (size_t i = 0; i < customer_rows; i++) {
        customer_idx_hash[c_custkey[i]] = i;
    }

    munmap(c_custkey, file_size);

#ifdef GENDB_PROFILE
    auto t3_end = std::chrono::high_resolution_clock::now();
    double ms3 = std::chrono::duration<double, std::milli>(t3_end - t3_start).count();
    printf("[TIMING] customer_build: %.2f ms\n", ms3);
#endif

    // ==================== STEP 4: Scan lineitem and aggregate at order level ====================
#ifdef GENDB_PROFILE
    auto t4_start = std::chrono::high_resolution_clock::now();
#endif

    // Load lineitem columns
    auto l_orderkey_2 = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    auto l_quantity_2 = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", file_size);
    if (!l_orderkey_2 || !l_quantity_2) return;

    // Integer-only aggregation: key = o_orderkey (unique within qualified set)
    // Value = (o_custkey, o_orderdate, o_totalprice, sum_qty)
    std::unordered_map<int32_t, OrderAgg> order_agg_hash;
    order_agg_hash.reserve(qualified_orderkeys.size());

    // Single pass through lineitem: probe qualified orders, aggregate quantity per order
    for (size_t i = 0; i < lineitem_rows; i++) {
        int32_t oid = l_orderkey_2[i];
        auto order_it = orders_hash.find(oid);
        if (order_it != orders_hash.end()) {
            // This orderkey is qualified
            auto agg_it = order_agg_hash.find(oid);
            if (agg_it == order_agg_hash.end()) {
                // First occurrence of this order: initialize with order metadata
                order_agg_hash[oid] = {
                    order_it->second.o_custkey,
                    order_it->second.o_orderdate,
                    order_it->second.o_totalprice,
                    l_quantity_2[i]
                };
            } else {
                // Subsequent occurrence: just add to sum_qty
                agg_it->second.sum_qty += l_quantity_2[i];
            }
        }
    }

    munmap(l_orderkey_2, file_size);
    munmap(l_quantity_2, file_size);

#ifdef GENDB_PROFILE
    auto t4_end = std::chrono::high_resolution_clock::now();
    double ms4 = std::chrono::duration<double, std::milli>(t4_end - t4_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms4);
#endif

    // ==================== STEP 5: Top-K sort (ORDER BY o_totalprice DESC, o_orderdate ASC LIMIT 100) ====================
#ifdef GENDB_PROFILE
    auto t5_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t K = 100;

    // Create intermediate results WITHOUT customer names (late materialization)
    struct IntermediateRow {
        int32_t o_orderkey;
        int32_t o_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
        int64_t sum_qty;

        bool operator<(const IntermediateRow& other) const {
            if (o_totalprice != other.o_totalprice) {
                return o_totalprice > other.o_totalprice;
            }
            return o_orderdate < other.o_orderdate;
        }
    };

    std::vector<IntermediateRow> all_results;
    all_results.reserve(order_agg_hash.size());

    for (auto& kv : order_agg_hash) {
        all_results.push_back({
            kv.first,
            kv.second.o_custkey,
            kv.second.o_orderdate,
            kv.second.o_totalprice,
            kv.second.sum_qty
        });
    }

    // Partial sort to get top K
    size_t limit = std::min(K, all_results.size());
    std::partial_sort(all_results.begin(), all_results.begin() + limit, all_results.end());

    // Late materialization: load customer names ONLY for top-K results
    std::vector<ResultRow> results;
    results.reserve(limit);

    for (size_t i = 0; i < limit; i++) {
        const auto& irow = all_results[i];
        auto cust_it = customer_idx_hash.find(irow.o_custkey);
        if (cust_it != customer_idx_hash.end()) {
            results.push_back({
                c_name[cust_it->second],
                irow.o_custkey,
                irow.o_orderkey,
                irow.o_orderdate,
                irow.o_totalprice,
                irow.sum_qty
            });
        }
    }

#ifdef GENDB_PROFILE
    auto t5_end = std::chrono::high_resolution_clock::now();
    double ms5 = std::chrono::duration<double, std::milli>(t5_end - t5_start).count();
    printf("[TIMING] sort_topk: %.2f ms\n", ms5);
#endif

    // ==================== OUTPUT ====================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q18.csv");
        out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";
        for (const auto& row : results) {
            // o_totalprice is in cents (scale 100), convert to dollars for output
            double price_dollars = (double)row.o_totalprice / 100.0;
            // l_quantity is in scale 100, convert to actual quantity
            double sum_qty = (double)row.sum_qty / 100.0;

            out << row.c_name << ","
                << row.c_custkey << ","
                << row.o_orderkey << ","
                << days_to_date_str(row.o_orderdate) << ","
                << std::fixed << std::setprecision(2) << price_dollars << ","
                << std::fixed << std::setprecision(2) << sum_qty << "\n";
        }
        out.close();
    }

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);

    auto total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(total_end - total_start).count();
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
    std::string results_dir = argc > 2 ? argv[2] : "";
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
