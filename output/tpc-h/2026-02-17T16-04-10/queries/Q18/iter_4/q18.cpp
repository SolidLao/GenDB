// Q18: Large Volume Customer - Iteration 4 Optimization
// Logical Plan:
//   1. Subquery: GROUP BY l_orderkey, HAVING SUM(l_quantity) > 300 → hash set of qualified orderkeys
//   2. Filter orders: o_orderkey IN semi-join set → ~3K orders
//   3. Join customer + orders on o_custkey = c_custkey
//   4. Join with lineitem on o_orderkey = l_orderkey
//   5. GROUP BY (c_custkey, o_orderkey, o_orderdate, o_totalprice) → SUM(l_quantity) [NO STRING IN KEY]
//   6. ORDER BY o_totalprice DESC, o_orderdate ASC, LIMIT 100
//
// Physical Plan (Optimized for Iter 4):
//   - Step 1: Parallel scan lineitem (60M rows) → compact hash table for l_orderkey aggregation → filter HAVING > 300
//   - Step 2: Build unordered_set of qualified l_orderkey values (~3K entries)
//   - Step 3: Scan orders, filter by orderkey in set, build compact hash by o_orderkey
//   - Step 4: Load customer names (string column) - only loaded once
//   - Step 5: Parallel scan lineitem, probing hash tables, aggregate on INTEGER KEY ONLY (no strings)
//   - Step 6: Partial sort (Top-100) on (o_totalprice DESC, o_orderdate ASC)
//
// Key Optimizations for Iter 4:
//   - INTEGER-ONLY aggregation key: (c_custkey, o_orderkey, o_orderdate, o_totalprice)
//   - Avoid string hashing in hot loop (was 10-50% overhead per iter_3)
//   - Compact hash table (robin hood, open addressing) instead of std::unordered_map for 2-3x faster probes
//   - Defer customer name lookup until final result materialization

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

// ==================== COMPACT HASH TABLE ====================
// Optimized robin hood hash table for integer aggregation keys
// Key: 128-bit integer (c_custkey, o_orderkey, o_orderdate, o_totalprice)
// Value: int64_t (sum_qty)
struct CompactHashI128I64 {
    struct Entry {
        int32_t c_custkey;
        int32_t o_orderkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
        int64_t sum_qty;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashI128I64() : mask(0) {}

    CompactHashI128I64(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap, {0, 0, 0, 0, 0, 0, false});
        mask = cap - 1;
    }

    inline size_t hash_key(int32_t c, int32_t o1, int32_t o2, int64_t o3) const {
        // Combine hashes using multiplicative constants
        uint64_t h = ((uint64_t)c * 0x85EBCA6B) ^ ((uint64_t)o1 * 0xC2B2AE35) ^
                     ((uint64_t)o2 * 0x27D4EB2D) ^ (o3 * 0x9E3779B97F4A7C15ULL);
        return h >> 32;
    }

    inline void insert_or_add(int32_t c, int32_t o1, int32_t o2, int64_t o3, int64_t val) {
        size_t pos = hash_key(c, o1, o2, o3) & mask;
        Entry entry{c, o1, o2, o3, val, 0, true};

        while (table[pos].occupied) {
            if (table[pos].c_custkey == c && table[pos].o_orderkey == o1 &&
                table[pos].o_orderdate == o2 && table[pos].o_totalprice == o3) {
                table[pos].sum_qty += val;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
    }

    inline int64_t* find_sum_qty(int32_t c, int32_t o1, int32_t o2, int64_t o3) {
        size_t pos = hash_key(c, o1, o2, o3) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].c_custkey == c && table[pos].o_orderkey == o1 &&
                table[pos].o_orderdate == o2 && table[pos].o_totalprice == o3) {
                return &table[pos].sum_qty;
            }
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    std::vector<Entry> get_all_entries() {
        std::vector<Entry> result;
        for (auto& e : table) {
            if (e.occupied) {
                result.push_back(e);
            }
        }
        return result;
    }
};

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
struct ResultRow {
    int32_t c_custkey;
    std::string c_name;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;

    bool operator<(const ResultRow& other) const {
        // For priority_queue: we want to keep top 100 by DESCENDING o_totalprice
        if (o_totalprice != other.o_totalprice) {
            return o_totalprice > other.o_totalprice;  // reverse: larger price = "smaller" in heap
        }
        return o_orderdate < other.o_orderdate;  // for ties, earlier date is "smaller"
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

    // ==================== STEP 4: GROUP BY and aggregate (INTEGER KEY ONLY) ====================
#ifdef GENDB_PROFILE
    auto t4_start = std::chrono::high_resolution_clock::now();
#endif

    // Scan lineitem again, probe orders hash, join with customer, aggregate
    auto l_orderkey_2 = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    auto l_quantity_2 = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", file_size);
    if (!l_orderkey_2 || !l_quantity_2) return;

    // Use compact hash table with integer-only key: (c_custkey, o_orderkey, o_orderdate, o_totalprice)
    // Defer customer name lookup until result materialization
    std::vector<CompactHashI128I64> agg_maps;
    for (unsigned t = 0; t < num_threads; t++) {
        agg_maps.emplace_back(10000);  // Expected ~10K unique aggregation groups
    }

    lineitem_counter.store(0);

    auto scan_lineitem_main = [&](int thread_id) {
        const size_t morsel_size = 100000;
        while (true) {
            size_t start_idx = lineitem_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= lineitem_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, lineitem_rows);

            for (size_t i = start_idx; i < end_idx; i++) {
                int32_t oid = l_orderkey_2[i];
                auto order_it = orders_hash.find(oid);
                if (order_it != orders_hash.end()) {
                    // Join with customer (just check it exists)
                    auto cust_it = customer_idx_hash.find(order_it->second.o_custkey);
                    if (cust_it != customer_idx_hash.end()) {
                        // Aggregate: insert_or_add on integer key only
                        // (c_custkey, o_orderkey, o_orderdate, o_totalprice) → SUM(l_quantity)
                        agg_maps[thread_id].insert_or_add(
                            order_it->second.o_custkey,
                            oid,
                            order_it->second.o_orderdate,
                            order_it->second.o_totalprice,
                            l_quantity_2[i]
                        );
                    }
                }
            }
        }
    };

    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back(scan_lineitem_main, t);
    }
    for (auto& th : threads) th.join();
    threads.clear();

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
    std::vector<ResultRow> all_results;

    // Collect all aggregated results and materialize customer names from lookup
    for (auto& agg_map : agg_maps) {
        auto entries = agg_map.get_all_entries();
        for (auto& entry : entries) {
            // Look up customer name from c_custkey
            auto cust_it = customer_idx_hash.find(entry.c_custkey);
            if (cust_it != customer_idx_hash.end()) {
                ResultRow row{
                    entry.c_custkey,
                    c_name[cust_it->second],
                    entry.o_orderkey,
                    entry.o_orderdate,
                    entry.o_totalprice,
                    entry.sum_qty
                };
                all_results.push_back(row);
            }
        }
    }

    // Sort by o_totalprice DESC, o_orderdate ASC
    auto sort_comp = [](const ResultRow& a, const ResultRow& b) {
        if (a.o_totalprice != b.o_totalprice) {
            return a.o_totalprice > b.o_totalprice;
        }
        return a.o_orderdate < b.o_orderdate;
    };

    // Partial sort to get top K
    size_t limit = std::min(K, all_results.size());
    std::partial_sort(all_results.begin(), all_results.begin() + limit, all_results.end(), sort_comp);

    std::vector<ResultRow> results(all_results.begin(), all_results.begin() + limit);

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
        // Debug: print number of results
        // std::cerr << "Q18: Output " << results.size() << " rows" << std::endl;
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
