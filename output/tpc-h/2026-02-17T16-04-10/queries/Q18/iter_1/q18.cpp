// Q18: Large Volume Customer - Iteration 1 Optimizations
//
// Logical Plan:
//   1. Subquery: GROUP BY l_orderkey, HAVING SUM(l_quantity) > 300 → hash set of qualified orderkeys
//   2. Filter orders: o_orderkey IN semi-join set → ~3K orders
//   3. Join customer + orders on o_custkey = c_custkey
//   4. Join with lineitem on o_orderkey = l_orderkey
//   5. GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) → SUM(l_quantity)
//   6. ORDER BY o_totalprice DESC, o_orderdate ASC, LIMIT 100
//
// Physical Plan (Optimized):
//   - Step 1: Parallel scan lineitem → compact hash aggregate by l_orderkey → filter HAVING > 300
//   - Step 2: Build hash set of qualified l_orderkey values (~3K entries)
//   - Step 3: Load customer, build compact hash by c_custkey (1.5M rows)
//   - Step 4: Build compact hash on filtered orders (~3K rows by qualified orderkeys)
//   - Step 5: Parallel scan lineitem, filter by qualified set, join orders+customer, compact hash aggregate
//   - Step 6: Partial sort (Top-100) on (o_totalprice DESC, o_orderdate ASC)
//
// Key Optimizations:
//   - Replace std::unordered_map with compact robin hood hash tables (2-5x faster)
//   - Pre-size all hash tables to avoid resizing
//   - Simplified aggregation key without embedded string
//   - Proper murmur hash instead of std::hash (avoid identity function clustering)

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

// ==================== COMPACT HASH TABLES ====================
// Compact robin hood hash table for int32_t → int64_t (aggregation)
// 2-5x faster than std::unordered_map due to open addressing and cache-friendly layout
struct CompactHashI32I64 {
    struct Entry {
        int32_t key;
        int64_t value;
        uint8_t dist;
        bool occupied;
    };
    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashI32I64(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap, {0, 0, 0, false});
        mask = cap - 1;
        count = 0;
    }

    // Murmur3-style hash for better distribution (avoid std::hash identity function)
    static inline size_t hash_key(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        return h ^ (h >> 32);
    }

    void insert_or_add(int32_t key, int64_t value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value += value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    int64_t* find(int32_t key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                return &table[pos].value;
            }
            if (dist > table[pos].dist) {
                return nullptr;
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }
};

// Compact hash table for int32_t → struct payload
struct OrderInfo {
    int32_t o_custkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
};

struct CompactHashI32Order {
    struct Entry {
        int32_t key;
        OrderInfo value;
        uint8_t dist;
        bool occupied;
    };
    std::vector<Entry> table;
    size_t mask;

    CompactHashI32Order(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap, {0, {0, 0, 0}, 0, false});
        mask = cap - 1;
    }

    static inline size_t hash_key(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        return h ^ (h >> 32);
    }

    void insert(int32_t key, const OrderInfo& value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
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

    OrderInfo* find(int32_t key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                return &table[pos].value;
            }
            if (dist > table[pos].dist) {
                return nullptr;
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }
};

// Compact hash table for int32_t → size_t (customer index lookup)
struct CompactHashI32Size {
    struct Entry {
        int32_t key;
        size_t value;
        uint8_t dist;
        bool occupied;
    };
    std::vector<Entry> table;
    size_t mask;

    CompactHashI32Size(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap, {0, 0, 0, false});
        mask = cap - 1;
    }

    static inline size_t hash_key(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        return h ^ (h >> 32);
    }

    void insert(int32_t key, size_t value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
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

    size_t* find(int32_t key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                return &table[pos].value;
            }
            if (dist > table[pos].dist) {
                return nullptr;
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
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
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;

    bool operator<(const ResultRow& other) const {
        // For priority_queue: we want to keep top 100 by DESCENDING o_totalprice
        // priority_queue is a max-heap by default: larger values percolate to top
        // To get descending order (largest first), reverse the comparison
        // Return true if this should be "less than" other (in heap terms)
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

    // Thread-local aggregation with compact hash tables (2-5x faster than std::unordered_map)
    // Estimate ~15M unique orderkeys, each thread processes ~1M orderkeys
    std::vector<CompactHashI32I64> quantity_by_orderkey_per_thread;
    quantity_by_orderkey_per_thread.reserve(num_threads);
    for (unsigned t = 0; t < num_threads; t++) {
        quantity_by_orderkey_per_thread.emplace_back(1500000);  // Pre-size for ~1M per thread
    }

    std::atomic<size_t> lineitem_counter(0);

    auto scan_lineitem_subquery = [&](int thread_id) {
        const size_t morsel_size = 100000;
        while (true) {
            size_t start_idx = lineitem_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= lineitem_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, lineitem_rows);

            for (size_t i = start_idx; i < end_idx; i++) {
                quantity_by_orderkey_per_thread[thread_id].insert_or_add(l_orderkey[i], l_quantity[i]);
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
    CompactHashI32I64 merged_quantities(15000000);  // Pre-size for all unique orderkeys
    for (auto& local_map : quantity_by_orderkey_per_thread) {
        for (const auto& entry : local_map.table) {
            if (entry.occupied) {
                merged_quantities.insert_or_add(entry.key, entry.value);
            }
        }
    }
    quantity_by_orderkey_per_thread.clear();

    // Filter HAVING > 300 and build qualified set
    std::unordered_set<int32_t> qualified_orderkeys;
    qualified_orderkeys.reserve(5000);  // Estimate ~3K qualified orders
    for (const auto& entry : merged_quantities.table) {
        if (entry.occupied && entry.value > 30000) {
            qualified_orderkeys.insert(entry.key);
        }
    }

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

    // Build compact hash map: orderkey → (custkey, orderdate, totalprice)
    CompactHashI32Order orders_hash(qualified_orderkeys.size());  // Pre-size for ~3K entries
    for (size_t i = 0; i < orders_rows; i++) {
        if (qualified_orderkeys.count(o_orderkey[i])) {
            orders_hash.insert(o_orderkey[i], {o_custkey[i], o_orderdate[i], o_totalprice[i]});
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

    // Build compact hash map: custkey → custkey_idx for later lookup
    CompactHashI32Size customer_idx_hash(customer_rows);  // Pre-size for 1.5M customers
    for (size_t i = 0; i < customer_rows; i++) {
        customer_idx_hash.insert(c_custkey[i], i);
    }

    munmap(c_custkey, file_size);

#ifdef GENDB_PROFILE
    auto t3_end = std::chrono::high_resolution_clock::now();
    double ms3 = std::chrono::duration<double, std::milli>(t3_end - t3_start).count();
    printf("[TIMING] customer_build: %.2f ms\n", ms3);
#endif

    // ==================== STEP 4: GROUP BY and aggregate ====================
#ifdef GENDB_PROFILE
    auto t4_start = std::chrono::high_resolution_clock::now();
#endif

    // Scan lineitem again (second pass), probe orders hash, join with customer, aggregate
    // Key insight: o_orderkey uniquely identifies the group (one customer, one date, one price per order)
    // So we use o_orderkey as the aggregation key and store other fields as payload

    // Aggregation payload: store customer and order info for each orderkey
    struct AggValue {
        int32_t c_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
        size_t c_name_idx;  // Index into c_name vector
        int64_t sum_qty;
    };

    // Load lineitem columns again for second scan
    auto l_orderkey_2 = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", file_size);
    auto l_quantity_2 = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", file_size);
    if (!l_orderkey_2 || !l_quantity_2) return;

    // Thread-local aggregation maps: orderkey → aggregate value
    std::vector<CompactHashI32I64> qty_per_orderkey_per_thread;
    qty_per_orderkey_per_thread.reserve(num_threads);
    for (unsigned t = 0; t < num_threads; t++) {
        qty_per_orderkey_per_thread.emplace_back(5000);  // Pre-size for ~3K qualified orders
    }

    // Store first occurrence info (custkey, orderdate, totalprice, c_name_idx) per orderkey
    // Since each orderkey appears in only one order, we only need to store this once
    std::vector<std::unordered_map<int32_t, AggValue>> info_per_orderkey_per_thread(num_threads);

    lineitem_counter.store(0);

    auto scan_lineitem_main = [&](int thread_id) {
        const size_t morsel_size = 100000;
        while (true) {
            size_t start_idx = lineitem_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_idx >= lineitem_rows) break;
            size_t end_idx = std::min(start_idx + morsel_size, lineitem_rows);

            for (size_t i = start_idx; i < end_idx; i++) {
                int32_t oid = l_orderkey_2[i];

                // Check if this orderkey is qualified
                if (qualified_orderkeys.count(oid) == 0) continue;

                // Lookup order info
                auto* order_info = orders_hash.find(oid);
                if (order_info == nullptr) continue;

                // Lookup customer info
                auto* cust_idx = customer_idx_hash.find(order_info->o_custkey);
                if (cust_idx == nullptr) continue;

                // Aggregate quantity
                qty_per_orderkey_per_thread[thread_id].insert_or_add(oid, l_quantity_2[i]);

                // Store order+customer info on first occurrence
                if (info_per_orderkey_per_thread[thread_id].find(oid) == info_per_orderkey_per_thread[thread_id].end()) {
                    info_per_orderkey_per_thread[thread_id][oid] = {
                        order_info->o_custkey,
                        order_info->o_orderdate,
                        order_info->o_totalprice,
                        *cust_idx,
                        0
                    };
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

    // Merge thread-local aggregations and collect results
    CompactHashI32I64 merged_qty(5000);  // Pre-size for ~3K qualified orders
    std::unordered_map<int32_t, AggValue> merged_info;

    for (auto& local_qty_map : qty_per_orderkey_per_thread) {
        for (const auto& entry : local_qty_map.table) {
            if (entry.occupied) {
                merged_qty.insert_or_add(entry.key, entry.value);
            }
        }
    }

    for (auto& local_info_map : info_per_orderkey_per_thread) {
        for (auto& kv : local_info_map) {
            merged_info[kv.first] = kv.second;
        }
    }

    // Build result rows
    for (const auto& entry : merged_qty.table) {
        if (entry.occupied) {
            int32_t orderkey = entry.key;
            int64_t sum_qty = entry.value;

            auto info_it = merged_info.find(orderkey);
            if (info_it != merged_info.end()) {
                const AggValue& info = info_it->second;
                ResultRow row{
                    c_name[info.c_name_idx],
                    info.c_custkey,
                    orderkey,
                    info.o_orderdate,
                    info.o_totalprice,
                    sum_qty
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
