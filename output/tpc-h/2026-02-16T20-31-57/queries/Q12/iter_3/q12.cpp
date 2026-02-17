/*
 * Q12: Shipping Modes and Order Priority
 *
 * QUERY PLAN:
 *
 * LOGICAL PLAN:
 * 1. Filter lineitem table:
 *    - l_shipmode IN ('MAIL', 'SHIP')          [selectivity: 2/7 ≈ 28%]
 *    - l_commitdate < l_receiptdate            [selectivity: ~70%]
 *    - l_shipdate < l_commitdate               [selectivity: ~70%]
 *    - l_receiptdate >= 1994-01-01 (8766)      [selectivity: ~100%]
 *    - l_receiptdate < 1995-01-01 (9131)       [selectivity: ~50%]
 *    Estimated: 59M * 0.28 * 0.7 * 0.7 * 1.0 * 0.5 ≈ 5.8M rows
 *
 * 2. Hash join (lineitem filtered -> orders):
 *    - Join key: l_orderkey = o_orderkey
 *    - Lineitem is the larger filtered side (5.8M)
 *    - Orders has 15M rows
 *    - Build hash table on filtered lineitem (5.8M), probe with orders
 *    - Actually: lineitem side is larger. Build on orders (15M), probe with filtered lineitem (5.8M)
 *    - Better strategy: Load o_orderpriority into hash table keyed by o_orderkey (15M entries)
 *    - Then scan lineitem, probe the orders hash table for priority, and accumulate into aggregation
 *
 * 3. Aggregation on l_shipmode:
 *    - Group by l_shipmode (low cardinality: 7 distinct values, but filtered to 2 values = MAIL, SHIP)
 *    - Use flat array for aggregation: array[shipmode_code] for high/low counts
 *    - Two counters per shipmode: high_line_count and low_line_count
 *
 * PHYSICAL PLAN:
 * 1. Scan orders: Load o_orderkey and o_orderpriority
 * 2. Build hash table on orders (15M entries) with o_orderkey → o_orderpriority
 * 3. Scan lineitem with filter predicates (zones, dates, shipmode):
 *    - Load required columns: l_orderkey, l_shipmode, l_commitdate, l_receiptdate, l_shipdate
 *    - Apply date/shipmode predicates to identify qualifying rows
 *    - Probe orders hash table to get o_orderpriority
 *    - Aggregate into result array indexed by l_shipmode
 * 4. Write aggregation results to CSV (MAIL, SHIP only)
 *
 * PARALLELISM:
 * - Orders scan: load into memory (single-threaded, small)
 * - Lineitem scan + filter + join + aggregate: OpenMP parallel for with thread-local aggregation buffers
 *   (64 cores available, morsel size ~100K rows)
 */

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <chrono>
#include <omp.h>
#include <algorithm>

// ============================================================================
// CONFIGURATION & CONSTANTS
// ============================================================================

const int32_t DATE_1994_01_01 = 8766;  // 1994-01-01
const int32_t DATE_1995_01_01 = 9131;  // 1995-01-01

// Dictionary code mapping for l_shipmode
// From l_shipmode_dict.txt: line N contains the value for code (N-1)
// Code 0 is "TRUCK", Code 1 is "MAIL", Code 6 is "SHIP"
const int32_t SHIPMODE_MAIL = 1;
const int32_t SHIPMODE_SHIP = 6;

// Dictionary code mapping for o_orderpriority (0-indexed in dict)
// From o_orderpriority_dict.txt:
// Code 0 = "5-LOW", Code 1 = "1-URGENT", Code 3 = "2-HIGH"
const int32_t PRIORITY_URGENT = 1;
const int32_t PRIORITY_HIGH = 3;

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// Memory-mapped file loading
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::perror("open");
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    size = file_size;
    lseek(fd, 0, SEEK_SET);

    void* data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::perror("mmap");
        return nullptr;
    }

    return data;
}

// Load dictionary file: parse "line0\nline1\n..." format
std::vector<std::string> load_dictionary(const std::string& dict_path) {
    std::vector<std::string> dict;
    FILE* f = fopen(dict_path.c_str(), "r");
    if (!f) {
        std::perror("fopen dict");
        return dict;
    }

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        // Remove trailing newline
        size_t len = strlen(line);
        if (len > 0 && line[len-1] == '\n') line[len-1] = '\0';
        dict.push_back(std::string(line));
    }

    fclose(f);
    return dict;
}

// Convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t epoch_days) {
    int year = 1970;
    int days_left = epoch_days;

    // Advance through years
    while (true) {
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_year = leap ? 366 : 365;
        if (days_left < days_in_year) break;
        days_left -= days_in_year;
        year++;
    }

    // Find month and day
    const int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

    int month = 1;
    int day = days_left + 1;  // Days are 1-indexed

    for (int m = 0; m < 12; m++) {
        int dim = days_per_month[m];
        if (m == 1 && leap) dim = 29;
        if (day <= dim) {
            month = m + 1;
            break;
        }
        day -= dim;
    }

    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// ============================================================================
// HASH TABLE FOR JOIN (open-addressing for 2-5x speedup over unordered_map)
// ============================================================================

template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

struct OrdersPriorityMap {
    CompactHashTable<int32_t, int32_t> data;  // orderkey -> priority_code

    OrdersPriorityMap() : data(15000000) {}
};

// ============================================================================
// AGGREGATION RESULT
// ============================================================================

struct AggResult {
    int64_t high_count;
    int64_t low_count;
};

// ============================================================================
// MAIN QUERY EXECUTION
// ============================================================================

void run_q12(const std::string& gendb_dir, const std::string& results_dir) {
    printf("[Q12] Starting query execution\n");

    // Timing for total query
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // PHASE 1: LOAD ORDERS AND BUILD HASH TABLE
    // ========================================================================

    printf("[Q12] Phase 1: Load orders table\n");

#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    std::string orders_orderkey_path = gendb_dir + "/orders/o_orderkey.bin";
    std::string orders_priority_path = gendb_dir + "/orders/o_orderpriority.bin";

    size_t orders_size_key = 0, orders_size_priority = 0;
    int32_t* o_orderkey = (int32_t*)mmap_file(orders_orderkey_path, orders_size_key);
    int32_t* o_orderpriority = (int32_t*)mmap_file(orders_priority_path, orders_size_priority);

    if (!o_orderkey || !o_orderpriority) {
        fprintf(stderr, "Failed to load orders tables\n");
        return;
    }

    int64_t num_orders = orders_size_key / sizeof(int32_t);
    printf("[Q12] Loaded %ld orders\n", num_orders);

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms_orders);
#endif

    // Build hash table: o_orderkey -> o_orderpriority
#ifdef GENDB_PROFILE
    auto t_ht_build_start = std::chrono::high_resolution_clock::now();
#endif

    OrdersPriorityMap ht;
    for (int64_t i = 0; i < num_orders; i++) {
        ht.data.insert(o_orderkey[i], o_orderpriority[i]);
    }

    printf("[Q12] Built hash table with %ld entries\n", num_orders);

#ifdef GENDB_PROFILE
    auto t_ht_build_end = std::chrono::high_resolution_clock::now();
    double ms_ht_build = std::chrono::duration<double, std::milli>(t_ht_build_end - t_ht_build_start).count();
    printf("[TIMING] build_orders_ht: %.2f ms\n", ms_ht_build);
#endif

    // ========================================================================
    // PHASE 2: SCAN LINEITEM WITH FILTERS AND JOIN
    // ========================================================================

    printf("[Q12] Phase 2: Scan lineitem with filters\n");

#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    std::string li_orderkey_path = gendb_dir + "/lineitem/l_orderkey.bin";
    std::string li_shipmode_path = gendb_dir + "/lineitem/l_shipmode.bin";
    std::string li_shipdate_path = gendb_dir + "/lineitem/l_shipdate.bin";
    std::string li_commitdate_path = gendb_dir + "/lineitem/l_commitdate.bin";
    std::string li_receiptdate_path = gendb_dir + "/lineitem/l_receiptdate.bin";

    size_t li_size_orderkey = 0, li_size_shipmode = 0, li_size_shipdate = 0;
    size_t li_size_commitdate = 0, li_size_receiptdate = 0;

    int32_t* l_orderkey = (int32_t*)mmap_file(li_orderkey_path, li_size_orderkey);
    int32_t* l_shipmode = (int32_t*)mmap_file(li_shipmode_path, li_size_shipmode);
    int32_t* l_shipdate = (int32_t*)mmap_file(li_shipdate_path, li_size_shipdate);
    int32_t* l_commitdate = (int32_t*)mmap_file(li_commitdate_path, li_size_commitdate);
    int32_t* l_receiptdate = (int32_t*)mmap_file(li_receiptdate_path, li_size_receiptdate);

    if (!l_orderkey || !l_shipmode || !l_shipdate || !l_commitdate || !l_receiptdate) {
        fprintf(stderr, "Failed to load lineitem tables\n");
        return;
    }

    int64_t num_lineitem = li_size_orderkey / sizeof(int32_t);
    printf("[Q12] Loaded %ld lineitem rows\n", num_lineitem);

#ifdef GENDB_PROFILE
    auto t_lineitem_load_end = std::chrono::high_resolution_clock::now();
    double ms_lineitem_load = std::chrono::duration<double, std::milli>(t_lineitem_load_end - t_lineitem_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms_lineitem_load);
#endif

    // ========================================================================
    // PHASE 3: AGGREGATION WITH PARALLEL PROCESSING
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_scan_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Global aggregation result
    // We have 2 output groups: MAIL (code 1) and SHIP (code 6)
    // Use a small array indexed by shipmode code (0-6)
    std::vector<AggResult> global_agg(7, {0, 0});

    // Thread-local aggregation buffers for lock-free parallel execution
    // Each thread has its own 7-element array to avoid contention
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<AggResult>> thread_local_agg(num_threads, std::vector<AggResult>(7, {0, 0}));

    // Parallel scan + filter + join + aggregate (NO CRITICAL SECTIONS)
    #pragma omp parallel for num_threads(64) schedule(static, 100000)
    for (int64_t i = 0; i < num_lineitem; i++) {
        int tid = omp_get_thread_num();

        // Apply predicates
        // 1. l_shipmode IN ('MAIL', 'SHIP') -> codes 1 or 6
        if (l_shipmode[i] != SHIPMODE_MAIL && l_shipmode[i] != SHIPMODE_SHIP) continue;

        // 2. l_commitdate < l_receiptdate
        if (l_commitdate[i] >= l_receiptdate[i]) continue;

        // 3. l_shipdate < l_commitdate
        if (l_shipdate[i] >= l_commitdate[i]) continue;

        // 4. l_receiptdate >= 1994-01-01
        if (l_receiptdate[i] < DATE_1994_01_01) continue;

        // 5. l_receiptdate < 1995-01-01
        if (l_receiptdate[i] >= DATE_1995_01_01) continue;

        // Row passed all filters, join with orders
        int32_t* o_priority_ptr = ht.data.find(l_orderkey[i]);
        if (o_priority_ptr == nullptr) continue;  // No matching order

        int32_t o_priority = *o_priority_ptr;
        int32_t shipmode = l_shipmode[i];

        // Determine if high or low priority
        bool is_high = (o_priority == PRIORITY_URGENT || o_priority == PRIORITY_HIGH);

        // Aggregate to thread-local buffer (no contention)
        if (is_high) {
            thread_local_agg[tid][shipmode].high_count++;
        } else {
            thread_local_agg[tid][shipmode].low_count++;
        }
    }

    // Merge thread-local aggregation into global result
    for (int t = 0; t < num_threads; t++) {
        for (int s = 0; s < 7; s++) {
            global_agg[s].high_count += thread_local_agg[t][s].high_count;
            global_agg[s].low_count += thread_local_agg[t][s].low_count;
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_filter_end = std::chrono::high_resolution_clock::now();
    double ms_scan_filter = std::chrono::duration<double, std::milli>(t_scan_filter_end - t_scan_filter_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms\n", ms_scan_filter);
#endif

    // ========================================================================
    // PHASE 4: WRITE RESULTS
    // ========================================================================

    printf("[Q12] Phase 4: Write results\n");

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    // Load shipmode dictionary for output
    std::string shipmode_dict_path = gendb_dir + "/lineitem/l_shipmode_dict.txt";
    std::vector<std::string> shipmode_dict = load_dictionary(shipmode_dict_path);

    std::string output_path = results_dir + "/Q12.csv";
    FILE* out = fopen(output_path.c_str(), "w");
    if (!out) {
        std::perror("fopen output");
        return;
    }

    // Write header
    fprintf(out, "l_shipmode,high_line_count,low_line_count\n");

    // Write results for MAIL and SHIP in sorted order
    // MAIL (code 1) and SHIP (code 6)
    std::vector<int32_t> output_codes = {SHIPMODE_MAIL, SHIPMODE_SHIP};

    for (int32_t code : output_codes) {
        if (code < (int32_t)shipmode_dict.size()) {
            fprintf(out, "%s,%ld,%ld\n",
                    shipmode_dict[code].c_str(),
                    global_agg[code].high_count,
                    global_agg[code].low_count);
        }
    }

    fclose(out);
    printf("[Q12] Results written to %s\n", output_path.c_str());

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    // ========================================================================
    // CLEANUP
    // ========================================================================

    munmap(o_orderkey, orders_size_key);
    munmap(o_orderpriority, orders_size_priority);
    munmap(l_orderkey, li_size_orderkey);
    munmap(l_shipmode, li_size_shipmode);
    munmap(l_shipdate, li_size_shipdate);
    munmap(l_commitdate, li_size_commitdate);
    munmap(l_receiptdate, li_size_receiptdate);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    printf("[Q12] Query execution complete\n");
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

    run_q12(gendb_dir, results_dir);

    return 0;
}
#endif
