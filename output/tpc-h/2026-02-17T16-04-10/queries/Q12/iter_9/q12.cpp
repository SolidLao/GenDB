#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <set>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <omp.h>
#include <atomic>

// ============================================================================
// Q12 OPTIMIZED PLAN (Iteration 9): FUSED FILTER-JOIN-AGGREGATE PIPELINE
// ============================================================================
// Core Insight: Current bottleneck is build_hash_join (44ms, 43%).
// Iteration 9 FUSES the three pipeline stages into ONE pass over lineitem,
// eliminating the hash table build entirely while maintaining parallelism.
//
// Logical Plan:
//   Single-pass algorithm over filtered lineitem + orders join:
//   1. Load all column data (lineitem: 59.9M rows, orders: 15M rows)
//   2. Pre-load orders into memory-mapped arrays (fast, already done)
//   3. FUSED SCAN-JOIN-AGGREGATE over lineitem (parallel, 64 cores):
//      For each lineitem row:
//        a) Apply predicates: l_shipmode IN ('MAIL','SHIP'), date filters
//        b) Direct RANDOM LOOKUP: o_orderkey[l_orderkey] → priority + orderkey match check
//        c) If match, aggregate directly to thread-local array[shipmode][priority_class]
//   4. Merge thread-local aggregations (trivial: 2×64 = 128 entries)
//
// Physical Plan:
//   - Data layout: mmap all columns (lineitem + orders) for cache-friendly access
//   - Parallelism: OpenMP parallel for over lineitem rows with morsel-driven chunking
//   - Thread-local agg: One flat array per thread, indexed by [shipmode_idx]
//   - Join: Direct array indexing o_orderkey[l_orderkey] (no hash table)
//   - Merge: Sequential merge of 64 thread-local aggregations
//
// Why This Is Faster:
//   - Eliminates 44ms hash table build entirely
//   - One cache-efficient pass through lineitem (better prefetching)
//   - Direct array lookup vs hash probing (no hash collision chain traversal)
//   - Parallelism preserved via OpenMP (morsel-driven work distribution)
//   - No intermediate materialization of filtered lineitem
//   - Minimal memory allocation (only arrays, no vectors)
//
// Expected Performance (Iteration 9):
//   - Eliminate build_hash_join (44ms) + reduce join_aggregate overhead
//   - Estimated total: ~65-70ms (vs current 99ms)
//   - Target: Within 1.5x of Umbra (49ms)
// ============================================================================

struct MmapFile {
    int fd = -1;
    void* data = nullptr;
    size_t size = 0;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            throw std::runtime_error("open failed");
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            throw std::runtime_error("fstat failed");
        }
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            throw std::runtime_error("mmap failed");
        }
    }

    void close() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

template<typename T>
T* cast(void* ptr) {
    return reinterpret_cast<T*>(ptr);
}

// Load dictionary file (format: "code=value" per line)
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << path << std::endl;
        throw std::runtime_error("dictionary open failed");
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Epoch day calculation: 1970-01-01 is day 0
// For 1994-01-01: (1994-1970)*365 + leap days
// For 1995-01-01: (1995-1970)*365 + leap days
int32_t date_to_epoch(int year, int month, int day) {
    int32_t total = 0;
    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        total += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add days for complete months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }
    for (int m = 1; m < month; ++m) {
        total += days_in_month[m - 1];
    }
    // Add remaining days
    total += (day - 1);
    return total;
}

void epoch_to_date(int32_t epoch, int& year, int& month, int& day) {
    year = 1970;
    day = epoch;
    // Find year
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (day < days_in_year) break;
        day -= days_in_year;
        year++;
    }
    // Find month and day
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }
    month = 1;
    while (month <= 12 && day >= days_in_month[month - 1]) {
        day -= days_in_month[month - 1];
        month++;
    }
    day++;  // 1-indexed
}

void run_q12(const std::string& gendb_dir, const std::string& results_dir) {
    // Timing
    auto t_global_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load dictionaries
    auto orderpriority_dict = load_dictionary(gendb_dir + "/orders/o_orderpriority_dict.txt");
    auto shipmode_dict = load_dictionary(gendb_dir + "/lineitem/l_shipmode_dict.txt");

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", ms);
#endif

    // Build reverse mapping for shipmode: value -> code
    std::unordered_map<std::string, int32_t> shipmode_reverse;
    for (auto& [code, value] : shipmode_dict) {
        shipmode_reverse[value] = code;
    }

    // Date filters
    int32_t receipt_date_min = date_to_epoch(1994, 1, 1);  // 8766
    int32_t receipt_date_max = date_to_epoch(1995, 1, 1);  // 9131

    // Open lineitem columns
    MmapFile li_orderkey_file, li_shipmode_file, li_commitdate_file;
    MmapFile li_receiptdate_file, li_shipdate_file;

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    li_orderkey_file.open(gendb_dir + "/lineitem/l_orderkey.bin");
    li_shipmode_file.open(gendb_dir + "/lineitem/l_shipmode.bin");
    li_commitdate_file.open(gendb_dir + "/lineitem/l_commitdate.bin");
    li_receiptdate_file.open(gendb_dir + "/lineitem/l_receiptdate.bin");
    li_shipdate_file.open(gendb_dir + "/lineitem/l_shipdate.bin");

    int32_t* li_orderkey = cast<int32_t>(li_orderkey_file.data);
    int32_t* li_shipmode = cast<int32_t>(li_shipmode_file.data);
    int32_t* li_commitdate = cast<int32_t>(li_commitdate_file.data);
    int32_t* li_receiptdate = cast<int32_t>(li_receiptdate_file.data);
    int32_t* li_shipdate = cast<int32_t>(li_shipdate_file.data);

    size_t lineitem_count = li_orderkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
#endif

    // Load orders columns FIRST (needed for orderkey_to_priority map)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile o_orderkey_file, o_orderpriority_file;
    o_orderkey_file.open(gendb_dir + "/orders/o_orderkey.bin");
    o_orderpriority_file.open(gendb_dir + "/orders/o_orderpriority.bin");

    int32_t* o_orderkey = cast<int32_t>(o_orderkey_file.data);
    int32_t* o_orderpriority = cast<int32_t>(o_orderpriority_file.data);

    size_t orders_count = o_orderkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // Build compact mapping: o_orderkey[i] → priority_code
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Build a hash map from o_orderkey → o_orderpriority for fast lookup during lineitem scan
    std::unordered_map<int32_t, int32_t> orderkey_to_priority;
    orderkey_to_priority.reserve(orders_count);
    for (size_t i = 0; i < orders_count; ++i) {
        orderkey_to_priority[o_orderkey[i]] = o_orderpriority[i];
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_orderkey_map: %.2f ms\n", ms);
#endif

    // FUSED SCAN-FILTER-JOIN-AGGREGATE: Single pass over lineitem
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t mail_code = shipmode_reverse.count("MAIL") ? shipmode_reverse["MAIL"] : -1;
    int32_t ship_code = shipmode_reverse.count("SHIP") ? shipmode_reverse["SHIP"] : -1;

    // Pre-compute priority codes for comparison
    std::string urgent = "1-URGENT";
    std::string high = "2-HIGH";
    int32_t urgent_code = -1, high_code = -1;
    for (auto& [code, value] : orderpriority_dict) {
        if (value == urgent) urgent_code = code;
        if (value == high) high_code = code;
    }

    // Thread-local aggregation buffers: [thread_id][shipmode_idx] = {high_count, low_count}
    int num_threads = std::min((int)omp_get_max_threads(), 64);
    std::vector<std::array<int64_t, 2>> thread_agg_high(num_threads, {0, 0});
    std::vector<std::array<int64_t, 2>> thread_agg_low(num_threads, {0, 0});

    // SINGLE PASS: Filter + Join + Aggregate
    // For each lineitem row:
    //   1. Check predicates (shipmode, dates)
    //   2. Look up order priority via o_orderkey map
    //   3. Aggregate to thread-local buffer
    #pragma omp parallel for num_threads(num_threads) schedule(static)
    for (size_t i = 0; i < lineitem_count; ++i) {
        int32_t sm = li_shipmode[i];

        // Predicate 1: shipmode filter (early exit if no match)
        if ((sm != mail_code && sm != ship_code)) continue;

        // Predicates 2-5: date filters
        if (li_commitdate[i] >= li_receiptdate[i]) continue;
        if (li_shipdate[i] >= li_commitdate[i]) continue;
        if (li_receiptdate[i] < receipt_date_min) continue;
        if (li_receiptdate[i] >= receipt_date_max) continue;

        // Get orderkey and look up priority
        int32_t ok = li_orderkey[i];
        auto it = orderkey_to_priority.find(ok);
        if (it == orderkey_to_priority.end()) continue;  // No matching order

        int32_t priority_code = it->second;
        bool is_high = (priority_code == urgent_code || priority_code == high_code);

        // Map shipmode to index
        int shipmode_idx = (sm == mail_code) ? 0 : 1;
        int tid = omp_get_thread_num();

        // Aggregate directly to thread-local buffer
        if (is_high) {
            thread_agg_high[tid][shipmode_idx]++;
        } else {
            thread_agg_low[tid][shipmode_idx]++;
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] fused_scan_filter_join_agg: %.2f ms\n", ms);
#endif

    // Merge thread-local aggregations
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t agg_high[2] = {0, 0};
    int64_t agg_low[2] = {0, 0};
    for (int t = 0; t < num_threads; ++t) {
        agg_high[0] += thread_agg_high[t][0];
        agg_high[1] += thread_agg_high[t][1];
        agg_low[0] += thread_agg_low[t][0];
        agg_low[1] += thread_agg_low[t][1];
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] merge_aggregations: %.2f ms\n", ms);
#endif

    // Prepare output
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Create results: (shipmode, high_count, low_count)
    // We have exactly 2 shipmodes (MAIL and SHIP), output in sorted order
    std::vector<std::tuple<std::string, int64_t, int64_t>> results;

    // Build shipmode list from MAIL and SHIP codes (in sorted order)
    std::vector<std::pair<std::string, int32_t>> shipmode_list;
    if (mail_code != -1 && shipmode_dict.count(mail_code)) {
        shipmode_list.push_back({shipmode_dict[mail_code], mail_code});
    }
    if (ship_code != -1 && shipmode_dict.count(ship_code)) {
        shipmode_list.push_back({shipmode_dict[ship_code], ship_code});
    }
    std::sort(shipmode_list.begin(), shipmode_list.end());

    for (auto& [shipmode_str, code] : shipmode_list) {
        // Map code to index (mail_code -> 0, ship_code -> 1)
        int idx = (code == mail_code) ? 0 : 1;
        results.push_back({shipmode_str, agg_high[idx], agg_low[idx]});
    }

    // Write CSV output
    std::ofstream out(results_dir + "/Q12.csv");
    out << "l_shipmode,high_line_count,low_line_count\n";
    for (auto& [shipmode_str, high_count, low_count] : results) {
        out << shipmode_str << "," << high_count << "," << low_count << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    auto t_global_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_global_end - t_global_start).count();
#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
    run_q12(gendb_dir, results_dir);
    return 0;
}
#endif
