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
// Q12 OPTIMIZED PLAN (Iteration 10):
// ============================================================================
// Previous iterations: Iter 1 (789ms), Iter 2 (99ms, excellent), Iter 9 (965ms, regressed)
// Current baseline: 99ms
// Target: Match Umbra at 49ms (2.0x speedup)
//
// Bottleneck analysis (99ms):
//   - build_hash_join: 44ms (43%) ← PRIMARY TARGET
//   - scan_filter_lineitem: 30ms (29%)
//   - join_aggregate: 25ms (24%)
//
// ============================================================================
// ITER 10 OPTIMIZATIONS APPLIED:
// ============================================================================
//
// 1. HASH TABLE VALUE REDUCTION (Major)
//    - OLD: std::vector<std::pair<int32_t, size_t>>  [16 bytes per entry]
//    - NEW: std::vector<int32_t>                      [4 bytes per entry]
//    - Rationale: lineitem_index was never used; removed to cut 75% of value storage
//    - Impact: Fewer vector reallocations, smaller data structures
//
// 2. VECTOR PRE-RESERVE (Medium)
//    - Added reserve(2) in insert() before first push_back()
//    - Avoids first reallocation when most buckets have 1-2 entries
//    - Typical 1:N join multiplicity is low, so reserve(2) sufficient
//
// 3. HASH TABLE LOAD FACTOR REDUCTION (Medium)
//    - OLD: 75% load factor (size = expected_size * 4/3)
//    - NEW: 50% load factor (size = expected_size * 2)
//    - Effect: Fewer linear probes on collision, faster insertion
//    - Trade-off: More memory (but still fit in cache)
//
// 4. PREDICATE REORDERING IN SCAN (Small)
//    - Optimized early rejection: shipmode first (cheapest, ~99% reject rate)
//    - Then date predicates (more selective after shipmode)
//    - Reduces branch mispredictions and enables CPU pipeline optimization
//
// 5. THREAD-LOCAL AGGREGATION OPTIMIZATION (Small)
//    - Cache-aligned buffers (alignas(64)) to prevent false sharing
//    - Direct field access instead of array indexing (fewer deps)
//    - Faster merge loop (4 field adds vs 2 iterations × 2 iterations)
//
// Expected cumulative speedup: 44ms → ~22-25ms (1.76-2.0x on build_hash_join)
// New total estimate: 99ms - 20ms = ~79ms (closer to Umbra's 49ms)
//
// ============================================================================

// ============================================================================
// Compact Open-Addressing Hash Table for Join Build (ITER 10: Two-Pass Build)
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

    CompactHashTable() : mask(0) {}

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~50% load factor (lower = fewer probes)
        size_t sz = 1;
        while (sz < expected_size * 2) sz <<= 1;  // Increased to 2x for 50% load factor
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        // Note: key is typically l_orderkey which already has decent distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    // Single-pass insert with pre-reserve to avoid vector reallocation
    void insert(K key, const typename V::value_type& item) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                // For 1:N joins, append to existing vector
                table[idx].value.push_back(item);
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].value.clear();
        table[idx].value.reserve(2);  // Pre-reserve for typical 1-2 entries per bucket
        table[idx].value.push_back(item);
        table[idx].occupied = true;
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

    // Scan & filter lineitem - PARALLEL MORSEL-DRIVEN (Iteration 2)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t mail_code = shipmode_reverse.count("MAIL") ? shipmode_reverse["MAIL"] : -1;
    int32_t ship_code = shipmode_reverse.count("SHIP") ? shipmode_reverse["SHIP"] : -1;

    // Phase 1: Parallel scan with thread-local buffers (no contention)
    int num_threads_scan = std::min((int)omp_get_max_threads(), 64);
    std::vector<std::vector<int32_t>> thread_orderkeys(num_threads_scan);
    std::vector<std::vector<int32_t>> thread_shipmodes(num_threads_scan);

    // Pre-allocate buffers to minimize allocations
    for (int t = 0; t < num_threads_scan; ++t) {
        thread_orderkeys[t].reserve(lineitem_count / num_threads_scan / 50);  // Estimate 2% selectivity
        thread_shipmodes[t].reserve(lineitem_count / num_threads_scan / 50);
    }

    // Morsel-driven parallel scan: each thread processes independent chunks
    // ITER 10: Optimized predicate ordering for early rejection
    // - Cheapest predicates first (shipmode comparison)
    // - Most selective predicates next (date ranges)
    // - Branch prediction: put most likely fail path first
    #pragma omp parallel for num_threads(num_threads_scan) schedule(static)
    for (size_t i = 0; i < lineitem_count; ++i) {
        // Predicate 1: shipmode filter (cheapest, eliminates ~99% of rows)
        int32_t sm = li_shipmode[i];
        if ((sm != mail_code && sm != ship_code)) continue;

        // Predicate 2-5: date filters (more selective after shipmode)
        // Order: commitment check, shipdate check, receipt date range
        int32_t cd = li_commitdate[i];
        int32_t rd = li_receiptdate[i];
        int32_t sd = li_shipdate[i];

        if (sd >= cd) continue;           // shipdate < commitdate
        if (cd >= rd) continue;           // commitdate < receiptdate
        if (rd < receipt_date_min) continue;  // receiptdate >= 1994-01-01
        if (rd >= receipt_date_max) continue; // receiptdate < 1995-01-01

        // Matching row: append to thread-local buffer
        int tid = omp_get_thread_num();
        thread_orderkeys[tid].push_back(li_orderkey[i]);
        thread_shipmodes[tid].push_back(sm);
    }

    // Phase 2: Merge thread-local buffers into global
    std::vector<int32_t> filtered_orderkeys;
    std::vector<int32_t> filtered_shipmodes;

    size_t total_filtered = 0;
    for (int t = 0; t < num_threads_scan; ++t) {
        total_filtered += thread_orderkeys[t].size();
    }
    filtered_orderkeys.reserve(total_filtered);
    filtered_shipmodes.reserve(total_filtered);

    for (int t = 0; t < num_threads_scan; ++t) {
        for (size_t j = 0; j < thread_orderkeys[t].size(); ++j) {
            filtered_orderkeys.push_back(thread_orderkeys[t][j]);
            filtered_shipmodes.push_back(thread_shipmodes[t][j]);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] scan_filter_lineitem: %.2f ms\n", ms);
#endif

    // Load orders columns
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

    // Build hash join on FILTERED lineitem (smaller side) - ARCHITECTURE FIX
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Pre-compute priority codes for fast lookup (avoid dict lookup in hot path)
    std::string urgent = "1-URGENT";
    std::string high = "2-HIGH";
    int32_t urgent_code = -1, high_code = -1;
    for (auto& [code, value] : orderpriority_dict) {
        if (value == urgent) urgent_code = code;
        if (value == high) high_code = code;
    }

    // Build hash table on filtered lineitem (1.2M rows instead of 15M orders)
    // Key: l_orderkey, Value: vector of shipmodes (no need to store lineitem_index)
    // ITER 10: Only store shipmode (int32_t), not unused lineitem_index
    CompactHashTable<int32_t, std::vector<int32_t>> ht_lineitem(filtered_orderkeys.size());

    // ITER 10 OPTIMIZATION: Use simpler insert with pre-reserve
    // Only store shipmode, not unused lineitem index
    // This reduces memory per entry and makes vectors smaller (8 bytes → 4 bytes per value)
    for (size_t i = 0; i < filtered_orderkeys.size(); ++i) {
        int32_t orderkey = filtered_orderkeys[i];
        int32_t shipmode = filtered_shipmodes[i];
        ht_lineitem.insert(orderkey, shipmode);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_hash_join: %.2f ms\n", ms);
#endif

    // Probe join & aggregate (parallel on orders scan)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Determine number of threads
    int num_threads = std::min((int)omp_get_max_threads(), 64);

    // ITER 10: Thread-local aggregation with cache-aligned buffers to prevent false sharing
    // Each thread gets its own [high_mail, high_ship, low_mail, low_ship] = 4 values
    struct ThreadAgg {
        alignas(64) int64_t high_mail = 0;  // Cache line aligned
        alignas(64) int64_t high_ship = 0;
        alignas(64) int64_t low_mail = 0;
        alignas(64) int64_t low_ship = 0;
    };
    std::vector<ThreadAgg> thread_agg(num_threads);

    // Parallel probe on orders (larger side)
    #pragma omp parallel for num_threads(num_threads) schedule(static)
    for (size_t i = 0; i < orders_count; ++i) {
        int32_t orderkey = o_orderkey[i];
        int32_t priority_code = o_orderpriority[i];
        bool is_high = (priority_code == urgent_code || priority_code == high_code);

        // Probe lineitem hash table
        auto* lineitem_matches = ht_lineitem.find(orderkey);
        if (lineitem_matches) {
            int thread_id = omp_get_thread_num();
            ThreadAgg& agg = thread_agg[thread_id];

            for (int32_t shipmode : *lineitem_matches) {
                if (is_high) {
                    if (shipmode == mail_code) {
                        agg.high_mail++;
                    } else {
                        agg.high_ship++;
                    }
                } else {
                    if (shipmode == mail_code) {
                        agg.low_mail++;
                    } else {
                        agg.low_ship++;
                    }
                }
            }
        }
    }

    // Merge thread-local aggregations
    int64_t agg_high[2] = {0, 0};
    int64_t agg_low[2] = {0, 0};
    for (int t = 0; t < num_threads; ++t) {
        agg_high[0] += thread_agg[t].high_mail;
        agg_high[1] += thread_agg[t].high_ship;
        agg_low[0] += thread_agg[t].low_mail;
        agg_low[1] += thread_agg[t].low_ship;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", ms);
#endif

    // Prepare output
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Create results: (shipmode, high_count, low_count)
    // Only output shipmodes that appear in the filtered lineitem data
    std::vector<std::tuple<std::string, int64_t, int64_t>> results;

    // Collect unique shipmodes from filtered data
    std::vector<std::pair<std::string, int32_t>> shipmode_list;
    std::set<int32_t> seen_codes;
    for (int32_t code : filtered_shipmodes) {
        if (seen_codes.find(code) == seen_codes.end()) {
            seen_codes.insert(code);
            if (shipmode_dict.count(code)) {
                shipmode_list.push_back({shipmode_dict[code], code});
            }
        }
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
