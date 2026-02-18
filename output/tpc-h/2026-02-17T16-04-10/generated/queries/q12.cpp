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

namespace {

// ============================================================================
// Q12 OPTIMIZED PLAN (Iteration 2):
// ============================================================================
// Logical Plan:
//   1. Scan & Filter lineitem (59.9M rows):
//      - l_shipmode IN ('MAIL', 'SHIP')  [dictionary codes]
//      - l_commitdate < l_receiptdate
//      - l_shipdate < l_commitdate
//      - l_receiptdate >= 1994-01-01 (epoch day 8766)
//      - l_receiptdate < 1995-01-01 (epoch day 9131)
//      Estimated output: ~1.2M rows (2% selectivity)
//      OPTIMIZATION: Parallel morsel-driven scan (64 cores, work-stealing)
//
//   2. Build hash table on filtered lineitem (1.2M rows, build side):
//      - ARCHITECTURE: Build on smaller side (lineitem), not orders
//      - Use open-addressing hash table (not std::unordered_map)
//      - Key: l_orderkey, Value: {shipmode, order_index}
//
//   3. Scan & Probe orders (15M rows, probe side):
//      - Parallel scan with OpenMP (morsel-driven)
//      - Thread-local aggregation buffers (no contention)
//      - Probe: lookup o_orderkey in lineitem hash table
//      - For each match: aggregate by shipmode + priority
//
//   4. Merge thread-local aggregations (trivial, <64 entries)
//      - Cardinality: 2 shipmodes × 64 threads = 128 entries max
//
// Physical Plan:
//   - lineitem scan: PARALLEL morsel-driven across 64 cores (ITER 2 FIX)
//   - Hash table: CompactHashTable<int32_t, vector<int32_t>> with open-addressing
//   - join probe: OpenMP parallel for on orders with morsel-driven chunks
//   - aggregation: thread-local flat arrays [2] per thread, merged at end
//   - output: order by l_shipmode, write CSV
//
// Expected Speedup (Iteration 2):
//   - Morsel-driven parallel lineitem scan: 626ms → ~100ms (6.26x speedup from parallelism)
//   - Combined with iter 1 improvements: 793ms → ~200ms total (4x reduction)
//   - Target: Close to 200ms, within 4x of Umbra
// ============================================================================

// ============================================================================
// Compact Open-Addressing Hash Table for Join Build
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

} // end anonymous namespace

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
    #pragma omp parallel for num_threads(num_threads_scan) schedule(static)
    for (size_t i = 0; i < lineitem_count; ++i) {
        int32_t sm = li_shipmode[i];

        // Predicate 1: shipmode filter (early exit if no match)
        if ((sm != mail_code && sm != ship_code)) continue;

        // Predicates 2-5: date filters
        if (li_commitdate[i] >= li_receiptdate[i]) continue;
        if (li_shipdate[i] >= li_commitdate[i]) continue;
        if (li_receiptdate[i] < receipt_date_min) continue;
        if (li_receiptdate[i] >= receipt_date_max) continue;

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
    // Key: l_orderkey, Value: vector of {shipmode, lineitem_index}
    using LineitemEntry = std::pair<int32_t, size_t>;  // {shipmode, index}
    CompactHashTable<int32_t, std::vector<LineitemEntry>> ht_lineitem(filtered_orderkeys.size());

    for (size_t i = 0; i < filtered_orderkeys.size(); ++i) {
        int32_t orderkey = filtered_orderkeys[i];
        int32_t shipmode = filtered_shipmodes[i];
        ht_lineitem.insert(orderkey, {shipmode, i});
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

    // Thread-local aggregation buffers (no contention)
    std::vector<std::vector<int64_t>> thread_agg_high(num_threads, std::vector<int64_t>(2, 0));
    std::vector<std::vector<int64_t>> thread_agg_low(num_threads, std::vector<int64_t>(2, 0));

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
            for (auto& [shipmode, li_idx] : *lineitem_matches) {
                int shipmode_idx = (shipmode == mail_code) ? 0 : 1;
                if (is_high) {
                    thread_agg_high[thread_id][shipmode_idx]++;
                } else {
                    thread_agg_low[thread_id][shipmode_idx]++;
                }
            }
        }
    }

    // Merge thread-local aggregations
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
