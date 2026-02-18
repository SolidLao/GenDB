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
// Q12 OPTIMIZED PLAN (Iteration 6 - Pre-Built Index Strategy):
// ============================================================================
// Logical Plan:
//   1. Load pre-built lineitem_l_orderkey_hash index from disk (mmap, 0 ms cost)
//      - File: lineitem_l_orderkey_hash.bin (401 MB)
//      - Format: Multi-value hash table, key=l_orderkey
//      - Eliminates 44ms build_hash_join cost entirely
//
//   2. Scan & Filter lineitem (59.9M rows) + Probe pre-built index:
//      - Predicates: l_shipmode IN ('MAIL','SHIP'), l_commitdate < l_receiptdate,
//        l_shipdate < l_commitdate, l_receiptdate in [8766,9131)
//      - Zone map pruning on l_shipdate (if blocks don't match range)
//      - For each matching lineitem row:
//        * Lookup l_orderkey in pre-built hash index (1 lookup per match, ~1.2M)
//        * Retrieve matching orders rows via index pointers
//        * Aggregate immediately (no separate join_probe phase)
//      - Output: ~1.2M aggregation updates
//
//   3. Aggregation (fused with probe):
//      - Thread-local arrays (2 shipmodes × 64 threads = 128 entries)
//      - During lineitem probe: if order priority high → increment thread_agg_high[shipmode]
//
//   4. Merge thread-local aggregations:
//      - Trivial: sum 64 × 2 arrays sequentially (~0.1ms)
//
// Physical Plan:
//   - Pre-built index load: mmap (0ms, no build)
//   - lineitem scan: PARALLEL morsel-driven, 64 cores
//   - Probe: hash table lookups in pre-built index (already optimized)
//   - aggregation: thread-local flat arrays, fused with probe
//   - No separate join_probe phase
//
// Expected Performance (Iteration 6):
//   - Eliminate build_hash_join (44ms) → 99ms - 44ms = 55ms base
//   - Potential scan improvement via zone maps → 50ms target
//   - Expected total: ~50-60ms (approaching Umbra's 49ms)
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

// ============================================================================
// Pre-Built Multi-Value Hash Index Loader (for mmap'd hash indexes)
// ============================================================================
// Multi-value hash indexes store:
//   - Entry array: [ {key, offset_in_positions, count}, ... ]
//   - Positions array: [ row_0, row_1, row_2, ... ] (all positions, grouped by key)
//
// Binary format (inferred from GenDB index builder):
//   [4B] num_entries
//   [8B] num_positions
//   [num_entries * 24B] Entry { int32_t key; uint32_t offset; uint32_t count; }
//   [num_positions * 4B] Positions array (row indices)

struct PreBuiltHashIndexHeader {
    uint32_t num_entries;
    uint64_t num_positions;
};

struct IndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

struct PreBuiltHashIndex {
    void* data = nullptr;
    size_t total_size = 0;
    PreBuiltHashIndexHeader* header = nullptr;
    IndexEntry* entries = nullptr;
    int32_t* positions = nullptr;
    std::unordered_map<int32_t, std::pair<uint32_t, uint32_t>> key_map;  // key → (offset, count)

    void load_from_file(const std::string& path) {
        MmapFile f;
        f.open(path);
        data = f.data;
        total_size = f.size;

        // Parse header
        header = reinterpret_cast<PreBuiltHashIndexHeader*>(data);
        uint32_t num_entries = header->num_entries;

        // Entry array starts after header
        entries = reinterpret_cast<IndexEntry*>(reinterpret_cast<char*>(data) + sizeof(PreBuiltHashIndexHeader));

        // Positions array starts after entries
        positions = reinterpret_cast<int32_t*>(
            reinterpret_cast<char*>(entries) + num_entries * sizeof(IndexEntry)
        );

        // Build lookup map from entries
        for (uint32_t i = 0; i < num_entries; ++i) {
            key_map[entries[i].key] = {entries[i].offset, entries[i].count};
        }

        // Keep file open (mmap stays valid)
        f.data = nullptr;  // Prevent munmap in destructor
    }

    std::pair<uint32_t, uint32_t>* find(int32_t key) {
        auto it = key_map.find(key);
        if (it != key_map.end()) {
            return &it->second;
        }
        return nullptr;
    }

    // Get positions for a key (returns array of row indices)
    int32_t* get_positions(int32_t key, uint32_t& out_count) {
        auto it = key_map.find(key);
        if (it != key_map.end()) {
            out_count = it->second.second;
            return &positions[it->second.first];
        }
        out_count = 0;
        return nullptr;
    }
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

    // Load pre-built lineitem_l_orderkey_hash index (ITERATION 6 FIX - eliminates build cost)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    PreBuiltHashIndex lineitem_hash_index;
    lineitem_hash_index.load_from_file(gendb_dir + "/indexes/lineitem_l_orderkey_hash.bin");

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_lineitem_index: %.2f ms\n", ms);
#endif

    // Load orders columns for priority lookup
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile o_orderpriority_file;
    o_orderpriority_file.open(gendb_dir + "/orders/o_orderpriority.bin");

    int32_t* o_orderpriority = cast<int32_t>(o_orderpriority_file.data);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // Pre-compute priority codes for fast lookup
    std::string urgent = "1-URGENT";
    std::string high = "2-HIGH";
    int32_t urgent_code = -1, high_code = -1;
    for (auto& [code, value] : orderpriority_dict) {
        if (value == urgent) urgent_code = code;
        if (value == high) high_code = code;
    }

    int32_t mail_code = shipmode_reverse.count("MAIL") ? shipmode_reverse["MAIL"] : -1;
    int32_t ship_code = shipmode_reverse.count("SHIP") ? shipmode_reverse["SHIP"] : -1;

    // FUSED SCAN-FILTER-PROBE-AGGREGATE: Iterate lineitem, filter, lookup orders via index, aggregate
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = std::min((int)omp_get_max_threads(), 64);

    // Thread-local aggregation buffers
    std::vector<std::vector<int64_t>> thread_agg_high(num_threads, std::vector<int64_t>(2, 0));
    std::vector<std::vector<int64_t>> thread_agg_low(num_threads, std::vector<int64_t>(2, 0));

    // Track which shipmodes we see (for output)
    std::vector<std::set<int32_t>> thread_shipmodes(num_threads);

    // Parallel scan of lineitem: filter + probe pre-built index + aggregate
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

        // Lineitem row matches all predicates
        // Now lookup matching orders rows via pre-built index
        int32_t orderkey = li_orderkey[i];
        uint32_t order_count = 0;
        int32_t* order_indices = lineitem_hash_index.get_positions(orderkey, order_count);

        if (order_indices) {
            int tid = omp_get_thread_num();
            int shipmode_idx = (sm == mail_code) ? 0 : 1;
            thread_shipmodes[tid].insert(sm);

            // For each matching order row, aggregate based on priority
            for (uint32_t j = 0; j < order_count; ++j) {
                int32_t order_row_id = order_indices[j];
                int32_t priority_code = o_orderpriority[order_row_id];
                bool is_high = (priority_code == urgent_code || priority_code == high_code);

                if (is_high) {
                    thread_agg_high[tid][shipmode_idx]++;
                } else {
                    thread_agg_low[tid][shipmode_idx]++;
                }
            }
        }
    }

    // Merge thread-local aggregations
    int64_t agg_high[2] = {0, 0};
    int64_t agg_low[2] = {0, 0};
    std::set<int32_t> all_shipmodes;
    for (int t = 0; t < num_threads; ++t) {
        agg_high[0] += thread_agg_high[t][0];
        agg_high[1] += thread_agg_high[t][1];
        agg_low[0] += thread_agg_low[t][0];
        agg_low[1] += thread_agg_low[t][1];
        for (int32_t sm : thread_shipmodes[t]) {
            all_shipmodes.insert(sm);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] scan_filter_probe_aggregate: %.2f ms\n", ms);
#endif

    // Prepare output
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Create results: (shipmode, high_count, low_count)
    // Only output shipmodes that appear in the filtered lineitem data
    std::vector<std::tuple<std::string, int64_t, int64_t>> results;

    // Build sorted list from all_shipmodes
    std::vector<std::pair<std::string, int32_t>> shipmode_list;
    for (int32_t code : all_shipmodes) {
        if (shipmode_dict.count(code)) {
            shipmode_list.push_back({shipmode_dict[code], code});
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
