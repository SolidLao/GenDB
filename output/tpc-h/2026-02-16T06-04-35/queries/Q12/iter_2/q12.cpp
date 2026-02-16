#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <omp.h>
#include <set>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// ============================================================================
// Helper Structures & Functions
// ============================================================================

// Result tuple: l_shipmode, high_line_count, low_line_count
struct Result {
    int8_t l_shipmode_code;
    int64_t high_line_count;
    int64_t low_line_count;
};

// Compact open-addressing hash table for joins (2-5x faster than std::unordered_map)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable() = default;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < (expected_size * 4) / 3) {
            sz <<= 1;
        }
        table.resize(sz);
        mask = sz - 1;
    }

    inline size_t hash(K key) const {
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
            if (table[idx].key == key) {
                return &table[idx].value;
            }
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Helper to load file via mmap
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to fstat " << path << std::endl;
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

// Helper to read dictionary from _dict.txt
std::unordered_map<int8_t, std::string> load_dict(const std::string& dict_path) {
    std::unordered_map<int8_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open " << dict_path << std::endl;
        exit(1);
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int8_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// Get dictionary code for a value (reverse lookup)
int8_t get_dict_code(const std::unordered_map<int8_t, std::string>& dict,
                      const std::string& target) {
    for (const auto& [code, value] : dict) {
        if (value == target) {
            return code;
        }
    }
    return -1;  // Not found
}

// Convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Compute date from days since epoch (1970-01-01)
    int32_t remaining = days;
    int32_t year = 1970;

    // Approximate year
    while (remaining >= 365) {
        int32_t days_in_year = 365;
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
            days_in_year = 366;
        }
        if (remaining >= days_in_year) {
            remaining -= days_in_year;
            year++;
        } else {
            break;
        }
    }

    // Days in each month (non-leap)
    static const int32_t days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int32_t month = 1;
    int32_t day = remaining + 1;

    // Check for leap year
    bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);

    for (int i = 0; i < 12; i++) {
        int32_t days_this_month = days_in_month[i];
        if (i == 1 && is_leap) days_this_month = 29;

        if (day <= days_this_month) {
            month = i + 1;
            break;
        }
        day -= days_this_month;
    }

    char buf[12];
    snprintf(buf, 12, "%04d-%02d-%02d", (int)year, (int)month, (int)day);
    return std::string(buf);
}

// ============================================================================
// Main Query Implementation
// ============================================================================

inline void run_q12(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Q12 Query Structure:\n");
    printf("  - Filter lineitem on l_shipmode IN (MAIL, SHIP)\n");
    printf("  - Filter lineitem on date predicates\n");
    printf("  - Join orders on o_orderkey = l_orderkey\n");
    printf("  - GROUP BY l_shipmode with conditional aggregations\n");
    printf("  - Output: l_shipmode, high_line_count, low_line_count\n");
#endif

    // ========================================================================
    // 1. Load Dictionaries
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    auto o_orderpriority_dict = load_dict(gendb_dir + "/orders/o_orderpriority_dict.txt");
    auto l_shipmode_dict = load_dict(gendb_dir + "/lineitem/l_shipmode_dict.txt");

    // Get codes for required values
    int8_t code_urgent = get_dict_code(o_orderpriority_dict, "1-URGENT");
    int8_t code_high = get_dict_code(o_orderpriority_dict, "2-HIGH");
    int8_t code_mail = get_dict_code(l_shipmode_dict, "MAIL");
    int8_t code_ship = get_dict_code(l_shipmode_dict, "SHIP");

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] dict_load: %.2f ms\n", dict_ms);
    printf("  - code_urgent=%d, code_high=%d, code_mail=%d, code_ship=%d\n",
           (int)code_urgent, (int)code_high, (int)code_mail, (int)code_ship);
#endif

    // ========================================================================
    // 2. Load Binary Columns
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t l_orderkey_size = 0, l_shipmode_size = 0, l_commitdate_size = 0;
    size_t l_receiptdate_size = 0, l_shipdate_size = 0;
    size_t o_orderkey_size = 0, o_orderpriority_size = 0;

    // Load lineitem columns
    const int32_t* l_orderkey =
        (const int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", l_orderkey_size);
    const int8_t* l_shipmode =
        (const int8_t*)mmap_file(gendb_dir + "/lineitem/l_shipmode.bin", l_shipmode_size);
    const int32_t* l_commitdate =
        (const int32_t*)mmap_file(gendb_dir + "/lineitem/l_commitdate.bin", l_commitdate_size);
    const int32_t* l_receiptdate =
        (const int32_t*)mmap_file(gendb_dir + "/lineitem/l_receiptdate.bin", l_receiptdate_size);
    const int32_t* l_shipdate =
        (const int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", l_shipdate_size);

    // Load orders columns
    const int32_t* o_orderkey =
        (const int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", o_orderkey_size);
    const int8_t* o_orderpriority =
        (const int8_t*)mmap_file(gendb_dir + "/orders/o_orderpriority.bin", o_orderpriority_size);

    // Load zone-map index for l_receiptdate (for pruning)
    size_t zone_map_size = 0;
    const uint32_t* zone_map_data = nullptr;
    std::string zone_map_path = gendb_dir + "/indexes/lineitem_l_receiptdate_zone.bin";
    FILE* zone_file = fopen(zone_map_path.c_str(), "rb");
    if (zone_file) {
        fseek(zone_file, 0, SEEK_END);
        zone_map_size = ftell(zone_file);
        fseek(zone_file, 0, SEEK_SET);
        void* zone_ptr = malloc(zone_map_size);
        size_t bytes_read = fread(zone_ptr, 1, zone_map_size, zone_file);
        (void)bytes_read;  // suppress unused warning
        fclose(zone_file);
        zone_map_data = (const uint32_t*)zone_ptr;
    }

    // Verify column sizes
    size_t lineitem_rows = l_orderkey_size / sizeof(int32_t);
    size_t orders_rows = o_orderkey_size / sizeof(int32_t);
    const size_t block_size = 200000;  // From storage guide
    size_t num_blocks = (lineitem_rows + block_size - 1) / block_size;

    assert(l_shipmode_size == lineitem_rows * sizeof(int8_t));
    assert(l_commitdate_size == lineitem_rows * sizeof(int32_t));
    assert(l_receiptdate_size == lineitem_rows * sizeof(int32_t));
    assert(l_shipdate_size == lineitem_rows * sizeof(int32_t));
    assert(o_orderpriority_size == orders_rows * sizeof(int8_t));

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    printf("  - lineitem_rows=%zu, orders_rows=%zu, zone_map_loaded=%d\n", lineitem_rows, orders_rows, zone_map_data != nullptr);
#endif

    // ========================================================================
    // 3. Build Hash Index for Orders (o_orderkey -> o_orderpriority code)
    //    Using CompactHashTable (open-addressing) for 2-5x speedup
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash table from orders columns: o_orderkey -> o_orderpriority code
    // Using compact open-addressing instead of std::unordered_map for performance
    CompactHashTable<int32_t, int8_t> order_priority(orders_rows);

    for (size_t i = 0; i < orders_rows; i++) {
        order_priority.insert(o_orderkey[i], o_orderpriority[i]);
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] join_build: %.2f ms\n", build_ms);
#endif

    // ========================================================================
    // 4. Compute Date Constants
    // ========================================================================
    // 1994-01-01 = ? days since 1970-01-01
    // Years: 1970-1993 (24 years)
    //   Leap years: 1972, 1976, 1980, 1984, 1988, 1992 = 6
    //   Non-leap: 18
    //   Total: 6*366 + 18*365 = 2196 + 6570 = 8766
    // 1994-01-01 epoch = 8766
    int32_t date_1994_01_01 = 8766;

    // 1995-01-01 = 8766 + 365 = 9131
    int32_t date_1995_01_01 = 9131;

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Date constants:\n");
    printf("  - 1994-01-01 epoch days = %d\n", date_1994_01_01);
    printf("  - 1995-01-01 epoch days = %d\n", date_1995_01_01);
#endif

    // ========================================================================
    // 5. Filter and Aggregate
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Use flat arrays for aggregation (only 2 shipmodes: MAIL and SHIP)
    // Find the shipmode codes to use as array indices
    std::vector<int8_t> shipmode_codes;
    shipmode_codes.push_back(code_mail);
    shipmode_codes.push_back(code_ship);

    // Create flat arrays indexed by shipmode code
    // Using -128 to 127 range for int8_t
    std::vector<int64_t> high_count_flat(256, 0);
    std::vector<int64_t> low_count_flat(256, 0);

    size_t matched_rows = 0;
    size_t join_misses = 0;

    // Parallel scan and filter with thread-local aggregation
    const int num_threads = omp_get_max_threads();
    std::vector<std::vector<int64_t>> thread_high_flat(num_threads, std::vector<int64_t>(256, 0));
    std::vector<std::vector<int64_t>> thread_low_flat(num_threads, std::vector<int64_t>(256, 0));

    size_t matched_rows_parallel = 0;
    size_t join_misses_parallel = 0;

    // Process blocks, skipping those that don't match date predicate
#pragma omp parallel for reduction(+:matched_rows_parallel, join_misses_parallel) schedule(static)
    for (size_t block_id = 0; block_id < num_blocks; block_id++) {
        size_t block_start = block_id * block_size;
        size_t block_end = std::min(block_start + block_size, lineitem_rows);

        // Zone-map pruning: skip blocks that can't possibly match date filters
        // Zone map format: [min, max] per block (if available)
        bool can_skip = false;
        if (zone_map_data && block_id < (zone_map_size / sizeof(uint32_t) / 2)) {
            uint32_t block_min = zone_map_data[block_id * 2];
            uint32_t block_max = zone_map_data[block_id * 2 + 1];
            // Skip if block_max < date_1994_01_01 (all too early)
            // Or block_min >= date_1995_01_01 (all too late)
            can_skip = (block_max < static_cast<uint32_t>(date_1994_01_01)) ||
                       (block_min >= static_cast<uint32_t>(date_1995_01_01));
        }

        if (can_skip) {
            continue;  // Skip this entire block
        }

        int tid = omp_get_thread_num();

        for (size_t i = block_start; i < block_end; i++) {
            // Filter predicates on lineitem
            // 1. l_shipmode IN ('MAIL', 'SHIP')
            if (l_shipmode[i] != code_mail && l_shipmode[i] != code_ship) {
                continue;
            }

            // 2. l_commitdate < l_receiptdate
            if (l_commitdate[i] >= l_receiptdate[i]) {
                continue;
            }

            // 3. l_shipdate < l_commitdate
            if (l_shipdate[i] >= l_commitdate[i]) {
                continue;
            }

            // 4. l_receiptdate >= 1994-01-01
            if (l_receiptdate[i] < date_1994_01_01) {
                continue;
            }

            // 5. l_receiptdate < 1995-01-01
            if (l_receiptdate[i] >= date_1995_01_01) {
                continue;
            }

            matched_rows_parallel++;

            // Join: lookup order priority by o_orderkey
            int8_t* priority_ptr = order_priority.find(l_orderkey[i]);
            if (!priority_ptr) {
                join_misses_parallel++;
                continue;
            }

            int8_t priority = *priority_ptr;
            int8_t shipmode = l_shipmode[i];

            // Conditional aggregation to thread-local flat arrays
            if (priority == code_urgent || priority == code_high) {
                thread_high_flat[tid][static_cast<unsigned char>(shipmode)]++;
            } else {
                thread_low_flat[tid][static_cast<unsigned char>(shipmode)]++;
            }
        }
    }

    // Merge thread-local aggregations
    for (int tid = 0; tid < num_threads; tid++) {
        for (int i = 0; i < 256; i++) {
            high_count_flat[i] += thread_high_flat[tid][i];
            low_count_flat[i] += thread_low_flat[tid][i];
        }
    }

    matched_rows = matched_rows_parallel;
    join_misses = join_misses_parallel;

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", filter_ms);
    printf("  - matched_lineitem_rows=%zu, join_misses=%zu\n", matched_rows, join_misses);
#endif

    // ========================================================================
    // 6. Sort Results by l_shipmode
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<Result> results;

    // Collect results from flat arrays (only for shipmodes with counts)
    for (int8_t code : shipmode_codes) {
        int idx = static_cast<unsigned char>(code);
        if (high_count_flat[idx] > 0 || low_count_flat[idx] > 0) {
            Result r;
            r.l_shipmode_code = code;
            r.high_line_count = high_count_flat[idx];
            r.low_line_count = low_count_flat[idx];
            results.push_back(r);
        }
    }

    // Sort by shipmode (using dictionary order)
    std::sort(results.begin(), results.end(),
              [](const Result& a, const Result& b) { return a.l_shipmode_code < b.l_shipmode_code; });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

    // ========================================================================
    // 7. Output Results to CSV
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q12.csv";
    std::ofstream out(output_path);

    // Header
    out << "l_shipmode,high_line_count,low_line_count\n";

    // Rows
    for (const auto& r : results) {
        auto it = l_shipmode_dict.find(r.l_shipmode_code);
        std::string shipmode_str = (it != l_shipmode_dict.end()) ? it->second : "UNKNOWN";
        out << shipmode_str << "," << r.high_line_count << "," << r.low_line_count << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // ========================================================================
    // 8. Cleanup
    // ========================================================================
    munmap((void*)l_orderkey, l_orderkey_size);
    munmap((void*)l_shipmode, l_shipmode_size);
    munmap((void*)l_commitdate, l_commitdate_size);
    munmap((void*)l_receiptdate, l_receiptdate_size);
    munmap((void*)l_shipdate, l_shipdate_size);
    munmap((void*)o_orderkey, o_orderkey_size);
    munmap((void*)o_orderpriority, o_orderpriority_size);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    printf("[RESULT] Query Q12 completed. Results written to %s\n", output_path.c_str());
#endif
}

// ============================================================================
// Main Entry Point
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
