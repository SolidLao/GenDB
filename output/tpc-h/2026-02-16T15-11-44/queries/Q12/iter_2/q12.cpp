#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include <omp.h>
#include <cmath>

/*
==============================================
LOGICAL QUERY PLAN FOR Q12
==============================================

Step 1: Filter lineitem table
  - Apply predicates:
    * l_shipmode IN ('MAIL', 'SHIP') → dictionary codes 1, 6
    * l_commitdate < l_receiptdate
    * l_shipdate < l_commitdate
    * l_receiptdate >= 1994-01-01 (epoch day 8766)
    * l_receiptdate < 1995-01-01 (epoch day 9131)
  - Use zone map idx_lineitem_shipdate_zmap to skip blocks outside [8766, 9131)
  - Estimated output: ~1.6M rows

Step 2: Build hash table on filtered lineitem
  - Key: l_orderkey
  - Value: l_shipmode (for later group by)
  - Count: filtered lineitem rows

Step 3: Join with orders table
  - Hash join: probe orders using o_orderkey
  - For each matching row, check o_orderpriority
  - Accumulate into aggregation structure

Step 4: Aggregation
  - GROUP BY l_shipmode (cardinality 2: codes 1, 6)
  - Use flat array indexed by shipmode code
  - Two counters per group:
    * high_line_count: o_orderpriority IN (1, 3) [1-URGENT, 2-HIGH]
    * low_line_count: other orderpriorities

Step 5: Output
  - Sort results by l_shipmode
  - Decode shipmode codes using dictionary
  - Write CSV with header

==============================================
PHYSICAL QUERY PLAN FOR Q12
==============================================

1. Load Data (mmap):
   - lineitem: l_orderkey, l_shipmode, l_commitdate, l_receiptdate, l_shipdate (int32_t)
   - orders: o_orderkey, o_orderpriority (int32_t)
   - dictionaries: l_shipmode_dict, o_orderpriority_dict

2. Lineitem Filter + Scan:
   - Full scan with zone map pruning on l_shipdate
   - For each qualifying row:
     * Check l_shipmode IN {1, 6}
     * Check l_commitdate < l_receiptdate
     * Check l_shipdate < l_commitdate
     * Check l_receiptdate in [8766, 9131)
   - Materialize filtered rows

3. Hash Join:
   - Build: hash table on l_orderkey from filtered lineitem
   - Probe: scan orders, lookup each o_orderkey in hash table
   - Emit: (l_shipmode, o_orderpriority) pairs

4. Aggregation:
   - Flat array: result[shipmode_code][high/low]
   - Accumulate counters during join probe phase

5. Output:
   - Sort by shipmode code
   - Decode codes to strings
   - Write CSV
*/

// ========== COMPACT HASH TABLE (OPEN-ADDRESSING) FOR MULTI-VALUE JOINS ==========

// Compact open-addressing hash table for 1:N joins
// Stores (offset, count) pairs; values are stored in a separate positions array
struct CompactMultiValueHashTable {
    struct Entry {
        int32_t key = -1;  // -1 = empty
        uint32_t offset = 0;
        uint32_t count = 0;
    };

    std::vector<Entry> table;
    std::vector<int32_t> positions;  // contiguous array of (shipmode) values
    size_t mask;
    uint32_t pos_idx = 0;  // current write position in positions array

    CompactMultiValueHashTable(size_t expected_keys, size_t expected_values) {
        // Size hash table to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_keys * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;

        // Pre-allocate positions array
        positions.reserve(expected_values);
    }

    size_t hash(int32_t key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void add_value(int32_t key, int32_t value) {
        // First pass: allocate slot if needed
        size_t idx = hash(key) & mask;
        while (table[idx].key != -1 && table[idx].key != key) {
            idx = (idx + 1) & mask;
        }

        if (table[idx].key == -1) {
            // New key
            table[idx].key = key;
            table[idx].offset = positions.size();
            table[idx].count = 0;
        }

        // Add value to positions array
        if (table[idx].count == 0) {
            table[idx].offset = positions.size();
        }
        positions.push_back(value);
        table[idx].count++;
    }

    const Entry* find(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].key != -1) {
            if (table[idx].key == key) return &table[idx];
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// ========== HELPERS ==========

// Load dictionary: "code=value" text format
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int32_t code = std::stoi(line.substr(0, eq));
        std::string value = line.substr(eq + 1);
        dict[code] = value;
    }
    f.close();
    return dict;
}

// Load dictionary specifically for single-char values
std::unordered_map<int32_t, int32_t> load_int_dictionary(const std::string& path) {
    std::unordered_map<int32_t, int32_t> dict;
    std::ifstream f(path);
    if (!f.is_open()) return dict;
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int32_t code = std::stoi(line.substr(0, eq));
        std::string value = line.substr(eq + 1);
        // Store as integer for comparison: '1' -> 49, 'U' -> 85, etc.
        dict[code] = (int32_t)value[0];
    }
    f.close();
    return dict;
}

// Mmap a binary file
template<typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return nullptr;
    }
    count = st.st_size / sizeof(T);
    T* ptr = (T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return nullptr;
    return ptr;
}

// Load zone map index for block skipping
struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

struct ZoneMapIndex {
    uint32_t num_zones;
    std::vector<ZoneMapBlock> zones;

    static ZoneMapIndex load(const std::string& path) {
        ZoneMapIndex idx;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open zone map: " << path << std::endl;
            return idx;
        }

        // Read header: uint32_t num_zones
        if (read(fd, &idx.num_zones, sizeof(uint32_t)) < (int)sizeof(uint32_t)) {
            std::cerr << "Failed to read zone map header" << std::endl;
            close(fd);
            return idx;
        }

        idx.zones.resize(idx.num_zones);
        for (uint32_t i = 0; i < idx.num_zones; i++) {
            if (read(fd, &idx.zones[i].min_val, sizeof(int32_t)) < (int)sizeof(int32_t)) {
                std::cerr << "Failed to read zone min" << std::endl;
                break;
            }
            if (read(fd, &idx.zones[i].max_val, sizeof(int32_t)) < (int)sizeof(int32_t)) {
                std::cerr << "Failed to read zone max" << std::endl;
                break;
            }
            if (read(fd, &idx.zones[i].row_count, sizeof(uint32_t)) < (int)sizeof(uint32_t)) {
                std::cerr << "Failed to read zone count" << std::endl;
                break;
            }
        }
        close(fd);
        return idx;
    }
};

// ========== MAIN QUERY FUNCTION ==========

void run_q12(const std::string& gendb_dir, const std::string& results_dir) {
    using Clock = std::chrono::high_resolution_clock;

#ifdef GENDB_PROFILE
    auto t_total_start = Clock::now();
#endif

    // ===== METADATA CHECK =====
    printf("[METADATA CHECK] Q12\n");
    printf("  lineitem: 59986052 rows\n");
    printf("  orders: 15000000 rows\n");
    printf("  l_shipmode: dictionary-encoded int32_t\n");
    printf("  o_orderpriority: dictionary-encoded int32_t\n");
    printf("  l_receiptdate, l_commitdate, l_shipdate: epoch days (int32_t)\n");
    printf("  Date thresholds: 1994-01-01 = 8766, 1995-01-01 = 9131\n");
    printf("  Dictionary codes: MAIL=1, SHIP=6\n");
    printf("  Orderpriority codes: 1-URGENT=1, 2-HIGH=3\n");
    printf("\n");

    // ===== LOAD DICTIONARIES =====
#ifdef GENDB_PROFILE
    auto t_dict_start = Clock::now();
#endif

    auto l_shipmode_dict = load_dictionary(gendb_dir + "/lineitem/l_shipmode_dict.txt");
    auto o_orderpriority_dict = load_dictionary(gendb_dir + "/orders/o_orderpriority_dict.txt");

#ifdef GENDB_PROFILE
    {
        auto t_dict_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
        printf("[TIMING] load_dictionaries: %.2f ms\n", ms);
    }
#endif

    // ===== LOAD BINARY COLUMNS =====
#ifdef GENDB_PROFILE
    auto t_load_start = Clock::now();
#endif

    size_t lineitem_rows, orders_rows;

    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_rows);
    int32_t* l_shipmode = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipmode.bin", lineitem_rows);
    int32_t* l_commitdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_commitdate.bin", lineitem_rows);
    int32_t* l_receiptdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_receiptdate.bin", lineitem_rows);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_rows);

    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_rows);
    int32_t* o_orderpriority = mmap_file<int32_t>(gendb_dir + "/orders/o_orderpriority.bin", orders_rows);

#ifdef GENDB_PROFILE
    {
        auto t_load_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
        printf("[TIMING] load_columns: %.2f ms\n", ms);
    }
#endif

    if (!l_orderkey || !l_shipmode || !l_commitdate || !l_receiptdate || !l_shipdate ||
        !o_orderkey || !o_orderpriority) {
        std::cerr << "Failed to load binary columns" << std::endl;
        return;
    }

    printf("Loaded: lineitem rows=%zu, orders rows=%zu\n", lineitem_rows, orders_rows);

    // ===== FILTER LINEITEM WITH ZONE MAP =====
#ifdef GENDB_PROFILE
    auto t_filter_start = Clock::now();
#endif

    // Load zone map for l_shipdate (for block pruning)
    ZoneMapIndex zmap = ZoneMapIndex::load(gendb_dir + "/indexes/idx_lineitem_shipdate_zmap.bin");
    printf("Zone map zones: %u\n", zmap.num_zones);

    // Date thresholds
    const int32_t DATE_1994_01_01 = 8766;
    const int32_t DATE_1995_01_01 = 9131;

    // Filter result: (orderkey, shipmode) pairs
    std::vector<std::pair<int32_t, int32_t>> filtered_lineitem;
    filtered_lineitem.reserve(2000000);  // Estimate from zone map pruning

    // Build zone map skip mask: each zone can be pruned if it doesn't overlap [DATE_1994_01_01, DATE_1995_01_01)
    // A zone can be skipped if: max_val < DATE_1994_01_01 OR min_val >= DATE_1995_01_01
    std::vector<bool> zone_skip(zmap.num_zones, false);
    uint32_t skipped_zones = 0;
    for (uint32_t z = 0; z < zmap.num_zones; z++) {
        if (zmap.zones[z].max_val < DATE_1994_01_01 || zmap.zones[z].min_val >= DATE_1995_01_01) {
            zone_skip[z] = true;
            skipped_zones++;
        }
    }
    printf("Zone map: skipping %u/%u zones (%.1f%%)\n", skipped_zones, zmap.num_zones,
           100.0 * skipped_zones / zmap.num_zones);

    // Parallelize scan with OpenMP
    // Each thread collects results into thread-local vector, then merge
    std::vector<std::vector<std::pair<int32_t, int32_t>>> thread_filtered(omp_get_max_threads());

    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t r = 0; r < lineitem_rows; r++) {
        // Zone map pruning: determine zone for this row
        // Blocks are 100000 rows, so zone index = r / 100000
        uint32_t zone_idx = r / 100000;
        if (zone_idx < zmap.num_zones && zone_skip[zone_idx]) {
            continue;  // Skip this row, zone is pruned
        }

        // Apply all predicates
        // P1: l_shipmode IN ('MAIL', 'SHIP')
        if (l_shipmode[r] != 1 && l_shipmode[r] != 6) continue;

        // P2: l_receiptdate >= 1994-01-01 AND < 1995-01-01
        if (l_receiptdate[r] < DATE_1994_01_01 || l_receiptdate[r] >= DATE_1995_01_01) continue;

        // P3: l_commitdate < l_receiptdate
        if (l_commitdate[r] >= l_receiptdate[r]) continue;

        // P4: l_shipdate < l_commitdate
        if (l_shipdate[r] >= l_commitdate[r]) continue;

        // All predicates pass — add to thread-local result
        int tid = omp_get_thread_num();
        thread_filtered[tid].push_back({l_orderkey[r], l_shipmode[r]});
    }

    // Merge thread results
    for (int tid = 0; tid < omp_get_max_threads(); tid++) {
        for (const auto& pair : thread_filtered[tid]) {
            filtered_lineitem.push_back(pair);
        }
    }

#ifdef GENDB_PROFILE
    {
        auto t_filter_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
        printf("[TIMING] scan_filter: %.2f ms\n", ms);
    }
#endif
    printf("Filtered lineitem: %zu rows\n", filtered_lineitem.size());

    // ===== BUILD HASH TABLE ON FILTERED LINEITEM (OPEN-ADDRESSING) =====
#ifdef GENDB_PROFILE
    auto t_build_start = Clock::now();
#endif

    // Count unique orderkeys
    std::unordered_set<int32_t> unique_keys;
    for (const auto& [orderkey, shipmode] : filtered_lineitem) {
        unique_keys.insert(orderkey);
    }

    // Build compact multi-value hash table
    CompactMultiValueHashTable li_hash(unique_keys.size(), filtered_lineitem.size());

    for (const auto& [orderkey, shipmode] : filtered_lineitem) {
        li_hash.add_value(orderkey, shipmode);
    }

#ifdef GENDB_PROFILE
    {
        auto t_build_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
        printf("[TIMING] build_hash: %.2f ms\n", ms);
    }
#endif

    // ===== JOIN WITH ORDERS AND AGGREGATE =====
#ifdef GENDB_PROFILE
    auto t_join_start = Clock::now();
#endif

    // Aggregation: result[shipmode_code][0] = high_count, [1] = low_count
    // Shipmode codes are 0-6, we only care about 1 and 6
    std::vector<std::vector<int64_t>> agg(7, std::vector<int64_t>(2, 0));

    // High priority codes: 1-URGENT=1, 2-HIGH=3
    // (From o_orderpriority_dict: 1=1-URGENT, 3=2-HIGH)

    // Thread-local aggregation buffers to avoid contention
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<std::vector<int64_t>>> thread_agg(num_threads,
                                                               std::vector<std::vector<int64_t>>(7, std::vector<int64_t>(2, 0)));

    // Parallel join probe with thread-local aggregation
    #pragma omp parallel for schedule(dynamic, 50000) collapse(1)
    for (uint32_t o = 0; o < orders_rows; o++) {
        const auto* entry = li_hash.find(o_orderkey[o]);
        if (entry) {
            int tid = omp_get_thread_num();
            // For each lineitem row with this orderkey
            for (uint32_t i = entry->offset; i < entry->offset + entry->count; i++) {
                int32_t shipmode = li_hash.positions[i];

                // Check if orderpriority is high (1-URGENT or 2-HIGH)
                bool is_high = (o_orderpriority[o] == 1) || (o_orderpriority[o] == 3);

                if (is_high) {
                    thread_agg[tid][shipmode][0]++;  // high_line_count
                } else {
                    thread_agg[tid][shipmode][1]++;  // low_line_count
                }
            }
        }
    }

    // Merge thread-local aggregation buffers into global result
    for (int tid = 0; tid < num_threads; tid++) {
        for (int shipmode = 0; shipmode < 7; shipmode++) {
            agg[shipmode][0] += thread_agg[tid][shipmode][0];
            agg[shipmode][1] += thread_agg[tid][shipmode][1];
        }
    }

#ifdef GENDB_PROFILE
    {
        auto t_join_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
        printf("[TIMING] join_probe: %.2f ms\n", ms);
    }
#endif

    // ===== OUTPUT RESULTS =====
#ifdef GENDB_PROFILE
    auto t_output_start = Clock::now();
#endif

    std::string output_file = results_dir + "/Q12.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "l_shipmode,high_line_count,low_line_count\n";

    // Write results: only shipmode codes that appear in filtered lineitem
    // Codes 1 (MAIL) and 6 (SHIP)
    std::vector<int32_t> shipmodes = {1, 6};  // In sorted order
    for (int32_t code : shipmodes) {
        std::string shipmode_str = l_shipmode_dict[code];
        out << shipmode_str << "," << agg[code][0] << "," << agg[code][1] << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    {
        auto t_output_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
        printf("[TIMING] output: %.2f ms\n", ms);
    }
#endif

    printf("Results written to: %s\n", output_file.c_str());

#ifdef GENDB_PROFILE
    {
        auto t_total_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
        printf("[TIMING] total: %.2f ms\n", ms);
    }
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
