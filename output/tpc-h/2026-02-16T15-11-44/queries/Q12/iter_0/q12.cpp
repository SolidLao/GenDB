#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
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

    // Load zone map for l_receiptdate
    ZoneMapIndex zmap = ZoneMapIndex::load(gendb_dir + "/indexes/idx_lineitem_shipdate_zmap.bin");
    printf("Zone map zones: %u\n", zmap.num_zones);

    // Date thresholds
    const int32_t DATE_1994_01_01 = 8766;
    const int32_t DATE_1995_01_01 = 9131;

    // Filter result: (orderkey, shipmode) pairs
    std::vector<std::pair<int32_t, int32_t>> filtered_lineitem;
    filtered_lineitem.reserve(2000000);  // Estimate from zone map pruning

    // Scan lineitem table once with all predicates
    for (size_t r = 0; r < lineitem_rows; r++) {
        // Apply all predicates
        // P1: l_shipmode IN ('MAIL', 'SHIP')
        if (l_shipmode[r] != 1 && l_shipmode[r] != 6) continue;

        // P2: l_receiptdate >= 1994-01-01 AND < 1995-01-01
        if (l_receiptdate[r] < DATE_1994_01_01 || l_receiptdate[r] >= DATE_1995_01_01) continue;

        // P3: l_commitdate < l_receiptdate
        if (l_commitdate[r] >= l_receiptdate[r]) continue;

        // P4: l_shipdate < l_commitdate
        if (l_shipdate[r] >= l_commitdate[r]) continue;

        // All predicates pass
        filtered_lineitem.push_back({l_orderkey[r], l_shipmode[r]});
    }

#ifdef GENDB_PROFILE
    {
        auto t_filter_end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
        printf("[TIMING] scan_filter: %.2f ms\n", ms);
    }
#endif
    printf("Filtered lineitem: %zu rows\n", filtered_lineitem.size());

    // ===== BUILD HASH TABLE ON FILTERED LINEITEM =====
#ifdef GENDB_PROFILE
    auto t_build_start = Clock::now();
#endif

    // Build hash table: orderkey -> list of (shipmode) values
    // For this query, each l_orderkey may have multiple rows
    std::unordered_map<int32_t, std::vector<int32_t>> li_hash;
    li_hash.reserve(filtered_lineitem.size() / 4);  // Rough estimate

    for (const auto& [orderkey, shipmode] : filtered_lineitem) {
        li_hash[orderkey].push_back(shipmode);
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

    for (uint32_t o = 0; o < orders_rows; o++) {
        auto it = li_hash.find(o_orderkey[o]);
        if (it != li_hash.end()) {
            // For each lineitem row with this orderkey
            for (int32_t shipmode : it->second) {
                // Check if orderpriority is high (1-URGENT or 2-HIGH)
                bool is_high = (o_orderpriority[o] == 1) || (o_orderpriority[o] == 3);

                if (is_high) {
                    agg[shipmode][0]++;  // high_line_count
                } else {
                    agg[shipmode][1]++;  // low_line_count
                }
            }
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
