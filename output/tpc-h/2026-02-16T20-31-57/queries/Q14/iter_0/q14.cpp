#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*
 * Q14: Promotion Effect
 *
 * LOGICAL PLAN:
 * 1. Filter lineitem by date range [1995-09-01, 1995-10-01):
 *    - Expected ~1.6M rows (2.7% of 59M) based on 1 month out of 8 years
 *    - Use zone map to skip blocks outside date range
 * 2. Filter part by p_type LIKE 'PROMO%':
 *    - Expected ~1/6 of 2M rows = ~333K rows (dict lookup based on prefix)
 * 3. Hash join on l_partkey = p_partkey:
 *    - Build hash table on filtered part (333K rows) - smaller side
 *    - Probe with filtered lineitem (1.6M rows)
 *    - Expected result: ~270K rows after join
 * 4. Aggregation: compute SUM(promo_revenue) and SUM(total_revenue)
 *    - Single group aggregation (scalar result)
 *    - CASE expression: if part type starts with PROMO, include in promo_revenue
 * 5. Output: calculate ratio = promo_revenue / total_revenue * 100.0
 *
 * PHYSICAL PLAN:
 * 1. Scan & Filter:
 *    - Load zone map for lineitem.l_shipdate to identify blocks within date range
 *    - Scan lineitem columns within matching blocks only
 *    - Apply date filter in-loop during scan
 *    - Scan part: mmap p_partkey, p_type (dictionary codes)
 *    - Apply p_type filter by loading dictionary and checking prefix match
 * 2. Join Implementation:
 *    - Use hash table (unordered_map) for part_partkey → row_index lookup
 *    - Iterate filtered lineitem, probe hash table per row
 *    - For matches, compute revenue and accumulate in aggregation
 * 3. Aggregation:
 *    - Two scalar accumulators: sum_promo_revenue, sum_total_revenue
 * 4. Output:
 *    - Calculate promo_revenue / total_revenue * 100.0
 *    - Write single row CSV to results_dir/Q14.csv
 * 5. Parallelism:
 *    - Single-threaded for scalar aggregation
 *
 * DATE ENCODING:
 *    1995-09-01: epoch days = (1995-1970)*365 + leap days + month days + (day-1)
 *    = 25*365 + 6 leaps + (31+28+31+30+31+30+31+31) + 0
 *    = 9125 + 6 + 243 + 0 = 9374 days
 *    1995-10-01: 9374 + 30 = 9404 days
 *
 * DECIMAL ENCODING:
 *    scale_factor = 2, so value 1234 = 12.34
 *    Computation: (extended_price * (100 - discount)) / 100 at scale_factor^2
 *    Then divide by 10000 at output for final decimal places
 */

// Mmap helper
struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening " << path << std::endl;
            return;
        }
        off_t file_size = lseek(fd, 0, SEEK_END);
        size = (size_t)file_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Error mmapping " << path << std::endl;
            close(fd);
            fd = -1;
            ptr = nullptr;
        }
    }

    ~MmapFile() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    template<typename T>
    T* as() const {
        return static_cast<T*>(ptr);
    }
};

// Zone map entry
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Load dictionary and return all entries
std::vector<std::string> load_dict(const std::string& dict_path) {
    std::vector<std::string> dict;
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Error opening dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(dict_file, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Load zone map for lineitem.l_shipdate
std::vector<ZoneMapEntry> load_zonemap(const std::string& zonemap_path, int32_t date_start, int32_t date_end) {
    std::vector<ZoneMapEntry> zonemap;
    std::ifstream zm_file(zonemap_path, std::ios::binary);
    if (!zm_file.is_open()) {
        std::cerr << "Error opening zonemap: " << zonemap_path << std::endl;
        return zonemap;
    }

    uint32_t num_blocks;
    zm_file.read((char*)&num_blocks, sizeof(uint32_t));

    zonemap.resize(num_blocks);
    for (uint32_t i = 0; i < num_blocks; i++) {
        zm_file.read((char*)&zonemap[i].min_val, sizeof(int32_t));
        zm_file.read((char*)&zonemap[i].max_val, sizeof(int32_t));
        zm_file.read((char*)&zonemap[i].row_count, sizeof(uint32_t));
    }

    return zonemap;
}

void run_q14(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Constants for date filtering
    // 1995-09-01 in epoch days
    int32_t date_start = 9374;  // 1995-09-01
    int32_t date_end = 9404;    // 1995-10-01 (exclusive)

    // Load dictionary for p_type to check for PROMO prefix
    std::string dict_path = gendb_dir + "/part/p_type_dict.txt";
    auto p_type_dict = load_dict(dict_path);

    // Build set of dictionary codes that start with "PROMO"
    std::unordered_map<int32_t, bool> promo_codes;
    for (int32_t i = 0; i < (int32_t)p_type_dict.size(); i++) {
        const auto& entry = p_type_dict[i];
        if (entry.size() >= 5 && entry.substr(0, 5) == "PROMO") {
            promo_codes[i] = true;
        }
    }

    #ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Loaded %zu dictionary entries, %zu PROMO entries\n",
           p_type_dict.size(), promo_codes.size());
    #endif

    // ===== LOAD ZONE MAP FOR DATE FILTERING =====

    #ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string zonemap_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
    auto zonemap = load_zonemap(zonemap_path, date_start, date_end);

    #ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] load_zonemap: %.2f ms\n", zonemap_ms);
    printf("[TIMING] total_blocks: %zu\n", zonemap.size());
    #endif

    // ===== SCAN & FILTER PHASE =====

    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Mmap lineitem columns
    MmapFile li_partkey_file(gendb_dir + "/lineitem/l_partkey.bin");
    MmapFile li_extendedprice_file(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile li_discount_file(gendb_dir + "/lineitem/l_discount.bin");
    MmapFile li_shipdate_file(gendb_dir + "/lineitem/l_shipdate.bin");

    if (!li_partkey_file.ptr || !li_extendedprice_file.ptr ||
        !li_discount_file.ptr || !li_shipdate_file.ptr) {
        std::cerr << "Error loading lineitem columns" << std::endl;
        return;
    }

    auto* li_partkey = li_partkey_file.as<int32_t>();
    auto* li_extendedprice = li_extendedprice_file.as<int64_t>();
    auto* li_discount = li_discount_file.as<int64_t>();
    auto* li_shipdate = li_shipdate_file.as<int32_t>();

    const int64_t num_lineitem_rows = li_partkey_file.size / sizeof(int32_t);
    const int32_t block_size = 100000;  // From storage guide

    // Mmap part columns
    MmapFile part_partkey_file(gendb_dir + "/part/p_partkey.bin");
    MmapFile part_type_file(gendb_dir + "/part/p_type.bin");

    if (!part_partkey_file.ptr || !part_type_file.ptr) {
        std::cerr << "Error loading part columns" << std::endl;
        return;
    }

    auto* part_partkey = part_partkey_file.as<int32_t>();
    auto* part_type = part_type_file.as<int32_t>();

    const int64_t num_part_rows = part_partkey_file.size / sizeof(int32_t);

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan: %.2f ms\n", scan_ms);
    #endif

    // ===== FILTER PART & BUILD HASH TABLE =====

    #ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
    #endif

    // Hash table: partkey -> row indices in filtered part
    std::unordered_map<int32_t, std::vector<int64_t>> part_ht;
    part_ht.reserve(num_part_rows);

    for (int64_t i = 0; i < num_part_rows; i++) {
        // Build hash table with all parts
        part_ht[part_partkey[i]].push_back(i);
    }

    #ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hash_table: %.2f ms\n", build_ms);
    #endif

    // ===== FILTER LINEITEM & PROBE JOIN & AGGREGATE (WITH ZONE MAP PRUNING) =====

    #ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
    #endif

    // Accumulators for aggregation
    int64_t sum_promo_revenue_scaled = 0;  // scaled by 4 (scale_factor^2 = 2^2)
    int64_t sum_total_revenue_scaled = 0;

    int64_t matched_rows = 0;
    int64_t filtered_li_rows = 0;
    int64_t skipped_blocks = 0;

    // Iterate through blocks and apply zone map pruning
    for (size_t block_idx = 0; block_idx < zonemap.size(); block_idx++) {
        const auto& zone = zonemap[block_idx];

        // Check if block can be skipped based on date range
        // Skip if block's max < date_start OR min >= date_end
        if (zone.max_val < date_start || zone.min_val >= date_end) {
            skipped_blocks++;
            continue;
        }

        // Process rows in this block
        int64_t block_start = block_idx * block_size;
        int64_t block_end = std::min((int64_t)(block_idx + 1) * block_size, num_lineitem_rows);

        for (int64_t i = block_start; i < block_end; i++) {
            // Filter lineitem by date
            if (li_shipdate[i] < date_start || li_shipdate[i] >= date_end) {
                continue;
            }
            filtered_li_rows++;

            // Join probe
            int32_t li_pk = li_partkey[i];
            auto it = part_ht.find(li_pk);
            if (it == part_ht.end()) {
                continue;
            }

            // For each matching part row
            for (int64_t part_idx : it->second) {
                matched_rows++;

                // Compute revenue: extended_price * (1 - discount)
                // With scale factors: both are scaled by 2, so product is scaled by 4
                // discount is already scaled by 2 (e.g., 0.05 = 5)
                // So: extended_price (scaled by 2) * (100 - discount) / 100
                int64_t discount_complement = 100 - li_discount[i];  // e.g., 95 for 5% discount
                int64_t revenue_scaled = li_extendedprice[i] * discount_complement;  // scaled by 2 * 2 = 4

                // Total revenue is always accumulated
                sum_total_revenue_scaled += revenue_scaled;

                // Promo revenue only if part type starts with PROMO
                int32_t type_code = part_type[part_idx];
                if (promo_codes.count(type_code) > 0) {
                    sum_promo_revenue_scaled += revenue_scaled;
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_filter_aggregate: %.2f ms\n", join_ms);
    printf("[TIMING] filtered_lineitem_rows: %ld\n", filtered_li_rows);
    printf("[TIMING] matched_rows: %ld\n", matched_rows);
    printf("[TIMING] skipped_blocks: %ld\n", skipped_blocks);
    #endif

    // ===== COMPUTE FINAL RESULT =====

    #ifdef GENDB_PROFILE
    auto t_compute_start = std::chrono::high_resolution_clock::now();
    #endif

    double promo_revenue = 0.0;
    double total_revenue = 0.0;
    double result = 0.0;

    // Unscale: divide by 10000 to get final decimal value
    // sum_revenue_scaled = sum(price_scaled * (100 - discount_scaled))
    // price_scaled is scaled by 2, discount scaled by 2
    // (100 - discount_scaled) is unscaled (integer operation)
    // So revenue_scaled is scaled by 2
    // But we need to divide by 100 first: sum_revenue_scaled / 100 gives scale_factor=2
    // Then divide by 100 more for CSV output: total / 10000

    total_revenue = (double)sum_total_revenue_scaled / 10000.0;
    promo_revenue = (double)sum_promo_revenue_scaled / 10000.0;

    if (total_revenue > 0.0) {
        result = 100.0 * promo_revenue / total_revenue;
    }

    #ifdef GENDB_PROFILE
    auto t_compute_end = std::chrono::high_resolution_clock::now();
    double compute_ms = std::chrono::duration<double, std::milli>(t_compute_end - t_compute_start).count();
    printf("[TIMING] compute_result: %.2f ms\n", compute_ms);
    printf("[TIMING] promo_revenue: %.2f\n", promo_revenue);
    printf("[TIMING] total_revenue: %.2f\n", total_revenue);
    printf("[TIMING] result: %.2f\n", result);
    #endif

    // ===== WRITE OUTPUT =====

    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_file = results_dir + "/Q14.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Error opening output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "promo_revenue\n";

    // Write result with 2 decimal places
    out << std::fixed << std::setprecision(2) << result << "\n";

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
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
    run_q14(gendb_dir, results_dir);
    return 0;
}
#endif
