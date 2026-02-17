/*
================================================================================
LOGICAL PLAN FOR Q14: Promotion Effect
================================================================================

Query:
  SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
         / SUM(l_extendedprice * (1 - l_discount))
  FROM lineitem, part
  WHERE l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'   (epoch day 9374)
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH  (epoch day 9404)

Logical Plan:
  1. SCAN lineitem with range filter on l_shipdate [9374, 9404)
     - Estimated rows: ~1,615,000 (from 59,986,052 rows with ~2.7% selectivity)
     - Output: (l_partkey, l_extendedprice, l_discount)

  2. BUILD hash table on filtered lineitem results
     - Key: l_partkey (int32_t)
     - Payload: pre-computed SUM(l_extendedprice * (1 - l_discount)) per partition
     - Note: We'll do aggregation during lineitem scan

  3. SCAN part table (full scan, no filters on part directly)
     - Output: (p_partkey, p_type code)
     - Rows: 2,000,000

  4. JOIN part with lineitem aggregation using p_partkey
     - Hash lookup on part.p_partkey → sum values from lineitem

  5. AGGREGATE over joined results:
     - SUM(revenue where p_type LIKE 'PROMO%') as promo_sum
     - SUM(revenue for all) as total_sum
     - Final result: 100.00 * promo_sum / total_sum

================================================================================
PHYSICAL PLAN FOR Q14
================================================================================

Scan lineitem with zone map pruning:
  - Load idx_lineitem_shipdate_zmap to skip blocks outside [9374, 9404)
  - For remaining rows, apply l_shipdate filter and compute (1 - discount)
  - Use Kahan summation for numerical stability
  - Accumulate revenue into two buckets: promo_revenue and total_revenue

Build hash table on lineitem aggregation:
  - Create hash table mapping p_partkey → (total_revenue, promo_revenue)
  - Access pattern: sequential scan with aggregation (no explicit hash build needed if we accumulate during scan)

Scan part with hash join:
  - For each part row, load p_type from dictionary
  - Check if p_type starts with 'PROMO'
  - Look up p_partkey in accumulated sums
  - Final division and scaling

Data Structures:
  - Hash table for part-to-lineitem join: std::unordered_map<int32_t, pair<int64_t, int64_t>>
    (small enough that std::unordered_map is acceptable for 2M entries)

Parallelism:
  - Lineitem scan: OpenMP parallel for (line-by-line with thread-local Kahan accumulators)
  - Part scan: Sequential (final computation is trivial)

Index Usage:
  - Load zone map for l_shipdate to skip irrelevant blocks
  - Read p_type dictionary to identify 'PROMO%' prefix matches

================================================================================
*/

#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <cassert>
#include <algorithm>
#include <omp.h>

// Structure for zone map entry
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Structure for aggregation results
struct AggResult {
    int64_t total_sum;
    int64_t promo_sum;
};

// Memory-mapped file handle
struct MmapFile {
    void* ptr;
    size_t size;
    int fd;

    MmapFile() : ptr(nullptr), size(0), fd(-1) {}

    ~MmapFile() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd != -1) {
            close(fd);
        }
    }
};

// Load a binary file via mmap
MmapFile load_mmap(const std::string& path) {
    MmapFile file;
    file.fd = open(path.c_str(), O_RDONLY);
    if (file.fd < 0) {
        std::cerr << "Error opening " << path << std::endl;
        return file;
    }

    struct stat sb;
    if (fstat(file.fd, &sb) < 0) {
        std::cerr << "Error fstat on " << path << std::endl;
        close(file.fd);
        file.fd = -1;
        return file;
    }

    file.size = sb.st_size;
    file.ptr = mmap(nullptr, file.size, PROT_READ, MAP_SHARED, file.fd, 0);

    if (file.ptr == MAP_FAILED) {
        std::cerr << "Error mmap on " << path << std::endl;
        close(file.fd);
        file.fd = -1;
        return file;
    }

    return file;
}

// Load dictionary for strings
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Error opening dictionary " << dict_path << std::endl;
        return dict;
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
    f.close();
    return dict;
}

// Check if a string starts with "PROMO"
bool starts_with_promo(const std::string& s) {
    return s.length() >= 5 && s.substr(0, 5) == "PROMO";
}

// Kahan summation for floating point accuracy
struct KahanSum {
    int64_t sum;
    int64_t comp;

    KahanSum() : sum(0), comp(0) {}

    void add(int64_t val) {
        int64_t y = val - comp;
        int64_t t = sum + y;
        comp = (t - sum) - y;
        sum = t;
    }

    int64_t get() const {
        return sum;
    }
};

void run_q14(const std::string& gendb_dir, const std::string& results_dir) {
    std::string lineitem_dir = gendb_dir + "/lineitem";
    std::string part_dir = gendb_dir + "/part";
    std::string indexes_dir = gendb_dir + "/indexes";

    // Date constants (epoch days)
    const int32_t DATE_1995_09_01 = 9374;
    const int32_t DATE_1995_10_01 = 9404;

    // ========== Load Zone Map for l_shipdate ==========
#ifdef GENDB_PROFILE
    auto t_zmap_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile zmap_file = load_mmap(indexes_dir + "/idx_lineitem_shipdate_zmap.bin");
    std::vector<ZoneMapEntry> zones;

    if (zmap_file.ptr && zmap_file.ptr != MAP_FAILED) {
        uint32_t* num_zones_ptr = (uint32_t*)zmap_file.ptr;
        uint32_t num_zones = *num_zones_ptr;

        ZoneMapEntry* zone_data = (ZoneMapEntry*)((uint8_t*)zmap_file.ptr + 4);
        for (uint32_t i = 0; i < num_zones; ++i) {
            zones.push_back(zone_data[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_zmap_end = std::chrono::high_resolution_clock::now();
    double zmap_ms = std::chrono::duration<double, std::milli>(t_zmap_end - t_zmap_start).count();
    printf("[TIMING] load_zmap: %.2f ms\n", zmap_ms);
#endif

    // ========== Load Dictionary for p_type ==========
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, std::string> p_type_dict = load_dictionary(part_dir + "/p_type_dict.txt");

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dict: %.2f ms\n", dict_ms);
#endif

    // ========== Load lineitem columns ==========
#ifdef GENDB_PROFILE
    auto t_load_li_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile li_partkey_file = load_mmap(lineitem_dir + "/l_partkey.bin");
    MmapFile li_extendedprice_file = load_mmap(lineitem_dir + "/l_extendedprice.bin");
    MmapFile li_discount_file = load_mmap(lineitem_dir + "/l_discount.bin");
    MmapFile li_shipdate_file = load_mmap(lineitem_dir + "/l_shipdate.bin");

    if (!li_partkey_file.ptr || !li_extendedprice_file.ptr || !li_discount_file.ptr || !li_shipdate_file.ptr) {
        std::cerr << "Error loading lineitem files" << std::endl;
        return;
    }

    int32_t* li_partkey = (int32_t*)li_partkey_file.ptr;
    int64_t* li_extendedprice = (int64_t*)li_extendedprice_file.ptr;
    int64_t* li_discount = (int64_t*)li_discount_file.ptr;
    int32_t* li_shipdate = (int32_t*)li_shipdate_file.ptr;

#ifdef GENDB_PROFILE
    auto t_load_li_end = std::chrono::high_resolution_clock::now();
    double load_li_ms = std::chrono::duration<double, std::milli>(t_load_li_end - t_load_li_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", load_li_ms);
#endif

    // ========== Load part columns ==========
#ifdef GENDB_PROFILE
    auto t_load_part_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile part_partkey_file = load_mmap(part_dir + "/p_partkey.bin");
    MmapFile part_type_file = load_mmap(part_dir + "/p_type.bin");

    if (!part_partkey_file.ptr || !part_type_file.ptr) {
        std::cerr << "Error loading part files" << std::endl;
        return;
    }

    int32_t* part_partkey = (int32_t*)part_partkey_file.ptr;
    int32_t* part_type = (int32_t*)part_type_file.ptr;

    uint64_t part_rows = part_partkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_part_end = std::chrono::high_resolution_clock::now();
    double load_part_ms = std::chrono::duration<double, std::milli>(t_load_part_end - t_load_part_start).count();
    printf("[TIMING] load_part: %.2f ms\n", load_part_ms);
#endif

    // ========== Scan part to build promo_partkeys set (BEFORE lineitem scan) ==========
#ifdef GENDB_PROFILE
    auto t_part_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Pre-build reverse map: p_type_code → is_promo (single pass through dict)
    // Use a vector indexed by type code for O(1) lookup instead of unordered_set
    // Find max type code first
    int32_t max_type_code = 0;
    for (auto& kv : p_type_dict) {
        max_type_code = std::max(max_type_code, kv.first);
    }

    // Create a boolean array: is_promo_type[code] = true if code starts with PROMO
    std::vector<bool> is_promo_type(max_type_code + 1, false);
    for (auto& kv : p_type_dict) {
        if (starts_with_promo(kv.second)) {
            is_promo_type[kv.first] = true;
        }
    }

    // Pre-compute set of promo partkeys from part table (2M rows)
    // Use a vector-based approach for faster lookups (cache-friendly)
    // First pass: find max partkey to size the vector
    int32_t max_partkey = 0;
    #pragma omp parallel for reduction(max:max_partkey)
    for (uint64_t i = 0; i < part_rows; ++i) {
        max_partkey = std::max(max_partkey, part_partkey[i]);
    }

    // Create a boolean vector: is_promo_partkey[key] = true if partkey is promo
    // Using vector<bool> is space-efficient
    std::vector<bool> is_promo_partkey(max_partkey + 1, false);

    // Second pass: mark promo partkeys (parallel)
    #pragma omp parallel for
    for (uint64_t i = 0; i < part_rows; ++i) {
        int32_t type_code = part_type[i];

        // Fast check: O(1) array lookup instead of hash table lookup
        if (type_code >= 0 && type_code <= max_type_code && is_promo_type[type_code]) {
            int32_t partkey = part_partkey[i];
            if (partkey >= 0 && partkey <= max_partkey) {
                is_promo_partkey[partkey] = true;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_part_scan_end = std::chrono::high_resolution_clock::now();
    double part_scan_ms = std::chrono::duration<double, std::milli>(t_part_scan_end - t_part_scan_start).count();
    printf("[TIMING] part_scan: %.2f ms\n", part_scan_ms);
#endif

    // ========== Scan lineitem with zone map pruning and parallel aggregation ==========
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Build zone-to-row-range mapping: each zone corresponds to a block of rows (100K rows per block based on storage guide)
    // The zones vector is ordered and represents sequential blocks of the lineitem file
    std::vector<std::pair<uint64_t, uint64_t>> zone_row_ranges;  // (start, end) row indices
    uint64_t row_index = 0;
    for (const auto& zone : zones) {
        uint64_t zone_start = row_index;
        uint64_t zone_end = row_index + zone.row_count;

        // Check if this zone overlaps with the date range [9374, 9404)
        // Skip if: zone.max_val < DATE_1995_09_01 (entire block is before range)
        //       OR zone.min_val >= DATE_1995_10_01 (entire block is after range)
        if (!(zone.max_val < DATE_1995_09_01 || zone.min_val >= DATE_1995_10_01)) {
            zone_row_ranges.push_back({zone_start, zone_end});
        }

        row_index = zone_end;
    }

    // Parallel scan over active zone ranges
    int64_t total_revenue = 0;
    int64_t promo_revenue = 0;
    uint64_t filtered_count = 0;

    #pragma omp parallel for collapse(1) reduction(+:total_revenue, promo_revenue, filtered_count)
    for (size_t z = 0; z < zone_row_ranges.size(); ++z) {
        uint64_t zone_start = zone_row_ranges[z].first;
        uint64_t zone_end = zone_row_ranges[z].second;

        for (uint64_t i = zone_start; i < zone_end; ++i) {
            int32_t shipdate = li_shipdate[i];

            // Zone already filtered by min/max, but apply exact date filter still
            if (shipdate < DATE_1995_09_01 || shipdate >= DATE_1995_10_01) {
                continue;
            }

            filtered_count++;

            int32_t partkey = li_partkey[i];
            int64_t price = li_extendedprice[i];
            int64_t discount = li_discount[i];

            // Compute revenue = price * (1 - discount)
            // price and discount are scaled by 100
            // (1 - discount/100) = (100 - discount) / 100
            // revenue = price * (100 - discount) / 100
            int64_t revenue = (price * (100 - discount)) / 100;

            // Accumulate to total
            total_revenue += revenue;

            // Check if this part is promo and accumulate (O(1) array lookup)
            if (partkey >= 0 && partkey <= max_partkey && is_promo_partkey[partkey]) {
                promo_revenue += revenue;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms (filtered %lu rows)\n", scan_ms, filtered_count);
#endif

    // ========== Aggregation timing (placeholder - work merged into scan) ==========
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
#endif

    // ========== Compute final result ==========
#ifdef GENDB_PROFILE
    auto t_result_start = std::chrono::high_resolution_clock::now();
#endif

    double result = 0.0;
    if (total_revenue > 0) {
        result = 100.0 * promo_revenue / total_revenue;
    }

#ifdef GENDB_PROFILE
    auto t_result_end = std::chrono::high_resolution_clock::now();
    double result_ms = std::chrono::duration<double, std::milli>(t_result_end - t_result_start).count();
    printf("[TIMING] compute_result: %.2f ms\n", result_ms);
#endif

    // ========== Write output CSV ==========
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q14.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Error opening output file " << output_file << std::endl;
        return;
    }

    // Write header
    out << "promo_revenue\n";

    // Write result with 2 decimal places
    char buf[64];
    snprintf(buf, sizeof(buf), "%.2f", result);
    out << buf << "\n";

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms (execution time only)\n", scan_ms + agg_ms + part_scan_ms + result_ms);
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
