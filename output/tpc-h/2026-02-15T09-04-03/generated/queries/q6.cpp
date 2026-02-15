#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cmath>
#include <chrono>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include <iomanip>
#include <sstream>

namespace {

// Q6: Forecasting Revenue Change
// Single-table scan with range predicates
// WHERE
//     l_shipdate >= DATE '1994-01-01' (8766 days since epoch)
//     AND l_shipdate < DATE '1995-01-01' (9131 days since epoch)
//     AND l_discount BETWEEN 0.05 AND 0.06 (scaled by 100: 5 to 6)
//     AND l_quantity < 24 (scaled by 100: < 2400)
// SELECT SUM(l_extendedprice * l_discount)

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t block_num;
    uint32_t row_count;
};

template <typename T>
T* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat " << path << std::endl;
        close(fd);
        exit(1);
    }

    out_size = sb.st_size;
    T* ptr = (T*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        close(fd);
        exit(1);
    }

    close(fd);
    return ptr;
}

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Predicates
    const int32_t date_min = 8766;      // 1994-01-01
    const int32_t date_max = 9131;      // 1995-01-01
    const int64_t discount_min = 5;     // 0.05 * 100
    const int64_t discount_max = 7;     // 0.06 * 100 + 1 (inclusive upper bound)
    const int64_t quantity_max = 2400;  // 24 * 100

    std::string lineitem_dir = gendb_dir + "/lineitem/";

    // Load columns
    size_t num_rows_actual = 0;
    int32_t* shipdate = mmap_file<int32_t>(lineitem_dir + "l_shipdate.bin", num_rows_actual);
    num_rows_actual /= sizeof(int32_t);

    size_t size_discount;
    int64_t* discount = mmap_file<int64_t>(lineitem_dir + "l_discount.bin", size_discount);

    size_t size_quantity;
    int64_t* quantity = mmap_file<int64_t>(lineitem_dir + "l_quantity.bin", size_quantity);

    size_t size_extendedprice;
    int64_t* extendedprice = mmap_file<int64_t>(lineitem_dir + "l_extendedprice.bin", size_extendedprice);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_total_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    // Load zone map to prune blocks
    size_t zone_file_size;
    uint32_t* zone_data_raw = mmap_file<uint32_t>(gendb_dir + "/indexes/lineitem_shipdate_zone.bin", zone_file_size);

    uint32_t num_zones = zone_data_raw[0];
    ZoneMapEntry* zones = (ZoneMapEntry*)(zone_data_raw + 1);

#ifdef GENDB_PROFILE
    auto t_zone_load_end = std::chrono::high_resolution_clock::now();
    double ms_zone_load = std::chrono::duration<double, std::milli>(t_zone_load_end - t_load_end).count();
    printf("[TIMING] zone_map_load: %.2f ms\n", ms_zone_load);
#endif

    // Identify blocks to scan using zone map
    std::vector<bool> block_skip(num_zones, false);
    uint32_t blocks_skipped = 0;

#ifdef GENDB_PROFILE
    auto t_zone_check_start = std::chrono::high_resolution_clock::now();
#endif

    for (uint32_t i = 0; i < num_zones; i++) {
        // Skip if zone's date range doesn't overlap [date_min, date_max)
        if (zones[i].max_val < date_min || zones[i].min_val >= date_max) {
            block_skip[i] = true;
            blocks_skipped++;
        }
    }

#ifdef GENDB_PROFILE
    auto t_zone_check_end = std::chrono::high_resolution_clock::now();
    double ms_zone_check = std::chrono::duration<double, std::milli>(t_zone_check_end - t_zone_check_start).count();
    printf("[TIMING] zone_map_check: %.2f ms\n", ms_zone_check);
    printf("[DEBUG] zones_scanned: %u, zones_skipped: %u\n", num_zones - blocks_skipped, blocks_skipped);
#endif

    // Scan and filter
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    long num_matches = 0;
    double revenue_sum = 0.0;

#pragma omp parallel for reduction(+:num_matches, revenue_sum)
    for (int64_t i = 0; i < (int64_t)num_rows_actual; i++) {
        // Apply filters
        if (shipdate[i] < date_min || shipdate[i] >= date_max) continue;
        if (discount[i] < discount_min || discount[i] > discount_max) continue;
        if (quantity[i] >= quantity_max) continue;

        num_matches++;
        // Compute revenue: (l_extendedprice * l_discount) / 100 / 100
        // Both are scaled by 100, so product needs to be divided by 10000
        double extended_price_actual = extendedprice[i] / 100.0;
        double discount_actual = discount[i] / 100.0;
        revenue_sum += extended_price_actual * discount_actual;
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_scan);
    printf("[DEBUG] matched_rows: %ld\n", num_matches);
#endif

    // Write results
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    out << "revenue\n";
    out << std::fixed << std::setprecision(4) << revenue_sum << "\n";
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    std::cout << "Q6 execution complete. Revenue = " << std::fixed << std::setprecision(4) << revenue_sum << std::endl;
    std::cout << "Matched rows: " << num_matches << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
