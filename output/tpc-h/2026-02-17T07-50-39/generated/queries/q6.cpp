/*
 * TPC-H Q6: Forecasting Revenue Change (Iteration 0)
 *
 * LOGICAL PLAN:
 *   - Single table: lineitem (59,986,052 rows)
 *   - Predicates:
 *     1. l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01' (8766 to 9130 as epoch days)
 *     2. l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7)
 *     3. l_quantity < 24 (scaled: < 2400)
 *   - Aggregation: SUM(l_extendedprice * l_discount)
 *
 * PHYSICAL PLAN:
 *   - Zone map pruning on l_shipdate (table sorted by l_shipdate)
 *   - Parallel scan with morsel-driven parallelism (64 cores, 100K morsel size)
 *   - Thread-local aggregation → merge
 *   - Fused pipeline: scan + 3 filters + accumulate in single pass
 *   - Data structures: thread-local int64_t accumulators (no hash table needed)
 *
 * PERFORMANCE STRATEGY:
 *   - Zone map skips blocks outside 1994 date range
 *   - Predicate ordering: shipdate (most selective after zone map), then discount, then quantity
 *   - Scaled integer arithmetic (all int64_t, no double)
 *   - Result scaled down by (100 * 100) = 10000 for final output
 */

#include <iostream>
#include <fstream>
#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <omp.h>
#include <atomic>

namespace {

// Zone map entry structure (from storage guide)
struct ZoneMapEntry {
    int32_t min_val;  // 4 bytes
    int32_t max_val;  // 4 bytes
};

// Helper: mmap a binary column file
template<typename T>
const T* mmap_column(const std::string& path, size_t expected_rows, size_t& actual_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    fstat(fd, &st);
    actual_rows = st.st_size / sizeof(T);
    if (expected_rows > 0 && actual_rows != expected_rows) {
        std::cerr << "Warning: " << path << " has " << actual_rows << " rows, expected " << expected_rows << std::endl;
    }
    void* addr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return static_cast<const T*>(addr);
}

// Helper: compute epoch days from date (1970-01-01 = day 0)
inline int32_t date_to_epoch_days(int year, int month, int day) {
    // Days per month (non-leap year)
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Count days from 1970 to year-1
    int days = 0;
    for (int y = 1970; y < year; y++) {
        bool is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += is_leap ? 366 : 365;
    }

    // Add days for complete months in current year
    for (int m = 1; m < month; m++) {
        days += days_in_month[m];
        if (m == 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
            days += 1; // Leap year February
        }
    }

    // Add remaining days
    days += (day - 1);
    return days;
}

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Constants
    const size_t expected_rows = 59986052;
    const size_t block_size = 100000;

    // Date range: 1994-01-01 to 1994-12-31 (inclusive end for INTERVAL '1' YEAR means < 1995-01-01)
    const int32_t date_low = date_to_epoch_days(1994, 1, 1);   // 8766
    const int32_t date_high = date_to_epoch_days(1995, 1, 1);  // 9131

    // Discount range: BETWEEN 0.05 AND 0.07 (scaled by 100)
    const int64_t discount_low = 5;
    const int64_t discount_high = 7;

    // Quantity threshold: < 24 (scaled by 100)
    const int64_t quantity_threshold = 2400;

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load lineitem columns
    size_t num_rows = 0;
    const int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", expected_rows, num_rows);
    const int64_t* l_discount = mmap_column<int64_t>(gendb_dir + "/lineitem/l_discount.bin", num_rows, num_rows);
    const int64_t* l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", num_rows, num_rows);
    const int64_t* l_extendedprice = mmap_column<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", num_rows, num_rows);

    if (!l_shipdate || !l_discount || !l_quantity || !l_extendedprice) {
        std::cerr << "Failed to load lineitem columns" << std::endl;
        return;
    }

    // Load zone map
    std::string zonemap_path = gendb_dir + "/indexes/lineitem_shipdate_zone.bin";
    int zm_fd = open(zonemap_path.c_str(), O_RDONLY);
    if (zm_fd < 0) {
        std::cerr << "Warning: zone map not found, proceeding with full scan" << std::endl;
    }

    struct stat zm_st;
    const ZoneMapEntry* zones = nullptr;
    size_t num_zones = 0;

    if (zm_fd >= 0) {
        fstat(zm_fd, &zm_st);
        // Skip uint32_t header (num_entries)
        size_t header_size = sizeof(uint32_t);
        size_t data_size = zm_st.st_size - header_size;
        num_zones = data_size / sizeof(ZoneMapEntry);

        void* zm_addr = mmap(nullptr, zm_st.st_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        close(zm_fd);

        if (zm_addr != MAP_FAILED) {
            zones = reinterpret_cast<const ZoneMapEntry*>(static_cast<char*>(zm_addr) + header_size);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // Determine scan ranges using zone map
    std::vector<std::pair<size_t, size_t>> scan_ranges;

    #ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
    #endif

    if (zones && num_zones > 0) {
        // Zone map pruning
        for (size_t z = 0; z < num_zones; z++) {
            // Skip blocks entirely outside the date range
            if (zones[z].max_val < date_low || zones[z].min_val >= date_high) {
                continue;
            }

            size_t start_row = z * block_size;
            size_t end_row = std::min(start_row + block_size, num_rows);
            scan_ranges.push_back({start_row, end_row});
        }
    } else {
        // No zone map: full scan
        scan_ranges.push_back({0, num_rows});
    }

    #ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] zone_map_pruning: %.2f ms\n", zonemap_ms);
    #endif

    // Parallel scan with thread-local aggregation
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_revenue(num_threads, 0);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int64_t local_revenue = 0;

        #pragma omp for schedule(dynamic, 1)
        for (size_t r = 0; r < scan_ranges.size(); r++) {
            size_t start = scan_ranges[r].first;
            size_t end = scan_ranges[r].second;

            for (size_t i = start; i < end; i++) {
                // Predicate evaluation (ordered by selectivity)
                if (l_shipdate[i] >= date_low && l_shipdate[i] < date_high) {
                    int64_t disc = l_discount[i];
                    if (disc >= discount_low && disc <= discount_high) {
                        if (l_quantity[i] < quantity_threshold) {
                            // Accumulate revenue (both values are scaled, result is scaled by 100*100)
                            local_revenue += l_extendedprice[i] * disc;
                        }
                    }
                }
            }
        }

        thread_revenue[tid] = local_revenue;
    }

    // Merge thread-local results
    int64_t total_revenue = 0;
    for (int t = 0; t < num_threads; t++) {
        total_revenue += thread_revenue[t];
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    // Write output
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    if (!out) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    // Header
    out << "revenue\n";

    // Scale down result: extendedprice is scaled by 100, discount is scaled by 100
    // Product is scaled by 10000
    double revenue_value = static_cast<double>(total_revenue) / 10000.0;
    out << std::fixed << std::setprecision(2) << revenue_value << "\n";

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
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
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
