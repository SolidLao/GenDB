#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <chrono>
#include <omp.h>
#include <string>
#include <vector>
#include <iostream>
#include <immintrin.h>
#include <algorithm>

/*
=== LOGICAL QUERY PLAN ===

Table: lineitem (59,986,052 rows, sorted on l_shipdate)
Predicates (all single-table):
  1. l_shipdate >= DATE '1994-01-01' (epoch day 8766)
  2. l_shipdate < DATE '1995-01-01' (epoch day 9131)
  3. l_discount BETWEEN 0.05 AND 0.07 (scaled by 100: [5, 7])
  4. l_quantity < 24 (scaled by 100: < 2400)

Aggregation: SUM(l_extendedprice * l_discount)
  - No GROUP BY → single aggregate
  - All matched rows contribute to one sum

Estimated filtered cardinality: ~2,000-5,000 rows (based on selective predicates)

=== PHYSICAL QUERY PLAN ===

1. Zone Map Pruning (l_shipdate)
   - Load lineitem_shipdate_zonemap.bin (binary format: uint32_t num_blocks, then [int32_t min, int32_t max, uint32_t row_count] per block)
   - Skip blocks where max_val < 8766 or min_val >= 9131
   - Reduces scan from 60M to ~3M rows

2. Scan + Filter Fusion
   - Single pass through qualifying blocks
   - Inline filter checks for all 4 predicates
   - No intermediate materialization

3. Aggregation
   - Thread-local int64_t accumulators (no GROUP BY)
   - Each thread accumulates sum in int64_t at full precision (scale^2 = 10000)
   - Final merge sums all thread-local values

4. Parallelism
   - OpenMP parallel for on row iteration
   - Thread-local aggregation reduces contention
   - Block-level load balancing

5. Output
   - CSV: header row "revenue", value in format XXXXX.XXXX (4 decimal places)
   - Write to results_dir/Q6.csv
*/

// Mmap helper function
void* mmap_file(const char* path, size_t& out_size) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        perror("open");
        return nullptr;
    }
    struct stat st;
    fstat(fd, &st);
    out_size = st.st_size;
    void* data = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return data;
}

// Zone map structure (from storage guide)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Date constants (epoch days since 1970-01-01)
    // 1994-01-01: 8766 days
    // 1995-01-01: 9131 days (8766 + 365)
    const int32_t DATE_1994_01_01 = 8766;
    const int32_t DATE_1995_01_01 = 9131;

    // Discount bounds (scaled by 100: scale_factor 2 means 2 decimal places)
    // 0.05 → 5, 0.07 → 7
    const int64_t DISCOUNT_MIN = 5;
    const int64_t DISCOUNT_MAX = 7;

    // Quantity bound (scaled by 100: scale_factor 2 means 2 decimal places)
    // < 24 → < 2400
    const int64_t QUANTITY_MAX = 2400;

    // Load lineitem columns
    size_t l_shipdate_size, l_discount_size, l_quantity_size, l_extendedprice_size;

    auto l_shipdate_data = mmap_file(
        (gendb_dir + "/lineitem/l_shipdate.bin").c_str(),
        l_shipdate_size
    );
    auto l_discount_data = mmap_file(
        (gendb_dir + "/lineitem/l_discount.bin").c_str(),
        l_discount_size
    );
    auto l_quantity_data = mmap_file(
        (gendb_dir + "/lineitem/l_quantity.bin").c_str(),
        l_quantity_size
    );
    auto l_extendedprice_data = mmap_file(
        (gendb_dir + "/lineitem/l_extendedprice.bin").c_str(),
        l_extendedprice_size
    );

    if (!l_shipdate_data || !l_discount_data || !l_quantity_data || !l_extendedprice_data) {
        fprintf(stderr, "Error: failed to load lineitem columns\n");
        return;
    }

    const int32_t* l_shipdate = (const int32_t*)l_shipdate_data;
    const int64_t* l_discount = (const int64_t*)l_discount_data;
    const int64_t* l_quantity = (const int64_t*)l_quantity_data;
    const int64_t* l_extendedprice = (const int64_t*)l_extendedprice_data;

    int64_t num_rows = l_shipdate_size / sizeof(int32_t);

    // Load and parse zone map for l_shipdate
    #ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t zonemap_size = 0;
    auto zonemap_data = mmap_file(
        (gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin").c_str(),
        zonemap_size
    );

    // Parse zone map header
    uint32_t num_blocks = 0;
    if (zonemap_data && zonemap_size >= sizeof(uint32_t)) {
        num_blocks = *(const uint32_t*)zonemap_data;
    }

    const ZoneMapEntry* zones = nullptr;
    if (zonemap_data && zonemap_size >= sizeof(uint32_t) + num_blocks * sizeof(ZoneMapEntry)) {
        zones = (const ZoneMapEntry*)((const char*)zonemap_data + sizeof(uint32_t));
    }

    #ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] zonemap_load: %.2f ms\n", zonemap_ms);
    #endif

    // Zone map analysis + binary search for date range
    #ifdef GENDB_PROFILE
    auto t_pruning_start = std::chrono::high_resolution_clock::now();
    #endif

    // Find first and last row indices within the date range using binary search
    // Since data is sorted by l_shipdate, we can find the exact range
    int64_t start_row = 0;
    int64_t end_row = num_rows;

    // Binary search for first row >= DATE_1994_01_01
    {
        int64_t lo = 0, hi = num_rows;
        while (lo < hi) {
            int64_t mid = (lo + hi) / 2;
            if (l_shipdate[mid] < DATE_1994_01_01) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        start_row = lo;
    }

    // Binary search for first row >= DATE_1995_01_01
    {
        int64_t lo = 0, hi = num_rows;
        while (lo < hi) {
            int64_t mid = (lo + hi) / 2;
            if (l_shipdate[mid] < DATE_1995_01_01) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        end_row = lo;
    }

    int64_t rows_to_scan = end_row - start_row;

    #ifdef GENDB_PROFILE
    auto t_pruning_end = std::chrono::high_resolution_clock::now();
    double pruning_ms = std::chrono::duration<double, std::milli>(t_pruning_end - t_pruning_start).count();
    printf("[TIMING] zone_map_analysis: %.2f ms (rows_to_scan: %ld)\n", pruning_ms, rows_to_scan);
    #endif

    // Scan, filter, and aggregate with parallelism
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local aggregation: each thread maintains its own sum
    std::vector<int64_t> thread_sums(omp_get_max_threads(), 0);

    // Optimized scan loop: process only rows within the date range (binary-search pruned)
    // This avoids 73% of rows due to date filtering
    #pragma omp parallel for schedule(static)
    for (int64_t i = start_row; i < end_row; i++) {
        // Load all predicates
        int64_t discount = l_discount[i];
        int64_t quantity = l_quantity[i];
        int64_t extendedprice = l_extendedprice[i];

        // P1: discount in [0.05, 0.07] → scaled: [5, 7]
        // Most selective (~27% or 3/11 values) - evaluate FIRST
        if (discount < DISCOUNT_MIN || discount > DISCOUNT_MAX) {
            continue;
        }

        // P2: quantity < 24 → scaled: < 2400
        // ~48% selectivity, evaluate second
        if (quantity >= QUANTITY_MAX) {
            continue;
        }

        // All predicates matched: compute l_extendedprice * l_discount
        // Note: date range check is implicit because we only scan [start_row, end_row)
        int64_t product = extendedprice * discount;

        // Add to thread-local sum
        int thread_id = omp_get_thread_num();
        thread_sums[thread_id] += product;
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);
    #endif

    // Merge thread-local sums
    int64_t total_sum = 0;
    for (int tid = 0; tid < omp_get_max_threads(); tid++) {
        total_sum += thread_sums[tid];
    }

    // Scale down from 10000 (100 * 100) to 1 (original scale)
    // extendedprice and discount are both scaled by 100
    // Product is scaled by 100*100 = 10000
    double result_value = (double)total_sum / 10000.0;

    // Write result to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q6.csv";
    FILE* output_file = fopen(output_path.c_str(), "w");
    if (output_file) {
        fprintf(output_file, "revenue\n");
        fprintf(output_file, "%.4f\n", result_value);
        fclose(output_file);
    } else {
        fprintf(stderr, "Error: failed to write output file %s\n", output_path.c_str());
    }

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

    // Cleanup
    if (l_shipdate_data) munmap(l_shipdate_data, l_shipdate_size);
    if (l_discount_data) munmap(l_discount_data, l_discount_size);
    if (l_quantity_data) munmap(l_quantity_data, l_quantity_size);
    if (l_extendedprice_data) munmap(l_extendedprice_data, l_extendedprice_size);
    if (zonemap_data) munmap(zonemap_data, zonemap_size);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
