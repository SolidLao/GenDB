#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"

#include <sys/mman.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>

// SIMD intrinsics for AVX-512
#ifdef __AVX512F__
#include <immintrin.h>
#endif

namespace gendb {
namespace queries {

// Check if AVX-512 is available at runtime
static bool has_avx512_q6() {
#ifdef __AVX512F__
    return true;
#else
    return false;
#endif
}

// SIMD-vectorized multi-predicate filtering for Q6
static inline double filter_and_aggregate_simd(
    const int32_t* l_shipdate,
    const double* l_discount,
    const double* l_quantity,
    const double* l_extendedprice,
    size_t start_row,
    size_t end_row,
    int32_t date_start,
    int32_t date_end,
    double disc_min,
    double disc_max,
    double qty_max
) {
#ifdef __AVX512F__
    // Process 8 doubles at a time
    const size_t simd_width = 8;
    __m512d sum_vec = _mm512_setzero_pd();

    // Broadcast filter constants for doubles
    __m512d disc_min_vec = _mm512_set1_pd(disc_min);
    __m512d disc_max_vec = _mm512_set1_pd(disc_max);
    __m512d qty_max_vec = _mm512_set1_pd(qty_max);

    size_t i = start_row;

    // Process full SIMD vectors
    for (; i + simd_width <= end_row; i += simd_width) {
        // Load 8 int32 shipdates using AVX2 and convert to AVX-512
        __m256i dates_256 = _mm256_loadu_si256((__m256i*)&l_shipdate[i]);
        __m512i dates_512 = _mm512_cvtepi32_epi64(dates_256);

        // Broadcast date bounds to 512-bit for comparison
        __m512i date_start_512 = _mm512_cvtepi32_epi64(_mm256_set1_epi32(date_start));
        __m512i date_end_512 = _mm512_cvtepi32_epi64(_mm256_set1_epi32(date_end));

        // Compare: date >= date_start AND date < date_end
        __mmask8 date_mask1 = _mm512_cmpge_epi64_mask(dates_512, date_start_512);
        __mmask8 date_mask2 = _mm512_cmplt_epi64_mask(dates_512, date_end_512);
        __mmask8 date_mask = date_mask1 & date_mask2;

        if (date_mask == 0) continue;

        // Load 8 doubles for discount, quantity, extendedprice
        __m512d discount = _mm512_loadu_pd(&l_discount[i]);
        __m512d quantity = _mm512_loadu_pd(&l_quantity[i]);
        __m512d price = _mm512_loadu_pd(&l_extendedprice[i]);

        // Compare: discount >= disc_min AND discount <= disc_max
        __mmask8 disc_mask1 = _mm512_cmp_pd_mask(discount, disc_min_vec, _CMP_GE_OQ);
        __mmask8 disc_mask2 = _mm512_cmp_pd_mask(discount, disc_max_vec, _CMP_LE_OQ);
        __mmask8 disc_mask = disc_mask1 & disc_mask2;

        // Compare: quantity < qty_max
        __mmask8 qty_mask = _mm512_cmp_pd_mask(quantity, qty_max_vec, _CMP_LT_OQ);

        // Combine all masks
        __mmask8 final_mask = date_mask & disc_mask & qty_mask;

        if (final_mask == 0) continue;

        // Compute revenue = price * discount (only for matching rows)
        __m512d revenue = _mm512_maskz_mul_pd(final_mask, price, discount);

        // Accumulate
        sum_vec = _mm512_add_pd(sum_vec, revenue);
    }

    // Horizontal sum of the vector
    double sum = _mm512_reduce_add_pd(sum_vec);

    // Handle tail with scalar code
    for (; i < end_row; ++i) {
        if (l_shipdate[i] >= date_start && l_shipdate[i] < date_end &&
            l_discount[i] >= disc_min && l_discount[i] <= disc_max &&
            l_quantity[i] < qty_max) {
            sum += l_extendedprice[i] * l_discount[i];
        }
    }

    return sum;
#else
    // Scalar fallback
    double sum = 0.0;
    for (size_t i = start_row; i < end_row; ++i) {
        if (l_shipdate[i] >= date_start && l_shipdate[i] < date_end &&
            l_discount[i] >= disc_min && l_discount[i] <= disc_max &&
            l_quantity[i] < qty_max) {
            sum += l_extendedprice[i] * l_discount[i];
        }
    }
    return sum;
#endif
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns
    size_t num_rows;
    int32_t* l_shipdate = storage::mmap_column<int32_t>(gendb_dir + "/lineitem_l_shipdate.bin", num_rows);
    double* l_discount = storage::mmap_column<double>(gendb_dir + "/lineitem_l_discount.bin", num_rows);
    double* l_quantity = storage::mmap_column<double>(gendb_dir + "/lineitem_l_quantity.bin", num_rows);
    double* l_extendedprice = storage::mmap_column<double>(gendb_dir + "/lineitem_l_extendedprice.bin", num_rows);

    // Filter predicates
    int32_t date_start = date_utils::date_to_days(1994, 1, 1);
    int32_t date_end = date_utils::date_to_days(1995, 1, 1);
    double disc_min = 0.05;
    double disc_max = 0.07;
    double qty_max = 24.0;

    // Load zone maps for multi-column pruning
    auto shipdate_zonemap = storage::read_zone_map(gendb_dir + "/lineitem_l_shipdate_zonemap.json");
    auto discount_zonemap = storage::read_zone_map(gendb_dir + "/lineitem_l_discount_zonemap.json");
    auto quantity_zonemap = storage::read_zone_map(gendb_dir + "/lineitem_l_quantity_zonemap.json");

    // Identify blocks to scan using zone map pruning (AND logic across all predicates)
    std::vector<storage::ZoneMapBlock> blocks_to_scan;

    if (!shipdate_zonemap.empty() && !discount_zonemap.empty() && !quantity_zonemap.empty()) {
        // Multi-column zone map pruning
        // Convert discount and quantity ranges to int32_t for comparison
        int32_t disc_min_int = static_cast<int32_t>(disc_min * 100);
        int32_t disc_max_int = static_cast<int32_t>(disc_max * 100);
        int32_t qty_max_int = static_cast<int32_t>(qty_max);

        for (size_t block_id = 0; block_id < shipdate_zonemap.size(); ++block_id) {
            const auto& shipdate_block = shipdate_zonemap[block_id];
            const auto& discount_block = discount_zonemap[block_id];
            const auto& quantity_block = quantity_zonemap[block_id];

            // Check if block passes all filters (zone ranges overlap with predicate ranges)
            bool shipdate_pass = !(shipdate_block.max_val < date_start || shipdate_block.min_val >= date_end);
            bool discount_pass = !(discount_block.max_val < disc_min_int || discount_block.min_val > disc_max_int);
            bool quantity_pass = !(quantity_block.min_val >= qty_max_int);

            if (shipdate_pass && discount_pass && quantity_pass) {
                blocks_to_scan.push_back(shipdate_block);
            }
        }

        // Use random access pattern for block skipping
        madvise(l_shipdate, num_rows * sizeof(int32_t), MADV_RANDOM);
        madvise(l_discount, num_rows * sizeof(double), MADV_RANDOM);
        madvise(l_quantity, num_rows * sizeof(double), MADV_RANDOM);
        madvise(l_extendedprice, num_rows * sizeof(double), MADV_RANDOM);
    } else {
        // No zone maps: scan entire table
        storage::ZoneMapBlock full_block;
        full_block.block_id = 0;
        full_block.start_row = 0;
        full_block.end_row = num_rows;
        blocks_to_scan.push_back(full_block);

        madvise(l_shipdate, num_rows * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise(l_discount, num_rows * sizeof(double), MADV_SEQUENTIAL);
        madvise(l_quantity, num_rows * sizeof(double), MADV_SEQUENTIAL);
        madvise(l_extendedprice, num_rows * sizeof(double), MADV_SEQUENTIAL);
    }

    // Parallel aggregation using thread-local sums
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<double> local_sums(num_threads, 0.0);
    std::atomic<size_t> block_counter(0);
    std::vector<std::thread> threads;

    bool use_simd = has_avx512_q6();

    for (unsigned int thread_id = 0; thread_id < num_threads; ++thread_id) {
        threads.emplace_back([&, thread_id]() {
            double local_sum = 0.0;

            while (true) {
                size_t block_idx = block_counter.fetch_add(1);
                if (block_idx >= blocks_to_scan.size()) break;

                const auto& block = blocks_to_scan[block_idx];

                if (use_simd) {
                    // Use SIMD-accelerated filtering
                    local_sum += filter_and_aggregate_simd(
                        l_shipdate, l_discount, l_quantity, l_extendedprice,
                        block.start_row, block.end_row,
                        date_start, date_end, disc_min, disc_max, qty_max
                    );
                } else {
                    // Scalar fallback
                    for (size_t i = block.start_row; i < block.end_row; ++i) {
                        if (l_shipdate[i] >= date_start && l_shipdate[i] < date_end &&
                            l_discount[i] >= disc_min && l_discount[i] <= disc_max &&
                            l_quantity[i] < qty_max) {
                            local_sum += l_extendedprice[i] * l_discount[i];
                        }
                    }
                }
            }

            local_sums[thread_id] = local_sum;
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Sum up local results
    double revenue = 0.0;
    for (double s : local_sums) {
        revenue += s;
    }

    // Cleanup mmap
    munmap(l_shipdate, num_rows * sizeof(int32_t));
    munmap(l_discount, num_rows * sizeof(double));
    munmap(l_quantity, num_rows * sizeof(double));
    munmap(l_extendedprice, num_rows * sizeof(double));

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print results
    std::cout << "Q6: 1 row in " << std::fixed << std::setprecision(3) << elapsed << "s" << std::endl;

    // Write to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue << "\n";
    }
}

} // namespace queries
} // namespace gendb
