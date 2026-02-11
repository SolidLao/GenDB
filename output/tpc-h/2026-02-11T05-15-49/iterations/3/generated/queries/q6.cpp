#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <thread>
#include <atomic>
#include <sys/mman.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace gendb {

void execute_q6(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "\n=== Q6: Forecasting Revenue Change ===" << std::endl;

    // Load required columns via mmap
    auto l_shipdate = mmap_int32_column(gendb_dir, "lineitem", "l_shipdate");
    auto l_discount = mmap_double_column(gendb_dir, "lineitem", "l_discount");
    auto l_quantity = mmap_double_column(gendb_dir, "lineitem", "l_quantity");
    auto l_extendedprice = mmap_double_column(gendb_dir, "lineitem", "l_extendedprice");

    // HDD optimization: Use MADV_SEQUENTIAL for sequential scan pattern
    madvise(l_shipdate.mmap_ptr, l_shipdate.mmap_size, MADV_SEQUENTIAL);
    madvise(l_discount.mmap_ptr, l_discount.mmap_size, MADV_SEQUENTIAL);
    madvise(l_quantity.mmap_ptr, l_quantity.mmap_size, MADV_SEQUENTIAL);
    madvise(l_extendedprice.mmap_ptr, l_extendedprice.mmap_size, MADV_SEQUENTIAL);

    size_t n = l_shipdate.size;

    // Date range: '1994-01-01' to '1994-12-31'
    int32_t date_start = date_to_days("1994-01-01");
    int32_t date_end = date_to_days("1995-01-01");  // Exclusive upper bound

    // CRITICAL OPTIMIZATION: Load zone map for block skipping (Priority 1)
    // Zone maps on l_shipdate enable skipping 86% of blocks (1 year out of 7)
    // Expected 2-2.5x speedup by reducing effective I/O from 60M to ~8.4M rows
    auto zone_map = load_zone_map(gendb_dir, "lineitem", "l_shipdate");

    // Build list of blocks to scan (only blocks that overlap [date_start, date_end))
    std::vector<std::pair<size_t, size_t>> blocks_to_scan;  // (start_idx, end_idx) pairs

    if (zone_map.block_granularity > 0 && !zone_map.blocks.empty()) {
        // Zone map available - use it for block skipping
        size_t total_rows_to_scan = 0;
        for (const auto& block : zone_map.blocks) {
            if (zone_overlaps(block.min_value, block.max_value, date_start, date_end)) {
                // Block overlaps query range - add to scan list
                blocks_to_scan.emplace_back(block.block_start, block.block_start + block.block_size);
                total_rows_to_scan += block.block_size;
            }
        }
        std::cout << "  Zone map pruning: scanning " << blocks_to_scan.size()
                  << " of " << zone_map.blocks.size() << " blocks ("
                  << (100.0 * blocks_to_scan.size() / zone_map.blocks.size()) << "%), "
                  << total_rows_to_scan << " rows" << std::endl;
    } else {
        // No zone map - scan entire table
        blocks_to_scan.emplace_back(0, n);
    }

    // Discount range: 0.05 to 0.07
    double discount_min = 0.05;
    double discount_max = 0.07;

    // Quantity threshold
    double quantity_max = 24.0;

    // Parallel aggregation over blocks_to_scan
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<double> local_revenues(num_threads, 0.0);

    // Distribute blocks across threads
    std::atomic<size_t> block_idx_counter(0);

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            double local_revenue = 0.0;

            // Each thread processes blocks dynamically
            while (true) {
                size_t block_idx = block_idx_counter.fetch_add(1);
                if (block_idx >= blocks_to_scan.size()) break;

                size_t start_idx = blocks_to_scan[block_idx].first;
                size_t end_idx = blocks_to_scan[block_idx].second;

#ifdef __AVX2_DISABLED_FOR_DEBUG__
            // SIMD-accelerated filter evaluation using AVX2 (process 8 rows at a time)
            // Fully vectorize date comparisons using AVX2 integer SIMD
            size_t i = start_idx;
            size_t vec_end = start_idx + ((end_idx - start_idx) / 8) * 8;  // Round down to multiple of 8

            __m256d revenue_acc = _mm256_setzero_pd();  // Accumulator for 4 partial revenues

            // SIMD constants for date comparisons (int32)
            __m256i date_start_vec = _mm256_set1_epi32(date_start);
            __m256i date_end_vec = _mm256_set1_epi32(date_end);

            // SIMD constants for double comparisons
            __m256d disc_min_vec = _mm256_set1_pd(discount_min);
            __m256d disc_max_vec = _mm256_set1_pd(discount_max);
            __m256d qty_max_vec = _mm256_set1_pd(quantity_max);

            for (; i < vec_end; i += 8) {
                // Use 8 int32 comparisons in 256-bit registers for date filtering
                __m256i dates_8 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&l_shipdate.data[i]));
                __m256i date_ge_start_8 = _mm256_or_si256(
                    _mm256_cmpgt_epi32(dates_8, _mm256_sub_epi32(date_start_vec, _mm256_set1_epi32(1))),
                    _mm256_cmpeq_epi32(dates_8, date_start_vec)
                );
                __m256i date_lt_end_8 = _mm256_cmpgt_epi32(date_end_vec, dates_8);
                __m256i date_mask_8 = _mm256_and_si256(date_ge_start_8, date_lt_end_8);

                // Extract masks for first and second halves
                int date_mask_all = _mm256_movemask_ps(_mm256_castsi256_ps(date_mask_8));

                // ===== Process first 4 rows =====
                __m256d disc_vec_lo = _mm256_loadu_pd(&l_discount.data[i]);
                __m256d qty_vec_lo = _mm256_loadu_pd(&l_quantity.data[i]);
                __m256d price_vec_lo = _mm256_loadu_pd(&l_extendedprice.data[i]);

                // SIMD comparisons on doubles
                __m256d disc_ge_min_lo = _mm256_cmp_pd(disc_vec_lo, disc_min_vec, _CMP_GE_OQ);
                __m256d disc_le_max_lo = _mm256_cmp_pd(disc_vec_lo, disc_max_vec, _CMP_LE_OQ);
                __m256d qty_lt_max_lo = _mm256_cmp_pd(qty_vec_lo, qty_max_vec, _CMP_LT_OQ);

                // Combine double filters
                __m256d double_mask_lo = _mm256_and_pd(disc_ge_min_lo, disc_le_max_lo);
                double_mask_lo = _mm256_and_pd(double_mask_lo, qty_lt_max_lo);

                // Convert int32 date mask to double mask
                alignas(32) double date_mask_dbl_lo[4];
                date_mask_dbl_lo[0] = (date_mask_all & (1 << 0)) ? -1.0 : 0.0;
                date_mask_dbl_lo[1] = (date_mask_all & (1 << 1)) ? -1.0 : 0.0;
                date_mask_dbl_lo[2] = (date_mask_all & (1 << 2)) ? -1.0 : 0.0;
                date_mask_dbl_lo[3] = (date_mask_all & (1 << 3)) ? -1.0 : 0.0;
                __m256d date_mask_dbl_lo_vec = _mm256_load_pd(date_mask_dbl_lo);

                // Combine all filters
                __m256d final_mask_lo = _mm256_and_pd(double_mask_lo, date_mask_dbl_lo_vec);

                // Compute revenue = price * disc
                __m256d revenue_vec_lo = _mm256_mul_pd(price_vec_lo, disc_vec_lo);

                // Use blendv to select revenues (blendv uses sign bit of mask: negative = select a, positive/zero = select b)
                __m256d zero_vec = _mm256_setzero_pd();
                revenue_vec_lo = _mm256_blendv_pd(zero_vec, revenue_vec_lo, final_mask_lo);
                revenue_acc = _mm256_add_pd(revenue_acc, revenue_vec_lo);

                // ===== Process second 4 rows (i+4 to i+7) =====
                __m256d disc_vec_hi = _mm256_loadu_pd(&l_discount.data[i+4]);
                __m256d qty_vec_hi = _mm256_loadu_pd(&l_quantity.data[i+4]);
                __m256d price_vec_hi = _mm256_loadu_pd(&l_extendedprice.data[i+4]);

                __m256d disc_ge_min_hi = _mm256_cmp_pd(disc_vec_hi, disc_min_vec, _CMP_GE_OQ);
                __m256d disc_le_max_hi = _mm256_cmp_pd(disc_vec_hi, disc_max_vec, _CMP_LE_OQ);
                __m256d qty_lt_max_hi = _mm256_cmp_pd(qty_vec_hi, qty_max_vec, _CMP_LT_OQ);

                __m256d double_mask_hi = _mm256_and_pd(disc_ge_min_hi, disc_le_max_hi);
                double_mask_hi = _mm256_and_pd(double_mask_hi, qty_lt_max_hi);

                alignas(32) double date_mask_dbl_hi[4];
                date_mask_dbl_hi[0] = (date_mask_all & (1 << 4)) ? -1.0 : 0.0;
                date_mask_dbl_hi[1] = (date_mask_all & (1 << 5)) ? -1.0 : 0.0;
                date_mask_dbl_hi[2] = (date_mask_all & (1 << 6)) ? -1.0 : 0.0;
                date_mask_dbl_hi[3] = (date_mask_all & (1 << 7)) ? -1.0 : 0.0;
                __m256d date_mask_dbl_hi_vec = _mm256_load_pd(date_mask_dbl_hi);

                __m256d final_mask_hi = _mm256_and_pd(double_mask_hi, date_mask_dbl_hi_vec);

                __m256d revenue_vec_hi = _mm256_mul_pd(price_vec_hi, disc_vec_hi);
                revenue_vec_hi = _mm256_blendv_pd(zero_vec, revenue_vec_hi, final_mask_hi);
                revenue_acc = _mm256_add_pd(revenue_acc, revenue_vec_hi);
            }

            // Horizontal sum of the 4 accumulated revenues
            alignas(32) double revenue_arr[4];
            _mm256_store_pd(revenue_arr, revenue_acc);
            local_revenue = revenue_arr[0] + revenue_arr[1] + revenue_arr[2] + revenue_arr[3];

            // Scalar tail: process remaining rows (< 8)
            for (; i < end_idx; ++i) {
                if (l_shipdate.data[i] < date_start || l_shipdate.data[i] >= date_end) continue;
                double disc = l_discount.data[i];
                if (disc < discount_min || disc > discount_max) continue;
                if (l_quantity.data[i] >= quantity_max) continue;
                local_revenue += l_extendedprice.data[i] * disc;
            }
#else
            // Scalar fallback (no AVX2 support)
            for (size_t i = start_idx; i < end_idx; ++i) {
                // Filter: l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
                if (l_shipdate.data[i] < date_start || l_shipdate.data[i] >= date_end) continue;

                // Filter: l_discount BETWEEN 0.05 AND 0.07
                double disc = l_discount.data[i];
                if (disc < discount_min || disc > discount_max) continue;

                // Filter: l_quantity < 24
                if (l_quantity.data[i] >= quantity_max) continue;

                // Compute revenue
                local_revenue += l_extendedprice.data[i] * disc;
            }
#endif
            }  // End while loop for block processing

            local_revenues[tid] = local_revenue;
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Sum up thread-local revenues
    double total_revenue = 0.0;
    for (double rev : local_revenues) {
        total_revenue += rev;
    }

    // Print result
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nREVENUE" << std::endl;
    std::cout << std::string(15, '-') << std::endl;
    std::cout << total_revenue << std::endl;

    // Cleanup
    unmap_column(l_shipdate.mmap_ptr, l_shipdate.mmap_size);
    unmap_column(l_discount.mmap_ptr, l_discount.mmap_size);
    unmap_column(l_quantity.mmap_ptr, l_quantity.mmap_size);
    unmap_column(l_extendedprice.mmap_ptr, l_extendedprice.mmap_size);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "\nQ6 execution time: " << elapsed.count() << " seconds" << std::endl;
}

}  // namespace gendb
