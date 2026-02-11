#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <thread>
#include <atomic>

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

    size_t n = l_shipdate.size;

    // Date range: '1994-01-01' to '1994-12-31'
    int32_t date_start = date_to_days("1994-01-01");
    int32_t date_end = date_to_days("1995-01-01");  // Exclusive upper bound

    // Discount range: 0.05 to 0.07
    double discount_min = 0.05;
    double discount_max = 0.07;

    // Quantity threshold
    double quantity_max = 24.0;

    // Parallel aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<double> local_revenues(num_threads, 0.0);

    size_t chunk_size = (n + num_threads - 1) / num_threads;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            size_t start_idx = tid * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, n);

            double local_revenue = 0.0;

#ifdef __AVX2__
            // SIMD-accelerated filter evaluation using AVX2 (process 4 doubles at a time)
            size_t i = start_idx;
            size_t vec_end = start_idx + ((end_idx - start_idx) / 4) * 4;  // Round down to multiple of 4

            // SIMD loop: process 4 rows at a time
            __m256d revenue_acc = _mm256_setzero_pd();  // Accumulator for 4 partial revenues
            for (; i < vec_end; i += 4) {
                // Load 4 shipdates (int32_t) - need to handle int32 comparison
                // For simplicity, process one at a time for int32 comparisons, then vectorize doubles
                // Check if all 4 rows pass the shipdate filter
                bool date_ok[4];
                for (int j = 0; j < 4; ++j) {
                    date_ok[j] = (l_shipdate.data[i+j] >= date_start && l_shipdate.data[i+j] < date_end);
                }

                // Load 4 discounts
                __m256d disc_vec = _mm256_loadu_pd(&l_discount.data[i]);

                // Load 4 quantities
                __m256d qty_vec = _mm256_loadu_pd(&l_quantity.data[i]);

                // Load 4 extended prices
                __m256d price_vec = _mm256_loadu_pd(&l_extendedprice.data[i]);

                // Create SIMD constants
                __m256d disc_min_vec = _mm256_set1_pd(discount_min);
                __m256d disc_max_vec = _mm256_set1_pd(discount_max);
                __m256d qty_max_vec = _mm256_set1_pd(quantity_max);

                // SIMD comparisons (returns all 1s if true, all 0s if false)
                __m256d disc_ge_min = _mm256_cmp_pd(disc_vec, disc_min_vec, _CMP_GE_OQ);  // disc >= 0.05
                __m256d disc_le_max = _mm256_cmp_pd(disc_vec, disc_max_vec, _CMP_LE_OQ);  // disc <= 0.07
                __m256d qty_lt_max = _mm256_cmp_pd(qty_vec, qty_max_vec, _CMP_LT_OQ);     // qty < 24

                // Combine all filter conditions with AND
                __m256d filter_mask = _mm256_and_pd(disc_ge_min, disc_le_max);
                filter_mask = _mm256_and_pd(filter_mask, qty_lt_max);

                // Apply date filter (scalar checks) to mask
                for (int j = 0; j < 4; ++j) {
                    if (!date_ok[j]) {
                        // Zero out the j-th lane in the mask
                        double mask_arr[4];
                        _mm256_storeu_pd(mask_arr, filter_mask);
                        mask_arr[j] = 0.0;
                        filter_mask = _mm256_loadu_pd(mask_arr);
                    }
                }

                // Compute revenue = price * disc, but only for rows that pass filter
                __m256d revenue_vec = _mm256_mul_pd(price_vec, disc_vec);

                // Mask out revenues for filtered-out rows (AND with filter_mask)
                revenue_vec = _mm256_and_pd(revenue_vec, filter_mask);

                // Accumulate revenues
                revenue_acc = _mm256_add_pd(revenue_acc, revenue_vec);
            }

            // Horizontal sum of the 4 accumulated revenues
            double revenue_arr[4];
            _mm256_storeu_pd(revenue_arr, revenue_acc);
            local_revenue = revenue_arr[0] + revenue_arr[1] + revenue_arr[2] + revenue_arr[3];

            // Scalar tail: process remaining rows (< 4)
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
