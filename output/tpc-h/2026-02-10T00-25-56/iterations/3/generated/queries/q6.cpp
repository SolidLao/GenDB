#include "queries.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <immintrin.h>  // AVX2 intrinsics

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= DATE '1994-01-01'
//   AND l_shipdate < DATE '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24;

void execute_q6(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date range: [1994-01-01, 1995-01-01)
    int32_t start_date = date_utils::parse_date("1994-01-01");
    int32_t end_date = date_utils::parse_date("1995-01-01");

    // Discount range: [0.05, 0.07]
    double min_discount = 0.05;
    double max_discount = 0.07;

    // Quantity threshold
    double max_quantity = 24.0;

    // SIMD-vectorized scan and aggregate
    double revenue = 0.0;
    size_t qualifying_rows = 0;
    size_t n = lineitem.size();

#ifdef __AVX2__
    // Full SIMD processing: process 4 doubles at a time for all operations
    // Using AVX2 for double precision (4-wide) and int32 (8-wide)

    // Date filtering: process 8 int32 dates at a time
    __m256i start_date_vec = _mm256_set1_epi32(start_date);
    __m256i end_date_vec = _mm256_set1_epi32(end_date);

    // Double filters: process 4 doubles at a time
    __m256d min_discount_vec = _mm256_set1_pd(min_discount);
    __m256d max_discount_vec = _mm256_set1_pd(max_discount);
    __m256d max_quantity_vec = _mm256_set1_pd(max_quantity);

    // Accumulator for revenue
    __m256d revenue_vec = _mm256_setzero_pd();

    size_t i = 0;

    // Main SIMD loop: process 8 rows at a time (limited by date filtering)
    // We process 8 dates but only 4 doubles at a time, so we do two passes
    constexpr size_t SIMD_WIDTH = 8;

    for (; i + SIMD_WIDTH <= n; i += SIMD_WIDTH) {
        // Load 8 shipdate values (int32)
        __m256i shipdate = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&lineitem.l_shipdate[i]));

        // Date range filter: shipdate >= start_date AND shipdate < end_date
        __m256i date_ge = _mm256_cmpgt_epi32(shipdate, _mm256_sub_epi32(start_date_vec, _mm256_set1_epi32(1)));
        __m256i date_lt = _mm256_cmpgt_epi32(end_date_vec, shipdate);
        __m256i date_pass = _mm256_and_si256(date_ge, date_lt);

        // Extract mask (each bit represents one int32)
        int date_mask = _mm256_movemask_ps(_mm256_castsi256_ps(date_pass));

        // If no dates pass, skip to next batch
        if (date_mask == 0) {
            continue;
        }

        // Process in two chunks of 4 for double operations
        // First chunk: indices i to i+3
        if (date_mask & 0x0F) {  // Check if any of first 4 passed
            // Load 4 discount values
            __m256d discount = _mm256_loadu_pd(&lineitem.l_discount[i]);

            // Load 4 quantity values
            __m256d quantity = _mm256_loadu_pd(&lineitem.l_quantity[i]);

            // Discount filter: discount >= min_discount AND discount <= max_discount
            __m256d disc_ge = _mm256_cmp_pd(discount, min_discount_vec, _CMP_GE_OQ);
            __m256d disc_le = _mm256_cmp_pd(discount, max_discount_vec, _CMP_LE_OQ);
            __m256d disc_pass = _mm256_and_pd(disc_ge, disc_le);

            // Quantity filter: quantity < max_quantity
            __m256d qty_pass = _mm256_cmp_pd(quantity, max_quantity_vec, _CMP_LT_OQ);

            // Combine all filters for this chunk
            __m256d all_pass = _mm256_and_pd(disc_pass, qty_pass);

            // Need to combine with date mask for first 4 elements
            // Convert date_mask bits to double mask
            int chunk_date_mask = date_mask & 0x0F;
            double date_masks[4];
            for (int j = 0; j < 4; j++) {
                date_masks[j] = (chunk_date_mask & (1 << j)) ? -1.0 : 0.0;
            }
            __m256d date_pass_dbl = _mm256_loadu_pd(date_masks);
            all_pass = _mm256_and_pd(all_pass, date_pass_dbl);

            // Load extendedprice and compute revenue for passing rows
            __m256d extendedprice = _mm256_loadu_pd(&lineitem.l_extendedprice[i]);

            // revenue = extendedprice * discount
            __m256d chunk_revenue = _mm256_mul_pd(extendedprice, discount);

            // Use blendv to conditionally select revenue (0.0 for non-passing, chunk_revenue for passing)
            // blendv uses sign bit: all 1s (negative) selects second arg, all 0s selects first arg
            chunk_revenue = _mm256_blendv_pd(_mm256_setzero_pd(), chunk_revenue, all_pass);

            // Accumulate to revenue vector
            revenue_vec = _mm256_add_pd(revenue_vec, chunk_revenue);

            // Count qualifying rows
            int mask_int = _mm256_movemask_pd(all_pass);
            qualifying_rows += __builtin_popcount(mask_int);
        }

        // Second chunk: indices i+4 to i+7
        if (date_mask & 0xF0) {  // Check if any of second 4 passed
            size_t idx = i + 4;

            // Load 4 discount values
            __m256d discount = _mm256_loadu_pd(&lineitem.l_discount[idx]);

            // Load 4 quantity values
            __m256d quantity = _mm256_loadu_pd(&lineitem.l_quantity[idx]);

            // Discount filter
            __m256d disc_ge = _mm256_cmp_pd(discount, min_discount_vec, _CMP_GE_OQ);
            __m256d disc_le = _mm256_cmp_pd(discount, max_discount_vec, _CMP_LE_OQ);
            __m256d disc_pass = _mm256_and_pd(disc_ge, disc_le);

            // Quantity filter
            __m256d qty_pass = _mm256_cmp_pd(quantity, max_quantity_vec, _CMP_LT_OQ);

            // Combine all filters
            __m256d all_pass = _mm256_and_pd(disc_pass, qty_pass);

            // Combine with date mask for second 4 elements
            int chunk_date_mask = (date_mask >> 4) & 0x0F;
            double date_masks[4];
            for (int j = 0; j < 4; j++) {
                date_masks[j] = (chunk_date_mask & (1 << j)) ? -1.0 : 0.0;
            }
            __m256d date_pass_dbl = _mm256_loadu_pd(date_masks);
            all_pass = _mm256_and_pd(all_pass, date_pass_dbl);

            // Load extendedprice and compute revenue
            __m256d extendedprice = _mm256_loadu_pd(&lineitem.l_extendedprice[idx]);

            // revenue = extendedprice * discount
            __m256d chunk_revenue = _mm256_mul_pd(extendedprice, discount);

            // Use blendv to conditionally select revenue (0.0 for non-passing, chunk_revenue for passing)
            chunk_revenue = _mm256_blendv_pd(_mm256_setzero_pd(), chunk_revenue, all_pass);

            // Accumulate
            revenue_vec = _mm256_add_pd(revenue_vec, chunk_revenue);

            // Count qualifying rows
            int mask_int = _mm256_movemask_pd(all_pass);
            qualifying_rows += __builtin_popcount(mask_int);
        }
    }

    // Horizontal reduction of revenue_vec to get final sum
    // Extract high and low 128-bit halves
    __m128d low = _mm256_castpd256_pd128(revenue_vec);
    __m128d high = _mm256_extractf128_pd(revenue_vec, 1);
    __m128d sum128 = _mm_add_pd(low, high);

    // Horizontal add within 128-bit
    __m128d shuf = _mm_shuffle_pd(sum128, sum128, 1);
    sum128 = _mm_add_pd(sum128, shuf);

    revenue = _mm_cvtsd_f64(sum128);

    // Scalar tail: process remaining rows
    for (; i < n; i++) {
        if (lineitem.l_shipdate[i] >= start_date && lineitem.l_shipdate[i] < end_date &&
            lineitem.l_discount[i] >= min_discount && lineitem.l_discount[i] <= max_discount &&
            lineitem.l_quantity[i] < max_quantity) {
            revenue += lineitem.l_extendedprice[i] * lineitem.l_discount[i];
            qualifying_rows++;
        }
    }
#else
    // Fallback: scalar implementation
    for (size_t i = 0; i < n; i++) {
        // Apply filters (ordered by selectivity: date first, then discount, then quantity)
        if (lineitem.l_shipdate[i] < start_date || lineitem.l_shipdate[i] >= end_date) {
            continue;
        }

        if (lineitem.l_discount[i] < min_discount || lineitem.l_discount[i] > max_discount) {
            continue;
        }

        if (lineitem.l_quantity[i] >= max_quantity) {
            continue;
        }

        // Compute revenue
        revenue += lineitem.l_extendedprice[i] * lineitem.l_discount[i];
        qualifying_rows++;
    }
#endif

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q6: Forecasting Revenue Change ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Revenue: " << revenue << std::endl;
    std::cout << "Qualifying rows: " << qualifying_rows << " / " << n
              << " (" << (100.0 * qualifying_rows / n) << "%)" << std::endl;
    std::cout << "\nExecution time: " << duration.count() << " ms" << std::endl;
}
