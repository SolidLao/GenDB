#include "queries.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace gendb {

void execute_q6(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date range: '1994-01-01' to '1994-12-31' (inclusive start, exclusive end in SQL)
    int32_t min_date = parse_date("1994-01-01");
    int32_t max_date = parse_date("1995-01-01");

    // Discount range: [0.05, 0.07]
    double min_discount = 0.05;
    double max_discount = 0.07;

    // Quantity threshold: < 24
    double max_quantity = 24.0;

    double revenue = 0.0;

    // Cache pointers for better performance
    const int32_t* shipdate = lineitem.l_shipdate.data();
    const double* price = lineitem.l_extendedprice.data();
    const double* discount = lineitem.l_discount.data();
    const double* quantity = lineitem.l_quantity.data();

    size_t n = lineitem.size();

    // Use zone maps to skip blocks
    const auto& zonemap = lineitem.shipdate_zonemap;
    size_t block_size = zonemap.block_size;
    size_t num_blocks = zonemap.block_min.size();

    // Check if AVX2 is available at runtime
    bool use_simd = false;
#ifdef __AVX2__
    use_simd = __builtin_cpu_supports("avx2");
#endif

    for (size_t block = 0; block < num_blocks; block++) {
        // Skip blocks that don't overlap with [min_date, max_date)
        if (zonemap.block_max[block] < min_date || zonemap.block_min[block] >= max_date) {
            continue;
        }

        size_t start = block * block_size;
        size_t end = std::min(start + block_size, n);

#ifdef __AVX2__
        if (use_simd && end - start >= 8) {
            // SIMD processing: handle 8 rows at a time
            __m256i min_date_vec = _mm256_set1_epi32(min_date);
            __m256i max_date_vec = _mm256_set1_epi32(max_date);
            __m256d min_disc_vec = _mm256_set1_pd(min_discount);
            __m256d max_disc_vec = _mm256_set1_pd(max_discount);
            __m256d max_qty_vec = _mm256_set1_pd(max_quantity);
            __m256d revenue_acc = _mm256_setzero_pd();

            size_t simd_end = start + ((end - start) / 8) * 8;

            for (size_t i = start; i < simd_end; i += 8) {
                // Load 8 dates
                __m256i dates = _mm256_loadu_si256((__m256i*)&shipdate[i]);

                // Check date range: date >= min_date && date < max_date
                __m256i date_ge = _mm256_cmpgt_epi32(dates, _mm256_sub_epi32(min_date_vec, _mm256_set1_epi32(1)));
                __m256i date_lt = _mm256_cmpgt_epi32(max_date_vec, dates);
                __m256i date_mask = _mm256_and_si256(date_ge, date_lt);

                // Process in two halves (4 doubles each) since AVX2 processes 4 doubles at a time
                for (int half = 0; half < 2; half++) {
                    size_t offset = i + half * 4;

                    // Extract date mask for this half
                    int date_mask_half = _mm256_movemask_epi8(date_mask) >> (half * 16);
                    if ((date_mask_half & 0xFFFF) == 0) continue;

                    // Load 4 discounts, quantities, prices
                    __m256d disc = _mm256_loadu_pd(&discount[offset]);
                    __m256d qty = _mm256_loadu_pd(&quantity[offset]);
                    __m256d prc = _mm256_loadu_pd(&price[offset]);

                    // Check discount range: disc >= min_discount && disc <= max_discount
                    __m256d disc_ge = _mm256_cmp_pd(disc, min_disc_vec, _CMP_GE_OQ);
                    __m256d disc_le = _mm256_cmp_pd(disc, max_disc_vec, _CMP_LE_OQ);
                    __m256d disc_mask = _mm256_and_pd(disc_ge, disc_le);

                    // Check quantity: qty < max_quantity
                    __m256d qty_mask = _mm256_cmp_pd(qty, max_qty_vec, _CMP_LT_OQ);

                    // Combine all masks
                    __m256d combined_mask = _mm256_and_pd(disc_mask, qty_mask);

                    // Apply mask to compute revenue
                    __m256d rev = _mm256_mul_pd(prc, disc);
                    rev = _mm256_and_pd(rev, combined_mask);  // Zero out rows that don't match
                    revenue_acc = _mm256_add_pd(revenue_acc, rev);
                }
            }

            // Horizontal sum of revenue_acc
            __m128d low = _mm256_castpd256_pd128(revenue_acc);
            __m128d high = _mm256_extractf128_pd(revenue_acc, 1);
            __m128d sum128 = _mm_add_pd(low, high);
            __m128d shuf = _mm_shuffle_pd(sum128, sum128, 1);
            sum128 = _mm_add_pd(sum128, shuf);
            revenue += _mm_cvtsd_f64(sum128);

            // Handle remaining rows with scalar code
            for (size_t i = simd_end; i < end; i++) {
                int32_t date = shipdate[i];
                double disc = discount[i];
                double qty = quantity[i];

                if (date >= min_date && date < max_date &&
                    disc >= min_discount && disc <= max_discount &&
                    qty < max_quantity) {
                    revenue += price[i] * disc;
                }
            }
        } else
#endif
        {
            // Scalar fallback
            for (size_t i = start; i < end; i++) {
                int32_t date = shipdate[i];
                double disc = discount[i];
                double qty = quantity[i];

                if (date >= min_date && date < max_date &&
                    disc >= min_discount && disc <= max_discount &&
                    qty < max_quantity) {
                    revenue += price[i] * disc;
                }
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q6: Forecasting Revenue Change ===\n";
    std::cout << "REVENUE: " << std::fixed << std::setprecision(2) << revenue << "\n";
    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
