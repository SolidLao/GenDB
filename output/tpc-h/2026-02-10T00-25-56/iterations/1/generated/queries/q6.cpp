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
    // SIMD processing: process 8 int32 dates at a time
    size_t i = 0;
    const size_t simd_width = 8;

    // Broadcast filter thresholds to SIMD vectors
    __m256i start_date_vec = _mm256_set1_epi32(start_date);
    __m256i end_date_vec = _mm256_set1_epi32(end_date);

    // Process 8 rows at a time for date filtering
    for (; i + simd_width <= n; i += simd_width) {
        // Load 8 shipdate values (int32)
        __m256i shipdate = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&lineitem.l_shipdate[i]));

        // Date range filter: shipdate >= start_date AND shipdate < end_date
        __m256i date_ge = _mm256_cmpgt_epi32(shipdate, _mm256_sub_epi32(start_date_vec, _mm256_set1_epi32(1)));
        __m256i date_lt = _mm256_cmpgt_epi32(end_date_vec, shipdate);
        __m256i date_pass = _mm256_and_si256(date_ge, date_lt);

        // Extract mask
        int date_mask = _mm256_movemask_ps(_mm256_castsi256_ps(date_pass));

        // Check each row that passed date filter
        for (size_t j = 0; j < simd_width; j++) {
            if (date_mask & (1 << j)) {
                size_t idx = i + j;
                double discount = lineitem.l_discount[idx];
                double quantity = lineitem.l_quantity[idx];

                // Apply remaining filters
                if (discount >= min_discount && discount <= max_discount && quantity < max_quantity) {
                    revenue += lineitem.l_extendedprice[idx] * discount;
                    qualifying_rows++;
                }
            }
        }
    }

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
