#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <immintrin.h> // AVX2 intrinsics

namespace gendb {

// Q6: Forecasting Revenue Change with SIMD vectorized filtering
void execute_q6(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns
    LineitemTable lineitem;
    std::vector<std::string> columns = {
        "l_shipdate", "l_discount", "l_quantity", "l_extendedprice"
    };
    load_lineitem(gendb_dir, lineitem, columns);

    // Filter predicates
    int32_t date_start = date_to_days(1994, 1, 1);
    int32_t date_end = date_to_days(1995, 1, 1);
    double discount_min = 0.05;
    double discount_max = 0.07;
    double quantity_max = 24.0;

    // Scan and aggregate with zone map block-level pruning
    double revenue = 0.0;
    size_t rows_scanned = 0;
    size_t blocks_skipped = 0;

    if (!lineitem.shipdate_zones.empty()) {
        // Use zone maps for aggressive block-level pruning
        for (const auto& zone : lineitem.shipdate_zones) {
            // Skip block if it doesn't overlap with [date_start, date_end)
            if (zone.max_value < date_start || zone.min_value >= date_end) {
                blocks_skipped++;
                continue;
            }

            // This block overlaps with our date range, scan it with SIMD
            size_t block_start = zone.row_offset;
            size_t block_end = block_start + zone.row_count;
            size_t i = block_start;

            // SIMD setup
            __m256d discount_min_vec = _mm256_set1_pd(discount_min);
            __m256d discount_max_vec = _mm256_set1_pd(discount_max);
            __m256d quantity_max_vec = _mm256_set1_pd(quantity_max);

            // Main SIMD loop: process 4 rows at a time
            for (; i + 4 <= block_end; i += 4) {
                rows_scanned += 4;

                // Load and check all predicates, accumulate selectively
                // This is simpler and more correct than trying to mask everything

                // Load discount (4 doubles)
                __m256d disc_vec = _mm256_loadu_pd(&lineitem.l_discount[i]);
                __m256d disc_ge_min = _mm256_cmp_pd(disc_vec, discount_min_vec, _CMP_GE_OQ);
                __m256d disc_le_max = _mm256_cmp_pd(disc_vec, discount_max_vec, _CMP_LE_OQ);
                __m256d disc_mask = _mm256_and_pd(disc_ge_min, disc_le_max);

                // Load quantity (4 doubles)
                __m256d qty_vec = _mm256_loadu_pd(&lineitem.l_quantity[i]);
                __m256d qty_mask = _mm256_cmp_pd(qty_vec, quantity_max_vec, _CMP_LT_OQ);

                // Combine discount and quantity masks
                __m256d combined_mask = _mm256_and_pd(disc_mask, qty_mask);

                // Extract mask bits to check dates individually (dates are int32, harder to SIMD)
                int mask = _mm256_movemask_pd(combined_mask);

                // For each lane that passed disc/qty check, also check date
                for (int lane = 0; lane < 4; lane++) {
                    if (mask & (1 << lane)) {
                        int32_t shipdate = lineitem.l_shipdate[i + lane];
                        if (shipdate >= date_start && shipdate < date_end) {
                            revenue += lineitem.l_extendedprice[i + lane] * lineitem.l_discount[i + lane];
                        }
                    }
                }
            }

            // Scalar tail: process remaining rows
            for (; i < block_end; i++) {
                rows_scanned++;
                double disc = lineitem.l_discount[i];
                if (disc >= discount_min && disc <= discount_max) {
                    double qty = lineitem.l_quantity[i];
                    if (qty < quantity_max) {
                        int32_t shipdate = lineitem.l_shipdate[i];
                        if (shipdate >= date_start && shipdate < date_end) {
                            revenue += lineitem.l_extendedprice[i] * disc;
                        }
                    }
                }
            }
        }
    } else {
        // Fallback: no zone maps, scan all rows with SIMD
        size_t i = 0;
        size_t n = lineitem.row_count;
        __m256d discount_min_vec = _mm256_set1_pd(discount_min);
        __m256d discount_max_vec = _mm256_set1_pd(discount_max);
        __m256d quantity_max_vec = _mm256_set1_pd(quantity_max);

        // Main SIMD loop: process 4 rows at a time
        for (; i + 4 <= n; i += 4) {
            rows_scanned += 4;

            // Load discount (4 doubles)
            __m256d disc_vec = _mm256_loadu_pd(&lineitem.l_discount[i]);
            __m256d disc_ge_min = _mm256_cmp_pd(disc_vec, discount_min_vec, _CMP_GE_OQ);
            __m256d disc_le_max = _mm256_cmp_pd(disc_vec, discount_max_vec, _CMP_LE_OQ);
            __m256d disc_mask = _mm256_and_pd(disc_ge_min, disc_le_max);

            // Load quantity (4 doubles)
            __m256d qty_vec = _mm256_loadu_pd(&lineitem.l_quantity[i]);
            __m256d qty_mask = _mm256_cmp_pd(qty_vec, quantity_max_vec, _CMP_LT_OQ);

            // Combine masks
            __m256d combined_mask = _mm256_and_pd(disc_mask, qty_mask);

            // Extract mask bits
            int mask = _mm256_movemask_pd(combined_mask);

            // Check dates individually for lanes that passed
            for (int lane = 0; lane < 4; lane++) {
                if (mask & (1 << lane)) {
                    int32_t shipdate = lineitem.l_shipdate[i + lane];
                    if (shipdate >= date_start && shipdate < date_end) {
                        revenue += lineitem.l_extendedprice[i + lane] * lineitem.l_discount[i + lane];
                    }
                }
            }
        }

        // Scalar tail: process remaining rows
        for (; i < n; i++) {
            rows_scanned++;
            double disc = lineitem.l_discount[i];
            if (disc >= discount_min && disc <= discount_max) {
                double qty = lineitem.l_quantity[i];
                if (qty < quantity_max) {
                    int32_t shipdate = lineitem.l_shipdate[i];
                    if (shipdate >= date_start && shipdate < date_end) {
                        revenue += lineitem.l_extendedprice[i] * disc;
                    }
                }
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print result
    std::cout << "\n=== Q6: Forecasting Revenue Change ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "revenue: " << revenue << "\n";
    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
