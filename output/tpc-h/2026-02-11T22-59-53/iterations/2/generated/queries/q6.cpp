#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>
#include <immintrin.h>  // AVX2 intrinsics

// Q6: Forecasting Revenue Change
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// FROM lineitem
// WHERE l_shipdate >= '1994-01-01'
//   AND l_shipdate < '1995-01-01'
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24

void execute_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load columns
    size_t row_count = get_row_count(gendb_dir, "lineitem");

    size_t size;
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", size);
    double* l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", size);
    double* l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", size);
    double* l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", size);

    // Date thresholds
    int32_t date_start = date_utils::date_to_days(1994, 1, 1);
    int32_t date_end = date_utils::date_to_days(1995, 1, 1);

    // Build zone map for shipdate (data is sorted by this column)
    // Block size: ~65K rows (fits in L3 cache per thread)
    const size_t BLOCK_SIZE = 65536;
    const size_t num_blocks = (row_count + BLOCK_SIZE - 1) / BLOCK_SIZE;

    struct ZoneMapEntry {
        int32_t min_date;
        int32_t max_date;
        size_t start_idx;
        size_t end_idx;
        bool can_skip;
    };

    std::vector<ZoneMapEntry> zone_map(num_blocks);

    // Build zone map (single-threaded, fast since we only scan dates)
    for (size_t b = 0; b < num_blocks; b++) {
        size_t start_idx = b * BLOCK_SIZE;
        size_t end_idx = std::min(start_idx + BLOCK_SIZE, row_count);

        int32_t min_date = l_shipdate[start_idx];
        int32_t max_date = l_shipdate[start_idx];

        // Since data is sorted, we can use binary search or just check endpoints
        // For sorted data: min is first, max is last
        if (end_idx > start_idx + 1) {
            max_date = l_shipdate[end_idx - 1];
        }

        // Determine if this block can be skipped
        bool can_skip = (max_date < date_start) || (min_date >= date_end);

        zone_map[b] = {min_date, max_date, start_idx, end_idx, can_skip};
    }

    // Parallel aggregation with zone map pruning
    const int num_threads = std::thread::hardware_concurrency();

    // Align to cache lines (64 bytes) to avoid false sharing
    struct alignas(64) AlignedDouble {
        double value = 0.0;
    };
    std::vector<AlignedDouble> local_revenues(num_threads);

    std::vector<std::thread> threads;

    // Distribute blocks (not rows) among threads for better load balancing
    std::atomic<size_t> next_block(0);

    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            double local_revenue = 0.0;

            // Each thread pulls blocks from the shared counter
            while (true) {
                size_t block_idx = next_block.fetch_add(1, std::memory_order_relaxed);
                if (block_idx >= num_blocks) break;

                const auto& zone = zone_map[block_idx];

                // Skip entire block if zone map indicates no matches
                if (zone.can_skip) continue;

                // Process this block with manual AVX2 SIMD
                // Date filtering is most selective (~95% of rows filtered), so SIMD it first
                size_t i = zone.start_idx;
                size_t block_end = zone.end_idx;

                // SIMD loop: process 8 rows at a time using AVX2
                __m256i date_start_vec = _mm256_set1_epi32(date_start);
                __m256i date_end_vec = _mm256_set1_epi32(date_end);
                __m256d discount_min_vec = _mm256_set1_pd(0.05);
                __m256d discount_max_vec = _mm256_set1_pd(0.07);
                __m256d quantity_max_vec = _mm256_set1_pd(24.0);

                // Process 8 rows per iteration
                for (; i + 8 <= block_end; i += 8) {
                    // Load 8 dates
                    __m256i dates = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&l_shipdate[i]));

                    // Date range check: date >= date_start && date < date_end
                    __m256i cmp_ge = _mm256_cmpgt_epi32(dates, _mm256_sub_epi32(date_start_vec, _mm256_set1_epi32(1)));
                    __m256i cmp_lt = _mm256_cmpgt_epi32(date_end_vec, dates);
                    __m256i date_mask = _mm256_and_si256(cmp_ge, cmp_lt);

                    // Convert to bitmask
                    int date_match_bits = _mm256_movemask_ps(_mm256_castsi256_ps(date_mask));

                    // For each matching row, check discount and quantity (scalar)
                    while (date_match_bits != 0) {
                        int bit_idx = __builtin_ctz(date_match_bits);
                        size_t row_idx = i + bit_idx;

                        double discount = l_discount[row_idx];
                        if (discount >= 0.05 && discount <= 0.07) {
                            double quantity = l_quantity[row_idx];
                            if (quantity < 24.0) {
                                local_revenue += l_extendedprice[row_idx] * discount;
                            }
                        }

                        date_match_bits &= (date_match_bits - 1);  // Clear lowest bit
                    }
                }

                // Scalar tail loop for remaining rows
                for (; i < block_end; i++) {
                    int32_t date = l_shipdate[i];
                    if (date >= date_start && date < date_end) {
                        double discount = l_discount[i];
                        if (discount >= 0.05 && discount <= 0.07) {
                            double quantity = l_quantity[i];
                            if (quantity < 24.0) {
                                local_revenue += l_extendedprice[i] * discount;
                            }
                        }
                    }
                }
            }

            local_revenues[t].value = local_revenue;
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge local revenues
    double total_revenue = 0.0;
    for (const auto& aligned_rev : local_revenues) {
        total_revenue += aligned_rev.value;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Q6: 1 row in " << elapsed << "s" << std::endl;

    // Write results if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2);
        out << total_revenue << "\n";
    }
}
