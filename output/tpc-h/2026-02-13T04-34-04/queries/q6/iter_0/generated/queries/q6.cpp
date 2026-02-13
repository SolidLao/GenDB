/**
 * Q6: Revenue from discounted lineitem sales in 1994
 *
 * SELECT SUM(l_extendedprice * l_discount) AS revenue
 * FROM lineitem
 * WHERE l_shipdate >= DATE '1994-01-01'
 *   AND l_shipdate < DATE '1995-01-01'
 *   AND l_discount BETWEEN 0.05 AND 0.07
 *   AND l_quantity < 24;
 *
 * Optimization strategy:
 *   - Row group pruning on l_shipdate (filter ~86% at I/O level)
 *   - Column projection (read 4 of 16 columns)
 *   - Thread-parallel processing (64 cores)
 *   - Fused filter + compute (single pass)
 *   - Kahan summation (numerical precision)
 */

#include "parquet_reader.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <thread>
#include <vector>
#include <mutex>
#include <cmath>

void run_q6(const std::string& parquet_dir, const std::string& results_dir) {
    auto t_start = std::chrono::high_resolution_clock::now();

    // Date constants: 1994-01-01 to 1995-01-01 (exclusive)
    const int32_t date_lo = date_to_days(1994, 1, 1);
    const int32_t date_hi = date_to_days(1995, 1, 1);

    // Discount range: [0.05, 0.07]
    const double discount_lo = 0.05;
    const double discount_hi = 0.07;

    // Quantity threshold
    const double quantity_max = 24.0;

    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    // ─────────────────────────────────────────────────────────────────────
    // STEP 1: Row Group Pruning on l_shipdate
    // ─────────────────────────────────────────────────────────────────────
    auto stats = get_row_group_stats(lineitem_path, "l_shipdate");
    std::vector<int> valid_rgs;
    int64_t total_rows = 0;
    int64_t pruned_rows = 0;

    for (const auto& s : stats) {
        total_rows += s.num_rows;
        if (s.has_min_max) {
            // Row group overlaps [date_lo, date_hi) if max >= date_lo AND min < date_hi
            if (s.max_int >= date_lo && s.min_int < date_hi) {
                valid_rgs.push_back(s.row_group_index);
            } else {
                pruned_rows += s.num_rows;
            }
        } else {
            // No stats — must include (conservative)
            valid_rgs.push_back(s.row_group_index);
        }
    }

    std::cout << "[Q6] Row groups: " << valid_rgs.size() << "/" << stats.size()
              << " (pruned " << pruned_rows << "/" << total_rows << " rows = "
              << std::fixed << std::setprecision(1)
              << (100.0 * pruned_rows / total_rows) << "%)\n";

    // ─────────────────────────────────────────────────────────────────────
    // STEP 2: Load only relevant row groups + columns
    // ─────────────────────────────────────────────────────────────────────
    ParquetTable lineitem = read_parquet_row_groups(
        lineitem_path,
        {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"},
        valid_rgs
    );

    const int32_t* l_shipdate = lineitem.column<int32_t>("l_shipdate");
    const double*  l_discount = lineitem.column<double>("l_discount");
    const double*  l_quantity = lineitem.column<double>("l_quantity");
    const double*  l_extendedprice = lineitem.column<double>("l_extendedprice");
    int64_t N = lineitem.num_rows;

    std::cout << "[Q6] Loaded " << N << " rows from " << valid_rgs.size() << " row groups\n";

    // ─────────────────────────────────────────────────────────────────────
    // STEP 3: Thread-Parallel Fused Filter + Aggregate
    // ─────────────────────────────────────────────────────────────────────
    unsigned num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 1;

    // Per-thread results
    std::vector<double> thread_sums(num_threads, 0.0);
    std::vector<int64_t> thread_counts(num_threads, 0);

    auto worker = [&](unsigned tid) {
        int64_t chunk_size = (N + num_threads - 1) / num_threads;
        int64_t start = tid * chunk_size;
        int64_t end = std::min(start + chunk_size, N);

        // Kahan summation for numerical precision
        double sum = 0.0;
        double c = 0.0;  // compensation for lost low-order bits
        int64_t count = 0;

        for (int64_t i = start; i < end; i++) {
            // Fused filter: all conditions in one pass
            if (l_shipdate[i] >= date_lo && l_shipdate[i] < date_hi &&
                l_discount[i] >= discount_lo && l_discount[i] <= discount_hi &&
                l_quantity[i] < quantity_max)
            {
                double revenue = l_extendedprice[i] * l_discount[i];

                // Kahan summation
                double y = revenue - c;
                double t = sum + y;
                c = (t - sum) - y;
                sum = t;

                count++;
            }
        }

        thread_sums[tid] = sum;
        thread_counts[tid] = count;
    };

    std::vector<std::thread> threads;
    for (unsigned t = 0; t < num_threads; t++) {
        threads.emplace_back(worker, t);
    }
    for (auto& th : threads) th.join();

    // ─────────────────────────────────────────────────────────────────────
    // STEP 4: Merge thread results with Kahan summation
    // ─────────────────────────────────────────────────────────────────────
    double total_revenue = 0.0;
    double c = 0.0;
    int64_t total_count = 0;

    for (unsigned t = 0; t < num_threads; t++) {
        double y = thread_sums[t] - c;
        double tmp = total_revenue + y;
        c = (tmp - total_revenue) - y;
        total_revenue = tmp;
        total_count += thread_counts[t];
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();

    // ─────────────────────────────────────────────────────────────────────
    // STEP 5: Write CSV output
    // ─────────────────────────────────────────────────────────────────────
    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    if (!out) {
        std::cerr << "[Q6] ERROR: Failed to open output file: " << output_path << "\n";
        return;
    }

    out << "revenue\n";
    out << std::fixed << std::setprecision(2) << total_revenue << "\n";
    out.close();

    std::cout << "[Q6] DONE in " << std::fixed << std::setprecision(2) << elapsed << "s"
              << " — matched " << total_count << " rows"
              << " — revenue = " << total_revenue
              << " — output: " << output_path << "\n";
}
