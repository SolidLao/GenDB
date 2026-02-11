#include "queries.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>

namespace gendb {

void execute_q1(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "\n=== Q1: Pricing Summary Report ===" << std::endl;

    // Load required columns via mmap
    auto l_shipdate = mmap_int32_column(gendb_dir, "lineitem", "l_shipdate");
    auto l_returnflag = mmap_uint8_column(gendb_dir, "lineitem", "l_returnflag");
    auto l_linestatus = mmap_uint8_column(gendb_dir, "lineitem", "l_linestatus");
    auto l_quantity = mmap_double_column(gendb_dir, "lineitem", "l_quantity");
    auto l_extendedprice = mmap_double_column(gendb_dir, "lineitem", "l_extendedprice");
    auto l_discount = mmap_double_column(gendb_dir, "lineitem", "l_discount");
    auto l_tax = mmap_double_column(gendb_dir, "lineitem", "l_tax");

    size_t n = l_shipdate.size;

    // Date filter: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t cutoff_date = date_to_days("1998-09-02");

    // OPTIMIZATION: Sorted aggregation with direct array indexing for ultra-low cardinality (4 groups)
    // Much faster than hash aggregation: direct array access (1-2ns) vs hash lookup (5-8ns)
    // Key encoding: index = (returnflag - 'A') * 2 + (linestatus == 'O' ? 1 : 0)
    // Valid keys: A|F=0, A|O=1, N|F=4, N|O=5, R|F=8, R|O=9 (but only 4 exist in data)

    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    // Thread-local aggregation arrays (64 slots to handle all possible returnflag/linestatus combinations)
    // Actual used: A|F, A|O, N|F, N|O, R|F, R|O (6 possible, 4 exist in data)
    std::vector<std::array<Q1AggResult, 64>> local_aggs(num_threads);

    size_t chunk_size = (n + num_threads - 1) / num_threads;

    for (size_t tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            size_t start_idx = tid * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, n);

            auto& local_agg = local_aggs[tid];

            for (size_t i = start_idx; i < end_idx; ++i) {
                // Filter: l_shipdate <= cutoff_date
                if (l_shipdate.data[i] > cutoff_date) continue;

                // Direct array indexing (no hash lookup!)
                uint8_t returnflag = l_returnflag.data[i];
                uint8_t linestatus = l_linestatus.data[i];

                // Encode key as array index: (returnflag - 'A') * 3 + (linestatus - 'F')
                // returnflag: A=0, N=13, R=17
                // linestatus: F=0, O=9
                // Max index: R(17) * 3 + O(9) = 51+9 = 60, but we'll size array to 64 for safety
                size_t idx = (returnflag - 'A') * 3 + (linestatus == 'O' ? 1 : 0);

                double qty = l_quantity.data[i];
                double price = l_extendedprice.data[i];
                double disc = l_discount.data[i];
                double tax_val = l_tax.data[i];

                double disc_price = price * (1.0 - disc);
                double charge = disc_price * (1.0 + tax_val);

                // Update local aggregate (no hash table, just array access)
                Q1AggResult& agg = local_agg[idx];
                agg.sum_qty += qty;
                agg.sum_base_price += price;
                agg.sum_disc_price += disc_price;
                agg.sum_charge += charge;
                agg.sum_discount += disc;
                agg.count += 1;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local arrays (trivial for 64 slots)
    std::array<Q1AggResult, 64> global_agg{};
    for (const auto& local_agg : local_aggs) {
        for (size_t i = 0; i < 64; ++i) {
            if (local_agg[i].count > 0) {
                global_agg[i].sum_qty += local_agg[i].sum_qty;
                global_agg[i].sum_base_price += local_agg[i].sum_base_price;
                global_agg[i].sum_disc_price += local_agg[i].sum_disc_price;
                global_agg[i].sum_charge += local_agg[i].sum_charge;
                global_agg[i].sum_discount += local_agg[i].sum_discount;
                global_agg[i].count += local_agg[i].count;
            }
        }
    }

    // Convert array results to sorted vector
    std::vector<std::pair<Q1GroupKey, Q1AggResult>> sorted_results;
    sorted_results.reserve(4);  // Expecting 4 groups

    for (size_t idx = 0; idx < 64; ++idx) {
        if (global_agg[idx].count > 0) {
            // Decode index back to returnflag/linestatus
            uint8_t returnflag = 'A' + (idx / 3);
            uint8_t linestatus = (idx % 3) == 1 ? 'O' : 'F';

            Q1GroupKey key{returnflag, linestatus};
            sorted_results.emplace_back(key, global_agg[idx]);
        }
    }

    // Sort results by returnflag, then linestatus
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) {
                  if (a.first.l_returnflag != b.first.l_returnflag)
                      return a.first.l_returnflag < b.first.l_returnflag;
                  return a.first.l_linestatus < b.first.l_linestatus;
              });

    // Print results
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\nRETURNFLAG | LINESTATUS | SUM_QTY | SUM_BASE_PRICE | SUM_DISC_PRICE | SUM_CHARGE | AVG_QTY | AVG_PRICE | AVG_DISC | COUNT_ORDER" << std::endl;
    std::cout << std::string(120, '-') << std::endl;

    for (const auto& [key, result] : sorted_results) {
        char returnflag = static_cast<char>(key.l_returnflag);
        char linestatus = static_cast<char>(key.l_linestatus);

        double avg_qty = result.sum_qty / result.count;
        double avg_price = result.sum_base_price / result.count;
        double avg_disc = result.sum_discount / result.count;

        std::cout << std::setw(10) << returnflag << " | "
                  << std::setw(10) << linestatus << " | "
                  << std::setw(10) << result.sum_qty << " | "
                  << std::setw(14) << result.sum_base_price << " | "
                  << std::setw(14) << result.sum_disc_price << " | "
                  << std::setw(10) << result.sum_charge << " | "
                  << std::setw(7) << avg_qty << " | "
                  << std::setw(9) << avg_price << " | "
                  << std::setw(8) << avg_disc << " | "
                  << std::setw(11) << result.count << std::endl;
    }

    // Cleanup
    unmap_column(l_shipdate.mmap_ptr, l_shipdate.mmap_size);
    unmap_column(l_returnflag.mmap_ptr, l_returnflag.mmap_size);
    unmap_column(l_linestatus.mmap_ptr, l_linestatus.mmap_size);
    unmap_column(l_quantity.mmap_ptr, l_quantity.mmap_size);
    unmap_column(l_extendedprice.mmap_ptr, l_extendedprice.mmap_size);
    unmap_column(l_discount.mmap_ptr, l_discount.mmap_size);
    unmap_column(l_tax.mmap_ptr, l_tax.mmap_size);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "\nQ1 execution time: " << elapsed.count() << " seconds" << std::endl;
}

}  // namespace gendb
