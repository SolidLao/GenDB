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

    // Use parallel hash aggregation operator
    operators::ParallelHashAgg<Q1GroupKey, Q1AggResult, Q1GroupKeyHash> agg_op;

    auto agg_fn = [&](size_t i, Q1GroupKey& key, Q1AggResult& state) {
        // Filter: l_shipdate <= cutoff_date
        if (l_shipdate.data[i] > cutoff_date) return;

        key = Q1GroupKey{l_returnflag.data[i], l_linestatus.data[i]};

        double qty = l_quantity.data[i];
        double price = l_extendedprice.data[i];
        double disc = l_discount.data[i];
        double tax_val = l_tax.data[i];

        double disc_price = price * (1.0 - disc);
        double charge = disc_price * (1.0 + tax_val);

        state.sum_qty = qty;
        state.sum_base_price = price;
        state.sum_disc_price = disc_price;
        state.sum_charge = charge;
        state.sum_discount = disc;
        state.count = 1;
    };

    // Execute aggregation
    auto global_agg = agg_op.execute(n, agg_fn);

    // Sort by returnflag, linestatus
    std::vector<std::pair<Q1GroupKey, Q1AggResult>> sorted_results(global_agg.begin(), global_agg.end());
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
