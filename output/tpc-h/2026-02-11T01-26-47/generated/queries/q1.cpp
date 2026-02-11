#include "queries.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <algorithm>

namespace gendb {

void execute_q1(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Q1: Pricing Summary Report
    // Filter: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t cutoff_date = date_to_days(1998, 9, 2);

    // Use parallel hash aggregation operator
    // Estimated groups: 3-4 (returnflag x linestatus combinations)
    ParallelHashAggregation<CompositeKey, Q1AggState, CompositeKeyHash> agg_op(4);

    // Define aggregation function
    auto agg_func = [&lineitem, cutoff_date](Q1AggTable& table, size_t i) {
        if (lineitem.l_shipdate[i] <= cutoff_date) {
            CompositeKey key{lineitem.l_returnflag[i], lineitem.l_linestatus[i]};
            auto& state = table[key];

            double disc_price = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);
            double charge = disc_price * (1.0 + lineitem.l_tax[i]);

            state.sum_qty += lineitem.l_quantity[i];
            state.sum_base_price += lineitem.l_extendedprice[i];
            state.sum_disc_price += disc_price;
            state.sum_charge += charge;
            state.sum_disc += lineitem.l_discount[i];
            state.count++;
        }
    };

    // Execute parallel aggregation and merge with custom function
    // First, execute the parallel aggregation (builds thread-local tables)
    agg_op.execute(lineitem.size(), agg_func);

    // Define custom merge function for Q1AggState
    auto merge_func = [](Q1AggState& dest, const Q1AggState& src) {
        dest.sum_qty += src.sum_qty;
        dest.sum_base_price += src.sum_base_price;
        dest.sum_disc_price += src.sum_disc_price;
        dest.sum_charge += src.sum_charge;
        dest.sum_disc += src.sum_disc;
        dest.count += src.count;
    };

    // Merge thread-local results with custom merge function
    Q1AggTable global_result = agg_op.merge_with(merge_func);

    // Sort by returnflag, linestatus
    std::vector<std::pair<CompositeKey, Q1AggState>> sorted_results(
        global_result.begin(), global_result.end());

    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) {
                  if (a.first.returnflag != b.first.returnflag)
                      return a.first.returnflag < b.first.returnflag;
                  return a.first.linestatus < b.first.linestatus;
              });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q1: Pricing Summary Report ===\n";
    std::cout << std::left << std::setw(12) << "returnflag"
              << std::setw(12) << "linestatus"
              << std::right << std::setw(15) << "sum_qty"
              << std::setw(18) << "sum_base_price"
              << std::setw(18) << "sum_disc_price"
              << std::setw(15) << "sum_charge"
              << std::setw(12) << "avg_qty"
              << std::setw(15) << "avg_price"
              << std::setw(12) << "avg_disc"
              << std::setw(12) << "count_order" << "\n";
    std::cout << std::string(160, '-') << "\n";

    std::cout << std::fixed << std::setprecision(2);
    for (const auto& [key, state] : sorted_results) {
        std::cout << std::left << std::setw(12) << key.returnflag
                  << std::setw(12) << key.linestatus
                  << std::right << std::setw(15) << state.sum_qty
                  << std::setw(18) << state.sum_base_price
                  << std::setw(18) << state.sum_disc_price
                  << std::setw(15) << state.sum_charge
                  << std::setw(12) << state.avg_qty()
                  << std::setw(15) << state.avg_price()
                  << std::setw(12) << state.avg_disc()
                  << std::setw(12) << state.count << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
    std::cout << "Rows returned: " << sorted_results.size() << "\n";
}

} // namespace gendb
