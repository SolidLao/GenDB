#include "queries.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <algorithm>
#include <vector>

namespace gendb {

struct Q1Aggregates {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;
    int64_t count = 0;
};

void execute_q1(const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date filter: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t max_date = parse_date("1998-09-02");

    // Direct-indexed aggregation: max 256 * 256 = 65536 groups (but in practice only 6)
    // Use array indexed by (returnflag_code * 256 + linestatus_code)
    constexpr size_t MAX_GROUPS = 256 * 256;
    std::vector<Q1Aggregates> agg_array(MAX_GROUPS);
    std::vector<bool> group_exists(MAX_GROUPS, false);

    // Cache pointers for better performance
    const int32_t* shipdate = lineitem.l_shipdate.data();
    const uint8_t* returnflag_code = lineitem.l_returnflag_code.data();
    const uint8_t* linestatus_code = lineitem.l_linestatus_code.data();
    const double* quantity = lineitem.l_quantity.data();
    const double* price = lineitem.l_extendedprice.data();
    const double* discount = lineitem.l_discount.data();
    const double* tax = lineitem.l_tax.data();

    size_t n = lineitem.size();

    // Use zone maps to skip blocks that don't match the filter
    const auto& zonemap = lineitem.shipdate_zonemap;
    size_t block_size = zonemap.block_size;
    size_t num_blocks = zonemap.block_min.size();

    for (size_t block = 0; block < num_blocks; block++) {
        // Skip blocks where all dates are > max_date
        if (zonemap.block_min[block] > max_date) {
            continue;
        }

        size_t start = block * block_size;
        size_t end = std::min(start + block_size, n);

        for (size_t i = start; i < end; i++) {
            if (shipdate[i] <= max_date) {
                // Direct index computation
                size_t idx = (static_cast<size_t>(returnflag_code[i]) << 8) | linestatus_code[i];
                group_exists[idx] = true;

                double qty = quantity[i];
                double prc = price[i];
                double disc = discount[i];
                double tx = tax[i];

                double disc_factor = 1.0 - disc;
                double disc_price = prc * disc_factor;

                agg_array[idx].sum_qty += qty;
                agg_array[idx].sum_base_price += prc;
                agg_array[idx].sum_disc_price += disc_price;
                agg_array[idx].sum_charge += disc_price * (1.0 + tx);
                agg_array[idx].sum_discount += disc;
                agg_array[idx].count++;
            }
        }
    }

    // Collect non-empty groups and sort
    std::vector<std::pair<Q1GroupKey, Q1Aggregates>> results;
    for (size_t idx = 0; idx < MAX_GROUPS; idx++) {
        if (group_exists[idx]) {
            uint8_t rf = static_cast<uint8_t>(idx >> 8);
            uint8_t ls = static_cast<uint8_t>(idx & 0xFF);
            results.push_back({{rf, ls}, agg_array[idx]});
        }
    }

    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            return a.first < b.first;
        });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q1: Pricing Summary Report ===\n";
    std::cout << std::left << std::setw(12) << "RETURNFLAG"
              << std::setw(12) << "LINESTATUS"
              << std::right << std::setw(15) << "SUM_QTY"
              << std::setw(18) << "SUM_BASE_PRICE"
              << std::setw(18) << "SUM_DISC_PRICE"
              << std::setw(18) << "SUM_CHARGE"
              << std::setw(12) << "AVG_QTY"
              << std::setw(15) << "AVG_PRICE"
              << std::setw(12) << "AVG_DISC"
              << std::setw(15) << "COUNT_ORDER" << "\n";
    std::cout << std::string(155, '-') << "\n";

    for (const auto& [key, agg] : results) {
        // Decode dictionary codes back to strings for output
        const std::string& returnflag_str = lineitem.l_returnflag_dict[key.returnflag_code];
        const std::string& linestatus_str = lineitem.l_linestatus_dict[key.linestatus_code];

        std::cout << std::left << std::setw(12) << returnflag_str
                  << std::setw(12) << linestatus_str
                  << std::right << std::fixed << std::setprecision(2)
                  << std::setw(15) << agg.sum_qty
                  << std::setw(18) << agg.sum_base_price
                  << std::setw(18) << agg.sum_disc_price
                  << std::setw(18) << agg.sum_charge
                  << std::setw(12) << (agg.sum_qty / agg.count)
                  << std::setw(15) << (agg.sum_base_price / agg.count)
                  << std::setw(12) << (agg.sum_discount / agg.count)
                  << std::setw(15) << agg.count << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
