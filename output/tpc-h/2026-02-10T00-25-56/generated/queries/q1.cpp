#include "queries.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <algorithm>
#include <vector>
#include <chrono>

// Q1: Pricing Summary Report
// SELECT l_returnflag, l_linestatus,
//        SUM(l_quantity) AS sum_qty,
//        SUM(l_extendedprice) AS sum_base_price,
//        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
//        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
//        AVG(l_quantity) AS avg_qty,
//        AVG(l_extendedprice) AS avg_price,
//        AVG(l_discount) AS avg_disc,
//        COUNT(*) AS count_order
// FROM lineitem
// WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus;

struct Q1AggregateState {
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
    int32_t cutoff_date = date_utils::parse_date("1998-09-02");

    // Hash aggregation
    std::unordered_map<Q1GroupKey, Q1AggregateState, Q1GroupKeyHash> agg_table;

    size_t n = lineitem.size();
    for (size_t i = 0; i < n; i++) {
        // Apply filter
        if (lineitem.l_shipdate[i] > cutoff_date) {
            continue;
        }

        // Extract group key
        Q1GroupKey key{lineitem.l_returnflag[i], lineitem.l_linestatus[i]};

        // Update aggregates
        auto& state = agg_table[key];
        double quantity = lineitem.l_quantity[i];
        double extendedprice = lineitem.l_extendedprice[i];
        double discount = lineitem.l_discount[i];
        double tax = lineitem.l_tax[i];

        state.sum_qty += quantity;
        state.sum_base_price += extendedprice;
        state.sum_disc_price += extendedprice * (1.0 - discount);
        state.sum_charge += extendedprice * (1.0 - discount) * (1.0 + tax);
        state.sum_discount += discount;
        state.count++;
    }

    // Extract and sort results
    std::vector<std::pair<Q1GroupKey, Q1AggregateState>> results(agg_table.begin(), agg_table.end());
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.first.returnflag != b.first.returnflag) {
            return a.first.returnflag < b.first.returnflag;
        }
        return a.first.linestatus < b.first.linestatus;
    });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q1: Pricing Summary Report ===" << std::endl;
    std::cout << std::left
              << std::setw(12) << "returnflag"
              << std::setw(12) << "linestatus"
              << std::setw(15) << "sum_qty"
              << std::setw(18) << "sum_base_price"
              << std::setw(18) << "sum_disc_price"
              << std::setw(18) << "sum_charge"
              << std::setw(12) << "avg_qty"
              << std::setw(15) << "avg_price"
              << std::setw(12) << "avg_disc"
              << std::setw(12) << "count_order"
              << std::endl;

    std::cout << std::fixed << std::setprecision(2);
    for (const auto& [key, state] : results) {
        std::cout << std::left
                  << std::setw(12) << key.returnflag
                  << std::setw(12) << key.linestatus
                  << std::setw(15) << state.sum_qty
                  << std::setw(18) << state.sum_base_price
                  << std::setw(18) << state.sum_disc_price
                  << std::setw(18) << state.sum_charge
                  << std::setw(12) << (state.sum_qty / state.count)
                  << std::setw(15) << (state.sum_base_price / state.count)
                  << std::setw(12) << (state.sum_discount / state.count)
                  << std::setw(12) << state.count
                  << std::endl;
    }

    std::cout << "\nExecution time: " << duration.count() << " ms" << std::endl;
}
