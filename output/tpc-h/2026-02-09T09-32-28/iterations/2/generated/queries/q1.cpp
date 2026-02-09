#include "queries.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <chrono>

// Q1: Pricing Summary Report
// SELECT l_returnflag, l_linestatus,
//        SUM(l_quantity), SUM(l_extendedprice),
//        SUM(l_extendedprice * (1 - l_discount)),
//        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//        AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
// FROM lineitem
// WHERE l_shipdate <= date '1998-12-01' - interval '90' day
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus

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

    // Filter date: '1998-12-01' - 90 days = '1998-09-02'
    int32_t cutoff_date = parse_date("1998-09-02");

    // Hash aggregation
    std::unordered_map<Q1GroupKey, Q1Aggregates, Q1GroupKeyHash> groups;

    for (size_t i = 0; i < lineitem.size(); ++i) {
        if (lineitem.l_shipdate[i] <= cutoff_date) {
            // Direct uint8_t constructor - no string operations
            Q1GroupKey key{lineitem.l_returnflag[i], lineitem.l_linestatus[i]};
            Q1Aggregates& agg = groups[key];

            double disc_price = lineitem.l_extendedprice[i] * (1.0 - lineitem.l_discount[i]);
            double charge = disc_price * (1.0 + lineitem.l_tax[i]);

            agg.sum_qty += lineitem.l_quantity[i];
            agg.sum_base_price += lineitem.l_extendedprice[i];
            agg.sum_disc_price += disc_price;
            agg.sum_charge += charge;
            agg.sum_discount += lineitem.l_discount[i];
            agg.count++;
        }
    }

    // Convert to vector and sort by (returnflag, linestatus)
    std::vector<std::pair<Q1GroupKey, Q1Aggregates>> results(groups.begin(), groups.end());
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) {
                  return a.first < b.first;
              });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Print results
    std::cout << "\n=== Q1: Pricing Summary Report ===\n";
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
              << "\n";

    std::cout << std::fixed << std::setprecision(2);
    for (const auto& [key, agg] : results) {
        double avg_qty = agg.sum_qty / agg.count;
        double avg_price = agg.sum_base_price / agg.count;
        double avg_disc = agg.sum_discount / agg.count;

        std::cout << std::left
                  << std::setw(12) << key.get_returnflag()
                  << std::setw(12) << key.get_linestatus()
                  << std::setw(15) << agg.sum_qty
                  << std::setw(18) << agg.sum_base_price
                  << std::setw(18) << agg.sum_disc_price
                  << std::setw(18) << agg.sum_charge
                  << std::setw(12) << avg_qty
                  << std::setw(15) << avg_price
                  << std::setw(12) << avg_disc
                  << std::setw(12) << agg.count
                  << "\n";
    }

    std::cout << "Execution time: " << duration << " ms\n";
}
