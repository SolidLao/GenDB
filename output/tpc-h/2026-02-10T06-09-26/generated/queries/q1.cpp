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

    // Hash aggregation on (returnflag, linestatus)
    std::unordered_map<Q1GroupKey, Q1Aggregates, Q1GroupKeyHash> agg_map;

    size_t n = lineitem.size();
    for (size_t i = 0; i < n; i++) {
        if (lineitem.l_shipdate[i] <= max_date) {
            Q1GroupKey key{lineitem.l_returnflag[i], lineitem.l_linestatus[i]};
            auto& agg = agg_map[key];

            double quantity = lineitem.l_quantity[i];
            double price = lineitem.l_extendedprice[i];
            double discount = lineitem.l_discount[i];
            double tax = lineitem.l_tax[i];

            agg.sum_qty += quantity;
            agg.sum_base_price += price;
            agg.sum_disc_price += price * (1.0 - discount);
            agg.sum_charge += price * (1.0 - discount) * (1.0 + tax);
            agg.sum_discount += discount;
            agg.count++;
        }
    }

    // Sort by (returnflag, linestatus)
    std::vector<std::pair<Q1GroupKey, Q1Aggregates>> results(agg_map.begin(), agg_map.end());
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (a.first.returnflag != b.first.returnflag)
                return a.first.returnflag < b.first.returnflag;
            return a.first.linestatus < b.first.linestatus;
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
        std::cout << std::left << std::setw(12) << key.returnflag
                  << std::setw(12) << key.linestatus
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
