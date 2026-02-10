#include "queries.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <chrono>

void execute_q1(const LineItem& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date filter: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t cutoff_date = date_utils::parse_date("1998-09-02");

    // Hash aggregation with composite key (returnflag, linestatus)
    std::unordered_map<Q1GroupKey, Q1Aggregate, Q1GroupKeyHash> aggregates;

    // Scan lineitem with filter and aggregate
    size_t n = lineitem.size();
    for (size_t i = 0; i < n; ++i) {
        if (lineitem.l_shipdate[i] <= cutoff_date) {
            Q1GroupKey key{lineitem.l_returnflag[i], lineitem.l_linestatus[i]};

            auto& agg = aggregates[key];

            double quantity = lineitem.l_quantity[i];
            double price = lineitem.l_extendedprice[i];
            double discount = lineitem.l_discount[i];
            double tax = lineitem.l_tax[i];

            // Compute disc_price and charge once (expression reuse optimization)
            double disc_price = price * (1.0 - discount);
            double charge = disc_price * (1.0 + tax);

            agg.sum_qty += quantity;
            agg.sum_base_price += price;
            agg.sum_disc_price += disc_price;
            agg.sum_charge += charge;
            agg.sum_discount += discount;
            agg.count += 1;
        }
    }

    // Convert to vector for sorting
    struct ResultRow {
        char returnflag;
        char linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count_order;
    };

    std::vector<ResultRow> results;
    results.reserve(aggregates.size());

    for (const auto& [key, agg] : aggregates) {
        ResultRow row;
        row.returnflag = key.l_returnflag;
        row.linestatus = key.l_linestatus;
        row.sum_qty = agg.sum_qty;
        row.sum_base_price = agg.sum_base_price;
        row.sum_disc_price = agg.sum_disc_price;
        row.sum_charge = agg.sum_charge;
        row.avg_qty = agg.sum_qty / agg.count;
        row.avg_price = agg.sum_base_price / agg.count;
        row.avg_disc = agg.sum_discount / agg.count;
        row.count_order = agg.count;
        results.push_back(row);
    }

    // Sort by returnflag, linestatus
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q1: Pricing Summary Report ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "returnflag | linestatus | sum_qty | sum_base_price | sum_disc_price | sum_charge | avg_qty | avg_price | avg_disc | count_order" << std::endl;
    std::cout << std::string(130, '-') << std::endl;

    for (const auto& row : results) {
        std::cout << row.returnflag << "          | "
                  << row.linestatus << "          | "
                  << std::setw(10) << row.sum_qty << " | "
                  << std::setw(14) << row.sum_base_price << " | "
                  << std::setw(14) << row.sum_disc_price << " | "
                  << std::setw(10) << row.sum_charge << " | "
                  << std::setw(7) << row.avg_qty << " | "
                  << std::setw(9) << row.avg_price << " | "
                  << std::setw(8) << row.avg_disc << " | "
                  << std::setw(11) << row.count_order << std::endl;
    }

    std::cout << "\nQ1 execution time: " << duration.count() << " ms" << std::endl;
}
