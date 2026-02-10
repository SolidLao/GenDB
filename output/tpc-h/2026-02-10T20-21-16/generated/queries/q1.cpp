#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <chrono>

namespace gendb {

// Q1: Pricing Summary Report
void execute_q1(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns
    LineitemTable lineitem;
    std::vector<std::string> columns = {
        "l_shipdate", "l_returnflag", "l_linestatus",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax"
    };
    load_lineitem(gendb_dir, lineitem, columns);

    // Filter date: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t cutoff_date = date_to_days(1998, 9, 2);

    // Aggregation state: (returnflag, linestatus) -> aggregates
    struct AggState {
        double sum_qty = 0.0;
        double sum_base_price = 0.0;
        double sum_disc_price = 0.0;
        double sum_charge = 0.0;
        double sum_discount = 0.0;
        int64_t count = 0;
    };

    // Use composite key (returnflag, linestatus)
    std::map<std::pair<uint8_t, uint8_t>, AggState> groups;

    // Scan and aggregate
    size_t n = lineitem.l_shipdate.size();
    for (size_t i = 0; i < n; i++) {
        if (lineitem.l_shipdate[i] <= cutoff_date) {
            auto key = std::make_pair(lineitem.l_returnflag[i], lineitem.l_linestatus[i]);
            auto& agg = groups[key];

            double qty = lineitem.l_quantity[i];
            double price = lineitem.l_extendedprice[i];
            double disc = lineitem.l_discount[i];
            double tax = lineitem.l_tax[i];

            agg.sum_qty += qty;
            agg.sum_base_price += price;
            agg.sum_disc_price += price * (1.0 - disc);
            agg.sum_charge += price * (1.0 - disc) * (1.0 + tax);
            agg.sum_discount += disc;
            agg.count++;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results (sorted by returnflag, linestatus)
    std::cout << "\n=== Q1: Pricing Summary Report ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "returnflag | linestatus | sum_qty | sum_base_price | sum_disc_price | sum_charge | avg_qty | avg_price | avg_disc | count_order\n";
    std::cout << "-----------|------------|---------|----------------|----------------|------------|---------|-----------|----------|------------\n";

    for (const auto& [key, agg] : groups) {
        std::string rf = lineitem.returnflag_dict[key.first];
        std::string ls = lineitem.linestatus_dict[key.second];

        std::cout << std::setw(10) << rf << " | "
                  << std::setw(10) << ls << " | "
                  << std::setw(7) << agg.sum_qty << " | "
                  << std::setw(14) << agg.sum_base_price << " | "
                  << std::setw(14) << agg.sum_disc_price << " | "
                  << std::setw(10) << agg.sum_charge << " | "
                  << std::setw(7) << (agg.sum_qty / agg.count) << " | "
                  << std::setw(9) << (agg.sum_base_price / agg.count) << " | "
                  << std::setw(8) << (agg.sum_discount / agg.count) << " | "
                  << std::setw(11) << agg.count << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
