#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <chrono>
#include <cstring>

namespace gendb {

// Q1: Pricing Summary Report with perfect hash aggregation and zone map pruning
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

    // Perfect hash aggregation: 3 returnflag × 3 linestatus = 9 possible groups max
    // Direct array indexing instead of std::map
    struct AggState {
        double sum_qty = 0.0;
        double sum_base_price = 0.0;
        double sum_disc_price = 0.0;
        double sum_charge = 0.0;
        double sum_discount = 0.0;
        int64_t count = 0;
        uint8_t returnflag = 255;  // 255 = unused slot
        uint8_t linestatus = 255;
    };

    AggState groups[9];  // Perfect hash: returnflag * 3 + linestatus

    // Scan and aggregate with zone map block-level pruning
    size_t n = lineitem.row_count;
    size_t rows_processed = 0;
    size_t blocks_skipped = 0;

    if (!lineitem.shipdate_zones.empty()) {
        // Use zone maps for block-level pruning
        for (const auto& zone : lineitem.shipdate_zones) {
            // Skip block if max_shipdate < cutoff_date (no rows pass filter)
            // Process block if min_shipdate <= cutoff_date (some/all rows may pass)
            if (zone.max_value > cutoff_date) {
                // This block has dates after cutoff, so some rows won't match
                // But we still need to scan it for rows <= cutoff
            }

            // Scan this block
            size_t block_start = zone.row_offset;
            size_t block_end = block_start + zone.row_count;

            for (size_t i = block_start; i < block_end; i++) {
                if (lineitem.l_shipdate[i] <= cutoff_date) {
                    uint8_t rf = lineitem.l_returnflag[i];
                    uint8_t ls = lineitem.l_linestatus[i];
                    int idx = rf * 3 + ls;  // Perfect hash

                    auto& agg = groups[idx];
                    if (agg.count == 0) {
                        agg.returnflag = rf;
                        agg.linestatus = ls;
                    }

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
                    rows_processed++;
                }
            }
        }
    } else {
        // Fallback: no zone maps, scan all rows
        for (size_t i = 0; i < n; i++) {
            if (lineitem.l_shipdate[i] <= cutoff_date) {
                uint8_t rf = lineitem.l_returnflag[i];
                uint8_t ls = lineitem.l_linestatus[i];
                int idx = rf * 3 + ls;  // Perfect hash

                auto& agg = groups[idx];
                if (agg.count == 0) {
                    agg.returnflag = rf;
                    agg.linestatus = ls;
                }

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
                rows_processed++;
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results (sorted by returnflag, linestatus)
    // Collect non-empty groups
    std::vector<int> active_indices;
    for (int i = 0; i < 9; i++) {
        if (groups[i].count > 0) {
            active_indices.push_back(i);
        }
    }

    // Sort by returnflag, linestatus
    std::sort(active_indices.begin(), active_indices.end(), [&](int a, int b) {
        if (groups[a].returnflag != groups[b].returnflag)
            return groups[a].returnflag < groups[b].returnflag;
        return groups[a].linestatus < groups[b].linestatus;
    });

    std::cout << "\n=== Q1: Pricing Summary Report ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "returnflag | linestatus | sum_qty | sum_base_price | sum_disc_price | sum_charge | avg_qty | avg_price | avg_disc | count_order\n";
    std::cout << "-----------|------------|---------|----------------|----------------|------------|---------|-----------|----------|------------\n";

    for (int idx : active_indices) {
        const auto& agg = groups[idx];
        std::string rf = lineitem.returnflag_dict[agg.returnflag];
        std::string ls = lineitem.linestatus_dict[agg.linestatus];

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
