#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <chrono>

namespace gendb {

// Q6: Forecasting Revenue Change with zone map block-level pruning
void execute_q6(const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns
    LineitemTable lineitem;
    std::vector<std::string> columns = {
        "l_shipdate", "l_discount", "l_quantity", "l_extendedprice"
    };
    load_lineitem(gendb_dir, lineitem, columns);

    // Filter predicates (reordered for early exit: discount first, then quantity, then shipdate)
    int32_t date_start = date_to_days(1994, 1, 1);
    int32_t date_end = date_to_days(1995, 1, 1);
    double discount_min = 0.05;
    double discount_max = 0.07;
    double quantity_max = 24.0;

    // Scan and aggregate with zone map block-level pruning
    double revenue = 0.0;
    size_t rows_scanned = 0;
    size_t blocks_skipped = 0;

    if (!lineitem.shipdate_zones.empty()) {
        // Use zone maps for aggressive block-level pruning
        for (const auto& zone : lineitem.shipdate_zones) {
            // Skip block if it doesn't overlap with [date_start, date_end)
            // Block is skippable if: max < date_start OR min >= date_end
            if (zone.max_value < date_start || zone.min_value >= date_end) {
                blocks_skipped++;
                continue;  // Skip this entire block
            }

            // This block overlaps with our date range, scan it
            size_t block_start = zone.row_offset;
            size_t block_end = block_start + zone.row_count;

            for (size_t i = block_start; i < block_end; i++) {
                rows_scanned++;
                // Reordered predicates: discount first (most selective), then quantity, then shipdate
                double disc = lineitem.l_discount[i];
                if (disc >= discount_min && disc <= discount_max) {
                    double qty = lineitem.l_quantity[i];
                    if (qty < quantity_max) {
                        int32_t shipdate = lineitem.l_shipdate[i];
                        if (shipdate >= date_start && shipdate < date_end) {
                            revenue += lineitem.l_extendedprice[i] * disc;
                        }
                    }
                }
            }
        }
    } else {
        // Fallback: no zone maps, scan all rows
        size_t n = lineitem.row_count;
        for (size_t i = 0; i < n; i++) {
            rows_scanned++;
            double disc = lineitem.l_discount[i];
            if (disc >= discount_min && disc <= discount_max) {
                double qty = lineitem.l_quantity[i];
                if (qty < quantity_max) {
                    int32_t shipdate = lineitem.l_shipdate[i];
                    if (shipdate >= date_start && shipdate < date_end) {
                        revenue += lineitem.l_extendedprice[i] * disc;
                    }
                }
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print result
    std::cout << "\n=== Q6: Forecasting Revenue Change ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "revenue: " << revenue << "\n";
    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
