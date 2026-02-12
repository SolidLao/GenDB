#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <vector>
#include <unordered_map>
#include <algorithm>

namespace gendb {

// Q1: Pricing Summary Report
// SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
//        SUM(l_extendedprice * (1 - l_discount)), SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//        AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
// FROM lineitem
// WHERE l_shipdate <= '1998-09-01'
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus

struct Q1Key {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const Q1Key& o) const {
        return returnflag == o.returnflag && linestatus == o.linestatus;
    }

    bool operator<(const Q1Key& o) const {
        if (returnflag != o.returnflag) return returnflag < o.returnflag;
        return linestatus < o.linestatus;
    }
};

struct Q1Agg {
    double sum_qty = 0;
    double sum_base_price = 0;
    double sum_disc_price = 0;
    double sum_charge = 0;
    double sum_discount = 0;
    size_t count = 0;
};

} // namespace gendb

namespace std {
    template<>
    struct hash<gendb::Q1Key> {
        size_t operator()(const gendb::Q1Key& k) const {
            return (size_t(k.returnflag) << 8) | size_t(k.linestatus);
        }
    };
}

namespace gendb {

void execute_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns via mmap
    size_t row_count;
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", row_count);
    auto l_returnflag = mmap_column<uint8_t>(gendb_dir, "lineitem", "l_returnflag", row_count);
    auto l_linestatus = mmap_column<uint8_t>(gendb_dir, "lineitem", "l_linestatus", row_count);
    auto l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", row_count);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", row_count);
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", row_count);
    auto l_tax = mmap_column<double>(gendb_dir, "lineitem", "l_tax", row_count);

    // Filter date: l_shipdate <= '1998-09-01' (which is '1998-12-01' - 90 days per SQL)
    int32_t cutoff_date = parse_date("1998-09-02");  // <= 1998-09-01 means < 1998-09-02

    // Use hash aggregation operator
    operators::HashAggregation<Q1Key, Q1Agg> hash_agg;

    // Define aggregation logic
    auto get_key = [&](size_t i) -> Q1Key {
        return Q1Key{l_returnflag[i], l_linestatus[i]};
    };

    auto update_agg = [&](Q1Agg& agg, size_t i) {
        double price = l_extendedprice[i];
        double disc = l_discount[i];
        double disc_price = price * (1.0 - disc);

        agg.sum_qty += l_quantity[i];
        agg.sum_base_price += price;
        agg.sum_disc_price += disc_price;
        agg.sum_charge += disc_price * (1.0 + l_tax[i]);
        agg.sum_discount += disc;
        agg.count++;
    };

    auto filter = [&](size_t i) -> bool {
        return l_shipdate[i] < cutoff_date;
    };

    // Perform aggregation
    hash_agg.aggregate(row_count, get_key, update_agg, filter, 4);

    // Sort results by (returnflag, linestatus)
    auto sorted_results = hash_agg.finalize([](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print to terminal: only row count and timing
    std::cout << "Q1: " << sorted_results.size() << " rows in " << std::fixed << std::setprecision(3)
              << elapsed << " seconds" << std::endl;

    // Write results to file if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& [key, agg] : sorted_results) {
            out << char(key.returnflag) << ","
                << char(key.linestatus) << ","
                << agg.sum_qty << ","
                << agg.sum_base_price << ","
                << agg.sum_disc_price << ","
                << agg.sum_charge << ","
                << (agg.sum_qty / agg.count) << ","
                << (agg.sum_base_price / agg.count) << ","
                << (agg.sum_discount / agg.count) << ","
                << agg.count << "\n";
        }
    }
}

} // namespace gendb
