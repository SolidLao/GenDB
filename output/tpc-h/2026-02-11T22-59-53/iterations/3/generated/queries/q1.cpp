#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/hash_agg.h"
#include <iostream>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <unordered_map>
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>

// Q1: Pricing Summary Report
// SELECT l_returnflag, l_linestatus,
//        SUM(l_quantity), SUM(l_extendedprice),
//        SUM(l_extendedprice * (1 - l_discount)),
//        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//        AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
//        COUNT(*)
// FROM lineitem
// WHERE l_shipdate <= '1998-09-02'
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus

struct Q1GroupKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const Q1GroupKey& o) const {
        return returnflag == o.returnflag && linestatus == o.linestatus;
    }
};

namespace std {
    template<>
    struct hash<Q1GroupKey> {
        size_t operator()(const Q1GroupKey& k) const {
            return (size_t(k.returnflag) << 8) | size_t(k.linestatus);
        }
    };
}

struct Q1Agg {
    double sum_qty = 0;
    double sum_base_price = 0;
    double sum_disc_price = 0;
    double sum_charge = 0;
    double sum_discount = 0;
    int64_t count = 0;
};

void execute_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load columns
    size_t row_count = get_row_count(gendb_dir, "lineitem");

    size_t size;
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", size);
    uint8_t* l_returnflag = mmap_column<uint8_t>(gendb_dir, "lineitem", "l_returnflag", size);
    uint8_t* l_linestatus = mmap_column<uint8_t>(gendb_dir, "lineitem", "l_linestatus", size);
    double* l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", size);
    double* l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", size);
    double* l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", size);
    double* l_tax = mmap_column<double>(gendb_dir, "lineitem", "l_tax", size);

    // Date threshold: 1998-12-01 - 90 days = 1998-09-02
    int32_t date_threshold = date_utils::date_to_days(1998, 9, 2);

    // Use parallel hash aggregation operator with predicate pushdown
    // Pre-size for 4-6 groups (Q1 has very low cardinality: returnflag x linestatus)
    auto global_agg = gendb::operators::parallel_hash_aggregate_filtered<Q1GroupKey, Q1Agg>(
        row_count,
        // Predicate: filter by shipdate
        [&](size_t i) {
            return l_shipdate[i] <= date_threshold;
        },
        // Group key function
        [&](size_t i) {
            return Q1GroupKey{l_returnflag[i], l_linestatus[i]};
        },
        // Aggregate function: update running aggregates
        [&](size_t i, Q1Agg& agg) {
            double qty = l_quantity[i];
            double price = l_extendedprice[i];
            double disc = l_discount[i];
            double tx = l_tax[i];

            agg.sum_qty += qty;
            agg.sum_base_price += price;
            agg.sum_disc_price += price * (1.0 - disc);
            agg.sum_charge += price * (1.0 - disc) * (1.0 + tx);
            agg.sum_discount += disc;
            agg.count++;
        },
        // Merge function: combine local aggregates
        [](Q1Agg& dest, const Q1Agg& src) {
            dest.sum_qty += src.sum_qty;
            dest.sum_base_price += src.sum_base_price;
            dest.sum_disc_price += src.sum_disc_price;
            dest.sum_charge += src.sum_charge;
            dest.sum_discount += src.sum_discount;
            dest.count += src.count;
        },
        0,  // num_threads (0 = auto-detect)
        6   // reserve_size (pre-size for 6 groups to avoid rehashing)
    );

    // Sort by returnflag, linestatus
    std::vector<std::pair<Q1GroupKey, Q1Agg>> results(global_agg.begin(), global_agg.end());
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.first.returnflag != b.first.returnflag)
            return a.first.returnflag < b.first.returnflag;
        return a.first.linestatus < b.first.linestatus;
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Q1: " << results.size() << " rows in " << elapsed << "s" << std::endl;

    // Write results if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& [key, agg] : results) {
            out << static_cast<char>(key.returnflag) << ","
                << static_cast<char>(key.linestatus) << ","
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
