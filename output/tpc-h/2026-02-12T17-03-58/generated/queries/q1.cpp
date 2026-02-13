#include "queries.h"
#include "../storage/storage.h"
#include "../operators/hash_agg.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <cstring>

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
// WHERE l_shipdate <= DATE '1998-09-02'  -- Adjusted to '1998-12-01' - 90 days
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus

struct Q1Key {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const Q1Key& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

namespace std {
    template<>
    struct hash<Q1Key> {
        size_t operator()(const Q1Key& k) const {
            return (static_cast<size_t>(k.returnflag) << 8) | k.linestatus;
        }
    };
}

struct Q1Agg {
    int64_t sum_qty = 0;
    int64_t sum_base_price = 0;
    int64_t sum_disc_price = 0;
    int64_t sum_charge = 0;
    int64_t sum_discount = 0;
    uint64_t count = 0;
};

void execute_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Read metadata
    auto metadata = Storage::readMetadata(gendb_dir, "lineitem");
    uint64_t row_count = metadata.row_count;

    // Load needed columns
    auto l_shipdate = Storage::mmapColumn(gendb_dir, "lineitem", "l_shipdate", "int32");
    auto l_returnflag = Storage::mmapColumn(gendb_dir, "lineitem", "l_returnflag", "uint8");
    auto l_linestatus = Storage::mmapColumn(gendb_dir, "lineitem", "l_linestatus", "uint8");
    auto l_quantity = Storage::mmapColumn(gendb_dir, "lineitem", "l_quantity", "int64");
    auto l_extendedprice = Storage::mmapColumn(gendb_dir, "lineitem", "l_extendedprice", "int64");
    auto l_discount = Storage::mmapColumn(gendb_dir, "lineitem", "l_discount", "int64");
    auto l_tax = Storage::mmapColumn(gendb_dir, "lineitem", "l_tax", "int64");

    const int32_t* shipdate_data = l_shipdate->as<int32_t>();
    const uint8_t* returnflag_data = l_returnflag->as<uint8_t>();
    const uint8_t* linestatus_data = l_linestatus->as<uint8_t>();
    const int64_t* quantity_data = l_quantity->as<int64_t>();
    const int64_t* extendedprice_data = l_extendedprice->as<int64_t>();
    const int64_t* discount_data = l_discount->as<int64_t>();
    const int64_t* tax_data = l_tax->as<int64_t>();

    // Filter date: '1998-12-01' - 90 days = '1998-09-02'
    int32_t max_shipdate = DateUtils::stringToDate("1998-09-02");

    // Aggregation
    HashAgg<Q1Key, Q1Agg> agg;

    for (uint64_t row = 0; row < row_count; ++row) {
        if (shipdate_data[row] <= max_shipdate) {
            Q1Key key{returnflag_data[row], linestatus_data[row]};

            agg.update(key, [&](Q1Agg& state) {
                int64_t qty = quantity_data[row];
                int64_t price = extendedprice_data[row];
                int64_t disc = discount_data[row];
                int64_t tax_val = tax_data[row];

                state.sum_qty += qty;
                state.sum_base_price += price;
                // disc_price = price * (1 - discount) = price * (100 - discount) / 100
                int64_t disc_price = (price * (10000 - disc)) / 10000;
                state.sum_disc_price += disc_price;
                // charge = disc_price * (1 + tax) = disc_price * (100 + tax) / 100
                int64_t charge = (disc_price * (10000 + tax_val)) / 10000;
                state.sum_charge += charge;
                state.sum_discount += disc;
                state.count++;
            });
        }
    }

    // Get sorted results
    auto results = agg.get_sorted_results([&](const auto& a, const auto& b) {
        if (a.first.returnflag != b.first.returnflag)
            return a.first.returnflag < b.first.returnflag;
        return a.first.linestatus < b.first.linestatus;
    });

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Output results
    std::cout << "Q1: " << results.size() << " rows, " << duration.count() << " ms" << std::endl;

    // Optionally write to CSV
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/q1_results.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& [key, val] : results) {
            std::string returnflag_str = metadata.dictionaries.at("l_returnflag").decode(key.returnflag);
            std::string linestatus_str = metadata.dictionaries.at("l_linestatus").decode(key.linestatus);

            out << returnflag_str << ","
                << linestatus_str << ","
                << std::fixed << std::setprecision(2) << (val.sum_qty / 100.0) << ","
                << (val.sum_base_price / 100.0) << ","
                << (val.sum_disc_price / 100.0) << ","
                << (val.sum_charge / 100.0) << ","
                << (val.sum_qty / 100.0 / val.count) << ","
                << (val.sum_base_price / 100.0 / val.count) << ","
                << (val.sum_discount / 100.0 / val.count) << ","
                << val.count << "\n";
        }
        out.close();
    }
}
