#include "queries.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstdio>

namespace gendb {

// Q1: Pricing Summary Report
// SELECT l_returnflag, l_linestatus, SUM(l_quantity), SUM(l_extendedprice),
//        SUM(l_extendedprice * (1 - l_discount)), SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
//        AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
// FROM lineitem
// WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus

void execute_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load columns
    const int32_t* l_shipdate;
    const double* l_quantity;
    const double* l_extendedprice;
    const double* l_discount;
    const double* l_tax;
    const char* l_returnflag;
    const char* l_linestatus;
    size_t count;

    load_lineitem_columns_q1(gendb_dir, &l_shipdate, &l_quantity, &l_extendedprice,
                              &l_discount, &l_tax, &l_returnflag, &l_linestatus, count);

    // Date filter: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t cutoff_date = parse_date("1998-09-02");

    // Parallel aggregation with thread-local hash tables
    int num_threads = std::min(16, (int)std::thread::hardware_concurrency());
    std::vector<Q1AggMap> thread_local_maps(num_threads);

    auto worker = [&](int thread_id) {
        size_t chunk_size = (count + num_threads - 1) / num_threads;
        size_t start = thread_id * chunk_size;
        size_t end = std::min(start + chunk_size, count);

        Q1AggMap& local_map = thread_local_maps[thread_id];

        for (size_t i = start; i < end; ++i) {
            if (l_shipdate[i] <= cutoff_date) {
                Q1GroupKey key{l_returnflag[i], l_linestatus[i]};
                Q1AggValue& agg = local_map[key];

                double qty = l_quantity[i];
                double price = l_extendedprice[i];
                double disc = l_discount[i];
                double tax_val = l_tax[i];

                agg.sum_qty += qty;
                agg.sum_base_price += price;
                agg.sum_disc_price += price * (1.0 - disc);
                agg.sum_charge += price * (1.0 - disc) * (1.0 + tax_val);
                agg.sum_discount += disc;
                agg.count++;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }
    for (auto& th : threads) {
        th.join();
    }

    // Merge thread-local results
    Q1AggMap final_map;
    for (const auto& local_map : thread_local_maps) {
        for (const auto& [key, val] : local_map) {
            Q1AggValue& agg = final_map[key];
            agg.sum_qty += val.sum_qty;
            agg.sum_base_price += val.sum_base_price;
            agg.sum_disc_price += val.sum_disc_price;
            agg.sum_charge += val.sum_charge;
            agg.sum_discount += val.sum_discount;
            agg.count += val.count;
        }
    }

    // Sort by returnflag, linestatus
    std::vector<std::pair<Q1GroupKey, Q1AggValue>> results(final_map.begin(), final_map.end());
    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.first.l_returnflag != b.first.l_returnflag)
            return a.first.l_returnflag < b.first.l_returnflag;
        return a.first.l_linestatus < b.first.l_linestatus;
    });

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();

    // Write results if requested
    if (!results_dir.empty()) {
        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (f) {
            fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
            for (const auto& [key, val] : results) {
                fprintf(f, "%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.4f,%zu\n",
                        key.l_returnflag, key.l_linestatus,
                        val.sum_qty, val.sum_base_price, val.sum_disc_price, val.sum_charge,
                        val.sum_qty / val.count, val.sum_base_price / val.count,
                        val.sum_discount / val.count, val.count);
            }
            fclose(f);
        }
    }

    // Print summary
    std::cout << "Q1: " << results.size() << " rows in " << std::fixed << std::setprecision(3)
              << elapsed << "s" << std::endl;
}

} // namespace gendb
