#include "queries.h"
#include "../storage/storage.h"
#include "../index/index.h"
#include "../utils/date_utils.h"
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <thread>
#include <atomic>
#include <mutex>

namespace gendb {

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
// WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
// GROUP BY l_returnflag, l_linestatus
// ORDER BY l_returnflag, l_linestatus;

struct Q1Aggregates {
    double sum_qty = 0;
    double sum_base_price = 0;
    double sum_disc_price = 0;
    double sum_charge = 0;
    double sum_discount = 0;
    size_t count = 0;
};

void execute_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Read metadata
    size_t row_count = read_row_count(gendb_dir, "lineitem");

    // Mmap only needed columns
    auto l_returnflag = mmap_column<char>(gendb_dir, "lineitem", "l_returnflag", row_count);
    auto l_linestatus = mmap_column<char>(gendb_dir, "lineitem", "l_linestatus", row_count);
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", row_count);
    auto l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", row_count);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", row_count);
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", row_count);
    auto l_tax = mmap_column<double>(gendb_dir, "lineitem", "l_tax", row_count);

    // Date filter: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t cutoff_date = parse_date("1998-09-02");

    // Parallel aggregation with thread-local hash tables
    const int num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::atomic<size_t> next_morsel{0};
    std::vector<std::unordered_map<CompositeKey2, Q1Aggregates>> local_aggs(num_threads);

    std::vector<std::thread> threads;
    for (int tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            auto& local_map = local_aggs[tid];

            while (true) {
                size_t start_idx = next_morsel.fetch_add(morsel_size);
                if (start_idx >= row_count) break;

                size_t end_idx = std::min(start_idx + morsel_size, row_count);

                for (size_t i = start_idx; i < end_idx; ++i) {
                    if (l_shipdate.data[i] <= cutoff_date) {
                        CompositeKey2 key{l_returnflag.data[i], l_linestatus.data[i]};
                        auto& agg = local_map[key];

                        double qty = l_quantity.data[i];
                        double price = l_extendedprice.data[i];
                        double disc = l_discount.data[i];
                        double tax = l_tax.data[i];

                        agg.sum_qty += qty;
                        agg.sum_base_price += price;
                        agg.sum_disc_price += price * (1.0 - disc);
                        agg.sum_charge += price * (1.0 - disc) * (1.0 + tax);
                        agg.sum_discount += disc;
                        agg.count++;
                    }
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    // Merge local aggregates
    std::unordered_map<CompositeKey2, Q1Aggregates> global_agg;
    for (const auto& local : local_aggs) {
        for (const auto& [key, agg] : local) {
            auto& g = global_agg[key];
            g.sum_qty += agg.sum_qty;
            g.sum_base_price += agg.sum_base_price;
            g.sum_disc_price += agg.sum_disc_price;
            g.sum_charge += agg.sum_charge;
            g.sum_discount += agg.sum_discount;
            g.count += agg.count;
        }
    }

    // Sort by returnflag, linestatus
    std::vector<std::pair<CompositeKey2, Q1Aggregates>> sorted_results(global_agg.begin(), global_agg.end());
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) {
                  if (a.first.key1 != b.first.key1) return a.first.key1 < b.first.key1;
                  return a.first.key2 < b.first.key2;
              });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Output results
    std::cout << "Q1 Results: " << sorted_results.size() << " rows in " << std::fixed << std::setprecision(3) << elapsed << "s\n";

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& [key, agg] : sorted_results) {
            out << key.key1 << "," << key.key2 << ","
                << agg.sum_qty << "," << agg.sum_base_price << ","
                << agg.sum_disc_price << "," << agg.sum_charge << ","
                << (agg.sum_qty / agg.count) << ","
                << (agg.sum_base_price / agg.count) << ","
                << (agg.sum_discount / agg.count) << ","
                << agg.count << "\n";
        }
        out.close();
    }
}

} // namespace gendb
