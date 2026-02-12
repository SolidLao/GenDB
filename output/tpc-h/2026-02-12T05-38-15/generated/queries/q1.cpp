#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"
#include "../operators/hash_agg.h"
#include "../operators/sort.h"

#include <sys/mman.h>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>
#include <algorithm>

namespace gendb {
namespace queries {

struct Q1Key {
    char returnflag;
    char linestatus;

    bool operator==(const Q1Key& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

struct Q1KeyHash {
    size_t operator()(const Q1Key& k) const {
        return (size_t)k.returnflag * 256 + (size_t)k.linestatus;
    }
};

struct Q1Agg {
    double sum_qty = 0;
    double sum_base_price = 0;
    double sum_disc_price = 0;
    double sum_charge = 0;
    double sum_discount = 0;
    size_t count = 0;

    // Support merging for parallel aggregation
    void merge(const Q1Agg& other) {
        sum_qty += other.sum_qty;
        sum_base_price += other.sum_base_price;
        sum_disc_price += other.sum_disc_price;
        sum_charge += other.sum_charge;
        sum_discount += other.sum_discount;
        count += other.count;
    }
};

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load only needed columns via mmap
    size_t num_rows;
    char* l_returnflag = storage::mmap_column<char>(gendb_dir + "/lineitem_l_returnflag.bin", num_rows);
    char* l_linestatus = storage::mmap_column<char>(gendb_dir + "/lineitem_l_linestatus.bin", num_rows);
    int32_t* l_shipdate = storage::mmap_column<int32_t>(gendb_dir + "/lineitem_l_shipdate.bin", num_rows);
    double* l_quantity = storage::mmap_column<double>(gendb_dir + "/lineitem_l_quantity.bin", num_rows);
    double* l_extendedprice = storage::mmap_column<double>(gendb_dir + "/lineitem_l_extendedprice.bin", num_rows);
    double* l_discount = storage::mmap_column<double>(gendb_dir + "/lineitem_l_discount.bin", num_rows);
    double* l_tax = storage::mmap_column<double>(gendb_dir + "/lineitem_l_tax.bin", num_rows);

    // Filter date: l_shipdate <= '1998-09-02' (90 days before 1998-12-01)
    int32_t cutoff_date = date_utils::date_to_days(1998, 9, 2);

    // Parallel aggregation using operator library
    unsigned int num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<Q1Key, Q1Agg, Q1KeyHash>> local_aggs(num_threads);

    // Use parallel scan operator
    operators::parallel_scan(num_rows, [&](size_t thread_id, size_t start_row, size_t end_row) {
        auto& agg_map = local_aggs[thread_id];

        for (size_t i = start_row; i < end_row; ++i) {
            if (l_shipdate[i] <= cutoff_date) {
                Q1Key key{l_returnflag[i], l_linestatus[i]};
                auto& agg = agg_map[key];

                double qty = l_quantity[i];
                double price = l_extendedprice[i];
                double disc = l_discount[i];
                double tax = l_tax[i];
                double disc_price = price * (1.0 - disc);

                agg.sum_qty += qty;
                agg.sum_base_price += price;
                agg.sum_disc_price += disc_price;
                agg.sum_charge += disc_price * (1.0 + tax);
                agg.sum_discount += disc;
                agg.count++;
            }
        }
    });

    // Merge thread-local results
    std::unordered_map<Q1Key, Q1Agg, Q1KeyHash> global_agg;
    for (const auto& local : local_aggs) {
        for (const auto& [key, agg] : local) {
            auto it = global_agg.find(key);
            if (it == global_agg.end()) {
                global_agg[key] = agg;
            } else {
                it->second.merge(agg);
            }
        }
    }

    // Sort by returnflag, linestatus using operator library
    std::vector<std::pair<Q1Key, Q1Agg>> results(global_agg.begin(), global_agg.end());
    operators::sort_results(results, [](const auto& a, const auto& b) {
        if (a.first.returnflag != b.first.returnflag)
            return a.first.returnflag < b.first.returnflag;
        return a.first.linestatus < b.first.linestatus;
    });

    // Cleanup mmap
    munmap(l_returnflag, num_rows * sizeof(char));
    munmap(l_linestatus, num_rows * sizeof(char));
    munmap(l_shipdate, num_rows * sizeof(int32_t));
    munmap(l_quantity, num_rows * sizeof(double));
    munmap(l_extendedprice, num_rows * sizeof(double));
    munmap(l_discount, num_rows * sizeof(double));
    munmap(l_tax, num_rows * sizeof(double));

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print results
    std::cout << "Q1: " << results.size() << " rows in " << std::fixed << std::setprecision(3)
              << elapsed << "s" << std::endl;

    // Write to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        out << std::fixed << std::setprecision(2);
        for (const auto& [key, agg] : results) {
            out << key.returnflag << "," << key.linestatus << ","
                << agg.sum_qty << "," << agg.sum_base_price << ","
                << agg.sum_disc_price << "," << agg.sum_charge << ","
                << (agg.sum_qty / agg.count) << ","
                << (agg.sum_base_price / agg.count) << ","
                << (agg.sum_discount / agg.count) << ","
                << agg.count << "\n";
        }
    }
}

} // namespace queries
} // namespace gendb
