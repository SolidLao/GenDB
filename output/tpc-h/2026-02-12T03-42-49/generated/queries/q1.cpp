#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include <sys/mman.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>

namespace gendb {

void execute_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load needed columns via mmap
    size_t row_count = 0;
    auto l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", row_count);
    size_t tmp;
    auto l_returnflag = mmap_column<uint8_t>(gendb_dir, "lineitem", "l_returnflag", tmp);
    auto l_linestatus = mmap_column<uint8_t>(gendb_dir, "lineitem", "l_linestatus", tmp);
    auto l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", tmp);
    auto l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", tmp);
    auto l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", tmp);
    auto l_tax = mmap_column<double>(gendb_dir, "lineitem", "l_tax", tmp);

    // Load dictionaries
    auto returnflag_dict = load_char_dictionary(gendb_dir + "/lineitem/l_returnflag.dict");
    auto linestatus_dict = load_char_dictionary(gendb_dir + "/lineitem/l_linestatus.dict");

    // Date filter: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02'
    int32_t date_threshold = parse_date("1998-09-02");

    // Aggregation key: (returnflag, linestatus)
    struct AggKey {
        uint8_t returnflag;
        uint8_t linestatus;
        bool operator==(const AggKey& o) const {
            return returnflag == o.returnflag && linestatus == o.linestatus;
        }
    };
    struct AggKeyHash {
        size_t operator()(const AggKey& k) const {
            return (size_t)k.returnflag * 256 + k.linestatus;
        }
    };
    struct AggValue {
        double sum_qty = 0;
        double sum_base_price = 0;
        double sum_disc_price = 0;
        double sum_charge = 0;
        double sum_discount = 0;
        size_t count = 0;
    };

    // Parallel aggregation with thread-local hash tables
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 10000;

    std::vector<std::unordered_map<AggKey, AggValue, AggKeyHash>> local_aggs(num_threads);
    std::atomic<size_t> next_morsel(0);

    auto worker = [&](size_t thread_id) {
        auto& local_agg = local_aggs[thread_id];

        while (true) {
            size_t start_idx = next_morsel.fetch_add(morsel_size);
            if (start_idx >= row_count) break;
            size_t end_idx = std::min(start_idx + morsel_size, row_count);

            for (size_t i = start_idx; i < end_idx; i++) {
                if (l_shipdate[i] <= date_threshold) {
                    AggKey key{l_returnflag[i], l_linestatus[i]};
                    auto& val = local_agg[key];

                    double disc_price = l_extendedprice[i] * (1.0 - l_discount[i]);
                    double charge = disc_price * (1.0 + l_tax[i]);

                    val.sum_qty += l_quantity[i];
                    val.sum_base_price += l_extendedprice[i];
                    val.sum_disc_price += disc_price;
                    val.sum_charge += charge;
                    val.sum_discount += l_discount[i];
                    val.count++;
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) t.join();

    // Merge local aggregations
    std::unordered_map<AggKey, AggValue, AggKeyHash> global_agg;
    for (const auto& local : local_aggs) {
        for (const auto& [key, val] : local) {
            auto& g = global_agg[key];
            g.sum_qty += val.sum_qty;
            g.sum_base_price += val.sum_base_price;
            g.sum_disc_price += val.sum_disc_price;
            g.sum_charge += val.sum_charge;
            g.sum_discount += val.sum_discount;
            g.count += val.count;
        }
    }

    // Sort results by returnflag, linestatus
    std::vector<std::pair<AggKey, AggValue>> results(global_agg.begin(), global_agg.end());
    std::sort(results.begin(), results.end(), [&](const auto& a, const auto& b) {
        if (a.first.returnflag != b.first.returnflag)
            return returnflag_dict[a.first.returnflag] < returnflag_dict[b.first.returnflag];
        return linestatus_dict[a.first.linestatus] < linestatus_dict[b.first.linestatus];
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print results
    std::cout << "Q1: " << results.size() << " rows in " << std::fixed << std::setprecision(3) << elapsed << "s\n";

    // Write to CSV if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& [key, val] : results) {
            out << returnflag_dict.at(key.returnflag) << ","
                << linestatus_dict.at(key.linestatus) << ","
                << val.sum_qty << ","
                << val.sum_base_price << ","
                << val.sum_disc_price << ","
                << val.sum_charge << ","
                << (val.sum_qty / val.count) << ","
                << (val.sum_base_price / val.count) << ","
                << (val.sum_discount / val.count) << ","
                << val.count << "\n";
        }
    }

    // Cleanup mmap
    munmap(l_shipdate, row_count * sizeof(int32_t));
    munmap(l_returnflag, row_count * sizeof(uint8_t));
    munmap(l_linestatus, row_count * sizeof(uint8_t));
    munmap(l_quantity, row_count * sizeof(double));
    munmap(l_extendedprice, row_count * sizeof(double));
    munmap(l_discount, row_count * sizeof(double));
    munmap(l_tax, row_count * sizeof(double));
}

} // namespace gendb
