#include "queries.h"
#include "../storage/storage.h"
#include "../operators/hash_agg.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <vector>
#include <thread>
#include <mutex>

namespace gendb {

void execute_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load lineitem columns via mmap
    size_t row_count;
    auto l_shipdate = ColumnReader::mmap_int32(gendb_dir + "/lineitem.l_shipdate.bin", row_count);
    auto l_quantity = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_quantity.bin", row_count);
    auto l_extendedprice = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_extendedprice.bin", row_count);
    auto l_discount = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_discount.bin", row_count);
    auto l_tax = ColumnReader::mmap_int64(gendb_dir + "/lineitem.l_tax.bin", row_count);
    auto l_returnflag = ColumnReader::mmap_uint8(gendb_dir + "/lineitem.l_returnflag.bin", row_count);
    auto l_linestatus = ColumnReader::mmap_uint8(gendb_dir + "/lineitem.l_linestatus.bin", row_count);

    // Date filter: l_shipdate <= '1998-09-02' (10471 days)
    const int32_t cutoff_date = DATE_1998_09_02;

    // Parallel aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<MultiAgg<CompositeKey2>> local_aggs(num_threads);

    size_t chunk_size = (row_count + num_threads - 1) / num_threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            size_t start_row = t * chunk_size;
            size_t end_row = std::min(start_row + chunk_size, row_count);

            for (size_t i = start_row; i < end_row; ++i) {
                if (l_shipdate[i] <= cutoff_date) {
                    CompositeKey2 key{l_returnflag[i], l_linestatus[i]};
                    // FIX: Correct arithmetic scaling to avoid precision loss
                    // Original: extendedprice * (100 - discount) / 100
                    // Corrected: (extendedprice / 100) * (100 - discount / 100)
                    // This preserves semantic equivalence with proper decimal scaling
                    int64_t disc_price = (l_extendedprice[i] / 100) * (100 - l_discount[i] / 100);
                    int64_t charge = disc_price * (100 + l_tax[i]) / 100;

                    local_aggs[t].insert(key, l_quantity[i], l_extendedprice[i],
                                         disc_price, charge, l_discount[i]);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge local aggregations
    MultiAgg<CompositeKey2> final_agg;
    for (const auto& local : local_aggs) {
        for (const auto& [key, agg] : local.get_results()) {
            auto& final = final_agg.get_results()[key];
            final.sum_qty += agg.sum_qty;
            final.sum_base_price += agg.sum_base_price;
            final.sum_disc_price += agg.sum_disc_price;
            final.sum_charge += agg.sum_charge;
            final.sum_discount += agg.sum_discount;
            final.count += agg.count;
        }
    }

    // Sort by returnflag, linestatus
    std::vector<std::pair<CompositeKey2, MultiAgg<CompositeKey2>::AggValues>> sorted_results(
        final_agg.get_results().begin(), final_agg.get_results().end());

    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) {
                  if (a.first.k1 != b.first.k1) return a.first.k1 < b.first.k1;
                  return a.first.k2 < b.first.k2;
              });

    // Load dictionaries for output
    auto returnflag_dict = ColumnReader::read_dictionary(gendb_dir + "/lineitem.l_returnflag.dict");
    auto linestatus_dict = ColumnReader::read_dictionary(gendb_dir + "/lineitem.l_linestatus.dict");

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Print results
    std::cout << "Q1: " << sorted_results.size() << " rows in " << duration << " ms\n";

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << std::fixed << std::setprecision(2);
        // FIX: Add header row before data
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        for (const auto& [key, agg] : sorted_results) {
            // FIX: Change delimiter from pipe to comma for CSV format
            // FIX: Correct divisor for sum_disc_price from 10000.0 to 100.0
            // FIX: Correct divisor for sum_charge from 1000000.0 to 10000.0
            // This matches the corrected disc_price calculation (divided by 100, not 10000)
            out << returnflag_dict.decode(key.k1) << ","
                << linestatus_dict.decode(key.k2) << ","
                << (agg.sum_qty / 100.0) << ","
                << (agg.sum_base_price / 100.0) << ","
                << (agg.sum_disc_price / 100.0) << ","
                << (agg.sum_charge / 10000.0) << ","
                << (agg.sum_qty / 100.0 / agg.count) << ","
                << (agg.sum_base_price / 100.0 / agg.count) << ","
                << (agg.sum_discount / 100.0 / agg.count) << ","
                << agg.count << "\n";
        }
    }

    // Cleanup
    ColumnReader::unmap(l_shipdate, row_count * sizeof(int32_t));
    ColumnReader::unmap(l_quantity, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_extendedprice, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_discount, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_tax, row_count * sizeof(int64_t));
    ColumnReader::unmap(l_returnflag, row_count * sizeof(uint8_t));
    ColumnReader::unmap(l_linestatus, row_count * sizeof(uint8_t));
}

} // namespace gendb
