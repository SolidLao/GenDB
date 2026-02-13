/**
 * TPC-H Query 1 — High-Performance Implementation
 *
 * Single-table aggregation over lineitem with date filter.
 * Optimizations:
 *   - Column projection (7 of 16 columns)
 *   - Row group pruning on l_shipdate
 *   - Thread-parallel processing (64 cores)
 *   - Fused filter + compute + aggregate
 *   - Kahan summation for numerical precision
 */

#include "parquet_reader.h"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <cmath>
#include <mutex>

// Aggregation key: (l_returnflag, l_linestatus)
struct GroupKey {
    char returnflag;
    char linestatus;

    bool operator==(const GroupKey& o) const {
        return returnflag == o.returnflag && linestatus == o.linestatus;
    }
};

// Hash function for GroupKey
struct GroupKeyHash {
    std::size_t operator()(const GroupKey& k) const {
        return (static_cast<size_t>(k.returnflag) << 8) | static_cast<size_t>(k.linestatus);
    }
};

// Aggregation values with Kahan summation state
struct AggValues {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;
    int64_t count = 0;

    // Kahan summation compensators
    double c_qty = 0.0;
    double c_base = 0.0;
    double c_disc_price = 0.0;
    double c_charge = 0.0;
    double c_discount = 0.0;

    // Kahan summation: compensated addition
    void add_qty(double val) {
        double y = val - c_qty;
        double t = sum_qty + y;
        c_qty = (t - sum_qty) - y;
        sum_qty = t;
    }

    void add_base_price(double val) {
        double y = val - c_base;
        double t = sum_base_price + y;
        c_base = (t - sum_base_price) - y;
        sum_base_price = t;
    }

    void add_disc_price(double val) {
        double y = val - c_disc_price;
        double t = sum_disc_price + y;
        c_disc_price = (t - sum_disc_price) - y;
        sum_disc_price = t;
    }

    void add_charge(double val) {
        double y = val - c_charge;
        double t = sum_charge + y;
        c_charge = (t - sum_charge) - y;
        sum_charge = t;
    }

    void add_discount(double val) {
        double y = val - c_discount;
        double t = sum_discount + y;
        c_discount = (t - sum_discount) - y;
        sum_discount = t;
    }

    void merge(const AggValues& other) {
        add_qty(other.sum_qty);
        add_base_price(other.sum_base_price);
        add_disc_price(other.sum_disc_price);
        add_charge(other.sum_charge);
        add_discount(other.sum_discount);
        count += other.count;
    }
};

using AggMap = std::unordered_map<GroupKey, AggValues, GroupKeyHash>;

// Process a chunk of rows (thread worker)
void process_chunk(
    int64_t start_row,
    int64_t end_row,
    const std::vector<std::string>& returnflag,
    const std::vector<std::string>& linestatus,
    const double* quantity,
    const double* extendedprice,
    const double* discount,
    const double* tax,
    const int32_t* shipdate,
    int32_t date_threshold,
    AggMap& local_map)
{
    for (int64_t i = start_row; i < end_row; i++) {
        // Filter: l_shipdate <= date_threshold
        if (shipdate[i] > date_threshold) continue;

        // Extract group key
        GroupKey key;
        key.returnflag = returnflag[i].empty() ? ' ' : returnflag[i][0];
        key.linestatus = linestatus[i].empty() ? ' ' : linestatus[i][0];

        // Get or create aggregation entry
        AggValues& agg = local_map[key];

        // Compute derived values
        double qty = quantity[i];
        double price = extendedprice[i];
        double disc = discount[i];
        double tx = tax[i];
        double disc_price = price * (1.0 - disc);
        double charge = disc_price * (1.0 + tx);

        // Accumulate using Kahan summation
        agg.add_qty(qty);
        agg.add_base_price(price);
        agg.add_disc_price(disc_price);
        agg.add_charge(charge);
        agg.add_discount(disc);
        agg.count++;
    }
}

void run_q1(const std::string& parquet_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Date filter: '1998-12-01' - 90 days = '1998-09-02'
    int32_t date_threshold = date_to_days(1998, 9, 2);

    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    // Step 1: Row group pruning on l_shipdate
    auto stats = get_row_group_stats(lineitem_path, "l_shipdate");
    std::vector<int> relevant_rgs;
    for (const auto& s : stats) {
        if (s.has_min_max) {
            // Include row group if max >= threshold (any row could match)
            if (s.max_int >= date_threshold) {
                relevant_rgs.push_back(s.row_group_index);
            }
        } else {
            // No stats, must include
            relevant_rgs.push_back(s.row_group_index);
        }
    }

    std::cout << "Row groups: " << stats.size() << " total, "
              << relevant_rgs.size() << " after pruning on l_shipdate\n";

    // Step 2: Load data (column projection + row group pruning)
    std::vector<std::string> columns = {
        "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice",
        "l_discount", "l_tax", "l_shipdate"
    };

    ParquetTable table;
    if (relevant_rgs.size() == stats.size()) {
        // All row groups needed, read full table
        table = read_parquet(lineitem_path, columns);
    } else {
        // Read only relevant row groups
        table = read_parquet_row_groups(lineitem_path, columns, relevant_rgs);
    }

    int64_t N = table.num_rows;
    std::cout << "Loaded " << N << " rows from lineitem\n";

    // Extract columns
    const auto& returnflag = table.string_column("l_returnflag");
    const auto& linestatus = table.string_column("l_linestatus");
    const double* quantity = table.column<double>("l_quantity");
    const double* extendedprice = table.column<double>("l_extendedprice");
    const double* discount = table.column<double>("l_discount");
    const double* tax = table.column<double>("l_tax");
    const int32_t* shipdate = table.column<int32_t>("l_shipdate");

    // Step 3: Thread-parallel processing
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;

    std::vector<std::thread> threads;
    std::vector<AggMap> thread_maps(num_threads);

    int64_t chunk_size = (N + num_threads - 1) / num_threads;

    for (unsigned int t = 0; t < num_threads; t++) {
        int64_t start_row = t * chunk_size;
        int64_t end_row = std::min(start_row + chunk_size, N);

        if (start_row >= N) break;

        threads.emplace_back(process_chunk,
            start_row, end_row,
            std::ref(returnflag), std::ref(linestatus),
            quantity, extendedprice, discount, tax, shipdate,
            date_threshold,
            std::ref(thread_maps[t]));
    }

    for (auto& th : threads) {
        th.join();
    }

    // Step 4: Merge thread-local results
    AggMap final_map;
    for (const auto& tmap : thread_maps) {
        for (const auto& [key, values] : tmap) {
            final_map[key].merge(values);
        }
    }

    // Step 5: Compute averages and prepare output
    struct OutputRow {
        char returnflag;
        char linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count_order;
    };

    std::vector<OutputRow> results;
    for (const auto& [key, agg] : final_map) {
        OutputRow row;
        row.returnflag = key.returnflag;
        row.linestatus = key.linestatus;
        row.sum_qty = agg.sum_qty;
        row.sum_base_price = agg.sum_base_price;
        row.sum_disc_price = agg.sum_disc_price;
        row.sum_charge = agg.sum_charge;
        row.avg_qty = agg.count > 0 ? agg.sum_qty / agg.count : 0.0;
        row.avg_price = agg.count > 0 ? agg.sum_base_price / agg.count : 0.0;
        row.avg_disc = agg.count > 0 ? agg.sum_discount / agg.count : 0.0;
        row.count_order = agg.count;
        results.push_back(row);
    }

    // Step 6: Sort by returnflag, linestatus
    std::sort(results.begin(), results.end(), [](const OutputRow& a, const OutputRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    // Step 7: Write CSV output
    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open output file: " + output_path);
    }

    out << std::fixed << std::setprecision(2);

    // Header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
        << "sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows
    for (const auto& row : results) {
        out << row.returnflag << ","
            << row.linestatus << ","
            << row.sum_qty << ","
            << row.sum_base_price << ","
            << row.sum_disc_price << ","
            << row.sum_charge << ","
            << row.avg_qty << ","
            << row.avg_price << ","
            << row.avg_disc << ","
            << row.count_order << "\n";
    }

    out.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Q1 completed: " << results.size() << " groups, "
              << duration.count() << " ms\n";
}
