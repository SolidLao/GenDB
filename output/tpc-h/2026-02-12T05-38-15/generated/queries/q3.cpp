#include "queries.h"
#include "../storage/storage.h"
#include "../utils/date_utils.h"
#include "../operators/scan.h"
#include "../operators/hash_join.h"
#include "../operators/hash_agg.h"
#include "../operators/sort.h"

#include <sys/mman.h>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <chrono>
#include <algorithm>
#include <queue>
#include <vector>

namespace gendb {
namespace queries {

// Helper to read string column
static std::vector<std::string> read_string_column(const std::string& file_path, size_t expected_rows) {
    FILE* f = fopen(file_path.c_str(), "rb");
    if (!f) throw std::runtime_error("Failed to open string column: " + file_path);

    std::vector<std::string> result;
    result.reserve(expected_rows);

    while (!feof(f)) {
        uint32_t len;
        if (fread(&len, sizeof(uint32_t), 1, f) != 1) break;
        std::string str(len, '\0');
        fread(&str[0], 1, len, f);
        result.push_back(std::move(str));
    }

    fclose(f);
    return result;
}

struct Q3Key {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const Q3Key& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct Q3KeyHash {
    size_t operator()(const Q3Key& k) const {
        return std::hash<int32_t>()(k.l_orderkey) ^
               (std::hash<int32_t>()(k.o_orderdate) << 1) ^
               (std::hash<int32_t>()(k.o_shippriority) << 2);
    }
};

struct Q3Result {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const Q3Result& other) const {
        if (revenue != other.revenue)
            return revenue < other.revenue; // Min heap for top-K
        return o_orderdate > other.o_orderdate;
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Step 1: Scan and filter customer by c_mktsegment = 'BUILDING'
    // Build semi-join hash table with filtered customer keys
    size_t c_num_rows = storage::read_row_count(gendb_dir + "/customer_metadata.json");
    int32_t* c_custkey = storage::mmap_column<int32_t>(gendb_dir + "/customer_c_custkey.bin", c_num_rows);
    auto c_mktsegment = read_string_column(gendb_dir + "/customer_c_mktsegment.bin", c_num_rows);

    // Use hash join build operator for customer filter
    auto filter_lambda = [&](size_t i) { return c_mktsegment[i] == "BUILDING"; };
    auto key_lambda = [&](size_t i) { return c_custkey[i]; };
    auto payload_lambda = [&](size_t i) { return true; };

    auto customer_filter = operators::hash_join_build<int32_t, bool>(
        c_num_rows,
        filter_lambda,
        key_lambda,
        payload_lambda  // Payload is just a marker
    );
    munmap(c_custkey, c_num_rows * sizeof(int32_t));

    // Step 2: Scan orders and filter by o_orderdate < '1995-03-15' AND customer filter
    // Build hash table: o_orderkey -> (o_orderdate, o_shippriority)
    size_t o_num_rows = storage::read_row_count(gendb_dir + "/orders_metadata.json");
    int32_t* o_orderkey = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_orderkey.bin", o_num_rows);
    int32_t* o_custkey = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_custkey.bin", o_num_rows);
    int32_t* o_orderdate = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_orderdate.bin", o_num_rows);
    int32_t* o_shippriority = storage::mmap_column<int32_t>(gendb_dir + "/orders_o_shippriority.bin", o_num_rows);

    int32_t date_cutoff = date_utils::date_to_days(1995, 3, 15);

    // Use hash join build operator for orders
    auto orders_filter_lambda = [&](size_t i) {
        return o_orderdate[i] < date_cutoff &&
               customer_filter.find(o_custkey[i]) != customer_filter.end();
    };
    auto orders_key_lambda = [&](size_t i) { return o_orderkey[i]; };
    auto orders_payload_lambda = [&](size_t i) { return std::make_pair(o_orderdate[i], o_shippriority[i]); };

    auto orders_ht = operators::hash_join_build<int32_t, std::pair<int32_t, int32_t>>(
        o_num_rows,
        orders_filter_lambda,
        orders_key_lambda,
        orders_payload_lambda
    );

    munmap(o_orderkey, o_num_rows * sizeof(int32_t));
    munmap(o_custkey, o_num_rows * sizeof(int32_t));
    munmap(o_orderdate, o_num_rows * sizeof(int32_t));
    munmap(o_shippriority, o_num_rows * sizeof(int32_t));

    // Step 3: Scan lineitem and join with orders, filter by l_shipdate > '1995-03-15'
    // Use hash join probe with aggregation
    size_t l_num_rows = storage::read_row_count(gendb_dir + "/lineitem_metadata.json");
    int32_t* l_orderkey = storage::mmap_column<int32_t>(gendb_dir + "/lineitem_l_orderkey.bin", l_num_rows);
    int32_t* l_shipdate = storage::mmap_column<int32_t>(gendb_dir + "/lineitem_l_shipdate.bin", l_num_rows);
    double* l_extendedprice = storage::mmap_column<double>(gendb_dir + "/lineitem_l_extendedprice.bin", l_num_rows);
    double* l_discount = storage::mmap_column<double>(gendb_dir + "/lineitem_l_discount.bin", l_num_rows);

    int32_t ship_cutoff = date_utils::date_to_days(1995, 3, 15);

    // Aggregate by (l_orderkey, o_orderdate, o_shippriority)
    std::unordered_map<Q3Key, double, Q3KeyHash> aggregates;

    // Use hash join probe operator with filter
    auto lineitem_filter_lambda = [&](size_t i) { return l_shipdate[i] > ship_cutoff; };
    auto lineitem_key_lambda = [&](size_t i) { return l_orderkey[i]; };
    auto lineitem_emit_lambda = [&](size_t i, const std::pair<int32_t, int32_t>& order_data) {
        Q3Key key{l_orderkey[i], order_data.first, order_data.second};
        double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
        aggregates[key] += revenue;
    };

    operators::hash_join_probe_filtered<int32_t, std::pair<int32_t, int32_t>>(
        l_num_rows,
        orders_ht,
        lineitem_filter_lambda,
        lineitem_key_lambda,
        lineitem_emit_lambda
    );

    munmap(l_orderkey, l_num_rows * sizeof(int32_t));
    munmap(l_shipdate, l_num_rows * sizeof(int32_t));
    munmap(l_extendedprice, l_num_rows * sizeof(double));
    munmap(l_discount, l_num_rows * sizeof(double));

    // Step 4: Top-K selection (LIMIT 10) using operator library
    // Convert aggregates to result vector
    std::vector<Q3Result> all_results;
    all_results.reserve(aggregates.size());
    for (const auto& [key, revenue] : aggregates) {
        all_results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
    }

    // Use top-K operator for efficient selection
    auto results = operators::top_k_selection(
        all_results,
        10,
        [](const Q3Result& a, const Q3Result& b) {
            // Min-heap comparator (smaller elements have higher priority)
            if (a.revenue != b.revenue)
                return a.revenue < b.revenue;
            return a.o_orderdate > b.o_orderdate;
        }
    );

    // Sort final results (descending revenue, ascending date)
    operators::sort_results(results, [](const Q3Result& a, const Q3Result& b) {
        if (a.revenue != b.revenue)
            return a.revenue > b.revenue; // Descending revenue
        return a.o_orderdate < b.o_orderdate; // Ascending date
    });

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    // Print results
    std::cout << "Q3: " << results.size() << " rows in " << std::fixed << std::setprecision(3)
              << elapsed << "s" << std::endl;

    // Write to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        out << std::fixed << std::setprecision(2);
        for (const auto& r : results) {
            out << r.l_orderkey << "," << r.revenue << ","
                << date_utils::days_to_date_str(r.o_orderdate) << ","
                << r.o_shippriority << "\n";
        }
    }
}

} // namespace queries
} // namespace gendb
