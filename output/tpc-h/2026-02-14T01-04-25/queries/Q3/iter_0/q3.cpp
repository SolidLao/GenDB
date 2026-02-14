#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <queue>

// ============================================================================
// Constants
// ============================================================================
const int32_t DATE_1995_03_15 = 9204;  // days since 1970-01-01
const int32_t MORSEL_SIZE = 100000;    // tuples per morsel for parallel execution
const int LIMIT_K = 10;                 // TOP-K result size

// ============================================================================
// Encoding Helpers
// ============================================================================

// Parse dictionary file (format: "column::value")
std::unordered_map<uint8_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) return dict;

    std::string line;
    uint8_t code = 0;
    while (std::getline(f, line)) {
        size_t pos = line.find("::");
        if (pos != std::string::npos) {
            dict[code] = line.substr(pos + 2);
        }
        code++;
    }
    return dict;
}

// Decode delta-encoded column (cumulative sum)
void decode_delta(const int32_t* encoded, int32_t* decoded, int32_t count) {
    if (count == 0) return;
    decoded[0] = encoded[0];
    for (int32_t i = 1; i < count; ++i) {
        decoded[i] = decoded[i - 1] + encoded[i];
    }
}

// Convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t days) {
    // Simplified: days since 1970-01-01
    int32_t z = days + 719468;  // Epoch adjustment
    int32_t era = z / 146097;
    int32_t doe = z % 146097;
    int32_t yoe = (doe - doe / 1461 + doe / 36524 - doe / 146096) / 365;
    int32_t y = yoe + era * 400;
    int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int32_t mp = (5 * doy + 2) / 153;
    int32_t d = doy - (153 * mp + 2) / 5 + 1;
    int32_t m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2 ? 1 : 0);

    char buf[20];
    snprintf(buf, 20, "%04d-%02d-%02d", (int)y, (int)m, (int)d);
    return std::string(buf);
}

// ============================================================================
// Data Structures
// ============================================================================

struct OrdersRow {
    int32_t o_orderkey;
    int32_t o_custkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct LineitemRow {
    int32_t l_orderkey;
    double l_extendedprice;
    double l_discount;
    int32_t l_shipdate;
};

struct AggResult {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator>(const AggResult& other) const {
        if (std::fabs(revenue - other.revenue) > 1e-6)
            return revenue < other.revenue;  // min-heap: smaller revenues at top
        return o_orderdate > other.o_orderdate;
    }
};

// Fast hash table for join
struct HashJoinEntry {
    int32_t key;      // o_custkey or o_orderkey
    int32_t orderkey; // o_orderkey (for orders join) or -1 (for customer)
    int32_t orderdate;
    int32_t shippriority;
    bool occupied;
};

// Hash aggregation entry
struct AggEntry {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double sum_revenue;
    bool occupied;
};

// ============================================================================
// Parallel Hash Join: Customer -> Orders
// ============================================================================

struct FilteredOrders {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<int32_t> o_orderdate;
    std::vector<int32_t> o_shippriority;
};

FilteredOrders filter_orders(const std::string& gendb_dir, int32_t threshold_date) {
    FilteredOrders result;

    std::string orders_dir = gendb_dir + "/orders";
    std::ifstream o_orderkey_file(orders_dir + "/o_orderkey.bin", std::ios::binary);
    std::ifstream o_custkey_file(orders_dir + "/o_custkey.bin", std::ios::binary);
    std::ifstream o_orderdate_file(orders_dir + "/o_orderdate.bin", std::ios::binary);
    std::ifstream o_shippriority_file(orders_dir + "/o_shippriority.bin", std::ios::binary);

    if (!o_orderkey_file || !o_custkey_file || !o_orderdate_file || !o_shippriority_file) {
        std::cerr << "Error: Could not open orders files\n";
        return result;
    }

    // Read o_orderkey
    std::vector<int32_t> orderkeys;
    int32_t val;
    while (o_orderkey_file.read((char*)&val, sizeof(int32_t))) {
        orderkeys.push_back(val);
    }
    int32_t num_orders = orderkeys.size();

    // Read and decode o_orderdate (delta-encoded)
    std::vector<int32_t> encoded_orderdate(num_orders);
    o_orderdate_file.read((char*)encoded_orderdate.data(), num_orders * sizeof(int32_t));
    std::vector<int32_t> decoded_orderdate(num_orders);
    decode_delta(encoded_orderdate.data(), decoded_orderdate.data(), num_orders);

    // Read o_custkey
    std::vector<int32_t> custkeys(num_orders);
    o_custkey_file.read((char*)custkeys.data(), num_orders * sizeof(int32_t));

    // Read o_shippriority
    std::vector<int32_t> shippriorities(num_orders);
    o_shippriority_file.read((char*)shippriorities.data(), num_orders * sizeof(int32_t));

    // Filter: o_orderdate < threshold
    for (int32_t i = 0; i < num_orders; ++i) {
        if (decoded_orderdate[i] < threshold_date) {
            result.o_orderkey.push_back(orderkeys[i]);
            result.o_custkey.push_back(custkeys[i]);
            result.o_orderdate.push_back(decoded_orderdate[i]);
            result.o_shippriority.push_back(shippriorities[i]);
        }
    }

    return result;
}

struct FilteredCustomers {
    std::vector<int32_t> c_custkey;
};

FilteredCustomers filter_customers(const std::string& gendb_dir, const std::string& target_mktsegment) {
    FilteredCustomers result;

    // Load dictionary
    std::string customer_dir = gendb_dir + "/customer";
    std::string dicts_path = customer_dir + "/customer_dicts.txt";
    auto dict = load_dictionary(dicts_path);

    // Find code for 'BUILDING'
    uint8_t building_code = 255;
    for (auto& p : dict) {
        if (p.second == target_mktsegment) {
            building_code = p.first;
            break;
        }
    }

    // Read c_custkey and c_mktsegment
    std::ifstream c_custkey_file(customer_dir + "/c_custkey.bin", std::ios::binary);
    std::ifstream c_mktsegment_file(customer_dir + "/c_mktsegment.bin", std::ios::binary);

    if (!c_custkey_file || !c_mktsegment_file) {
        std::cerr << "Error: Could not open customer files\n";
        return result;
    }

    int32_t custkey;
    uint8_t mktsegment_code;
    while (c_custkey_file.read((char*)&custkey, sizeof(int32_t)) &&
           c_mktsegment_file.read((char*)&mktsegment_code, sizeof(uint8_t))) {
        if (mktsegment_code == building_code) {
            result.c_custkey.push_back(custkey);
        }
    }

    return result;
}

// Build hash table on customer keys (smaller set, ~330K rows)
std::unordered_map<int32_t, int32_t> build_customer_hash(
    const std::vector<int32_t>& c_custkeylist) {
    std::unordered_map<int32_t, int32_t> ht;
    for (auto key : c_custkeylist) {
        ht[key] = 1;  // Dummy value: customer exists
    }
    return ht;
}

// ============================================================================
// Parallel Lineitem Scan & Join & Aggregation
// ============================================================================

struct ThreadLocalAggregation {
    std::unordered_map<int64_t, std::pair<int32_t, double>> agg;  // key -> (count, sum_revenue)
};

void scan_lineitem_and_join(
    const std::string& gendb_dir,
    const FilteredOrders& orders,
    const std::unordered_map<int32_t, int32_t>& cust_hash,
    int32_t threshold_shipdate,
    std::unordered_map<int64_t, AggEntry>& global_agg,
    std::mutex& agg_mutex) {

    std::string lineitem_dir = gendb_dir + "/lineitem";

    // Read all lineitem columns
    std::ifstream l_orderkey_file(lineitem_dir + "/l_orderkey.bin", std::ios::binary);
    std::ifstream l_extendedprice_file(lineitem_dir + "/l_extendedprice.bin", std::ios::binary);
    std::ifstream l_discount_file(lineitem_dir + "/l_discount.bin", std::ios::binary);
    std::ifstream l_shipdate_file(lineitem_dir + "/l_shipdate.bin", std::ios::binary);

    // Read l_shipdate (delta-encoded)
    std::vector<int32_t> encoded_shipdate;
    int32_t val;
    while (l_shipdate_file.read((char*)&val, sizeof(int32_t))) {
        encoded_shipdate.push_back(val);
    }
    int32_t num_lineitem = encoded_shipdate.size();
    std::vector<int32_t> decoded_shipdate(num_lineitem);
    decode_delta(encoded_shipdate.data(), decoded_shipdate.data(), num_lineitem);

    // Read other columns
    std::vector<int32_t> orderkeys(num_lineitem);
    l_orderkey_file.read((char*)orderkeys.data(), num_lineitem * sizeof(int32_t));

    std::vector<double> extendedprices(num_lineitem);
    l_extendedprice_file.read((char*)extendedprices.data(), num_lineitem * sizeof(double));

    std::vector<double> discounts(num_lineitem);
    l_discount_file.read((char*)discounts.data(), num_lineitem * sizeof(double));

    // Build hash table on orders (key: o_orderkey, value: (o_orderdate, o_shippriority))
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orders_hash;
    for (size_t i = 0; i < orders.o_orderkey.size(); ++i) {
        orders_hash[orders.o_orderkey[i]] = {orders.o_orderdate[i], orders.o_shippriority[i]};
    }

    // Scan lineitem, filter, and aggregate
    std::unordered_map<int64_t, AggEntry> local_agg;

    for (int32_t i = 0; i < num_lineitem; ++i) {
        // Filter: l_shipdate > threshold
        if (decoded_shipdate[i] <= threshold_shipdate) continue;

        int32_t l_orderkey = orderkeys[i];

        // Join with orders
        auto orders_it = orders_hash.find(l_orderkey);
        if (orders_it == orders_hash.end()) continue;

        int32_t o_orderdate = orders_it->second.first;
        int32_t o_shippriority = orders_it->second.second;

        // Compute revenue
        double revenue = extendedprices[i] * (1.0 - discounts[i]);

        // Group key: (l_orderkey, o_orderdate, o_shippriority)
        int64_t group_key = ((int64_t)l_orderkey << 32) | (o_shippriority & 0xFFFF);  // simplified encoding

        if (local_agg.find(group_key) == local_agg.end()) {
            local_agg[group_key] = {l_orderkey, o_orderdate, o_shippriority, revenue};
        } else {
            local_agg[group_key].sum_revenue += revenue;
        }
    }

    // Merge into global aggregation
    {
        std::lock_guard<std::mutex> lock(agg_mutex);
        for (auto& p : local_agg) {
            if (global_agg.find(p.first) == global_agg.end()) {
                global_agg[p.first] = p.second;
            } else {
                global_agg[p.first].sum_revenue += p.second.sum_revenue;
            }
        }
    }
}

// ============================================================================
// Top-K Selection using Priority Queue
// ============================================================================

std::vector<AggResult> select_topk(
    const std::unordered_map<int64_t, AggEntry>& agg_table,
    int limit_k) {

    // Min-heap (greater comparator for min-heap behavior)
    std::priority_queue<AggResult, std::vector<AggResult>, std::greater<AggResult>> topk_heap;

    for (auto& p : agg_table) {
        AggResult res = {
            p.second.l_orderkey,
            p.second.sum_revenue,
            p.second.o_orderdate,
            p.second.o_shippriority
        };

        if ((int)topk_heap.size() < limit_k) {
            topk_heap.push(res);
        } else if (res.revenue > topk_heap.top().revenue) {
            topk_heap.pop();
            topk_heap.push(res);
        }
    }

    // Extract results (will be in reverse order)
    std::vector<AggResult> results;
    while (!topk_heap.empty()) {
        results.push_back(topk_heap.top());
        topk_heap.pop();
    }
    std::reverse(results.begin(), results.end());

    return results;
}

// ============================================================================
// Main Query Function
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Step 1: Filter customers on c_mktsegment = 'BUILDING'
    FilteredCustomers filtered_cust = filter_customers(gendb_dir, "BUILDING");

    // Step 2: Filter orders on o_orderdate < 1995-03-15
    FilteredOrders filtered_orders = filter_orders(gendb_dir, DATE_1995_03_15);

    // Verify join: filter orders by customer keys
    std::unordered_map<int32_t, int32_t> cust_hash = build_customer_hash(filtered_cust.c_custkey);

    FilteredOrders joined_orders;
    for (size_t i = 0; i < filtered_orders.o_orderkey.size(); ++i) {
        if (cust_hash.find(filtered_orders.o_custkey[i]) != cust_hash.end()) {
            joined_orders.o_orderkey.push_back(filtered_orders.o_orderkey[i]);
            joined_orders.o_custkey.push_back(filtered_orders.o_custkey[i]);
            joined_orders.o_orderdate.push_back(filtered_orders.o_orderdate[i]);
            joined_orders.o_shippriority.push_back(filtered_orders.o_shippriority[i]);
        }
    }

    // Step 3: Scan lineitem, filter, join with orders, and aggregate
    std::unordered_map<int64_t, AggEntry> global_agg;
    std::mutex agg_mutex;

    scan_lineitem_and_join(gendb_dir, joined_orders, cust_hash, DATE_1995_03_15, global_agg, agg_mutex);

    // Step 4: Select TOP-K
    std::vector<AggResult> topk_results = select_topk(global_agg, LIMIT_K);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Step 5: Output results
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (auto& res : topk_results) {
            out << res.l_orderkey << ","
                << std::fixed << std::setprecision(2) << res.revenue << ","
                << format_date(res.o_orderdate) << ","
                << res.o_shippriority << "\n";
        }
        out.close();
    }

    // Print timing to stdout
    std::cout << "Q3 execution time: " << std::fixed << std::setprecision(2) << elapsed_ms << " ms\n";
    std::cout << "Result rows: " << topk_results.size() << "\n";
}

// ============================================================================
// Main Entry Point
// ============================================================================

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";

    run_q3(gendb_dir, results_dir);

    return 0;
}
#endif
