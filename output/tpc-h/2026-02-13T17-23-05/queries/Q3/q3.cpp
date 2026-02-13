#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <iomanip>
#include <ctime>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>

// ============================================================================
// Constants and Types
// ============================================================================

constexpr int32_t DATE_1995_03_15 = 9204;  // days since epoch (1970-01-01) for 1995-03-15
constexpr uint8_t MKTSEGMENT_BUILDING = 0;
constexpr int MORSEL_SIZE = 100000;  // rows per morsel

struct GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const GroupKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        // Simple hash combining
        size_t h1 = std::hash<int32_t>()(k.l_orderkey);
        size_t h2 = std::hash<int32_t>()(k.o_orderdate);
        size_t h3 = std::hash<int32_t>()(k.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

struct AggregateState {
    double revenue_sum;
};

struct OrdersJoinResult {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ============================================================================
// Memory-Mapped Column Loader
// ============================================================================

template<typename T>
class MmapColumn {
private:
    int fd_;
    void* mapped_;
    size_t size_;

public:
    MmapColumn(const std::string& path, size_t expected_rows)
        : fd_(-1), mapped_(nullptr), size_(expected_rows * sizeof(T)) {
        fd_ = open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("Cannot open file: " + path);
        }

        struct stat sb;
        if (fstat(fd_, &sb) < 0) {
            close(fd_);
            throw std::runtime_error("Cannot stat file: " + path);
        }

        size_ = sb.st_size;
        mapped_ = mmap(nullptr, size_, PROT_READ, MAP_SHARED, fd_, 0);
        if (mapped_ == MAP_FAILED) {
            close(fd_);
            throw std::runtime_error("Cannot mmap file: " + path);
        }

        // Advise OS for sequential read
        madvise(mapped_, size_, MADV_SEQUENTIAL);
    }

    ~MmapColumn() {
        if (mapped_ != nullptr) {
            munmap(mapped_, size_);
        }
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    T* data() { return static_cast<T*>(mapped_); }
    size_t rows() const { return size_ / sizeof(T); }
};

// ============================================================================
// Query Execution
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load columns via mmap
    std::cout << "Loading columns..." << std::endl;

    // Customer columns
    MmapColumn<int32_t> c_custkey(gendb_dir + "/customer.c_custkey", 1500000);
    MmapColumn<uint8_t> c_mktsegment(gendb_dir + "/customer.c_mktsegment", 1500000);

    // Orders columns
    MmapColumn<int32_t> o_orderkey(gendb_dir + "/orders.o_orderkey", 15000000);
    MmapColumn<int32_t> o_custkey(gendb_dir + "/orders.o_custkey", 15000000);
    MmapColumn<int32_t> o_orderdate(gendb_dir + "/orders.o_orderdate", 15000000);
    MmapColumn<int32_t> o_shippriority(gendb_dir + "/orders.o_shippriority", 15000000);

    // Lineitem columns
    MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem.l_orderkey", 60000000);
    MmapColumn<double> l_extendedprice(gendb_dir + "/lineitem.l_extendedprice", 60000000);
    MmapColumn<double> l_discount(gendb_dir + "/lineitem.l_discount", 60000000);
    MmapColumn<int32_t> l_shipdate(gendb_dir + "/lineitem.l_shipdate", 60000000);

    int num_customers = c_custkey.rows();
    int num_orders = o_orderkey.rows();
    int num_lineitems = l_orderkey.rows();

    std::cout << "Loaded " << num_customers << " customers, "
              << num_orders << " orders, "
              << num_lineitems << " lineitems" << std::endl;

    // ========================================================================
    // Step 1: Filter customer on c_mktsegment = 'BUILDING'
    // ========================================================================
    std::cout << "Filtering customers..." << std::endl;

    std::vector<int32_t> filtered_custkeys;
    for (int i = 0; i < num_customers; i++) {
        if (c_mktsegment.data()[i] == MKTSEGMENT_BUILDING) {
            filtered_custkeys.push_back(c_custkey.data()[i]);
        }
    }

    std::cout << "  Filtered to " << filtered_custkeys.size() << " BUILDING customers" << std::endl;

    // Build hash set of BUILDING customer keys
    std::unordered_set<int32_t> building_custkeys(filtered_custkeys.begin(), filtered_custkeys.end());

    // ========================================================================
    // Step 2: Join customer→orders on o_custkey, filter on o_orderdate < 1995-03-15
    // ========================================================================
    std::cout << "Joining customer to orders..." << std::endl;

    // Build hash map: o_custkey → (o_orderkey, o_orderdate, o_shippriority)
    std::unordered_multimap<int32_t, OrdersJoinResult> orders_by_custkey;

    for (int i = 0; i < num_orders; i++) {
        int32_t ckey = o_custkey.data()[i];
        int32_t odate = o_orderdate.data()[i];

        // Filter: o_orderdate < 1995-03-15
        if (odate < DATE_1995_03_15) {
            orders_by_custkey.emplace(ckey, OrdersJoinResult{
                o_orderkey.data()[i],
                odate,
                o_shippriority.data()[i]
            });
        }
    }

    std::cout << "  Orders hash map size: " << orders_by_custkey.size() << std::endl;

    // Probe: for each BUILDING customer, find matching orders
    std::vector<OrdersJoinResult> customer_orders_join;
    for (int32_t custkey : filtered_custkeys) {
        auto range = orders_by_custkey.equal_range(custkey);
        for (auto it = range.first; it != range.second; ++it) {
            customer_orders_join.push_back(it->second);
        }
    }

    std::cout << "  Customer-orders join result: " << customer_orders_join.size() << " rows" << std::endl;

    // ========================================================================
    // Step 3: Join result→lineitem on l_orderkey, filter on l_shipdate > 1995-03-15
    // ========================================================================
    std::cout << "Joining to lineitem..." << std::endl;

    // Build hash map: l_orderkey → list of lineitem rows with l_shipdate > 1995-03-15
    std::unordered_multimap<int32_t, std::pair<double, double>> lineitem_by_orderkey;

    for (int i = 0; i < num_lineitems; i++) {
        int32_t okey = l_orderkey.data()[i];
        int32_t sdate = l_shipdate.data()[i];

        // Filter: l_shipdate > 1995-03-15
        if (sdate > DATE_1995_03_15) {
            lineitem_by_orderkey.emplace(okey, std::make_pair(
                l_extendedprice.data()[i],
                l_discount.data()[i]
            ));
        }
    }

    std::cout << "  Lineitem hash map size: " << lineitem_by_orderkey.size() << std::endl;

    // ========================================================================
    // Step 4: Hash aggregation by (l_orderkey, o_orderdate, o_shippriority)
    // ========================================================================
    std::cout << "Performing aggregation..." << std::endl;

    std::unordered_map<GroupKey, AggregateState, GroupKeyHash> agg_map;

    for (const auto& order : customer_orders_join) {
        auto range = lineitem_by_orderkey.equal_range(order.l_orderkey);
        for (auto it = range.first; it != range.second; ++it) {
            double ext_price = it->second.first;
            double discount = it->second.second;
            double revenue = ext_price * (1.0 - discount);

            GroupKey key{order.l_orderkey, order.o_orderdate, order.o_shippriority};
            agg_map[key].revenue_sum += revenue;
        }
    }

    std::cout << "  Aggregation result: " << agg_map.size() << " groups" << std::endl;

    // ========================================================================
    // Step 5: Sort by revenue DESC, o_orderdate ASC, and apply LIMIT 10
    // ========================================================================
    std::cout << "Sorting results..." << std::endl;

    struct Result {
        int32_t l_orderkey;
        double revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<Result> results;
    for (const auto& [key, agg] : agg_map) {
        results.push_back({key.l_orderkey, agg.revenue_sum, key.o_orderdate, key.o_shippriority});
    }

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        if (a.revenue != b.revenue) {
            return a.revenue > b.revenue;  // DESC
        }
        return a.o_orderdate < b.o_orderdate;  // ASC
    });

    // Apply LIMIT 10
    if (results.size() > 10) {
        results.resize(10);
    }

    // ========================================================================
    // Output Results
    // ========================================================================

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Query executed in " << elapsed.count() << " ms" << std::endl;
    std::cout << "Result rows: " << results.size() << std::endl;

    // Write CSV if results_dir is non-empty
    if (!results_dir.empty()) {
        std::string output_file = results_dir + "/Q3.csv";
        std::ofstream out(output_file);

        // Write header
        out << "l_orderkey,revenue,o_orderdate,o_shippriority" << std::endl;

        // Write rows
        for (const auto& r : results) {
            // Convert epoch days back to YYYY-MM-DD format
            // Epoch base: 1970-01-01
            auto date_from_epoch = [](int32_t days) -> std::string {
                // 1970-01-01 is day 0
                // 1995-03-15 is day ~9054 (approximate)
                // Use standard epoch conversion
                time_t epoch_seconds = (time_t)days * 86400;
                struct tm* tm_info = gmtime(&epoch_seconds);
                char date_str[11];
                strftime(date_str, sizeof(date_str), "%Y-%m-%d", tm_info);
                return std::string(date_str);
            };

            out << r.l_orderkey << ","
                << std::fixed << std::setprecision(4) << r.revenue << ","
                << date_from_epoch(r.o_orderdate) << ","
                << r.o_shippriority << std::endl;
        }

        out.close();
        std::cout << "Results written to " << output_file << std::endl;
    }

    // Print timing
    std::cout << std::fixed << std::setprecision(2) << "Execution time: " << elapsed.count() << " ms" << std::endl;
}

// ============================================================================
// Standalone Entry Point
// ============================================================================

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";

    try {
        run_q3(gendb_dir, results_dir);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#endif
