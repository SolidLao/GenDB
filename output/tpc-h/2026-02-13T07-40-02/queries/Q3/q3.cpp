// Q3: Shipping Priority Query - Self-contained implementation
// Parallel 3-way join: customer -> orders -> lineitem with filtering and aggregation

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <cstdint>
#include <queue>

// ============================================================================
// Date Utilities (inline)
// ============================================================================

inline int32_t date_to_days(int year, int month, int day) {
    // Days since 1970-01-01
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 719469;
}

// ============================================================================
// Memory-mapped Column Loader (inline)
// ============================================================================

template<typename T>
class MMapColumn {
public:
    T* data;
    size_t size;
    int fd;
    void* mapped_addr;
    size_t mapped_size;

    MMapColumn() : data(nullptr), size(0), fd(-1), mapped_addr(nullptr), mapped_size(0) {}

    bool load(const std::string& path, size_t row_count) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        mapped_size = row_count * sizeof(T);
        mapped_addr = mmap(nullptr, mapped_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped_addr == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            close(fd);
            fd = -1;
            return false;
        }

        // Hint for sequential access
        madvise(mapped_addr, mapped_size, MADV_SEQUENTIAL);

        data = static_cast<T*>(mapped_addr);
        size = row_count;
        return true;
    }

    ~MMapColumn() {
        if (mapped_addr && mapped_addr != MAP_FAILED) {
            munmap(mapped_addr, mapped_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
};

// ============================================================================
// Dictionary Loader (inline)
// ============================================================================

std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream file(path);
    if (!file.is_open()) {
        return dict;
    }
    std::string line;
    while (std::getline(file, line)) {
        dict.push_back(line);
    }
    return dict;
}

// ============================================================================
// Metadata Loader (inline)
// ============================================================================

struct TableMetadata {
    size_t row_count;
    std::string sorted_by;
};

TableMetadata load_metadata(const std::string& path) {
    TableMetadata meta = {0, ""};
    std::ifstream file(path);
    if (!file.is_open()) {
        return meta;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.find("\"row_count\"") != std::string::npos) {
            size_t pos = line.find(":");
            if (pos != std::string::npos) {
                std::string val = line.substr(pos + 1);
                val.erase(std::remove(val.begin(), val.end(), ','), val.end());
                val.erase(std::remove(val.begin(), val.end(), ' '), val.end());
                meta.row_count = std::stoull(val);
            }
        }
        if (line.find("\"sorted_by\"") != std::string::npos) {
            size_t pos1 = line.find(": \"");
            size_t pos2 = line.find("\"", pos1 + 3);
            if (pos1 != std::string::npos && pos2 != std::string::npos) {
                meta.sorted_by = line.substr(pos1 + 3, pos2 - pos1 - 3);
            }
        }
    }
    return meta;
}

// ============================================================================
// Aggregation Key and Result (inline, specialized for Q3)
// ============================================================================

struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey
            && o_orderdate == other.o_orderdate
            && o_shippriority == other.o_shippriority;
    }
};

namespace std {
    template<>
    struct hash<AggKey> {
        size_t operator()(const AggKey& k) const {
            size_t h1 = std::hash<int32_t>()(k.l_orderkey);
            size_t h2 = std::hash<int32_t>()(k.o_orderdate);
            size_t h3 = std::hash<int32_t>()(k.o_shippriority);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
}

struct AggValue {
    int64_t revenue_sum; // in cents
};

// ============================================================================
// Result Row for Top-K
// ============================================================================

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const ResultRow& other) const {
        // Standard less-than for sorting
        if (revenue != other.revenue) return revenue < other.revenue;
        return o_orderdate > other.o_orderdate;
    }

    bool operator>(const ResultRow& other) const {
        // For std::greater in priority_queue (min-heap)
        if (revenue != other.revenue) return revenue > other.revenue;
        return o_orderdate < other.o_orderdate;
    }
};

// ============================================================================
// Main Query Execution
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc >= 3 ? argv[2] : "";

    auto start_time = std::chrono::high_resolution_clock::now();

    // ------------------------------------------------------------------------
    // Step 1: Load metadata
    // ------------------------------------------------------------------------

    std::string cust_dir = gendb_dir + "/customer";
    std::string orders_dir = gendb_dir + "/orders";
    std::string lineitem_dir = gendb_dir + "/lineitem";

    auto cust_meta = load_metadata(cust_dir + "/metadata.json");
    auto orders_meta = load_metadata(orders_dir + "/metadata.json");
    auto lineitem_meta = load_metadata(lineitem_dir + "/metadata.json");

    if (cust_meta.row_count == 0 || orders_meta.row_count == 0 || lineitem_meta.row_count == 0) {
        std::cerr << "Failed to load metadata" << std::endl;
        return 1;
    }

    // ------------------------------------------------------------------------
    // Step 2: Load customer columns (c_custkey, c_mktsegment)
    // ------------------------------------------------------------------------

    MMapColumn<int32_t> c_custkey;
    MMapColumn<uint8_t> c_mktsegment;

    if (!c_custkey.load(cust_dir + "/c_custkey.bin", cust_meta.row_count)) return 1;
    if (!c_mktsegment.load(cust_dir + "/c_mktsegment.bin", cust_meta.row_count)) return 1;

    auto mktsegment_dict = load_dictionary(cust_dir + "/c_mktsegment.dict");

    // Find code for 'BUILDING'
    uint8_t building_code = 255;
    for (size_t i = 0; i < mktsegment_dict.size(); i++) {
        if (mktsegment_dict[i] == "BUILDING") {
            building_code = static_cast<uint8_t>(i);
            break;
        }
    }

    if (building_code == 255) {
        std::cerr << "BUILDING not found in dictionary" << std::endl;
        return 1;
    }

    // ------------------------------------------------------------------------
    // Step 3: Filter customer (c_mktsegment = 'BUILDING') and build hash set
    // ------------------------------------------------------------------------

    std::unordered_map<int32_t, bool> filtered_customers;
    filtered_customers.reserve(cust_meta.row_count / 5); // ~20% selectivity

    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;

    const size_t cust_morsel_size = 50000;
    std::atomic<size_t> cust_morsel_idx{0};
    std::vector<std::thread> threads;
    std::mutex cust_mutex;

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            std::vector<int32_t> local_custkeys;
            local_custkeys.reserve(10000);

            while (true) {
                size_t start = cust_morsel_idx.fetch_add(cust_morsel_size, std::memory_order_relaxed);
                if (start >= cust_meta.row_count) break;
                size_t end = std::min(start + cust_morsel_size, cust_meta.row_count);

                for (size_t i = start; i < end; i++) {
                    if (c_mktsegment.data[i] == building_code) {
                        local_custkeys.push_back(c_custkey.data[i]);
                    }
                }
            }

            if (!local_custkeys.empty()) {
                std::lock_guard<std::mutex> lock(cust_mutex);
                for (auto ck : local_custkeys) {
                    filtered_customers[ck] = true;
                }
            }
        });
    }

    for (auto& th : threads) th.join();
    threads.clear();

    std::cout << "Filtered customers: " << filtered_customers.size() << std::endl;

    // ------------------------------------------------------------------------
    // Step 4: Load orders columns and filter by date + customer
    // ------------------------------------------------------------------------

    MMapColumn<int32_t> o_orderkey;
    MMapColumn<int32_t> o_custkey;
    MMapColumn<int32_t> o_orderdate;
    MMapColumn<int32_t> o_shippriority;

    if (!o_orderkey.load(orders_dir + "/o_orderkey.bin", orders_meta.row_count)) return 1;
    if (!o_custkey.load(orders_dir + "/o_custkey.bin", orders_meta.row_count)) return 1;
    if (!o_orderdate.load(orders_dir + "/o_orderdate.bin", orders_meta.row_count)) return 1;
    if (!o_shippriority.load(orders_dir + "/o_shippriority.bin", orders_meta.row_count)) return 1;

    int32_t date_threshold = date_to_days(1995, 3, 15); // o_orderdate < '1995-03-15'

    // Build hash map: o_orderkey -> (o_orderdate, o_shippriority)
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> filtered_orders;
    filtered_orders.reserve(orders_meta.row_count / 3); // rough estimate

    const size_t orders_morsel_size = 50000;
    std::atomic<size_t> orders_morsel_idx{0};
    std::mutex orders_mutex;

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            std::vector<std::tuple<int32_t, int32_t, int32_t>> local_orders;
            local_orders.reserve(10000);

            while (true) {
                size_t start = orders_morsel_idx.fetch_add(orders_morsel_size, std::memory_order_relaxed);
                if (start >= orders_meta.row_count) break;
                size_t end = std::min(start + orders_morsel_size, orders_meta.row_count);

                for (size_t i = start; i < end; i++) {
                    if (o_orderdate.data[i] < date_threshold) {
                        int32_t ck = o_custkey.data[i];
                        if (filtered_customers.count(ck)) {
                            local_orders.emplace_back(
                                o_orderkey.data[i],
                                o_orderdate.data[i],
                                o_shippriority.data[i]
                            );
                        }
                    }
                }
            }

            if (!local_orders.empty()) {
                std::lock_guard<std::mutex> lock(orders_mutex);
                for (auto& tup : local_orders) {
                    filtered_orders[std::get<0>(tup)] = {std::get<1>(tup), std::get<2>(tup)};
                }
            }
        });
    }

    for (auto& th : threads) th.join();
    threads.clear();

    std::cout << "Filtered orders: " << filtered_orders.size() << std::endl;

    // ------------------------------------------------------------------------
    // Step 5: Load lineitem columns and join with orders, aggregate
    // ------------------------------------------------------------------------

    MMapColumn<int32_t> l_orderkey;
    MMapColumn<int64_t> l_extendedprice;
    MMapColumn<int64_t> l_discount;
    MMapColumn<int32_t> l_shipdate;

    if (!l_orderkey.load(lineitem_dir + "/l_orderkey.bin", lineitem_meta.row_count)) return 1;
    if (!l_extendedprice.load(lineitem_dir + "/l_extendedprice.bin", lineitem_meta.row_count)) return 1;
    if (!l_discount.load(lineitem_dir + "/l_discount.bin", lineitem_meta.row_count)) return 1;
    if (!l_shipdate.load(lineitem_dir + "/l_shipdate.bin", lineitem_meta.row_count)) return 1;

    int32_t shipdate_threshold = date_to_days(1995, 3, 15); // l_shipdate > '1995-03-15'

    // Thread-local aggregation
    std::vector<std::unordered_map<AggKey, AggValue>> local_agg_tables(num_threads);
    for (auto& table : local_agg_tables) {
        table.reserve(10000);
    }

    const size_t lineitem_morsel_size = 100000;
    std::atomic<size_t> lineitem_morsel_idx{0};

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local_agg = local_agg_tables[t];

            while (true) {
                size_t start = lineitem_morsel_idx.fetch_add(lineitem_morsel_size, std::memory_order_relaxed);
                if (start >= lineitem_meta.row_count) break;
                size_t end = std::min(start + lineitem_morsel_size, lineitem_meta.row_count);

                for (size_t i = start; i < end; i++) {
                    if (l_shipdate.data[i] > shipdate_threshold) {
                        int32_t ok = l_orderkey.data[i];
                        auto it = filtered_orders.find(ok);
                        if (it != filtered_orders.end()) {
                            // Compute revenue: l_extendedprice * (1 - l_discount)
                            // price stored as cents (int64), discount as 0-10 (representing 0.00-0.10)
                            int64_t price = l_extendedprice.data[i];
                            int64_t discount = l_discount.data[i];
                            int64_t revenue = price * (100 - discount) / 100;

                            AggKey key{ok, it->second.first, it->second.second};
                            local_agg[key].revenue_sum += revenue;
                        }
                    }
                }
            }
        });
    }

    for (auto& th : threads) th.join();
    threads.clear();

    // ------------------------------------------------------------------------
    // Step 6: Merge thread-local aggregation results
    // ------------------------------------------------------------------------

    std::unordered_map<AggKey, AggValue> global_agg;
    global_agg.reserve(100000);

    for (auto& local_agg : local_agg_tables) {
        for (auto& kv : local_agg) {
            global_agg[kv.first].revenue_sum += kv.second.revenue_sum;
        }
    }

    std::cout << "Aggregation groups: " << global_agg.size() << std::endl;

    // ------------------------------------------------------------------------
    // Step 7: Sort by revenue DESC, o_orderdate ASC and take top 10
    // ------------------------------------------------------------------------

    // Use min-heap to maintain top-10 (min-heap keeps smallest at top)
    std::priority_queue<ResultRow, std::vector<ResultRow>, std::greater<ResultRow>> min_heap;

    for (auto& kv : global_agg) {
        ResultRow row{kv.first.l_orderkey, kv.second.revenue_sum,
                      kv.first.o_orderdate, kv.first.o_shippriority};

        if (min_heap.size() < 10) {
            min_heap.push(row);
        } else if (min_heap.top() < row) {
            // row is better than the worst in heap
            min_heap.pop();
            min_heap.push(row);
        }
    }

    // Extract top 10 and sort properly
    std::vector<ResultRow> top10;
    while (!min_heap.empty()) {
        top10.push_back(min_heap.top());
        min_heap.pop();
    }

    std::sort(top10.begin(), top10.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue; // DESC
        return a.o_orderdate < b.o_orderdate; // ASC
    });

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();

    // ------------------------------------------------------------------------
    // Step 8: Output results
    // ------------------------------------------------------------------------

    std::cout << "Result rows: " << top10.size() << std::endl;
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << elapsed << " seconds" << std::endl;

    if (!results_dir.empty()) {
        std::string output_path = results_dir + "/Q3.csv";
        std::ofstream out(output_path);
        if (!out.is_open()) {
            std::cerr << "Failed to open output file: " << output_path << std::endl;
            return 1;
        }

        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (auto& row : top10) {
            // Convert date back to string (YYYY-MM-DD)
            int32_t days = row.o_orderdate;
            int z = days + 719468;
            int era = (z >= 0 ? z : z - 146096) / 146097;
            unsigned doe = static_cast<unsigned>(z - era * 146097);
            unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            int y = static_cast<int>(yoe) + era * 400;
            unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
            unsigned mp = (5*doy + 2)/153;
            unsigned d = doy - (153*mp+2)/5 + 1;
            unsigned m = mp + (mp < 10 ? 3 : -9);
            y += (m <= 2);

            char date_str[11];
            snprintf(date_str, sizeof(date_str), "%04d-%02u-%02u", y, m, d);

            // Convert revenue from cents to dollars with 2 decimal places
            double revenue_dollars = row.revenue / 100.0;

            out << row.l_orderkey << ","
                << std::fixed << std::setprecision(2) << revenue_dollars << ","
                << date_str << ","
                << row.o_shippriority << "\n";
        }

        out.close();
        std::cout << "Results written to: " << output_path << std::endl;
    }

    return 0;
}
