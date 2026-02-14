#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <queue>
#include <iomanip>
#include <ctime>
#include <chrono>

struct ResultRow {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const ResultRow& other) const {
        // Min-heap: use reverse comparison for max-heap on revenue
        // revenue DESC, o_orderdate ASC
        if (revenue != other.revenue) {
            return revenue > other.revenue;  // min-heap: put largest at top
        }
        return o_orderdate < other.o_orderdate;
    }
};

// Fast hash function for three int32_t values (orderkey, orderdate, shippriority)
struct GroupKeyHash {
    uint64_t operator()(const std::tuple<int32_t, int32_t, int32_t>& key) const {
        uint64_t h = 14695981039346656037ULL;  // FNV offset basis
        const uint64_t prime = 1099511628211ULL;  // FNV prime

        h ^= std::get<0>(key);
        h *= prime;
        h ^= std::get<1>(key);
        h *= prime;
        h ^= std::get<2>(key);
        h *= prime;

        return h;
    }
};

// Memory-mapped file wrapper
class MmapFile {
public:
    MmapFile(const std::string& path) : ptr(nullptr), size(0), fd(-1) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Cannot open file: " + path);
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            close(fd);
            throw std::runtime_error("Cannot stat file: " + path);
        }
        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("Cannot mmap file: " + path);
        }
        madvise(ptr, size, MADV_SEQUENTIAL);
    }

    ~MmapFile() {
        if (ptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }

    const char* data() const { return (const char*)ptr; }
    size_t get_size() const { return size; }

private:
    void* ptr;
    size_t size;
    int fd;
};

// Load dictionary for c_mktsegment
std::unordered_map<uint8_t, std::string> loadDictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, std::string> dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open dictionary: " + dict_path);
    }
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            uint8_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Convert epoch days to YYYY-MM-DD string
inline std::string epochDaysToString(int32_t days) {
    std::time_t t = (std::time_t)days * 86400LL;
    struct tm* tm_ptr = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_ptr);
    return std::string(buf);
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Constants for date filtering
    const int32_t cutoff_orderdate = 9204;    // 1995-03-15 in epoch days
    const int32_t cutoff_shipdate = 9204;     // 1995-03-15 in epoch days
    const size_t morsel_size = 100000;        // Process 100K rows per morsel
    const int num_threads = std::min(32, (int)std::thread::hardware_concurrency());

    // Load dictionaries
    std::string dict_path = gendb_dir + "/customer/c_mktsegment.bin.dict";
    auto mktsegment_dict = loadDictionary(dict_path);
    uint8_t building_code = 0;
    for (auto& [code, value] : mktsegment_dict) {
        if (value == "BUILDING") {
            building_code = code;
            break;
        }
    }

    // mmap files
    MmapFile customer_custkey(gendb_dir + "/customer/c_custkey.bin");
    MmapFile customer_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

    MmapFile orders_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MmapFile orders_custkey(gendb_dir + "/orders/o_custkey.bin");
    MmapFile orders_orderdate(gendb_dir + "/orders/o_orderdate.bin");
    MmapFile orders_shippriority(gendb_dir + "/orders/o_shippriority.bin");

    MmapFile lineitem_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapFile lineitem_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile lineitem_discount(gendb_dir + "/lineitem/l_discount.bin");
    MmapFile lineitem_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

    const int32_t* c_custkey_data = (const int32_t*)customer_custkey.data();
    const uint8_t* c_mktsegment_data = (const uint8_t*)customer_mktsegment.data();

    const int32_t* o_orderkey_data = (const int32_t*)orders_orderkey.data();
    const int32_t* o_custkey_data = (const int32_t*)orders_custkey.data();
    const int32_t* o_orderdate_data = (const int32_t*)orders_orderdate.data();
    const int32_t* o_shippriority_data = (const int32_t*)orders_shippriority.data();

    const int32_t* l_orderkey_data = (const int32_t*)lineitem_orderkey.data();
    const double* l_extendedprice_data = (const double*)lineitem_extendedprice.data();
    const double* l_discount_data = (const double*)lineitem_discount.data();
    const int32_t* l_shipdate_data = (const int32_t*)lineitem_shipdate.data();

    // Row counts
    size_t customer_rows = customer_custkey.get_size() / sizeof(int32_t);
    size_t orders_rows = orders_orderkey.get_size() / sizeof(int32_t);
    size_t lineitem_rows = lineitem_orderkey.get_size() / sizeof(int32_t);

    // ===== STEP 1: Filter customer by c_mktsegment = 'BUILDING' (Parallel) =====
    std::atomic<size_t> customer_morsel_counter(0);
    std::unordered_map<int32_t, int32_t> customer_custkey_map;
    std::mutex customer_map_lock;

    auto filter_customer_morsels = [&]() {
        while (true) {
            size_t morsel_id = customer_morsel_counter.fetch_add(1, std::memory_order_relaxed);
            size_t start = morsel_id * morsel_size;
            if (start >= customer_rows) break;
            size_t end = std::min(start + morsel_size, customer_rows);

            // Thread-local results
            std::unordered_map<int32_t, int32_t> local_results;
            for (size_t i = start; i < end; ++i) {
                if (c_mktsegment_data[i] == building_code) {
                    local_results[c_custkey_data[i]] = c_custkey_data[i];
                }
            }

            // Merge to global map
            {
                std::lock_guard<std::mutex> lock(customer_map_lock);
                for (const auto& [k, v] : local_results) {
                    customer_custkey_map[k] = v;
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(filter_customer_morsels);
    }
    for (auto& t : threads) t.join();
    threads.clear();

    // ===== STEP 2: Hash join orders with customer, filter by o_orderdate (Parallel) =====
    struct OrderRecord {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };

    std::atomic<size_t> orders_morsel_counter(0);
    std::unordered_map<int32_t, std::vector<OrderRecord>> orders_map;
    std::mutex orders_map_lock;

    auto filter_orders_morsels = [&]() {
        while (true) {
            size_t morsel_id = orders_morsel_counter.fetch_add(1, std::memory_order_relaxed);
            size_t start = morsel_id * morsel_size;
            if (start >= orders_rows) break;
            size_t end = std::min(start + morsel_size, orders_rows);

            // Thread-local results
            std::unordered_map<int32_t, std::vector<OrderRecord>> local_results;
            for (size_t i = start; i < end; ++i) {
                int32_t custkey = o_custkey_data[i];
                if (customer_custkey_map.count(custkey) && o_orderdate_data[i] < cutoff_orderdate) {
                    local_results[o_orderkey_data[i]].push_back({
                        o_orderkey_data[i],
                        o_orderdate_data[i],
                        o_shippriority_data[i]
                    });
                }
            }

            // Merge to global map
            {
                std::lock_guard<std::mutex> lock(orders_map_lock);
                for (auto& [k, v] : local_results) {
                    orders_map[k].insert(orders_map[k].end(), v.begin(), v.end());
                }
            }
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(filter_orders_morsels);
    }
    for (auto& t : threads) t.join();
    threads.clear();

    // ===== STEP 3: Hash join lineitem with orders, with parallel aggregation =====
    // Use unordered_map with custom hash for fast aggregation
    std::atomic<size_t> lineitem_morsel_counter(0);

    // Type alias for composite key: (orderkey, orderdate, shippriority)
    using GroupKey = std::tuple<int32_t, int32_t, int32_t>;

    // Thread-local aggregation tables
    std::vector<std::unordered_map<GroupKey, double, GroupKeyHash>> local_aggregates(num_threads);

    auto process_lineitem_morsels = [&](int thread_id) {
        auto& local_agg = local_aggregates[thread_id];
        while (true) {
            size_t morsel_id = lineitem_morsel_counter.fetch_add(1, std::memory_order_relaxed);
            size_t start = morsel_id * morsel_size;
            if (start >= lineitem_rows) break;
            size_t end = std::min(start + morsel_size, lineitem_rows);

            for (size_t i = start; i < end; ++i) {
                int32_t orderkey = l_orderkey_data[i];

                if (orders_map.count(orderkey) && l_shipdate_data[i] > cutoff_shipdate) {
                    double revenue_item = l_extendedprice_data[i] * (1.0 - l_discount_data[i]);

                    for (const auto& order : orders_map[orderkey]) {
                        GroupKey key = std::make_tuple(orderkey, order.orderdate, order.shippriority);
                        local_agg[key] += revenue_item;
                    }
                }
            }
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_lineitem_morsels, i);
    }
    for (auto& t : threads) t.join();
    threads.clear();

    // ===== STEP 4: Merge thread-local aggregations =====
    std::unordered_map<GroupKey, double, GroupKeyHash> global_aggregates;
    for (int i = 0; i < num_threads; ++i) {
        for (const auto& [key, revenue] : local_aggregates[i]) {
            global_aggregates[key] += revenue;
        }
    }

    // ===== STEP 5: Top-K optimization with min-heap (priority queue) =====
    // Use priority queue to maintain top 10 instead of sorting all results
    std::priority_queue<ResultRow> topk_heap;  // max-heap on revenue (due to custom < operator)

    for (const auto& [key, revenue] : global_aggregates) {
        ResultRow row = {
            std::get<0>(key),
            revenue,
            std::get<1>(key),
            std::get<2>(key)
        };

        if (topk_heap.size() < 10) {
            topk_heap.push(row);
        } else if (row < topk_heap.top()) {  // Using custom < operator
            topk_heap.pop();
            topk_heap.push(row);
        }
    }

    // Extract results from heap in reverse order (they come out smallest first)
    std::vector<ResultRow> results;
    while (!topk_heap.empty()) {
        results.push_back(topk_heap.top());
        topk_heap.pop();
    }

    // Reverse to get descending order (largest revenue first)
    std::reverse(results.begin(), results.end());

    // ===== STEP 6: Write CSV output =====
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q3.csv");
        outfile << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (const auto& row : results) {
            outfile << row.l_orderkey << ","
                    << std::fixed << std::setprecision(4) << row.revenue << ","
                    << epochDaysToString(row.o_orderdate) << ","
                    << row.o_shippriority << "\n";
        }
        outfile.close();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Query returned " << results.size() << " rows\n";
    std::cout << "Execution time: " << duration.count() << " ms\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    try {
        run_q3(argv[1], argc > 2 ? argv[2] : "");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
#endif
