#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <thread>
#include <atomic>
#include <mutex>
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
#include <bitset>

// ============================================================================
// Constants and Types
// ============================================================================

constexpr int32_t DATE_1995_03_15 = 9204;  // days since epoch (1970-01-01) for 1995-03-15
constexpr uint8_t MKTSEGMENT_BUILDING = 0;
constexpr int MORSEL_SIZE = 100000;  // rows per morsel

// Bloom filter for semi-join reduction on customer keys
// Estimate: ~300K BUILDING customers, using ~4MB Bloom filter (1.3 bits per key)
constexpr size_t BLOOM_FILTER_BITS = 4000000;  // ~500KB

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

// Simple Bloom filter for semi-join reduction
class SimpleBloomFilter {
private:
    std::vector<uint8_t> bits_;
    size_t num_bits_;
    static constexpr size_t NUM_HASH_FUNCS = 2;

public:
    SimpleBloomFilter(size_t num_bits) : num_bits_(num_bits) {
        bits_.resize((num_bits + 7) / 8, 0);
    }

    void insert(int32_t key) {
        for (size_t h = 0; h < NUM_HASH_FUNCS; h++) {
            size_t idx = hash_fn(key, h) % num_bits_;
            size_t byte_idx = idx / 8;
            size_t bit_idx = idx % 8;
            bits_[byte_idx] |= (1 << bit_idx);
        }
    }

    bool might_exist(int32_t key) const {
        for (size_t h = 0; h < NUM_HASH_FUNCS; h++) {
            size_t idx = hash_fn(key, h) % num_bits_;
            size_t byte_idx = idx / 8;
            size_t bit_idx = idx % 8;
            if (!(bits_[byte_idx] & (1 << bit_idx))) {
                return false;  // Definitely not present
            }
        }
        return true;  // Might be present
    }

private:
    static size_t hash_fn(int32_t key, size_t seed) {
        // Mix key with seed
        size_t h = key ^ seed;
        h ^= (h >> 16);
        h *= 0x85ebca6b;
        h ^= (h >> 13);
        h *= 0xc2b2ae35;
        h ^= (h >> 16);
        return h;
    }
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
    // Step 1: Filter customer on c_mktsegment = 'BUILDING' (PARALLEL)
    // ========================================================================
    std::cout << "Filtering customers (parallel)..." << std::endl;

    unsigned int num_threads_cust = std::thread::hardware_concurrency();
    if (num_threads_cust == 0) num_threads_cust = 4;

    const int customer_morsel_size = MORSEL_SIZE;
    int num_customer_morsels = (num_customers + customer_morsel_size - 1) / customer_morsel_size;

    // Thread-local vectors to collect filtered customer keys
    std::vector<std::vector<int32_t>> thread_local_custkeys(num_threads_cust);
    for (auto& vec : thread_local_custkeys) {
        vec.reserve(num_customers / num_threads_cust / 5);  // estimate ~20% selectivity
    }

    // Atomic counter for morsel distribution
    std::atomic<int> customer_morsel_idx(0);

    // Worker function for parallel customer filtering
    auto customer_worker = [&](int thread_id) {
        auto& local_keys = thread_local_custkeys[thread_id];

        int morsel_idx;
        while ((morsel_idx = customer_morsel_idx.fetch_add(1)) < num_customer_morsels) {
            int start = morsel_idx * customer_morsel_size;
            int end = std::min(start + customer_morsel_size, num_customers);

            // Filter on c_mktsegment = BUILDING
            for (int i = start; i < end; i++) {
                if (c_mktsegment.data()[i] == MKTSEGMENT_BUILDING) {
                    local_keys.push_back(c_custkey.data()[i]);
                }
            }
        }
    };

    // Launch threads for customer filtering
    std::vector<std::thread> customer_threads;
    for (unsigned int t = 0; t < num_threads_cust; t++) {
        customer_threads.emplace_back(customer_worker, t);
    }

    for (auto& t : customer_threads) {
        t.join();
    }

    // Merge filtered customer keys
    std::vector<int32_t> filtered_custkeys;
    for (const auto& vec : thread_local_custkeys) {
        filtered_custkeys.insert(filtered_custkeys.end(), vec.begin(), vec.end());
    }

    std::cout << "  Filtered to " << filtered_custkeys.size() << " BUILDING customers" << std::endl;

    // Build hash set of BUILDING customer keys
    std::unordered_set<int32_t> building_custkeys(filtered_custkeys.begin(), filtered_custkeys.end());

    // ========================================================================
    // Step 2: Join customer→orders on o_custkey, filter on o_orderdate < 1995-03-15
    //         PARALLEL FILTERING OF ORDERS, THEN PROBE WITH CUSTOMER SET
    // ========================================================================
    std::cout << "Joining customer to orders (parallel filter)..." << std::endl;

    unsigned int num_threads_orders = std::thread::hardware_concurrency();
    if (num_threads_orders == 0) num_threads_orders = 4;

    const int orders_morsel_size = MORSEL_SIZE;
    int num_orders_morsels = (num_orders + orders_morsel_size - 1) / orders_morsel_size;

    // Thread-local vectors to collect filtered orders
    std::vector<std::vector<OrdersJoinResult>> thread_local_orders(num_threads_orders);
    for (auto& vec : thread_local_orders) {
        vec.reserve(num_orders / num_threads_orders / 10);  // estimate ~10% selectivity
    }

    // Atomic counter for morsel distribution
    std::atomic<int> orders_morsel_idx(0);

    // Worker function for parallel orders filtering
    auto orders_worker = [&](int thread_id) {
        auto& local_orders = thread_local_orders[thread_id];

        int morsel_idx;
        while ((morsel_idx = orders_morsel_idx.fetch_add(1)) < num_orders_morsels) {
            int start = morsel_idx * orders_morsel_size;
            int end = std::min(start + orders_morsel_size, num_orders);

            // Filter matching orders and collect if customer is in BUILDING set
            for (int i = start; i < end; i++) {
                int32_t ckey = o_custkey.data()[i];
                int32_t odate = o_orderdate.data()[i];

                // Filter: o_orderdate < 1995-03-15 AND c_custkey is BUILDING
                if (odate < DATE_1995_03_15 && building_custkeys.count(ckey) > 0) {
                    local_orders.push_back(OrdersJoinResult{
                        o_orderkey.data()[i],
                        odate,
                        o_shippriority.data()[i]
                    });
                }
            }
        }
    };

    // Launch threads for orders filtering
    std::vector<std::thread> orders_threads;
    for (unsigned int t = 0; t < num_threads_orders; t++) {
        orders_threads.emplace_back(orders_worker, t);
    }

    for (auto& t : orders_threads) {
        t.join();
    }

    // Merge filtered orders
    std::vector<OrdersJoinResult> customer_orders_join;
    for (const auto& vec : thread_local_orders) {
        for (const auto& order : vec) {
            customer_orders_join.push_back(order);
        }
    }

    std::cout << "  Customer-orders join result: " << customer_orders_join.size() << " rows" << std::endl;

    // ========================================================================
    // Step 3: Build Bloom filter on ORDER keys for semi-join reduction
    //         This allows us to skip 80%+ of lineitem probes without hash lookup
    //         (Bloom filter is on the orderkeys that matched customer+orderdate filters)
    // ========================================================================
    std::cout << "Building Bloom filter for semi-join reduction..." << std::endl;

    SimpleBloomFilter orderkey_bloom(BLOOM_FILTER_BITS);
    for (const auto& order : customer_orders_join) {
        orderkey_bloom.insert(order.l_orderkey);
    }

    std::cout << "  Bloom filter built for " << customer_orders_join.size() << " order keys" << std::endl;

    // ========================================================================
    // Step 4: Build compact hash map of order keys (for probing during lineitem scan)
    //         Much smaller intermediate than 32M lineitem entries
    // ========================================================================
    std::cout << "Building order key lookup table..." << std::endl;

    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orderkey_to_date_priority;
    orderkey_to_date_priority.reserve(customer_orders_join.size());
    for (const auto& order : customer_orders_join) {
        orderkey_to_date_priority[order.l_orderkey] = {order.o_orderdate, order.o_shippriority};
    }

    std::cout << "  Order lookup table: " << orderkey_to_date_priority.size() << " entries" << std::endl;

    // ========================================================================
    // Step 5: Parallel scan of lineitem with on-the-fly aggregation
    //         Use Bloom filter to skip non-matching rows before expensive hash lookup
    // ========================================================================
    std::cout << "Scanning lineitem and aggregating (parallel pass with Bloom filter)..." << std::endl;

    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;

    const int lineitem_morsel_size = MORSEL_SIZE;
    int num_lineitem_morsels = (num_lineitems + lineitem_morsel_size - 1) / lineitem_morsel_size;

    std::cout << "  Using " << num_threads << " threads, " << num_lineitem_morsels << " lineitem morsels" << std::endl;

    // Allocate thread-local aggregation maps
    std::vector<std::unordered_map<GroupKey, AggregateState, GroupKeyHash>> thread_local_aggs(num_threads);

    // Pre-reserve space in thread-local maps
    size_t local_capacity = (customer_orders_join.size() / 20) / num_threads + 1000;
    for (auto& agg : thread_local_aggs) {
        agg.reserve(local_capacity);
    }

    // Atomic counter for work distribution
    std::atomic<int> lineitem_morsel_idx(0);

    // Worker function: scan lineitem morsels with Bloom filter optimization
    // 1. Fast Bloom filter check to skip non-matching rows
    // 2. Hash lookup only on Bloom-filter-positive rows
    // 3. Local aggregation to avoid contention
    auto worker = [&](int thread_id) {
        auto& local_agg = thread_local_aggs[thread_id];

        int morsel_idx;
        while ((morsel_idx = lineitem_morsel_idx.fetch_add(1)) < num_lineitem_morsels) {
            int start = morsel_idx * lineitem_morsel_size;
            int end = std::min(start + lineitem_morsel_size, num_lineitems);

            // Process this morsel of lineitem with Bloom filter optimization
            for (int i = start; i < end; i++) {
                int32_t okey = l_orderkey.data()[i];
                int32_t sdate = l_shipdate.data()[i];

                // Filter 1: l_shipdate > 1995-03-15 (early filter on lineitem)
                if (sdate > DATE_1995_03_15) {
                    // Filter 2: Bloom filter check on order key (very cheap, in L3 cache)
                    // This skips ~80% of non-matching rows WITHOUT hash table lookup
                    if (orderkey_bloom.might_exist(okey)) {
                        // Filter 3: Actual hash table lookup (only on Bloom-positive rows)
                        auto it = orderkey_to_date_priority.find(okey);
                        if (it != orderkey_to_date_priority.end()) {
                            // Join match found - compute revenue and aggregate
                            double ext_price = l_extendedprice.data()[i];
                            double discount = l_discount.data()[i];
                            double revenue = ext_price * (1.0 - discount);

                            int32_t o_orderdate = it->second.first;
                            int32_t o_shippriority = it->second.second;

                            // Accumulate in thread-local aggregation (no contention)
                            GroupKey key{okey, o_orderdate, o_shippriority};
                            local_agg[key].revenue_sum += revenue;
                        }
                    }
                }
            }
        }
    };

    // Launch threads and wait for completion
    std::vector<std::thread> threads;
    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back(worker, t);
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge all thread-local aggregations into global map
    std::unordered_map<GroupKey, AggregateState, GroupKeyHash> agg_map;
    for (const auto& local_agg : thread_local_aggs) {
        for (const auto& [key, agg] : local_agg) {
            agg_map[key].revenue_sum += agg.revenue_sum;
        }
    }

    std::cout << "  Final aggregation: " << agg_map.size() << " groups" << std::endl;

    // ========================================================================
    // Step 6: Sort by revenue DESC, o_orderdate ASC, and apply LIMIT 10
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
