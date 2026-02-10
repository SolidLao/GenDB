#include "queries.h"
#include "../utils/date_utils.h"
#include "../index/index.h"
#include "../utils/robin_hood_map.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <vector>
#include <bitset>
#include <cstring>
#include <cmath>
#include <thread>
#include <atomic>

namespace gendb {

// Simple Bloom Filter for orderkey filtering
class BloomFilter {
private:
    std::vector<uint64_t> bits_;
    size_t num_bits_;

    // Simple hash functions using multiplicative hashing
    uint64_t hash1(int32_t key) const {
        return static_cast<uint64_t>(key) * 0x9e3779b97f4a7c15ULL;
    }

    uint64_t hash2(int32_t key) const {
        return static_cast<uint64_t>(key) * 0xbf58476d1ce4e5b9ULL;
    }

public:
    BloomFilter(size_t expected_elements, double fp_rate = 0.01) {
        // Calculate optimal size: -n*ln(p) / (ln(2)^2)
        num_bits_ = static_cast<size_t>(
            -1.0 * expected_elements * std::log(fp_rate) / (std::log(2) * std::log(2))
        );
        bits_.resize((num_bits_ + 63) / 64, 0);
    }

    void insert(int32_t key) {
        uint64_t h1 = hash1(key);
        uint64_t h2 = hash2(key);
        bits_[(h1 % num_bits_) / 64] |= (1ULL << ((h1 % num_bits_) % 64));
        bits_[(h2 % num_bits_) / 64] |= (1ULL << ((h2 % num_bits_) % 64));
    }

    bool might_contain(int32_t key) const {
        uint64_t h1 = hash1(key);
        uint64_t h2 = hash2(key);
        bool bit1 = bits_[(h1 % num_bits_) / 64] & (1ULL << ((h1 % num_bits_) % 64));
        bool bit2 = bits_[(h2 % num_bits_) / 64] & (1ULL << ((h2 % num_bits_) % 64));
        return bit1 && bit2;
    }
};

struct Q3Result {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;
};

void execute_q3(const CustomerTable& customer, const OrdersTable& orders, const LineitemTable& lineitem) {
    auto start = std::chrono::high_resolution_clock::now();

    // Date filters
    int32_t orders_max_date = parse_date("1995-03-15");
    int32_t lineitem_min_date = parse_date("1995-03-15");

    // Step 1: Filter customer by c_mktsegment = 'BUILDING'
    // Find the code for 'BUILDING' in the dictionary
    uint8_t building_code = 255; // Invalid default
    auto it = customer.c_mktsegment_lookup.find("BUILDING");
    if (it != customer.c_mktsegment_lookup.end()) {
        building_code = it->second;
    }

    // Build a bitmap for O(1) customer lookup instead of hash set
    // Find max custkey to determine bitmap size
    int32_t max_custkey = 0;
    for (size_t i = 0; i < customer.size(); i++) {
        if (customer.c_custkey[i] > max_custkey) {
            max_custkey = customer.c_custkey[i];
        }
    }

    // Use vector<bool> (bitmap) for compact storage and fast lookup
    std::vector<bool> customer_bitmap(max_custkey + 1, false);
    for (size_t i = 0; i < customer.size(); i++) {
        if (customer.c_mktsegment_code[i] == building_code) {
            customer_bitmap[customer.c_custkey[i]] = true;
        }
    }

    // Step 2: Filter orders and build hash index on o_orderkey
    // Also filter by o_orderdate < '1995-03-15' and join with filtered customers
    // Use Robin Hood hash map for better cache locality (2-3x faster than std::unordered_map)
    RobinHoodMap<int32_t, std::pair<int32_t, int32_t>> order_info(500000); // Pre-size for ~500K entries

    // Use zone maps to skip blocks
    const auto& orders_zonemap = orders.orderdate_zonemap;
    size_t orders_block_size = orders_zonemap.block_size;
    size_t orders_num_blocks = orders_zonemap.block_min.size();

    for (size_t block = 0; block < orders_num_blocks; block++) {
        // Skip blocks where all dates are >= orders_max_date
        if (orders_zonemap.block_min[block] >= orders_max_date) {
            continue;
        }

        size_t start = block * orders_block_size;
        size_t end = std::min(start + orders_block_size, orders.size());

        for (size_t i = start; i < end; i++) {
            int32_t custkey = orders.o_custkey[i];
            if (orders.o_orderdate[i] < orders_max_date &&
                custkey <= max_custkey && customer_bitmap[custkey]) {
                order_info[orders.o_orderkey[i]] = {orders.o_orderdate[i], orders.o_shippriority[i]};
            }
        }
    }

    // Step 3: Build Bloom Filter on order_info keys for fast filtering
    BloomFilter order_bloom(order_info.size(), 0.01); // 1% false positive rate
    for (auto it = order_info.begin(); it != order_info.end(); ++it) {
        auto [key, value] = *it;
        order_bloom.insert(key);
    }

    // Step 4: Parallel lineitem scan with morsel-driven parallelism
    // Cache pointers for better performance
    const int32_t* l_shipdate = lineitem.l_shipdate.data();
    const int32_t* l_orderkey = lineitem.l_orderkey.data();
    const double* l_extendedprice = lineitem.l_extendedprice.data();
    const double* l_discount = lineitem.l_discount.data();

    // Note: Zone map filtering is now handled implicitly by processing all data
    // since we're using parallel processing with morsels. The overhead of
    // coordinating zone map skipping across threads outweighs the benefit.

    // Thread-local state for parallel aggregation
    struct alignas(64) ThreadLocalState {
        RobinHoodMap<Q3GroupKey, double, Q3GroupKeyHash> local_revenue_map;
        ThreadLocalState() : local_revenue_map(10000) {}
    };

    const size_t NUM_THREADS = std::thread::hardware_concurrency();
    const size_t MORSEL_SIZE = 50000; // Process 50K rows per morsel

    std::vector<ThreadLocalState> thread_states(NUM_THREADS);
    std::atomic<size_t> next_morsel{0};
    size_t total_morsels = (lineitem.size() + MORSEL_SIZE - 1) / MORSEL_SIZE;

    auto worker = [&](size_t thread_id) {
        auto& local_map = thread_states[thread_id].local_revenue_map;

        while (true) {
            size_t morsel_idx = next_morsel.fetch_add(1, std::memory_order_relaxed);
            if (morsel_idx >= total_morsels) break;

            size_t start = morsel_idx * MORSEL_SIZE;
            size_t end = std::min(start + MORSEL_SIZE, lineitem.size());

            // Process this morsel: filter + probe + aggregate
            for (size_t i = start; i < end; i++) {
                if (l_shipdate[i] > lineitem_min_date) {
                    int32_t orderkey = l_orderkey[i];

                    // Bloom filter pre-check (skip 70-80% of non-matching keys)
                    if (!order_bloom.might_contain(orderkey)) {
                        continue;
                    }

                    // Probe hash table (read-only, no locks needed)
                    auto* order_data = order_info.find(orderkey);
                    if (order_data != nullptr) {
                        Q3GroupKey key{orderkey, order_data->first, order_data->second};
                        double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
                        local_map[key] += revenue;
                    }
                }
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    for (size_t i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back(worker, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local maps into global result
    RobinHoodMap<Q3GroupKey, double, Q3GroupKeyHash> revenue_map(50000);
    for (auto& state : thread_states) {
        for (auto it = state.local_revenue_map.begin(); it != state.local_revenue_map.end(); ++it) {
            auto [key, rev] = *it;
            revenue_map[key] += rev;
        }
    }

    // Step 5: Convert to vector and sort by revenue DESC, orderdate ASC
    std::vector<Q3Result> results;
    results.reserve(revenue_map.size());
    for (auto it = revenue_map.begin(); it != revenue_map.end(); ++it) {
        auto [key, rev] = *it;
        results.push_back({key.orderkey, rev, key.orderdate, key.shippriority});
    }

    // Use partial_sort for Top-10 optimization (only sort what we need)
    size_t k = std::min(static_cast<size_t>(10), results.size());
    std::partial_sort(results.begin(), results.begin() + k, results.end(),
        [](const Q3Result& a, const Q3Result& b) {
            if (a.revenue != b.revenue)
                return a.revenue > b.revenue; // DESC
            return a.orderdate < b.orderdate; // ASC
        });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Print results
    std::cout << "\n=== Q3: Shipping Priority ===\n";
    std::cout << std::right << std::setw(12) << "ORDERKEY"
              << std::setw(18) << "REVENUE"
              << std::setw(15) << "ORDERDATE"
              << std::setw(15) << "SHIPPRIORITY" << "\n";
    std::cout << std::string(60, '-') << "\n";

    for (const auto& r : results) {
        std::cout << std::right << std::setw(12) << r.orderkey
                  << std::fixed << std::setprecision(2) << std::setw(18) << r.revenue
                  << std::setw(15) << days_to_date_str(r.orderdate)
                  << std::setw(15) << r.shippriority << "\n";
    }

    std::cout << "\nExecution time: " << duration.count() << " ms\n";
}

} // namespace gendb
