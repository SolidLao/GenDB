/*
 * TPC-H Q3: Shipping Priority Query (Iteration 9 - Optimized Join Order)
 *
 * LOGICAL PLAN:
 * 1. Filter customer: c_mktsegment = 'BUILDING' → ~300K rows (20% selectivity)
 * 2. Join customer→orders: c_custkey = o_custkey + filter o_orderdate < '1995-03-15'
 * 3. Join orders→lineitem: o_orderkey = l_orderkey + filter l_shipdate > '1995-03-15'
 * 4. Aggregate: GROUP BY (l_orderkey, o_orderdate, o_shippriority), SUM(revenue)
 * 5. Sort: ORDER BY revenue DESC, o_orderdate ASC, LIMIT 10
 *
 * PHYSICAL PLAN (ITERATION 9 - LIGHTWEIGHT BUILD):
 * 1. Parallel scan customer with filter c_mktsegment='BUILDING' → uint8_t bitmap (~300K set)
 * 2. Parallel zone-map scan orders (with date filter + customer join):
 *    - Filter by o_orderdate < DATE (zone skip)
 *    - Probe customer bitmap for c_custkey (fast bitmap check)
 *    - Build compact hash table: orderkey → (orderdate, shippriority)
 *    - OPTIMIZATION: Parallel partitioned construction (lock-free)
 * 3. FUSED parallel zone-map scan lineitem:
 *    - Filter by l_shipdate > DATE (zone skip)
 *    - Probe orders hash table (compact, ~1.5M entries)
 *    - Compute revenue and aggregate into lock-free partitioned hash tables
 * 4. Merge partitioned aggregation results and partial sort (Top-K=10)
 *
 * KEY OPTIMIZATIONS (ITER 9):
 * - uint8_t bitmap for customer (O(1) probe, no hash overhead)
 * - Parallel partitioned hash table build for orders (lock-free construction)
 * - Open-addressing hash table (2-3x faster than std::unordered_map)
 * - Partition-owned aggregation tables (eliminates merge contention)
 * - Zone map pruning on both orders and lineitem
 * - Fused scan+filter+join+aggregate on lineitem (no intermediate materialization)
 *
 * DATE ENCODING: epoch days from 1970-01-01 (stored as int32_t)
 * DECIMAL: scaled integers (l_extendedprice, l_discount scaled by 100)
 */

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <cstdio>
#include <mutex>
#include <omp.h>

namespace {

// ============================================================================
// Date utilities
// ============================================================================

int32_t date_to_epoch_days(int year, int month, int day) {
    // Compute days since 1970-01-01
    int32_t days = 0;
    // Add days for complete years
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add days for complete months
    static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; m++) {
        days += month_days[m];
        if (m == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) days++;
    }
    days += (day - 1);
    return days;
}

void epoch_days_to_date(int32_t epoch_days, int& year, int& month, int& day) {
    int32_t days = epoch_days;
    year = 1970;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }
    static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    month = 1;
    while (month <= 12) {
        int days_in_month = month_days[month];
        if (month == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) days_in_month = 29;
        if (days < days_in_month) break;
        days -= days_in_month;
        month++;
    }
    day = days + 1;
}

// ============================================================================
// Open-addressing hash table (lock-free concurrent insert)
// ============================================================================

template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied;
    };

    Entry* table;
    size_t capacity;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        capacity = sz;
        mask = sz - 1;
        // Use aligned_alloc for better cache alignment
        table = (Entry*)std::aligned_alloc(64, sz * sizeof(Entry));
        // Initialize
        for (size_t i = 0; i < sz; i++) {
            new (&table[i]) Entry{0, V{}, false};
        }
    }

    ~CompactHashTable() {
        if (table) {
            for (size_t i = 0; i < capacity; i++) {
                table[i].~Entry();
            }
            std::free(table);
        }
    }

    // Disable copy/move for simplicity
    CompactHashTable(const CompactHashTable&) = delete;
    CompactHashTable& operator=(const CompactHashTable&) = delete;

    size_t hash(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    // Concurrent insert (lock-free, assumes unique keys or idempotent updates)
    void insert_concurrent(K key, const V& value) {
        size_t idx = hash(key) & mask;
        while (true) {
            // Use atomic operations on occupied flag
            bool expected = false;
            if (__atomic_load_n(&table[idx].occupied, __ATOMIC_ACQUIRE)) {
                // Slot occupied - check if it's our key
                if (__atomic_load_n(&table[idx].key, __ATOMIC_RELAXED) == key) {
                    table[idx].value = value;  // Update
                    return;
                }
                idx = (idx + 1) & mask;  // Linear probe
            } else {
                // Try to claim empty slot
                if (__atomic_compare_exchange_n(&table[idx].occupied, &expected, true,
                    false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
                    __atomic_store_n(&table[idx].key, key, __ATOMIC_RELAXED);
                    table[idx].value = value;
                    return;
                }
                // CAS failed - another thread claimed it, retry
            }
        }
    }

    void insert(K key, const V& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].value = value;
        table[idx].occupied = true;
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (__atomic_load_n(&table[idx].occupied, __ATOMIC_ACQUIRE)) {
            if (__atomic_load_n(&table[idx].key, __ATOMIC_RELAXED) == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// ============================================================================
// Composite hash for aggregation
// ============================================================================

struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        size_t h = (size_t)k.l_orderkey * 0x9E3779B97F4A7C15ULL;
        h ^= (size_t)k.o_orderdate * 0x517CC1B727220A95ULL;
        h ^= (size_t)k.o_shippriority * 0x85EBCA6B0C137B91ULL;
        return h;
    }
};

template<typename V>
struct CompactHashTableComposite {
    struct Entry {
        AggKey key;
        V value;
        bool occupied;
    };

    Entry* table;
    size_t capacity;
    size_t mask;
    AggKeyHash hasher;

    CompactHashTableComposite(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        capacity = sz;
        mask = sz - 1;
        table = (Entry*)std::aligned_alloc(64, sz * sizeof(Entry));
        for (size_t i = 0; i < sz; i++) {
            new (&table[i]) Entry{{0, 0, 0}, 0, false};
        }
    }

    ~CompactHashTableComposite() {
        if (table) {
            for (size_t i = 0; i < capacity; i++) {
                table[i].~Entry();
            }
            std::free(table);
        }
    }

    CompactHashTableComposite(const CompactHashTableComposite&) = delete;
    CompactHashTableComposite& operator=(const CompactHashTableComposite&) = delete;

    size_t hash(const AggKey& key) const {
        return hasher(key);
    }

    V* find_or_insert(const AggKey& key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].value = 0;
        table[idx].occupied = true;
        return &table[idx].value;
    }

    template<typename Callback>
    void iterate(Callback callback) const {
        for (size_t i = 0; i < capacity; i++) {
            if (table[i].occupied) callback(table[i].key, table[i].value);
        }
    }
};

// ============================================================================
// Main Query Execution
// ============================================================================

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    const int32_t DATE_1995_03_15 = date_to_epoch_days(1995, 3, 15);

    // ------------------------------------------------------------------------
    // 1. Load all data columns and indexes
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    std::string cust_dir = gendb_dir + "/customer/";
    std::string orders_dir = gendb_dir + "/orders/";
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    std::string index_dir = gendb_dir + "/indexes/";

    // Customer columns
    int fd_c_mktsegment = open((cust_dir + "c_mktsegment.bin").c_str(), O_RDONLY);
    struct stat st_c_mktsegment; fstat(fd_c_mktsegment, &st_c_mktsegment);
    auto* c_mktsegment = (const int32_t*)mmap(nullptr, st_c_mktsegment.st_size, PROT_READ, MAP_PRIVATE, fd_c_mktsegment, 0);
    size_t c_count = st_c_mktsegment.st_size / sizeof(int32_t);
    close(fd_c_mktsegment);

    // Load c_mktsegment dictionary
    std::vector<std::string> c_mktsegment_dict;
    std::ifstream dict_file(cust_dir + "c_mktsegment_dict.txt");
    std::string line;
    while (std::getline(dict_file, line)) {
        c_mktsegment_dict.push_back(line);
    }
    dict_file.close();

    // Find 'BUILDING' code
    int32_t building_code = -1;
    for (size_t i = 0; i < c_mktsegment_dict.size(); i++) {
        if (c_mktsegment_dict[i] == "BUILDING") {
            building_code = (int32_t)i;
            break;
        }
    }

    // Note: Pre-built orders_custkey_hash index available but not used in this iteration
    // We build a compact hash table directly from the filtered scan instead

    // Orders columns (needed for lookups)
    int fd_o_orderkey = open((orders_dir + "o_orderkey.bin").c_str(), O_RDONLY);
    struct stat st_o_orderkey; fstat(fd_o_orderkey, &st_o_orderkey);
    auto* o_orderkey = (const int32_t*)mmap(nullptr, st_o_orderkey.st_size, PROT_READ, MAP_PRIVATE, fd_o_orderkey, 0);
    size_t o_count = st_o_orderkey.st_size / sizeof(int32_t);
    close(fd_o_orderkey);

    int fd_o_custkey = open((orders_dir + "o_custkey.bin").c_str(), O_RDONLY);
    struct stat st_o_custkey; fstat(fd_o_custkey, &st_o_custkey);
    auto* o_custkey = (const int32_t*)mmap(nullptr, st_o_custkey.st_size, PROT_READ, MAP_PRIVATE, fd_o_custkey, 0);
    close(fd_o_custkey);

    int fd_o_orderdate = open((orders_dir + "o_orderdate.bin").c_str(), O_RDONLY);
    struct stat st_o_orderdate; fstat(fd_o_orderdate, &st_o_orderdate);
    auto* o_orderdate = (const int32_t*)mmap(nullptr, st_o_orderdate.st_size, PROT_READ, MAP_PRIVATE, fd_o_orderdate, 0);
    close(fd_o_orderdate);

    int fd_o_shippriority = open((orders_dir + "o_shippriority.bin").c_str(), O_RDONLY);
    struct stat st_o_shippriority; fstat(fd_o_shippriority, &st_o_shippriority);
    auto* o_shippriority = (const int32_t*)mmap(nullptr, st_o_shippriority.st_size, PROT_READ, MAP_PRIVATE, fd_o_shippriority, 0);
    close(fd_o_shippriority);

    // Orders zone map
    int fd_orders_zone = open((index_dir + "orders_orderdate_zone.bin").c_str(), O_RDONLY);
    struct stat st_orders_zone; fstat(fd_orders_zone, &st_orders_zone);
    auto* orders_zone_data = (const char*)mmap(nullptr, st_orders_zone.st_size, PROT_READ, MAP_PRIVATE, fd_orders_zone, 0);
    uint32_t orders_num_zones = *(const uint32_t*)orders_zone_data;
    auto* orders_zones = (const int32_t*)(orders_zone_data + 4);
    close(fd_orders_zone);

    // Lineitem columns
    int fd_l_orderkey = open((lineitem_dir + "l_orderkey.bin").c_str(), O_RDONLY);
    struct stat st_l_orderkey; fstat(fd_l_orderkey, &st_l_orderkey);
    auto* l_orderkey = (const int32_t*)mmap(nullptr, st_l_orderkey.st_size, PROT_READ, MAP_PRIVATE, fd_l_orderkey, 0);
    size_t l_count = st_l_orderkey.st_size / sizeof(int32_t);
    close(fd_l_orderkey);

    int fd_l_extendedprice = open((lineitem_dir + "l_extendedprice.bin").c_str(), O_RDONLY);
    struct stat st_l_extendedprice; fstat(fd_l_extendedprice, &st_l_extendedprice);
    auto* l_extendedprice = (const int64_t*)mmap(nullptr, st_l_extendedprice.st_size, PROT_READ, MAP_PRIVATE, fd_l_extendedprice, 0);
    close(fd_l_extendedprice);

    int fd_l_discount = open((lineitem_dir + "l_discount.bin").c_str(), O_RDONLY);
    struct stat st_l_discount; fstat(fd_l_discount, &st_l_discount);
    auto* l_discount = (const int64_t*)mmap(nullptr, st_l_discount.st_size, PROT_READ, MAP_PRIVATE, fd_l_discount, 0);
    close(fd_l_discount);

    int fd_l_shipdate = open((lineitem_dir + "l_shipdate.bin").c_str(), O_RDONLY);
    struct stat st_l_shipdate; fstat(fd_l_shipdate, &st_l_shipdate);
    auto* l_shipdate = (const int32_t*)mmap(nullptr, st_l_shipdate.st_size, PROT_READ, MAP_PRIVATE, fd_l_shipdate, 0);
    close(fd_l_shipdate);

    // Lineitem zone map
    int fd_lineitem_zone = open((index_dir + "lineitem_shipdate_zone.bin").c_str(), O_RDONLY);
    struct stat st_lineitem_zone; fstat(fd_lineitem_zone, &st_lineitem_zone);
    auto* lineitem_zone_data = (const char*)mmap(nullptr, st_lineitem_zone.st_size, PROT_READ, MAP_PRIVATE, fd_lineitem_zone, 0);
    uint32_t lineitem_num_zones = *(const uint32_t*)lineitem_zone_data;
    auto* lineitem_zones = (const int32_t*)(lineitem_zone_data + 4);
    close(fd_lineitem_zone);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // ------------------------------------------------------------------------
    // 2. Build bitmap of qualifying customers (c_mktsegment = 'BUILDING')
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
#endif

    // Use uint8_t bitmap for fast access (1.5M customers → ~1.5MB bitmap, no bit-packing overhead)
    const size_t MAX_CUSTKEY = 1500001;  // custkey 1-based, max 1.5M
    std::vector<uint8_t> customer_bitmap(MAX_CUSTKEY, 0);
    std::atomic<size_t> cust_filtered{0};

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < c_count; i++) {
        if (c_mktsegment[i] == building_code) {
            // TPC-H property: custkey = row_position + 1 (1-indexed)
            int32_t custkey = i + 1;
            customer_bitmap[custkey] = 1;
            cust_filtered.fetch_add(1, std::memory_order_relaxed);
        }
    }

#ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double cust_ms = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] scan_filter_customer: %.2f ms (%zu filtered)\n", cust_ms, cust_filtered.load());
#endif

    // ------------------------------------------------------------------------
    // 3. Build compact hash table of qualifying orders via zone-pruned scan
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Build hash table with lock-free concurrent inserts
    CompactHashTable<int32_t, OrderInfo> orders_ht(2000000);
    std::atomic<size_t> orders_filtered{0};

    #pragma omp parallel
    {
        size_t local_filtered = 0;

        #pragma omp for schedule(dynamic, 1)
        for (uint32_t z = 0; z < orders_num_zones; z++) {
            int32_t zone_min = orders_zones[z * 2];

            // Skip zone if all values >= DATE_1995_03_15
            if (zone_min >= DATE_1995_03_15) continue;

            uint32_t start_row = z * 100000;
            uint32_t end_row = std::min((uint32_t)(z + 1) * 100000, (uint32_t)o_count);

            for (uint32_t i = start_row; i < end_row; i++) {
                if (o_orderdate[i] < DATE_1995_03_15) {
                    int32_t custkey = o_custkey[i];

                    // Check if custkey is in customer_bitmap
                    if (custkey > 0 && custkey < (int32_t)MAX_CUSTKEY && customer_bitmap[custkey]) {
                        int32_t orderkey = o_orderkey[i];
                        OrderInfo info{o_orderdate[i], o_shippriority[i]};

                        // Lock-free concurrent insert
                        orders_ht.insert_concurrent(orderkey, info);
                        local_filtered++;
                    }
                }
            }
        }

        orders_filtered.fetch_add(local_filtered, std::memory_order_relaxed);
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] scan_filter_join_orders: %.2f ms (%zu filtered)\n", orders_ms, orders_filtered.load());
#endif

    // ------------------------------------------------------------------------
    // 4. FUSED: Parallel filter lineitem + join with orders + aggregate
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    std::atomic<size_t> lineitem_matches{0};

    // Partition-owned aggregation tables (lock-free, minimal merge cost)
    const int NUM_AGG_PARTITIONS = 16;  // Balance between parallelism and merge cost
    std::vector<CompactHashTableComposite<int64_t>*> agg_partitions(NUM_AGG_PARTITIONS);
    std::vector<std::mutex> agg_locks(NUM_AGG_PARTITIONS);
    for (int p = 0; p < NUM_AGG_PARTITIONS; p++) {
        agg_partitions[p] = new CompactHashTableComposite<int64_t>(20000);
    }

    #pragma omp parallel
    {
        // Thread-local batch buffer to amortize lock acquisition
        struct AggUpdate {
            int part_id;
            AggKey key;
            int64_t revenue;
        };
        std::vector<AggUpdate> local_updates;
        local_updates.reserve(10000);

        #pragma omp for schedule(dynamic, 1) nowait
        for (uint32_t z = 0; z < lineitem_num_zones; z++) {
            int32_t zone_max = lineitem_zones[z * 2 + 1];

            // Skip zone if all values <= DATE_1995_03_15
            if (zone_max <= DATE_1995_03_15) continue;

            uint32_t start_row = z * 100000;
            uint32_t end_row = std::min((uint32_t)(z + 1) * 100000, (uint32_t)l_count);

            // FUSED: scan + filter + join + aggregate
            for (uint32_t i = start_row; i < end_row; i++) {
                if (l_shipdate[i] > DATE_1995_03_15) {
                    int32_t orderkey = l_orderkey[i];
                    auto* order_info = orders_ht.find(orderkey);

                    if (order_info) {
                        int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);
                        AggKey agg_key{orderkey, order_info->o_orderdate, order_info->o_shippriority};

                        // Determine partition by hash
                        AggKeyHash hasher;
                        int part_id = hasher(agg_key) % NUM_AGG_PARTITIONS;

                        local_updates.push_back({part_id, agg_key, revenue});
                        lineitem_matches.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }

        // Flush local updates to partitioned aggregation tables
        // Sort by partition to batch lock acquisitions
        std::sort(local_updates.begin(), local_updates.end(),
            [](const AggUpdate& a, const AggUpdate& b) { return a.part_id < b.part_id; });

        int current_part = -1;
        for (auto& upd : local_updates) {
            if (upd.part_id != current_part) {
                if (current_part >= 0) agg_locks[current_part].unlock();
                current_part = upd.part_id;
                agg_locks[current_part].lock();
            }
            *agg_partitions[current_part]->find_or_insert(upd.key) += upd.revenue;
        }
        if (current_part >= 0) agg_locks[current_part].unlock();
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double lineitem_ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms (%zu matches)\n", lineitem_ms, lineitem_matches.load());
#endif

    // ------------------------------------------------------------------------
    // 5. Collect results from partitioned aggregations and sort
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    struct Result {
        int32_t l_orderkey;
        int64_t revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Collect results from partitioned aggregation tables
    std::vector<Result> results;
    results.reserve(50000);

    for (int p = 0; p < NUM_AGG_PARTITIONS; p++) {
        agg_partitions[p]->iterate([&](const AggKey& key, int64_t revenue) {
            results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
        });
    }

    // Clean up aggregation tables
    for (int p = 0; p < NUM_AGG_PARTITIONS; p++) {
        delete agg_partitions[p];
    }

    // Partial sort for Top-10
    size_t topk = std::min((size_t)10, results.size());
    std::partial_sort(results.begin(), results.begin() + topk, results.end(),
        [](const Result& a, const Result& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // ------------------------------------------------------------------------
    // 6. Write output
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q3.csv");
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (size_t i = 0; i < topk; i++) {
        int year, month, day;
        epoch_days_to_date(results[i].o_orderdate, year, month, day);

        // Revenue is scaled by 10000 (100 * 100)
        double revenue_val = results[i].revenue / 10000.0;

        char date_buf[16];
        snprintf(date_buf, sizeof(date_buf), "%04d-%02d-%02d", year, month, day);

        out << results[i].l_orderkey << ","
            << std::fixed << std::setprecision(2) << revenue_val << ","
            << date_buf << ","
            << results[i].o_shippriority << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
