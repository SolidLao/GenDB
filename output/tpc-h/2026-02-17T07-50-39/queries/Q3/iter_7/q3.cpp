/*
 * TPC-H Q3: Shipping Priority Query (Iteration 7 - Index-Based Semi-Join + Bloom Filter)
 *
 * LOGICAL PLAN:
 * 1. Filter customer: c_mktsegment = 'BUILDING' → ~300K rows (20% selectivity)
 * 2. Join customer→orders: c_custkey = o_custkey + filter o_orderdate < '1995-03-15'
 * 3. Join orders→lineitem: o_orderkey = l_orderkey + filter l_shipdate > '1995-03-15'
 * 4. Aggregate: GROUP BY (l_orderkey, o_orderdate, o_shippriority), SUM(revenue)
 * 5. Sort: ORDER BY revenue DESC, o_orderdate ASC, LIMIT 10
 *
 * PHYSICAL PLAN (ITERATION 7 - REVERSE PIPELINE WITH BLOOM FILTER):
 * 1. Build customer filter bitmap: c_mktsegment='BUILDING' → ~300K custkeys
 * 2. Build bloom filter from qualifying custkeys (small, 300K entries)
 * 3. LINEITEM FIRST: Parallel zone-map scan lineitem with l_shipdate > DATE filter
 *    - Extract qualifying l_orderkey values → compact hash set (~2.3M unique orderkeys)
 * 4. Scan orders ONCE with THREE filters in single pass:
 *    - o_orderdate < DATE (zone map)
 *    - o_custkey IN customer_bitmap (bitmap probe)
 *    - o_orderkey IN lineitem_orderkeys (hash probe from step 3)
 *    → Build TINY hash table: orderkey → (orderdate, shippriority) (~11K entries)
 * 5. Re-scan qualifying lineitem rows (already filtered in step 3) + probe tiny orders hash + aggregate
 * 6. Partial sort (Top-K=10)
 *
 * KEY OPTIMIZATIONS (ITER 7):
 * - Avoid building large orders hash table (1.46M → 11K by pre-filtering with lineitem keys)
 * - Lineitem scanned twice but with zone maps → cheaper than building large hash table
 * - Bloom filter for custkey reduces orders scan pressure
 * - Final orders hash table fits entirely in L3 cache
 * - Fused aggregation with partitioned hash tables (lock-free)
 *
 * ARCHITECTURE CHANGE: Treat lineitem as semi-join filter BEFORE building orders hash table.
 * This inverts the traditional customer→orders→lineitem pipeline to exploit selectivity.
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
// Bloom filter (for semi-join reduction)
// ============================================================================

struct BloomFilter {
    std::vector<uint64_t> bits;
    size_t num_bits;
    static constexpr int NUM_HASHES = 3;

    BloomFilter(size_t expected_size) {
        num_bits = expected_size * 10;  // 10 bits per element
        bits.resize((num_bits + 63) / 64, 0);
    }

    void insert(int32_t key) {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        for (int i = 0; i < NUM_HASHES; i++) {
            size_t bit_idx = (h + i * 0x517CC1B727220A95ULL) % num_bits;
            bits[bit_idx >> 6] |= (1ULL << (bit_idx & 63));
        }
    }

    bool contains(int32_t key) const {
        uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
        for (int i = 0; i < NUM_HASHES; i++) {
            size_t bit_idx = (h + i * 0x517CC1B727220A95ULL) % num_bits;
            if (!(bits[bit_idx >> 6] & (1ULL << (bit_idx & 63)))) return false;
        }
        return true;
    }
};

// ============================================================================
// Open-addressing hash table
// ============================================================================

template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
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
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// ============================================================================
// Hash set for orderkey tracking
// ============================================================================

struct CompactHashSet {
    struct Entry {
        int32_t key;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;
    std::atomic<size_t> size_atomic{0};

    CompactHashSet(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    // Copy constructor
    CompactHashSet(const CompactHashSet& other)
        : table(other.table), mask(other.mask), size_atomic(other.size_atomic.load()) {}

    // Move constructor
    CompactHashSet(CompactHashSet&& other) noexcept
        : table(std::move(other.table)), mask(other.mask), size_atomic(other.size_atomic.load()) {}

    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    bool insert(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return false;  // already exists
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, true};
        size_atomic.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool contains(int32_t key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return true;
            idx = (idx + 1) & mask;
        }
        return false;
    }

    size_t size() const {
        return size_atomic.load(std::memory_order_relaxed);
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
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;
    AggKeyHash hasher;

    CompactHashTableComposite(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

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
        for (const auto& entry : table) {
            if (entry.occupied) callback(entry.key, entry.value);
        }
    }
};

// ============================================================================
// Main Query Execution
// ============================================================================

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

    // Note: Pre-built customer index not needed for this query
    // We use direct scan of c_mktsegment with bitmap for compactness

    // Orders columns
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
    // 2. Build customer filter bitmap + bloom filter
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
#endif

    // Use bitmap for compact representation (1.5M customers → ~187KB bitmap)
    const size_t MAX_CUSTKEY = 1500001;  // custkey 1-based, max 1.5M
    std::vector<bool> customer_bitmap(MAX_CUSTKEY, false);

    // Collect qualifying custkeys for bloom filter
    std::vector<int32_t> qualifying_custkeys;
    qualifying_custkeys.reserve(300000);

    for (size_t i = 0; i < c_count; i++) {
        if (c_mktsegment[i] == building_code) {
            int32_t custkey = i + 1;
            customer_bitmap[custkey] = true;
            qualifying_custkeys.push_back(custkey);
        }
    }

    // Build bloom filter for fast negative lookups
    BloomFilter custkey_bloom(qualifying_custkeys.size());
    for (int32_t custkey : qualifying_custkeys) {
        custkey_bloom.insert(custkey);
    }

#ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double cust_ms = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] scan_filter_customer: %.2f ms (%zu filtered)\n", cust_ms, qualifying_custkeys.size());
#endif

    // ------------------------------------------------------------------------
    // 3. LINEITEM FIRST: Extract qualifying orderkeys (semi-join reduction)
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_lineitem_extract_start = std::chrono::high_resolution_clock::now();
#endif

    // Partitioned hash sets for lock-free insertion
    const int NUM_PARTITIONS = 64;
    std::vector<CompactHashSet> lineitem_orderkey_partitions;
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        lineitem_orderkey_partitions.emplace_back(100000);
    }
    std::atomic<size_t> lineitem_qualifying_rows{0};

    #pragma omp parallel
    {
        #pragma omp for schedule(dynamic, 1)
        for (uint32_t z = 0; z < lineitem_num_zones; z++) {
            int32_t zone_max = lineitem_zones[z * 2 + 1];

            // Skip zone if all values <= DATE_1995_03_15
            if (zone_max <= DATE_1995_03_15) continue;

            uint32_t start_row = z * 100000;
            uint32_t end_row = std::min((uint32_t)(z + 1) * 100000, (uint32_t)l_count);

            for (uint32_t i = start_row; i < end_row; i++) {
                if (l_shipdate[i] > DATE_1995_03_15) {
                    int32_t orderkey = l_orderkey[i];
                    size_t part_id = ((size_t)orderkey * 0x9E3779B97F4A7C15ULL) % NUM_PARTITIONS;
                    lineitem_orderkey_partitions[part_id].insert(orderkey);
                    lineitem_qualifying_rows.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }

    // Count unique orderkeys
    size_t unique_orderkeys = 0;
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        unique_orderkeys += lineitem_orderkey_partitions[p].size();
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_extract_end = std::chrono::high_resolution_clock::now();
    double lineitem_extract_ms = std::chrono::duration<double, std::milli>(t_lineitem_extract_end - t_lineitem_extract_start).count();
    printf("[TIMING] lineitem_extract_orderkeys: %.2f ms (%zu rows, %zu unique orderkeys)\n",
           lineitem_extract_ms, lineitem_qualifying_rows.load(), unique_orderkeys);
#endif

    // ------------------------------------------------------------------------
    // 4. Scan orders with TRIPLE filter → build TINY hash table
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    struct OrderInfo {
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    // Partitioned hash tables for orders
    std::vector<CompactHashTable<int32_t, OrderInfo>> orders_partitions;
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        orders_partitions.emplace_back(5000);  // Much smaller now
    }
    std::atomic<size_t> orders_filtered{0};

    #pragma omp parallel
    {
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
                    int32_t orderkey = o_orderkey[i];

                    // TRIPLE filter:
                    // 1. Bloom filter (fast negative check)
                    if (!custkey_bloom.contains(custkey)) continue;
                    // 2. Bitmap probe (precise check)
                    if (custkey <= 0 || custkey >= (int32_t)MAX_CUSTKEY || !customer_bitmap[custkey]) continue;
                    // 3. Check if orderkey is in lineitem qualifying set
                    size_t part_id = ((size_t)orderkey * 0x9E3779B97F4A7C15ULL) % NUM_PARTITIONS;
                    if (!lineitem_orderkey_partitions[part_id].contains(orderkey)) continue;

                    // All three filters passed → insert into tiny hash table
                    orders_partitions[part_id].insert(orderkey, {o_orderdate[i], o_shippriority[i]});
                    orders_filtered.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] scan_filter_join_orders: %.2f ms (%zu filtered)\n", orders_ms, orders_filtered.load());
#endif

    // ------------------------------------------------------------------------
    // 5. Re-scan lineitem + join tiny orders hash + aggregate
    // ------------------------------------------------------------------------
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    std::atomic<size_t> lineitem_matches{0};

    // Thread-local aggregation tables (truly lock-free)
    std::vector<CompactHashTableComposite<int64_t>> thread_local_agg[64];
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        // Each thread has its own aggregation table
        CompactHashTableComposite<int64_t> local_agg(5000);

        #pragma omp for schedule(dynamic, 1) nowait
        for (uint32_t z = 0; z < lineitem_num_zones; z++) {
            int32_t zone_max = lineitem_zones[z * 2 + 1];

            // Skip zone if all values <= DATE_1995_03_15
            if (zone_max <= DATE_1995_03_15) continue;

            uint32_t start_row = z * 100000;
            uint32_t end_row = std::min((uint32_t)(z + 1) * 100000, (uint32_t)l_count);

            // FUSED: scan + filter + join + aggregate in single tight loop
            for (uint32_t i = start_row; i < end_row; i++) {
                if (l_shipdate[i] > DATE_1995_03_15) {
                    int32_t orderkey = l_orderkey[i];
                    // Probe partitioned orders hash table (now TINY)
                    size_t part_id = ((size_t)orderkey * 0x9E3779B97F4A7C15ULL) % NUM_PARTITIONS;
                    auto* order_info = orders_partitions[part_id].find(orderkey);

                    if (order_info) {
                        int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);
                        AggKey agg_key{orderkey, order_info->o_orderdate, order_info->o_shippriority};

                        // Aggregate into thread-local table (no contention)
                        *local_agg.find_or_insert(agg_key) += revenue;
                        lineitem_matches.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }

        // Store thread-local results
        #pragma omp critical
        {
            thread_local_agg[tid].push_back(std::move(local_agg));
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms (%zu matches)\n", agg_ms, lineitem_matches.load());
#endif

    // ------------------------------------------------------------------------
    // 5. Merge thread-local aggregations and collect results
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

    // Merge all thread-local aggregation tables into a final hash table
    CompactHashTableComposite<int64_t> final_agg(100000);
    for (int t = 0; t < 64; t++) {
        for (auto& local_table : thread_local_agg[t]) {
            local_table.iterate([&](const AggKey& key, int64_t revenue) {
                *final_agg.find_or_insert(key) += revenue;
            });
        }
    }

    // Collect results from final aggregation
    std::vector<Result> results;
    results.reserve(50000);

    final_agg.iterate([&](const AggKey& key, int64_t revenue) {
        results.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
    });

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
