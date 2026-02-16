#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <chrono>
#include <cmath>
#include <thread>
#include <iomanip>
#include <omp.h>

// ============================================================================
// CONSTANTS & HELPER FUNCTIONS
// ============================================================================

// Epoch date constants (days since 1970-01-01)
// 1995-03-15 = 9204 days
// 1995-03-15 (exclusive) means l_shipdate > 9204
static constexpr int32_t DATE_1995_03_15 = 9204;
static constexpr int32_t DATE_1995_03_15_EXCLUSIVE = 9205;

// DECIMAL scale factor
static constexpr int64_t SCALE_FACTOR = 100;

// ============================================================================
// COMPACT HASH TABLE (Open Addressing)
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
    size_t count;

    CompactHashTable(size_t expected_size) : count(0) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert_or_update(K key, const V& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
        count++;
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    size_t size() const { return count; }
};

struct ResultRow {
    int32_t l_orderkey;
    __int128 revenue;  // in scaled form (scaled by 10000 = SCALE_FACTOR^2)
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t days) {
    // Based on standard epoch formula
    int32_t day_count = days;

    // Adjust for epoch (1970-01-01)
    int32_t year = 1970;

    // Add years
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (day_count < days_in_year) break;
        day_count -= days_in_year;
        year++;
    }

    // Add months
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    int32_t month = 0;
    while (day_count >= days_in_month[month]) {
        day_count -= days_in_month[month];
        month++;
    }

    int32_t day = day_count + 1;  // Days are 1-indexed

    char buf[12];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month + 1, day);
    return std::string(buf);
}

// Load dictionary from file and find code for target string
int8_t find_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "ERROR: Cannot open dictionary file: " << dict_path << std::endl;
        return -1;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            std::string value = line.substr(eq_pos + 1);
            if (value == target) {
                return static_cast<int8_t>(std::stoi(line.substr(0, eq_pos)));
            }
        }
    }
    return -1;
}

// Mmap helper
template<typename T>
const T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open file: " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return nullptr;
    }

    count = st.st_size / sizeof(T);
    auto* ptr = (const T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "ERROR: mmap failed for " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// ============================================================================
// ZONE MAP STRUCTURES
// ============================================================================

struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
    uint32_t row_count;
};

const ZoneMapEntry* load_zone_map(const std::string& path, size_t& num_zones) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open zone map: " << path << std::endl;
        return nullptr;
    }

    uint32_t header;
    if (read(fd, &header, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        return nullptr;
    }
    num_zones = header;

    // Mmap the entire file from offset 0, then access data after header
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return nullptr;
    }

    auto* base = (const uint8_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (base == MAP_FAILED) {
        std::cerr << "ERROR: mmap failed for zone map: " << path << std::endl;
        return nullptr;
    }

    // Cast to ZoneMapEntry pointer at offset sizeof(uint32_t)
    return (const ZoneMapEntry*)(base + sizeof(uint32_t));
}

// ============================================================================
// HASH INDEX STRUCTURES & LOADING
// ============================================================================

struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

struct HashIndex {
    uint32_t num_unique;
    uint32_t table_size;
    const HashIndexEntry* entries;
    const uint32_t* positions;
};

HashIndex load_hash_index(const std::string& path) {
    HashIndex idx = {};

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "ERROR: Cannot open hash index: " << path << std::endl;
        return idx;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return idx;
    }

    auto* base = (const uint8_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return idx;
    }
    close(fd);

    const uint32_t* ptr = (const uint32_t*)base;
    idx.num_unique = *ptr++;
    idx.table_size = *ptr++;
    idx.entries = (const HashIndexEntry*)ptr;
    idx.positions = (const uint32_t*)(idx.entries + idx.table_size);

    return idx;
}

// ============================================================================
// MAIN QUERY EXECUTION
// ============================================================================

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ========================================================================
    // PHASE 1: Load Customer Data
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_customers = 0;
    const int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", num_customers);
    const int8_t* c_mktsegment = mmap_file<int8_t>(gendb_dir + "/customer/c_mktsegment.bin", num_customers);

    if (!c_custkey || !c_mktsegment) {
        std::cerr << "ERROR: Failed to load customer data" << std::endl;
        return;
    }

    // Load BUILDING code from dictionary
    int8_t building_code = find_dict_code(gendb_dir + "/customer/c_mktsegment_dict.txt", "BUILDING");
    if (building_code < 0) {
        std::cerr << "ERROR: Cannot find BUILDING code in dictionary" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms);
#endif

    // Filter customers: c_mktsegment = 'BUILDING'
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<int32_t> building_customers;
    for (size_t i = 0; i < num_customers; i++) {
        if (c_mktsegment[i] == building_code) {
            building_customers.push_back(c_custkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_building_customers: %.2f ms (count=%zu)\n", ms, building_customers.size());
#endif

    // ========================================================================
    // PHASE 2: Load Orders Data
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_orders = 0;
    const int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", num_orders);
    const int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", num_orders);
    const int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", num_orders);
    const int32_t* o_shippriority = mmap_file<int32_t>(gendb_dir + "/orders/o_shippriority.bin", num_orders);

    if (!o_orderkey || !o_custkey || !o_orderdate || !o_shippriority) {
        std::cerr << "ERROR: Failed to load orders data" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // Load zone map for orders o_orderdate to enable pruning
    size_t num_zones_orders = 0;
    const ZoneMapEntry* zones_orderdate = load_zone_map(gendb_dir + "/indexes/orders_o_orderdate_zone.bin", num_zones_orders);

    // Build sorted vector of BUILDING customers for binary search (faster than unordered_set)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(building_customers.begin(), building_customers.end());

    // Filter orders: c_custkey in BUILDING and o_orderdate < 1995-03-15
    // Use CompactHashTable for orders (2-5x faster than unordered_map)
    CompactHashTable<int32_t, std::pair<int32_t, int32_t>> orders_map(2000000);

    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTable<int32_t, std::pair<int32_t, int32_t>>> thread_orders;
    for (int t = 0; t < num_threads; t++) {
        thread_orders.emplace_back(2000000 / num_threads + 10000);
    }

    if (zones_orderdate) {
        // Build a list of row ranges to process based on zone map
        std::vector<std::pair<size_t, size_t>> row_ranges;
        size_t rows_per_zone = 150000;  // orders block size from storage guide

        for (size_t z = 0; z < num_zones_orders; z++) {
            // Skip blocks where all dates >= 1995-03-15 (i.e., min_value >= DATE_1995_03_15)
            if (zones_orderdate[z].min_value >= DATE_1995_03_15) continue;

            // Calculate row range for this block
            size_t block_start = z * rows_per_zone;
            size_t block_end = std::min(block_start + rows_per_zone, num_orders);
            row_ranges.push_back({block_start, block_end});
        }

        // Parallel scan on qualified ranges
        #pragma omp parallel for schedule(dynamic) num_threads(num_threads)
        for (size_t range_idx = 0; range_idx < row_ranges.size(); range_idx++) {
            int thread_id = omp_get_thread_num();
            auto& local_ht = thread_orders[thread_id];

            size_t start = row_ranges[range_idx].first;
            size_t end = row_ranges[range_idx].second;

            for (size_t i = start; i < end; i++) {
                // Use binary search on sorted building_customers
                if (std::binary_search(building_customers.begin(), building_customers.end(), o_custkey[i])
                    && o_orderdate[i] < DATE_1995_03_15) {
                    local_ht.insert_or_update(o_orderkey[i], std::make_pair(o_orderdate[i], o_shippriority[i]));
                }
            }
        }
    } else {
        // Fallback: no zone map, use standard parallel scan
        #pragma omp parallel for schedule(static, 150000) num_threads(num_threads)
        for (size_t i = 0; i < num_orders; i++) {
            int thread_id = omp_get_thread_num();
            auto& local_ht = thread_orders[thread_id];

            if (std::binary_search(building_customers.begin(), building_customers.end(), o_custkey[i])
                && o_orderdate[i] < DATE_1995_03_15) {
                local_ht.insert_or_update(o_orderkey[i], std::make_pair(o_orderdate[i], o_shippriority[i]));
            }
        }
    }

    // Merge thread-local hash tables into global orders_map
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_orders[t].table) {
            if (entry.occupied) {
                orders_map.insert_or_update(entry.key, entry.value);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_and_build_orders: %.2f ms (count=%zu)\n", ms, orders_map.size());
#endif

    // ========================================================================
    // PHASE 3: Load Lineitem Data
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_lineitem = 0;
    const int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", num_lineitem);
    const int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", num_lineitem);
    const int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", num_lineitem);
    const int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", num_lineitem);

    if (!l_orderkey || !l_extendedprice || !l_discount || !l_shipdate) {
        std::cerr << "ERROR: Failed to load lineitem data" << std::endl;
        return;
    }

    // Load zone map for l_shipdate to enable pruning
    size_t num_zones_li = 0;
    const ZoneMapEntry* zones_shipdate = load_zone_map(gendb_dir + "/indexes/lineitem_l_shipdate_zone.bin", num_zones_li);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 4: Join and Aggregate (Parallel)
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Hash map: l_orderkey -> (revenue accumulator, o_orderdate, o_shippriority)
    // Use __int128 for intermediate products to avoid overflow and preserve precision
    struct OrderData {
        __int128 total_revenue;  // Accumulated as full-precision product
        int32_t orderdate;
        int32_t shippriority;
    };

    // Thread-local aggregation: each thread builds its own compact hash table
    int num_threads_ja = omp_get_max_threads();
    std::vector<CompactHashTable<int32_t, OrderData>> thread_results;
    for (int t = 0; t < num_threads_ja; t++) {
        thread_results.emplace_back(orders_map.size() / num_threads_ja + 10000);
    }

    // Parallel scan with zone map pruning
    if (zones_shipdate) {
        // Build a list of qualified row ranges based on zone map
        std::vector<std::pair<size_t, size_t>> row_ranges;
        size_t rows_per_block = 200000;  // lineitem block size from storage guide

        for (size_t z = 0; z < num_zones_li; z++) {
            // Zone map pruning: skip blocks where all shipdate <= DATE_1995_03_15
            if (zones_shipdate[z].max_value <= DATE_1995_03_15) continue;

            // Calculate row range for this block
            size_t block_start = z * rows_per_block;
            size_t block_end = std::min(block_start + rows_per_block, num_lineitem);
            row_ranges.push_back({block_start, block_end});
        }

        // Parallel scan on qualified ranges
        #pragma omp parallel for schedule(dynamic)
        for (size_t range_idx = 0; range_idx < row_ranges.size(); range_idx++) {
            int thread_id = omp_get_thread_num();
            auto& local_map = thread_results[thread_id];

            size_t start = row_ranges[range_idx].first;
            size_t end = row_ranges[range_idx].second;

            for (size_t i = start; i < end; i++) {
                int32_t orderkey = l_orderkey[i];
                auto* order_data = orders_map.find(orderkey);
                if (!order_data) continue;

                // Check l_shipdate > 1995-03-15
                if (l_shipdate[i] <= DATE_1995_03_15) continue;

                int32_t orderdate = order_data->first;
                int32_t shippriority = order_data->second;

                // Calculate revenue = l_extendedprice * (1 - l_discount)
                __int128 product = (__int128)l_extendedprice[i] * (SCALE_FACTOR - l_discount[i]);

                auto* existing = local_map.find(orderkey);
                if (!existing) {
                    local_map.insert_or_update(orderkey, {product, orderdate, shippriority});
                } else {
                    existing->total_revenue += product;
                }
            }
        }
    } else {
        // Fallback: zone map not available, use standard parallel scan
        #pragma omp parallel
        {
            int thread_id = omp_get_thread_num();
            auto& local_map = thread_results[thread_id];

            #pragma omp for schedule(static, 200000)
            for (size_t i = 0; i < num_lineitem; i++) {
                int32_t orderkey = l_orderkey[i];

                // Check if orderkey exists in filtered orders
                auto* order_data = orders_map.find(orderkey);
                if (!order_data) continue;

                int32_t orderdate = order_data->first;
                int32_t shippriority = order_data->second;

                // Check l_shipdate > 1995-03-15
                if (l_shipdate[i] <= DATE_1995_03_15) continue;

                // Calculate revenue = l_extendedprice * (1 - l_discount)
                __int128 product = (__int128)l_extendedprice[i] * (SCALE_FACTOR - l_discount[i]);

                auto* existing = local_map.find(orderkey);
                if (!existing) {
                    local_map.insert_or_update(orderkey, {product, orderdate, shippriority});
                } else {
                    existing->total_revenue += product;
                }
            }
        }
    }

    // Merge thread-local results into global compact hash table
    CompactHashTable<int32_t, OrderData> result_groups(orders_map.size());

    for (int t = 0; t < num_threads_ja; t++) {
        for (const auto& entry : thread_results[t].table) {
            if (entry.occupied) {
                auto* existing = result_groups.find(entry.key);
                if (!existing) {
                    result_groups.insert_or_update(entry.key, entry.value);
                } else {
                    existing->total_revenue += entry.value.total_revenue;
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_and_aggregate: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 5: GROUP BY and SUM
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    for (const auto& entry : result_groups.table) {
        if (entry.occupied) {
            results.push_back({entry.key, entry.value.total_revenue, entry.value.orderdate, entry.value.shippriority});
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] group_and_sum: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 6: ORDER BY revenue DESC, o_orderdate
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) {
            return a.revenue > b.revenue;  // DESC
        }
        return a.o_orderdate < b.o_orderdate;  // ASC
    });

    // LIMIT 10
    if (results.size() > 10) {
        results.resize(10);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort_and_limit: %.2f ms\n", ms);
#endif

    // ========================================================================
    // PHASE 7: Write Results
    // ========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "ERROR: Cannot open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    // Write results
    for (const auto& row : results) {
        // revenue is scaled by 10000 (SCALE_FACTOR^2), so divide to get original value
        double revenue_double = static_cast<double>(row.revenue) / (SCALE_FACTOR * SCALE_FACTOR);
        std::string orderdate_str = format_date(row.o_orderdate);
        out << row.l_orderkey << ","
            << std::fixed << std::setprecision(4) << revenue_double << ","
            << orderdate_str << ","
            << row.o_shippriority << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Query Q3 executed successfully. Results written to " << output_path << std::endl;
    std::cout << "Result count: " << results.size() << std::endl;
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
