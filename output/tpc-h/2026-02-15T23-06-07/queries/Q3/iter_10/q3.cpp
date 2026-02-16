#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <unordered_map>
#include <iomanip>
#include <cstdint>
#include <thread>
#include <atomic>
#include <omp.h>
#include <immintrin.h>  // AVX2 SIMD intrinsics

// Zone map entry structure (based on storage guide)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t start_row;
    uint32_t row_count;
};

// Hash index entry structure (from Storage Guide: hash_multi_value layout)
struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// Default hash for int32_t
struct DefaultHash {
    size_t operator()(int32_t key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }
};

// Compact open-addressing hash table for joins
template<typename K, typename V, typename Hash = DefaultHash>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;
    Hash hasher;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    void insert(K key, V value) {
        size_t idx = hasher(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hasher(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Helper function to mmap a file
template<typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat " << path << std::endl;
        close(fd);
        return nullptr;
    }
    count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }
    return static_cast<T*>(addr);
}

// Helper function to compute epoch days from date
inline int32_t date_to_epoch(int year, int month, int day) {
    // Days since 1970-01-01
    int days = 0;
    // Count days for complete years (1970 to year-1)
    for (int y = 1970; y < year; ++y) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }
    // Days in each month (non-leap year)
    int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap_year) month_days[1] = 29;

    // Count days for complete months (1 to month-1)
    for (int m = 1; m < month; ++m) {
        days += month_days[m - 1];
    }
    // Add remaining days (day is 1-indexed, so subtract 1)
    days += (day - 1);
    return days;
}

// Helper function to convert epoch days to YYYY-MM-DD
std::string epoch_to_date(int32_t epoch_days) {
    int year = 1970;
    int days_left = epoch_days;

    while (true) {
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int year_days = leap ? 366 : 365;
        if (days_left < year_days) break;
        days_left -= year_days;
        year++;
    }

    int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap_year) month_days[1] = 29;

    int month = 1;
    for (int m = 0; m < 12; ++m) {
        if (days_left < month_days[m]) {
            month = m + 1;
            break;
        }
        days_left -= month_days[m];
    }

    int day = days_left + 1;

    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Result tuple for aggregation
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

// Hash function for GroupKey
struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        // Combine hashes
        size_t h1 = std::hash<int32_t>()(k.l_orderkey);
        size_t h2 = std::hash<int32_t>()(k.o_orderdate);
        size_t h3 = std::hash<int32_t>()(k.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

struct AggResult {
    int64_t revenue_scaled;  // Keep at scale_factor^2 precision during aggregation
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // 1. Load dictionary for c_mktsegment
    #ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Failed to open dictionary file: " << dict_path << std::endl;
        return;
    }

    std::vector<std::string> mktsegment_dict;
    std::string line;
    while (std::getline(dict_file, line)) {
        mktsegment_dict.push_back(line);
    }
    dict_file.close();

    // Find code for 'BUILDING'
    int32_t building_code = -1;
    for (size_t i = 0; i < mktsegment_dict.size(); ++i) {
        if (mktsegment_dict[i] == "BUILDING") {
            building_code = static_cast<int32_t>(i);
            break;
        }
    }

    if (building_code < 0) {
        std::cerr << "BUILDING not found in dictionary!" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionary: %.2f ms\n", dict_ms);
    #endif

    // 2. Load customer data and zone maps
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t c_count, o_count, l_count;

    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", c_count);
    int32_t* c_mktsegment = mmap_file<int32_t>(gendb_dir + "/customer/c_mktsegment.bin", c_count);

    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", o_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", o_count);
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", o_count);
    int32_t* o_shippriority = mmap_file<int32_t>(gendb_dir + "/orders/o_shippriority.bin", o_count);

    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", l_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", l_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", l_count);

    // Load zone map for orders only (lineitem zone map not needed with hash index)
    int fd_o_zone = open((gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin").c_str(), O_RDONLY);

    struct stat st_o_zone;
    fstat(fd_o_zone, &st_o_zone);

    void* o_zone_addr = mmap(nullptr, st_o_zone.st_size, PROT_READ, MAP_PRIVATE, fd_o_zone, 0);
    close(fd_o_zone);

    uint32_t o_zonemap_count = *(uint32_t*)o_zone_addr;

    ZoneMapEntry* o_orderdate_zonemap = (ZoneMapEntry*)((char*)o_zone_addr + sizeof(uint32_t));

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", load_ms);
    #endif

    // Compute date thresholds
    int32_t date_1995_03_15 = date_to_epoch(1995, 3, 15);

    // 3. Filter customer: c_mktsegment = 'BUILDING'
    #ifdef GENDB_PROFILE
    auto t_filter_cust_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<int32_t> filtered_custkeys;
    filtered_custkeys.reserve(c_count / 5);  // Estimate 1/5 for one segment

    for (size_t i = 0; i < c_count; ++i) {
        if (c_mktsegment[i] == building_code) {
            filtered_custkeys.push_back(c_custkey[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_cust_end = std::chrono::high_resolution_clock::now();
    double filter_cust_ms = std::chrono::duration<double, std::milli>(t_filter_cust_end - t_filter_cust_start).count();
    printf("[TIMING] filter_customer: %.2f ms (filtered to %zu rows)\n", filter_cust_ms, filtered_custkeys.size());
    #endif

    // 4. Build hash set for filtered customer keys
    #ifdef GENDB_PROFILE
    auto t_hash_cust_start = std::chrono::high_resolution_clock::now();
    #endif

    CompactHashTable<int32_t, bool> cust_set(filtered_custkeys.size());
    for (int32_t key : filtered_custkeys) {
        cust_set.insert(key, true);
    }

    #ifdef GENDB_PROFILE
    auto t_hash_cust_end = std::chrono::high_resolution_clock::now();
    double hash_cust_ms = std::chrono::duration<double, std::milli>(t_hash_cust_end - t_hash_cust_start).count();
    printf("[TIMING] build_customer_hash: %.2f ms\n", hash_cust_ms);
    #endif

    // 5. Filter orders: o_custkey in filtered set AND o_orderdate < '1995-03-15'
    // Use zone map to skip blocks where o_orderdate >= date_1995_03_15
    // OPTIMIZATION: Collect filtered orderkeys directly (no hash table build)
    #ifdef GENDB_PROFILE
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    // Collect blocks that pass zone map pruning
    std::vector<size_t> valid_blocks;
    for (size_t z = 0; z < o_zonemap_count; ++z) {
        if (o_orderdate_zonemap[z].min_val < date_1995_03_15) {
            valid_blocks.push_back(z);
        }
    }

    int num_threads = omp_get_max_threads();
    std::vector<std::vector<std::tuple<int32_t, int32_t, int32_t>>*> thread_order_vecs(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        thread_order_vecs[t] = new std::vector<std::tuple<int32_t, int32_t, int32_t>>();
        thread_order_vecs[t]->reserve(o_count / (8 * num_threads));
    }

    std::atomic<size_t> orders_scanned(0);
    std::atomic<size_t> orders_matched(0);

    // Parallel block processing
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        size_t local_scanned = 0;
        size_t local_matched = 0;

        #pragma omp for schedule(dynamic, 2)
        for (size_t idx = 0; idx < valid_blocks.size(); ++idx) {
            size_t z = valid_blocks[idx];
            uint32_t start_row = o_orderdate_zonemap[z].start_row;
            uint32_t end_row = start_row + o_orderdate_zonemap[z].row_count;

            for (uint32_t i = start_row; i < end_row; ++i) {
                local_scanned++;
                if (o_orderdate[i] < date_1995_03_15 && cust_set.find(o_custkey[i]) != nullptr) {
                    thread_order_vecs[tid]->push_back({o_orderkey[i], o_orderdate[i], o_shippriority[i]});
                    local_matched++;
                }
            }
        }

        orders_scanned += local_scanned;
        orders_matched += local_matched;
    }

    // Flatten thread results into single vector
    std::vector<std::tuple<int32_t, int32_t, int32_t>> filtered_orders;
    filtered_orders.reserve(orders_matched.load());
    for (int t = 0; t < num_threads; ++t) {
        filtered_orders.insert(filtered_orders.end(),
                               thread_order_vecs[t]->begin(),
                               thread_order_vecs[t]->end());
        delete thread_order_vecs[t];
    }

    // Build orderkey -> metadata map
    CompactHashTable<int32_t, std::pair<int32_t, int32_t>> order_map(filtered_orders.size() * 4 / 3);
    for (const auto& tuple : filtered_orders) {
        order_map.insert(std::get<0>(tuple), {std::get<1>(tuple), std::get<2>(tuple)});
    }

    #ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double filter_orders_ms = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_join_orders: %.2f ms (scanned %zu, matched %zu orders)\n", filter_orders_ms, orders_scanned.load(), orders_matched.load());
    #endif

    // 6. Load pre-built lineitem l_orderkey hash index and scan with SIMD
    // OPTIMIZATION: Use hash_multi_value index to reverse join direction
    #ifdef GENDB_PROFILE
    auto t_load_index_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load lineitem hash index (hash_multi_value format)
    int fd_l_hash = open((gendb_dir + "/indexes/lineitem_l_orderkey_hash.bin").c_str(), O_RDONLY);
    if (fd_l_hash < 0) {
        std::cerr << "Failed to open lineitem hash index" << std::endl;
        return;
    }
    struct stat st_l_hash;
    fstat(fd_l_hash, &st_l_hash);
    void* l_hash_addr = mmap(nullptr, st_l_hash.st_size, PROT_READ, MAP_PRIVATE, fd_l_hash, 0);
    close(fd_l_hash);

    uint32_t l_num_unique = *(uint32_t*)l_hash_addr;
    uint32_t l_table_size = *((uint32_t*)l_hash_addr + 1);
    HashIndexEntry* l_hash_table = (HashIndexEntry*)((char*)l_hash_addr + 8);

    // Positions array starts after hash table
    uint32_t* l_positions_header = (uint32_t*)((char*)l_hash_addr + 8 + l_table_size * 12);
    uint32_t* l_positions = l_positions_header + 1;

    #ifdef GENDB_PROFILE
    auto t_load_index_end = std::chrono::high_resolution_clock::now();
    double load_index_ms = std::chrono::duration<double, std::milli>(t_load_index_end - t_load_index_start).count();
    printf("[TIMING] load_lineitem_index: %.2f ms (unique keys: %u, table size: %u)\n", load_index_ms, l_num_unique, l_table_size);
    #endif

    // 7. Join + aggregate: iterate filtered orders, lookup lineitem via hash index
    #ifdef GENDB_PROFILE
    auto t_scan_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local aggregation hash tables
    std::vector<CompactHashTable<GroupKey, AggResult, GroupKeyHash>*> thread_agg_maps(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        thread_agg_maps[t] = new CompactHashTable<GroupKey, AggResult, GroupKeyHash>(filtered_orders.size() * 2 / num_threads);
    }

    std::atomic<size_t> lineitem_joined(0);

    // SIMD threshold for date comparison
    __m256i date_threshold = _mm256_set1_epi32(date_1995_03_15);

    // Parallel processing: iterate filtered orders and lookup lineitem
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        size_t local_joined = 0;

        #pragma omp for schedule(dynamic, 64)
        for (size_t oi = 0; oi < filtered_orders.size(); ++oi) {
            int32_t orderkey = std::get<0>(filtered_orders[oi]);
            int32_t orderdate = std::get<1>(filtered_orders[oi]);
            int32_t shippriority = std::get<2>(filtered_orders[oi]);

            // Hash lookup in lineitem index
            DefaultHash hasher;
            size_t idx = hasher(orderkey) & (l_table_size - 1);

            // Linear probing
            while (l_hash_table[idx].count != 0 || l_hash_table[idx].key != 0) {
                if (l_hash_table[idx].key == orderkey && l_hash_table[idx].count > 0) {
                    uint32_t offset = l_hash_table[idx].offset;
                    uint32_t count = l_hash_table[idx].count;

                    // Process lineitem rows for this order with SIMD filtering
                    uint32_t i = 0;

                    // SIMD loop: process 8 rows at a time
                    for (; i + 8 <= count; i += 8) {
                        // Load 8 positions
                        uint32_t pos[8];
                        for (int j = 0; j < 8; ++j) {
                            pos[j] = l_positions[offset + i + j];
                        }

                        // Load 8 shipdate values
                        __m256i shipdates = _mm256_set_epi32(
                            l_shipdate[pos[7]], l_shipdate[pos[6]],
                            l_shipdate[pos[5]], l_shipdate[pos[4]],
                            l_shipdate[pos[3]], l_shipdate[pos[2]],
                            l_shipdate[pos[1]], l_shipdate[pos[0]]
                        );

                        // Compare: l_shipdate > date_1995_03_15
                        __m256i cmp = _mm256_cmpgt_epi32(shipdates, date_threshold);
                        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));

                        // Process matching rows
                        for (int j = 0; j < 8; ++j) {
                            if (mask & (1 << j)) {
                                uint32_t row_idx = pos[j];
                                GroupKey key{orderkey, orderdate, shippriority};
                                int64_t revenue = l_extendedprice[row_idx] * (100 - l_discount[row_idx]);

                                auto* agg = thread_agg_maps[tid]->find(key);
                                if (agg != nullptr) {
                                    agg->revenue_scaled += revenue;
                                } else {
                                    thread_agg_maps[tid]->insert(key, {revenue});
                                }
                                local_joined++;
                            }
                        }
                    }

                    // Scalar tail processing
                    for (; i < count; ++i) {
                        uint32_t row_idx = l_positions[offset + i];
                        if (l_shipdate[row_idx] > date_1995_03_15) {
                            GroupKey key{orderkey, orderdate, shippriority};
                            int64_t revenue = l_extendedprice[row_idx] * (100 - l_discount[row_idx]);

                            auto* agg = thread_agg_maps[tid]->find(key);
                            if (agg != nullptr) {
                                agg->revenue_scaled += revenue;
                            } else {
                                thread_agg_maps[tid]->insert(key, {revenue});
                            }
                            local_joined++;
                        }
                    }
                    break;
                }
                idx = (idx + 1) & (l_table_size - 1);
            }
        }

        lineitem_joined += local_joined;
    }

    // Merge thread-local aggregation results
    CompactHashTable<GroupKey, AggResult, GroupKeyHash> agg_map(lineitem_joined.load() / 2 + 1000);
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& entry : thread_agg_maps[t]->table) {
            if (entry.occupied) {
                auto* existing = agg_map.find(entry.key);
                if (existing != nullptr) {
                    existing->revenue_scaled += entry.value.revenue_scaled;
                } else {
                    agg_map.insert(entry.key, entry.value);
                }
            }
        }
        delete thread_agg_maps[t];
    }

    #ifdef GENDB_PROFILE
    auto t_scan_agg_end = std::chrono::high_resolution_clock::now();
    double scan_agg_ms = std::chrono::duration<double, std::milli>(t_scan_agg_end - t_scan_agg_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms (joined %zu rows)\n", scan_agg_ms, lineitem_joined.load());
    #endif

    // 7. Convert to vector and sort
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::tuple<int32_t, double, int32_t, int32_t>> results;
    results.reserve(lineitem_joined / 4);  // Estimate number of unique groups

    // Iterate through compact hash table
    for (const auto& entry : agg_map.table) {
        if (entry.occupied) {
            // Scale down from 10000 to get actual revenue with 2 decimal places
            double revenue = static_cast<double>(entry.value.revenue_scaled) / 10000.0;
            results.push_back({entry.key.l_orderkey, revenue, entry.key.o_orderdate, entry.key.o_shippriority});
        }
    }

    // Sort by revenue DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (std::get<1>(a) != std::get<1>(b)) {
                return std::get<1>(a) > std::get<1>(b);  // revenue DESC
            }
            return std::get<2>(a) < std::get<2>(b);  // o_orderdate ASC
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

    // 8. Write output (LIMIT 10)
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    size_t limit = std::min<size_t>(10, results.size());
    for (size_t i = 0; i < limit; ++i) {
        out << std::get<0>(results[i]) << ","
            << std::fixed << std::setprecision(4) << std::get<1>(results[i]) << ","
            << epoch_to_date(std::get<2>(results[i])) << ","
            << std::get<3>(results[i]) << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    std::cout << "Query Q3 completed. Results written to " << output_path << std::endl;
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
