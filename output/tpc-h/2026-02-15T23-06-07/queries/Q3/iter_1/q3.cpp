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

// Zone map entry structure (based on storage guide)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t start_row;
    uint32_t row_count;
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

    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", l_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", l_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", l_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", l_count);

    // Load zone maps (binary format: [uint32_t num_entries] then [ZoneMapEntry...]
    int fd_o_zone = open((gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin").c_str(), O_RDONLY);
    int fd_l_zone = open((gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin").c_str(), O_RDONLY);

    struct stat st_o_zone, st_l_zone;
    fstat(fd_o_zone, &st_o_zone);
    fstat(fd_l_zone, &st_l_zone);

    void* o_zone_addr = mmap(nullptr, st_o_zone.st_size, PROT_READ, MAP_PRIVATE, fd_o_zone, 0);
    void* l_zone_addr = mmap(nullptr, st_l_zone.st_size, PROT_READ, MAP_PRIVATE, fd_l_zone, 0);
    close(fd_o_zone);
    close(fd_l_zone);

    uint32_t o_zonemap_count = *(uint32_t*)o_zone_addr;
    uint32_t l_zonemap_count = *(uint32_t*)l_zone_addr;

    ZoneMapEntry* o_orderdate_zonemap = (ZoneMapEntry*)((char*)o_zone_addr + sizeof(uint32_t));
    ZoneMapEntry* l_shipdate_zonemap = (ZoneMapEntry*)((char*)l_zone_addr + sizeof(uint32_t));

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

    // 5. Filter orders and build hash map: o_custkey in filtered set AND o_orderdate < '1995-03-15'
    // Use zone map to skip blocks where o_orderdate >= date_1995_03_15
    #ifdef GENDB_PROFILE
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    // Map from o_orderkey -> (o_orderdate, o_shippriority)
    CompactHashTable<int32_t, std::pair<int32_t, int32_t>> order_map(o_count / 4);

    size_t orders_scanned = 0;
    size_t orders_matched = 0;

    // Zone map pruning: skip blocks where min_val >= date_1995_03_15
    for (size_t z = 0; z < o_zonemap_count; ++z) {
        if (o_orderdate_zonemap[z].min_val >= date_1995_03_15) {
            // Skip entire block - all rows fail the predicate
            continue;
        }

        uint32_t start_row = o_orderdate_zonemap[z].start_row;
        uint32_t end_row = start_row + o_orderdate_zonemap[z].row_count;

        for (uint32_t i = start_row; i < end_row; ++i) {
            orders_scanned++;
            if (o_orderdate[i] < date_1995_03_15 && cust_set.find(o_custkey[i]) != nullptr) {
                order_map.insert(o_orderkey[i], {o_orderdate[i], o_shippriority[i]});
                orders_matched++;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double filter_orders_ms = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_join_orders: %.2f ms (scanned %zu, matched %zu orders)\n", filter_orders_ms, orders_scanned, orders_matched);
    #endif

    // 6. Scan lineitem with filter l_shipdate > '1995-03-15' and join + aggregate
    // Use zone map to skip blocks where l_shipdate <= date_1995_03_15
    #ifdef GENDB_PROFILE
    auto t_scan_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    CompactHashTable<GroupKey, AggResult, GroupKeyHash> agg_map(orders_matched * 4);  // Estimate ~4 lineitems per order

    size_t lineitem_scanned = 0;
    size_t lineitem_joined = 0;

    // Zone map pruning: skip blocks where max_val <= date_1995_03_15
    for (size_t z = 0; z < l_zonemap_count; ++z) {
        if (l_shipdate_zonemap[z].max_val <= date_1995_03_15) {
            // Skip entire block - all rows fail the predicate
            continue;
        }

        uint32_t start_row = l_shipdate_zonemap[z].start_row;
        uint32_t end_row = start_row + l_shipdate_zonemap[z].row_count;

        for (uint32_t i = start_row; i < end_row; ++i) {
            lineitem_scanned++;
            if (l_shipdate[i] > date_1995_03_15) {
                auto* match = order_map.find(l_orderkey[i]);
                if (match != nullptr) {
                    lineitem_joined++;
                    GroupKey key{l_orderkey[i], match->first, match->second};

                    // Compute revenue: l_extendedprice * (1 - l_discount)
                    // Both are scaled by 100, so result is scaled by 100*100=10000
                    // Keep at full precision for aggregation
                    int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);

                    auto* agg = agg_map.find(key);
                    if (agg != nullptr) {
                        agg->revenue_scaled += revenue;
                    } else {
                        agg_map.insert(key, {revenue});
                    }
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_agg_end = std::chrono::high_resolution_clock::now();
    double scan_agg_ms = std::chrono::duration<double, std::milli>(t_scan_agg_end - t_scan_agg_start).count();
    printf("[TIMING] scan_filter_join_aggregate: %.2f ms (scanned %zu, joined %zu rows)\n", scan_agg_ms, lineitem_scanned, lineitem_joined);
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
