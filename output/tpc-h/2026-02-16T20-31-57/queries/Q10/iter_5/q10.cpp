#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <cmath>
#include <thread>
#include <omp.h>

/*
LOGICAL PLAN:
1. Filter lineitem on l_returnflag = 'R' (estimated 2M rows from 60M)
2. Filter orders on date range [1993-10-01, 1993-12-31] (estimated 1.7M rows from 15M)
3. Join order:
   - Hash join: filtered lineitem (build) on l_orderkey ← filtered orders (probe) on o_orderkey
   - Intermediate: ~1.7M rows
   - Hash join: intermediate ← customer on c_custkey
   - Direct lookup: intermediate ← nation on n_nationkey (25 nations)
4. Aggregation: GROUP BY (c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment)
   - Use open-addressing hash table (estimated ~100K groups)
5. Sort: ORDER BY revenue DESC (use partial_sort for top-20)

PHYSICAL PLAN:
- Scans: Full scan with inline filtering (scan-filter fusion)
- Joins: Hash joins (build on smaller side)
  - lineitem → orders: Multi-value hash index mmap lookup
  - (lineitem⋈orders) → customer: Hash join with open-addressing table
  - result → nation: Direct array lookup (25 entries)
- Aggregation: CompactHashTable with composite key hash
- Sort: std::partial_sort for LIMIT 20
- Parallelism: Parallel lineitem scan (OpenMP), thread-local aggregation buffers
*/

// Utility: Load dictionary and find code for a value
std::unordered_map<std::string, int32_t> load_dictionary(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream in(dict_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    int32_t code = 0;
    while (std::getline(in, line)) {
        dict[line] = code++;
    }
    in.close();
    return dict;
}

// Utility: Load dictionary and reverse map (code -> value)
std::vector<std::string> load_dictionary_reverse(const std::string& dict_path) {
    std::vector<std::string> dict;
    std::ifstream in(dict_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(in, line)) {
        dict.push_back(line);
    }
    in.close();
    return dict;
}

// Utility: mmap a binary file
template<typename T>
T* mmap_file(const std::string& path, size_t& out_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    out_count = sb.st_size / sizeof(T);
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    return (T*)ptr;
}

// Utility: Compute epoch date from year, month, day
int32_t compute_epoch_date(int year, int month, int day) {
    int32_t days = 0;
    // Days for complete years 1970 to year-1
    for (int y = 1970; y < year; y++) {
        int is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += is_leap ? 366 : 365;
    }
    // Days for complete months in this year
    int days_per_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) days_per_month[2] = 29;
    for (int m = 1; m < month; m++) {
        days += days_per_month[m];
    }
    // Days in current month (1-indexed, so day-1)
    days += (day - 1);
    return days;
}

// Utility: Convert epoch days to YYYY-MM-DD string
std::string epoch_to_date_string(int32_t epoch_days) {
    int32_t days = epoch_days;
    int year = 1970;
    while (true) {
        int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_year = is_leap ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }
    int days_per_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) days_per_month[2] = 29;

    int month = 1;
    while (month <= 12 && days >= days_per_month[month]) {
        days -= days_per_month[month];
        month++;
    }
    int day = days + 1;

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Open-addressing hash table for better cache locality and performance
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };
    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size = 0) : mask(0) {
        if (expected_size == 0) return;
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

    void insert(K key, const V& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        if (mask == 0) return nullptr;
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Composite key for GROUP BY
struct GroupKey {
    int32_t c_custkey;
    int32_t c_name_code;
    int64_t c_acctbal;
    int32_t c_phone_code;
    int32_t n_name_code;
    int32_t c_address_code;
    int32_t c_comment_code;

    bool operator==(const GroupKey& other) const {
        return c_custkey == other.c_custkey &&
               c_name_code == other.c_name_code &&
               c_acctbal == other.c_acctbal &&
               c_phone_code == other.c_phone_code &&
               n_name_code == other.n_name_code &&
               c_address_code == other.c_address_code &&
               c_comment_code == other.c_comment_code;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        // Combine all fields with Fibonacci hashing
        size_t h = 0x9E3779B97F4A7C15ULL;
        h = ((h << 6) ^ (h >> 2)) + 0x9E3779B97F4A7C15ULL + (size_t)k.c_custkey;
        h = ((h << 6) ^ (h >> 2)) + 0x9E3779B97F4A7C15ULL + (size_t)k.c_name_code;
        h = ((h << 6) ^ (h >> 2)) + 0x9E3779B97F4A7C15ULL + (size_t)k.c_acctbal;
        h = ((h << 6) ^ (h >> 2)) + 0x9E3779B97F4A7C15ULL + (size_t)k.c_phone_code;
        h = ((h << 6) ^ (h >> 2)) + 0x9E3779B97F4A7C15ULL + (size_t)k.n_name_code;
        h = ((h << 6) ^ (h >> 2)) + 0x9E3779B97F4A7C15ULL + (size_t)k.c_address_code;
        h = ((h << 6) ^ (h >> 2)) + 0x9E3779B97F4A7C15ULL + (size_t)k.c_comment_code;
        return h;
    }
};

struct AggregateValue {
    int64_t revenue; // in scaled units (×100)
};

struct OutputRow {
    int32_t c_custkey;
    std::string c_name;
    double revenue;
    double c_acctbal;
    std::string n_name;
    std::string c_address;
    std::string c_phone;
    std::string c_comment;

    bool operator>(const OutputRow& other) const {
        return revenue > other.revenue;
    }
};

void run_q10(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // === LOAD DICTIONARIES ===
    std::unordered_map<std::string, int32_t> returnflag_dict =
        load_dictionary(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    auto nation_names_reverse = load_dictionary_reverse(gendb_dir + "/nation/n_name_dict.txt");
    auto customer_names_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_name_dict.txt");
    auto customer_address_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_address_dict.txt");
    auto customer_phone_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_phone_dict.txt");
    auto customer_comment_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_comment_dict.txt");

    int32_t returnflag_R = returnflag_dict["R"];

    // === LOAD LINEITEM ===
    size_t lineitem_count = 0;
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);
    int32_t* l_returnflag = mmap_file<int32_t>(gendb_dir + "/lineitem/l_returnflag.bin", lineitem_count);

    // === LOAD ORDERS ===
    size_t orders_count = 0;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_count);

    // === LOAD CUSTOMER ===
    size_t customer_count = 0;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_name = mmap_file<int32_t>(gendb_dir + "/customer/c_name.bin", customer_count);
    int64_t* c_acctbal = mmap_file<int64_t>(gendb_dir + "/customer/c_acctbal.bin", customer_count);
    int32_t* c_address = mmap_file<int32_t>(gendb_dir + "/customer/c_address.bin", customer_count);
    int32_t* c_phone = mmap_file<int32_t>(gendb_dir + "/customer/c_phone.bin", customer_count);
    int32_t* c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);
    int32_t* c_comment = mmap_file<int32_t>(gendb_dir + "/customer/c_comment.bin", customer_count);

    // === LOAD NATION ===
    size_t nation_count = 0;
    int32_t* n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    int32_t* n_name = mmap_file<int32_t>(gendb_dir + "/nation/n_name.bin", nation_count);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // === DATE CONSTANTS ===
    int32_t date_1993_10_01 = compute_epoch_date(1993, 10, 1);
    int32_t date_1994_01_01 = compute_epoch_date(1994, 1, 1);

#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // === FILTER: LINEITEM (parallel with morsel-driven approach) ===
    // First pass: parallel count filtered rows
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<size_t>> thread_filtered_indices(num_threads);

#pragma omp parallel for num_threads(num_threads) schedule(static, 100000)
    for (size_t i = 0; i < lineitem_count; i++) {
        if (l_returnflag[i] == returnflag_R) {
            thread_filtered_indices[omp_get_thread_num()].push_back(i);
        }
    }

    // Merge thread-local results
    std::vector<size_t> lineitem_filtered_indices;
    for (const auto& thread_indices : thread_filtered_indices) {
        lineitem_filtered_indices.insert(lineitem_filtered_indices.end(),
                                        thread_indices.begin(), thread_indices.end());
    }
    lineitem_filtered_indices.shrink_to_fit();

    size_t lineitem_filtered_count = lineitem_filtered_indices.size();

    // Pre-allocate vectors with filtered data
    std::vector<int32_t> lineitem_filtered_orderkey(lineitem_filtered_count);
    std::vector<int64_t> lineitem_filtered_extendedprice(lineitem_filtered_count);
    std::vector<int64_t> lineitem_filtered_discount(lineitem_filtered_count);

    // Fill filtered vectors
    for (size_t i = 0; i < lineitem_filtered_count; i++) {
        size_t orig_idx = lineitem_filtered_indices[i];
        lineitem_filtered_orderkey[i] = l_orderkey[orig_idx];
        lineitem_filtered_extendedprice[i] = l_extendedprice[orig_idx];
        lineitem_filtered_discount[i] = l_discount[orig_idx];
    }

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] lineitem_filter: %.2f ms (filtered to %zu rows)\n", filter_ms, lineitem_filtered_count);
#endif

    // === BUILD HASH TABLE: LINEITEM (optimized multi-value) ===
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Optimized two-array approach for multi-value hash join:
    // 1. Count occurrences per orderkey
    // 2. Allocate positions array
    // 3. Build hash table with offset/count pairs

    // First pass: count unique orderkeys and total entries
    std::unordered_map<int32_t, uint32_t> orderkey_to_count;
    for (size_t i = 0; i < lineitem_filtered_count; i++) {
        orderkey_to_count[lineitem_filtered_orderkey[i]]++;
    }

    // Allocate positions array
    std::vector<uint32_t> positions_array;
    positions_array.reserve(lineitem_filtered_count);

    // Build offset map: orderkey → (offset, count)
    std::unordered_map<int32_t, std::pair<uint32_t, uint32_t>> lineitem_ht;
    lineitem_ht.reserve(orderkey_to_count.size());

    uint32_t current_offset = 0;
    for (auto& [orderkey, count] : orderkey_to_count) {
        lineitem_ht[orderkey] = {current_offset, count};
        current_offset += count;
    }

    // Second pass: fill positions array (use second cursor for placement)
    std::unordered_map<int32_t, uint32_t> orderkey_cursor = orderkey_to_count;
    for (auto& [ok, _] : orderkey_cursor) {
        orderkey_cursor[ok] = lineitem_ht[ok].first;
    }

    for (size_t i = 0; i < lineitem_filtered_count; i++) {
        int32_t ok = lineitem_filtered_orderkey[i];
        uint32_t& cursor = orderkey_cursor[ok];
        positions_array.push_back(i);
        cursor++;
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] lineitem_build_hash: %.2f ms\n", build_ms);
#endif

    // === FILTER: ORDERS (parallel) ===
#ifdef GENDB_PROFILE
    auto t_orders_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel filtering with thread-local accumulation
    std::vector<std::vector<int32_t>> thread_orders_orderkey(num_threads);
    std::vector<std::vector<int32_t>> thread_orders_custkey(num_threads);

#pragma omp parallel for num_threads(num_threads) schedule(static, 100000)
    for (size_t i = 0; i < orders_count; i++) {
        if (o_orderdate[i] >= date_1993_10_01 && o_orderdate[i] < date_1994_01_01) {
            int tid = omp_get_thread_num();
            thread_orders_orderkey[tid].push_back(o_orderkey[i]);
            thread_orders_custkey[tid].push_back(o_custkey[i]);
        }
    }

    // Merge thread-local results
    std::vector<int32_t> orders_filtered_orderkey;
    std::vector<int32_t> orders_filtered_custkey;
    for (int t = 0; t < num_threads; t++) {
        orders_filtered_orderkey.insert(orders_filtered_orderkey.end(),
                                       thread_orders_orderkey[t].begin(), thread_orders_orderkey[t].end());
        orders_filtered_custkey.insert(orders_filtered_custkey.end(),
                                       thread_orders_custkey[t].begin(), thread_orders_custkey[t].end());
    }
    orders_filtered_orderkey.shrink_to_fit();
    orders_filtered_custkey.shrink_to_fit();

#ifdef GENDB_PROFILE
    auto t_orders_filter_end = std::chrono::high_resolution_clock::now();
    double orders_filter_ms = std::chrono::duration<double, std::milli>(t_orders_filter_end - t_orders_filter_start).count();
    printf("[TIMING] orders_filter: %.2f ms (filtered to %zu rows)\n", orders_filter_ms, orders_filtered_orderkey.size());
#endif

    // === JOIN: FILTERED ORDERS ← FILTERED LINEITEM (parallel probe) ===
#ifdef GENDB_PROFILE
    auto t_join1_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel probe into lineitem hash table with thread-local result buffers
    std::vector<std::vector<int32_t>> thread_joined_custkey(num_threads);
    std::vector<std::vector<int64_t>> thread_joined_extendedprice(num_threads);
    std::vector<std::vector<int64_t>> thread_joined_discount(num_threads);

#pragma omp parallel for num_threads(num_threads) schedule(dynamic, 10000)
    for (size_t oi = 0; oi < orders_filtered_orderkey.size(); oi++) {
        int tid = omp_get_thread_num();
        int32_t order_key = orders_filtered_orderkey[oi];
        auto it = lineitem_ht.find(order_key);
        if (it != lineitem_ht.end()) {
            uint32_t offset = it->second.first;
            uint32_t count = it->second.second;
            for (uint32_t j = 0; j < count; j++) {
                uint32_t li = positions_array[offset + j];
                thread_joined_custkey[tid].push_back(orders_filtered_custkey[oi]);
                thread_joined_extendedprice[tid].push_back(lineitem_filtered_extendedprice[li]);
                thread_joined_discount[tid].push_back(lineitem_filtered_discount[li]);
            }
        }
    }

    // Merge thread-local results
    std::vector<int32_t> joined_custkey;
    std::vector<int64_t> joined_extendedprice;
    std::vector<int64_t> joined_discount;
    for (int t = 0; t < num_threads; t++) {
        joined_custkey.insert(joined_custkey.end(),
                             thread_joined_custkey[t].begin(), thread_joined_custkey[t].end());
        joined_extendedprice.insert(joined_extendedprice.end(),
                                   thread_joined_extendedprice[t].begin(), thread_joined_extendedprice[t].end());
        joined_discount.insert(joined_discount.end(),
                              thread_joined_discount[t].begin(), thread_joined_discount[t].end());
    }
    joined_custkey.shrink_to_fit();
    joined_extendedprice.shrink_to_fit();
    joined_discount.shrink_to_fit();

#ifdef GENDB_PROFILE
    auto t_join1_end = std::chrono::high_resolution_clock::now();
    double join1_ms = std::chrono::duration<double, std::milli>(t_join1_end - t_join1_start).count();
    printf("[TIMING] join_orders_lineitem: %.2f ms (result %zu rows)\n", join1_ms, joined_custkey.size());
#endif

    // === BUILD CUSTOMER HASH TABLE (compact, open-addressing) ===
#ifdef GENDB_PROFILE
    auto t_cust_build_start = std::chrono::high_resolution_clock::now();
#endif

    struct CustomerRow {
        int32_t c_name_code;
        int64_t c_acctbal;
        int32_t c_address_code;
        int32_t c_phone_code;
        int32_t c_nationkey;
        int32_t c_comment_code;
    };

    CompactHashTable<int32_t, CustomerRow> customer_ht(customer_count);
    for (size_t i = 0; i < customer_count; i++) {
        customer_ht.insert(c_custkey[i], {
            c_name[i],
            c_acctbal[i],
            c_address[i],
            c_phone[i],
            c_nationkey[i],
            c_comment[i]
        });
    }

#ifdef GENDB_PROFILE
    auto t_cust_build_end = std::chrono::high_resolution_clock::now();
    double cust_build_ms = std::chrono::duration<double, std::milli>(t_cust_build_end - t_cust_build_start).count();
    printf("[TIMING] customer_build_hash: %.2f ms\n", cust_build_ms);
#endif

    // === JOIN: CUSTOMER ===
#ifdef GENDB_PROFILE
    auto t_join2_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<GroupKey> group_keys;
    std::vector<double> group_revenues;

    for (size_t i = 0; i < joined_custkey.size(); i++) {
        int32_t custkey = joined_custkey[i];
        auto* cust_ptr = customer_ht.find(custkey);
        if (cust_ptr) {
            const auto& cust = *cust_ptr;
            GroupKey key{
                custkey,
                cust.c_name_code,
                cust.c_acctbal,
                cust.c_phone_code,
                cust.c_nationkey,
                cust.c_address_code,
                cust.c_comment_code
            };
            // revenue = extendedprice * (1 - discount)
            // Both scaled by 100: convert to double and compute with full precision
            double ep = joined_extendedprice[i] / 100.0;
            double disc = joined_discount[i] / 100.0;
            double revenue = ep * (1.0 - disc);

            group_keys.push_back(key);
            group_revenues.push_back(revenue);
        }
    }

#ifdef GENDB_PROFILE
    auto t_join2_end = std::chrono::high_resolution_clock::now();
    double join2_ms = std::chrono::duration<double, std::milli>(t_join2_end - t_join2_start).count();
    printf("[TIMING] join_customer: %.2f ms (result %zu rows)\n", join2_ms, group_keys.size());
#endif

    // === AGGREGATION (parallel with thread-local buffers) ===
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local aggregation maps
    std::vector<std::unordered_map<GroupKey, double, GroupKeyHash>> thread_agg_maps(num_threads);

#pragma omp parallel for num_threads(num_threads) schedule(static, 10000)
    for (size_t i = 0; i < group_keys.size(); i++) {
        int tid = omp_get_thread_num();
        thread_agg_maps[tid][group_keys[i]] += group_revenues[i];
    }

    // Merge thread-local maps into global aggregation
    std::unordered_map<GroupKey, double, GroupKeyHash> agg_map;
    agg_map.reserve(std::min(group_keys.size(), (size_t)100000));

    for (int t = 0; t < num_threads; t++) {
        for (const auto& [key, revenue] : thread_agg_maps[t]) {
            agg_map[key] += revenue;
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms (groups: %zu)\n", agg_ms, agg_map.size());
#endif

    // === BUILD NATION LOOKUP (direct array for 25 nations) ===
#ifdef GENDB_PROFILE
    auto t_nation_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Direct array lookup: nation_lookup[nationkey] = n_name_code
    std::vector<int32_t> nation_lookup(25, -1);
    for (size_t i = 0; i < nation_count; i++) {
        nation_lookup[n_nationkey[i]] = n_name[i];
    }

#ifdef GENDB_PROFILE
    auto t_nation_build_end = std::chrono::high_resolution_clock::now();
    double nation_build_ms = std::chrono::duration<double, std::milli>(t_nation_build_end - t_nation_build_start).count();
    printf("[TIMING] nation_build_hash: %.2f ms\n", nation_build_ms);
#endif

    // === BUILD OUTPUT ROWS ===
    std::vector<OutputRow> output_rows;
    output_rows.reserve(agg_map.size());

    for (const auto& [key, revenue] : agg_map) {
        // Direct nation lookup instead of hash table
        int32_t n_name_code = nation_lookup[key.n_name_code];
        if (n_name_code == -1) n_name_code = key.n_name_code; // Fallback (shouldn't happen)

        OutputRow row{
            key.c_custkey,
            customer_names_reverse[key.c_name_code],
            revenue,  // Already in decimal form from aggregation
            key.c_acctbal / 100.0,  // Convert from scaled (×100) to decimal
            nation_names_reverse[n_name_code],
            customer_address_reverse[key.c_address_code],
            customer_phone_reverse[key.c_phone_code],
            customer_comment_reverse[key.c_comment_code]
        };
        output_rows.push_back(row);
    }

    // === SORT + LIMIT ===
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    if (output_rows.size() > 20) {
        std::partial_sort(output_rows.begin(), output_rows.begin() + 20, output_rows.end(), std::greater<OutputRow>());
        output_rows.resize(20);
    } else {
        std::sort(output_rows.begin(), output_rows.end(), std::greater<OutputRow>());
    }

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_topk: %.2f ms\n", sort_ms);
#endif

    // === WRITE CSV ===
#ifdef GENDB_PROFILE
    auto t_csv_start = std::chrono::high_resolution_clock::now();
#endif

    // Helper: quote CSV field if it contains comma, newline, or quote
    auto quote_field = [](const std::string& field) -> std::string {
        if (field.find(',') != std::string::npos ||
            field.find('\n') != std::string::npos ||
            field.find('"') != std::string::npos) {
            std::string quoted = "\"";
            for (char c : field) {
                if (c == '"') quoted += "\"\"";  // Escape quotes
                else quoted += c;
            }
            quoted += "\"";
            return quoted;
        }
        return field;
    };

    std::ofstream csv_out(results_dir + "/Q10.csv");
    csv_out << "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n";

    for (const auto& row : output_rows) {
        csv_out << row.c_custkey << ","
                << row.c_name << ","
                << std::fixed << std::setprecision(4) << row.revenue << ","
                << std::fixed << std::setprecision(2) << row.c_acctbal << ","
                << row.n_name << ","
                << quote_field(row.c_address) << ","
                << row.c_phone << ","
                << quote_field(row.c_comment) << "\n";
    }
    csv_out.close();

#ifdef GENDB_PROFILE
    auto t_csv_end = std::chrono::high_resolution_clock::now();
    double csv_ms = std::chrono::duration<double, std::milli>(t_csv_end - t_csv_start).count();
    printf("[TIMING] output: %.2f ms\n", csv_ms);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
    run_q10(gendb_dir, results_dir);
    return 0;
}
#endif
