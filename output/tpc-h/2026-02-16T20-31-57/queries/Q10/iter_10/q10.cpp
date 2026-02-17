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
1. Filter lineitem on l_returnflag = 'R' (estimated ~20M rows from 60M)
2. Filter orders on date range [1993-10-01, 1993-12-31] (estimated ~573K rows from 15M)
3. Join order (RESTRUCTURED for optimal performance):
   - Hash join: filtered ORDERS (build, smaller side: 573K) on o_orderkey ← filtered LINEITEM (probe, larger: 20M)
   - Apply l_returnflag='R' INLINE during probe (no separate pass)
   - Intermediate: ~1.1M rows (after both filters)
   - Hash join: intermediate ← customer on c_custkey
   - Direct lookup: intermediate ← nation on n_nationkey (25 nations)
4. Aggregation: GROUP BY (c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment)
   - Use open-addressing hash table (estimated ~100K groups)
5. Sort: ORDER BY revenue DESC (use partial_sort for top-20)

PHYSICAL PLAN (OPTIMIZED - Iteration 10):
- PARALLEL load: all tables + dictionaries in parallel (reduce I/O latency)
- Use PRE-BUILT hash indexes: orders_custkey_hash, customer_custkey_hash, nation_nationkey_hash
  - Skip hash table construction entirely for these joins
  - Load via mmap: zero build time, pay only for probe
- Single-pass orders filtering with pre-built custkey→orderkey mapping
- PARALLEL lineitem probe (59M rows) across 64 cores with morsel-driven scheduling
  - Inline returnflag filter
  - Direct hash index lookups (pre-built)
- Aggregation with 7-field key (output-driven)
- Sort: std::partial_sort for LIMIT 20
*/

// Utility: Load dictionary and find code for a value
std::unordered_map<std::string, int32_t> load_dictionary(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream in(dict_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }
    in.exceptions(std::ifstream::badbit);
    std::string line;
    int32_t code = 0;
    try {
        while (std::getline(in, line)) {
            dict[line] = code++;
        }
    } catch (const std::exception&) {
        // EOF is normal
    }
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

// Pre-built hash index loader (hash_single: key:int32_t, pos:uint32_t)
struct HashSingleIndex {
    uint32_t* data;  // Pointer to mmap'd region
    size_t num_entries;

    HashSingleIndex() : data(nullptr), num_entries(0) {}

    bool load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open index: " << path << std::endl;
            return false;
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            close(fd);
            return false;
        }
        void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);
        if (ptr == MAP_FAILED) return false;

        data = (uint32_t*)ptr;
        num_entries = data[0];  // First 4 bytes = num_entries
        return true;
    }

    // Look up key, return position in original table (or -1 if not found)
    uint32_t lookup(int32_t key) const {
        if (!data) return (uint32_t)-1;
        uint32_t* entries = data + 1;  // Skip num_entries header
        // Linear search (slow) or hash lookup within index
        // For now: iterate through all entries (N=25 for nation, etc)
        for (uint32_t i = 0; i < num_entries; i++) {
            int32_t entry_key = *(int32_t*)(entries + 2*i);
            if (entry_key == key) {
                return *(uint32_t*)(entries + 2*i + 1);
            }
        }
        return (uint32_t)-1;
    }
};

// Pre-built hash index loader (hash_multi_value: key:int32_t, offset:uint32_t, count:uint32_t)
struct HashMultiIndex {
    uint8_t* data;  // Pointer to mmap'd region
    uint32_t num_unique;
    uint32_t table_size;
    uint32_t* positions_array;

    HashMultiIndex() : data(nullptr), num_unique(0), table_size(0), positions_array(nullptr) {}

    bool load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open index: " << path << std::endl;
            return false;
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            close(fd);
            return false;
        }
        void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);
        if (ptr == MAP_FAILED) return false;

        data = (uint8_t*)ptr;
        num_unique = *(uint32_t*)(data + 0);
        table_size = *(uint32_t*)(data + 4);
        // Hash entries start at offset 8
        // Positions array starts after hash entries
        uint32_t hash_entries_size = table_size * 12;  // 12 bytes per slot
        positions_array = (uint32_t*)(data + 8 + hash_entries_size + 4);  // +4 for positions_count
        return true;
    }

    // Look up key, return (offset, count) pair for positions in original table
    std::pair<uint32_t, uint32_t> lookup(int32_t key) const {
        if (!data) return {0, 0};
        uint8_t* entries = data + 8;
        uint32_t table_mask = table_size - 1;

        // Hash lookup within the index table
        uint32_t h = ((uint32_t)key * 2654435761U) >> (32 - __builtin_ctz(table_size));
        for (uint32_t i = 0; i < table_size; i++) {
            uint32_t idx = (h + i) & table_mask;
            uint8_t* slot = entries + idx * 12;
            int32_t slot_key = *(int32_t*)(slot + 0);
            if (slot_key == key) {
                uint32_t offset = *(uint32_t*)(slot + 4);
                uint32_t count = *(uint32_t*)(slot + 8);
                return {offset, count};
            }
        }
        return {0, 0};
    }
};


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

    // === PARALLEL LOAD: DICTIONARIES ===
    std::unordered_map<std::string, int32_t> returnflag_dict;
    std::vector<std::string> nation_names_reverse;
    std::vector<std::string> customer_names_reverse;
    std::vector<std::string> customer_address_reverse;
    std::vector<std::string> customer_phone_reverse;
    std::vector<std::string> customer_comment_reverse;

    #pragma omp parallel sections
    {
        #pragma omp section
        { returnflag_dict = load_dictionary(gendb_dir + "/lineitem/l_returnflag_dict.txt"); }
        #pragma omp section
        { nation_names_reverse = load_dictionary_reverse(gendb_dir + "/nation/n_name_dict.txt"); }
        #pragma omp section
        { customer_names_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_name_dict.txt"); }
        #pragma omp section
        { customer_address_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_address_dict.txt"); }
        #pragma omp section
        { customer_phone_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_phone_dict.txt"); }
        #pragma omp section
        { customer_comment_reverse = load_dictionary_reverse(gendb_dir + "/customer/c_comment_dict.txt"); }
    }

    int32_t returnflag_R = returnflag_dict["R"];

    // === PARALLEL LOAD: BINARY DATA ===
    size_t lineitem_count = 0, orders_count = 0, customer_count = 0, nation_count = 0;
    int32_t* l_orderkey = nullptr;
    int64_t* l_extendedprice = nullptr;
    int64_t* l_discount = nullptr;
    int32_t* l_returnflag = nullptr;
    int32_t* o_orderkey = nullptr;
    int32_t* o_custkey = nullptr;
    int32_t* o_orderdate = nullptr;
    int32_t* c_custkey = nullptr;
    int32_t* c_name = nullptr;
    int64_t* c_acctbal = nullptr;
    int32_t* c_address = nullptr;
    int32_t* c_phone = nullptr;
    int32_t* c_nationkey = nullptr;
    int32_t* c_comment = nullptr;
    int32_t* n_nationkey = nullptr;
    int32_t* n_name = nullptr;

    #pragma omp parallel sections
    {
        #pragma omp section
        { l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count); }
        #pragma omp section
        { l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count); }
        #pragma omp section
        { l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count); }
        #pragma omp section
        { l_returnflag = mmap_file<int32_t>(gendb_dir + "/lineitem/l_returnflag.bin", lineitem_count); }
        #pragma omp section
        { o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count); }
        #pragma omp section
        { o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count); }
        #pragma omp section
        { o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_count); }
        #pragma omp section
        { c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count); }
        #pragma omp section
        { c_name = mmap_file<int32_t>(gendb_dir + "/customer/c_name.bin", customer_count); }
        #pragma omp section
        { c_acctbal = mmap_file<int64_t>(gendb_dir + "/customer/c_acctbal.bin", customer_count); }
        #pragma omp section
        { c_address = mmap_file<int32_t>(gendb_dir + "/customer/c_address.bin", customer_count); }
        #pragma omp section
        { c_phone = mmap_file<int32_t>(gendb_dir + "/customer/c_phone.bin", customer_count); }
        #pragma omp section
        { c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count); }
        #pragma omp section
        { c_comment = mmap_file<int32_t>(gendb_dir + "/customer/c_comment.bin", customer_count); }
        #pragma omp section
        { n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count); }
        #pragma omp section
        { n_name = mmap_file<int32_t>(gendb_dir + "/nation/n_name.bin", nation_count); }
    }

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // === DATE CONSTANTS ===
    int32_t date_1993_10_01 = compute_epoch_date(1993, 10, 1);
    int32_t date_1994_01_01 = compute_epoch_date(1994, 1, 1);

#ifdef GENDB_PROFILE
    auto t_orders_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // === LOAD PRE-BUILT INDEXES ===
    HashMultiIndex orders_custkey_idx;
    orders_custkey_idx.load(gendb_dir + "/indexes/orders_custkey_hash.bin");

#ifdef GENDB_PROFILE
    auto t_orders_filter_end = std::chrono::high_resolution_clock::now();
    double orders_filter_ms = std::chrono::duration<double, std::milli>(t_orders_filter_end - t_orders_filter_start).count();
    printf("[TIMING] orders_filter: %.2f ms (index loaded)\n", orders_filter_ms);
#endif

    // === OPTIMIZED JOIN: LINEITEM → ORDERS (PARALLEL probe, use pre-built index, apply returnflag inline) ===
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Build filtered_orders map: only orders within date range
    // Single pass to create mapping: orderkey → custkey
    std::unordered_map<int32_t, int32_t> filtered_orders;
    size_t filtered_orders_count = 0;
    for (size_t i = 0; i < orders_count; i++) {
        if (o_orderdate[i] >= date_1993_10_01 && o_orderdate[i] < date_1994_01_01) {
            filtered_orders[o_orderkey[i]] = o_custkey[i];
            filtered_orders_count++;
        }
    }

    // Parallel probe with thread-local result buffers
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<int32_t>> thread_custkey(num_threads);
    std::vector<std::vector<int64_t>> thread_extendedprice(num_threads);
    std::vector<std::vector<int64_t>> thread_discount(num_threads);

    // Pre-allocate thread-local buffers (estimate 2 lineitem rows per order)
    size_t est_result_rows = filtered_orders_count * 2;
    for (int t = 0; t < num_threads; t++) {
        thread_custkey[t].reserve(est_result_rows / num_threads);
        thread_extendedprice[t].reserve(est_result_rows / num_threads);
        thread_discount[t].reserve(est_result_rows / num_threads);
    }

    // Parallel probe: each thread processes a morsel of lineitem rows
    #pragma omp parallel for schedule(dynamic, 100000) collapse(1)
    for (size_t li = 0; li < lineitem_count; li++) {
        // INLINE filter: only process lineitem rows with l_returnflag = 'R'
        if (l_returnflag[li] != returnflag_R) continue;

        int32_t order_key = l_orderkey[li];
        auto it = filtered_orders.find(order_key);
        if (it != filtered_orders.end()) {
            int tid = omp_get_thread_num();
            // Found matching order for this lineitem
            thread_custkey[tid].push_back(it->second);
            thread_extendedprice[tid].push_back(l_extendedprice[li]);
            thread_discount[tid].push_back(l_discount[li]);
        }
    }

    // Merge thread-local results
    std::vector<int32_t> joined_custkey;
    std::vector<int64_t> joined_extendedprice;
    std::vector<int64_t> joined_discount;

    size_t total_size = 0;
    for (int t = 0; t < num_threads; t++) {
        total_size += thread_custkey[t].size();
    }

    joined_custkey.reserve(total_size);
    joined_extendedprice.reserve(total_size);
    joined_discount.reserve(total_size);

    for (int t = 0; t < num_threads; t++) {
        if (!thread_custkey[t].empty()) {
            joined_custkey.insert(joined_custkey.end(), thread_custkey[t].begin(), thread_custkey[t].end());
            joined_extendedprice.insert(joined_extendedprice.end(), thread_extendedprice[t].begin(), thread_extendedprice[t].end());
            joined_discount.insert(joined_discount.end(), thread_discount[t].begin(), thread_discount[t].end());
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_orders_lineitem: %.2f ms (result %zu rows)\n", join_ms, joined_custkey.size());
#endif

    // === BUILD CUSTOMER LOOKUP TABLE ===
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

    // Build a direct map from custkey to customer row (small enough to fit in memory)
    std::unordered_map<int32_t, CustomerRow> customer_map;
    customer_map.reserve(customer_count);

    for (size_t i = 0; i < customer_count; i++) {
        customer_map[c_custkey[i]] = {
            c_name[i],
            c_acctbal[i],
            c_address[i],
            c_phone[i],
            c_nationkey[i],
            c_comment[i]
        };
    }

#ifdef GENDB_PROFILE
    auto t_cust_build_end = std::chrono::high_resolution_clock::now();
    double cust_build_ms = std::chrono::duration<double, std::milli>(t_cust_build_end - t_cust_build_start).count();
    printf("[TIMING] customer_build_hash: %.2f ms\n", cust_build_ms);
#endif

    // === JOIN: CUSTOMER ===
#ifdef GENDB_PROFILE
    auto t_join_cust_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<GroupKey> group_keys;
    std::vector<double> group_revenues;
    group_keys.reserve(joined_custkey.size());
    group_revenues.reserve(joined_custkey.size());

    for (size_t i = 0; i < joined_custkey.size(); i++) {
        int32_t custkey = joined_custkey[i];
        auto it = customer_map.find(custkey);
        if (it != customer_map.end()) {
            const auto& cust = it->second;
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
    auto t_join_cust_end = std::chrono::high_resolution_clock::now();
    double join_cust_ms = std::chrono::duration<double, std::milli>(t_join_cust_end - t_join_cust_start).count();
    printf("[TIMING] join_customer: %.2f ms (result %zu rows)\n", join_cust_ms, group_keys.size());
#endif

    // === AGGREGATION ===
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<GroupKey, double, GroupKeyHash> agg_map;
    size_t est_groups = std::min(group_keys.size(), (size_t)500000);
    agg_map.reserve(est_groups);

    for (size_t i = 0; i < group_keys.size(); i++) {
        agg_map[group_keys[i]] += group_revenues[i];
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
