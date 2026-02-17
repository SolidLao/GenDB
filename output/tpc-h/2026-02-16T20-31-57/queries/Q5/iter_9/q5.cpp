#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <omp.h>
#include <immintrin.h>

/*
====================================================================================
Q5 QUERY PLAN: Local Supplier Volume
====================================================================================

LOGICAL PLAN:
1. Filter region: r_name = 'ASIA' → 1 row (out of 5)
2. Filter nation: n_regionkey = target_region → ~5 nations
3. Filter orders: o_orderdate >= 1994-01-01 AND < 1995-01-01 → ~1.5M rows (out of 15M)
4. Scan lineitem: Full scan (59M rows) → will be filtered via joins
5. Join: orders JOIN lineitem on o_orderkey = l_orderkey
6. Join: result JOIN supplier on l_suppkey = s_suppkey
7. Join: result JOIN customer on o_custkey = c_custkey
8. Filter: customer.c_nationkey = supplier.s_nationkey = nation keys
9. Aggregate: GROUP BY n_name → SUM(l_extendedprice * (1 - l_discount))
10. Sort: ORDER BY revenue DESC

PHYSICAL PLAN:
- Region: Direct array lookup (5 items, O(1))
- Nation: Direct array lookup (25 items, O(1))
- Orders: Full scan + date filter → ~1.5M rows
- Lineitem: Full scan 59M rows, parallel with OpenMP
- Join strategy:
  * orders (1.5M filtered) INNER JOIN lineitem (59M) via hash table on o_orderkey
  * Build hash table on filtered orders (1.5M) → probe with lineitem (59M)
  * lineitem result JOIN supplier via hash table on l_suppkey (100K unique)
  * supplier result JOIN customer via hash table on o_custkey (1.5M unique)
- Aggregation: Flat array (25 nations) indexed by n_nationkey
- Output: Sort by revenue DESC, write n_name + revenue

CORRECTNESS REQUIREMENTS:
- DATE: int32_t days since epoch (compare as integers)
- DECIMAL: int64_t scaled by scale_factor=2 (e.g., 19.99 stored as 1999)
- Dictionary: Load n_name_dict.txt and r_name_dict.txt at runtime
- Precision: Revenue aggregation at full precision (int64_t scale²), scale down once
- Parallelism: OpenMP for lineitem scan and aggregation
====================================================================================
*/

// Memory-mapped file helper
template<typename T>
class MmapArray {
public:
    T* data;
    size_t size;
    int fd;

    MmapArray(const std::string& path) : data(nullptr), size(0), fd(-1) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening " << path << std::endl;
            return;
        }
        off_t file_size = lseek(fd, 0, SEEK_END);
        size = file_size / sizeof(T);
        data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Error mmapping " << path << std::endl;
            data = nullptr;
            size = 0;
        }
    }

    ~MmapArray() {
        if (data) munmap(data, size * sizeof(T));
        if (fd >= 0) close(fd);
    }

    T operator[](size_t idx) const { return data[idx]; }
    T* get() const { return data; }
};

// Load dictionary from text file: one value per line (newline-delimited)
// Code is the line number (0-indexed)
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Error opening dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        // Remove trailing whitespace/carriage returns
        while (!line.empty() && (line.back() == '\r' || line.back() == ' ')) {
            line.pop_back();
        }
        if (!line.empty()) {
            dict[code] = line;
        }
        code++;
    }
    f.close();
    return dict;
}

// Find dictionary code for a target value (reverse lookup)
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& target) {
    for (const auto& [code, value] : dict) {
        if (value == target) return code;
    }
    return -1;  // Not found
}


// DATE computation: days since epoch 1970-01-01
// 1994-01-01: year=1994, month=1, day=1
// Epoch formula: sum of days for complete years 1970..1993, plus days for complete months 1..0, plus day-1
int32_t compute_epoch_days(int year, int month, int day) {
    // Days in each month (non-leap year)
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Count leap years
    int leap_count = 0;
    for (int y = 1970; y < year; y++) {
        if ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)) leap_count++;
    }

    // Days for complete years
    int days = (year - 1970) * 365 + leap_count;

    // Days for complete months in current year
    int month_days = 0;
    for (int m = 1; m < month; m++) {
        month_days += days_in_month[m];
        if (m == 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
            month_days++;  // leap year February has 29 days
        }
    }
    days += month_days;

    // Add day - 1 (days are 1-indexed, epoch is 0-indexed)
    days += (day - 1);

    return days;
}


// Compact open-addressing hash table for fast joins
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };
    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~60% load factor (optimal balance)
        size_t sz = 1;
        while (sz < expected_size * 5 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Multiply-shift hash with better avalanche properties for int32_t
        uint64_t x = (uint64_t)(uint32_t)key;
        x = x * 0xBF58476D1CE4E5B9ULL;
        return (x >> 32) ^ x;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
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

struct ResultRow {
    int32_t nation_code;  // dictionary code for n_name
    double revenue;       // in actual decimal value
};

void run_q5(const std::string& gendb_dir, const std::string& results_dir) {
    // Load dictionaries
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif
    auto nation_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");
    auto region_dict = load_dictionary(gendb_dir + "/region/r_name_dict.txt");
#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", dict_ms);
#endif

    // Find dictionary codes
    int32_t asia_code = find_dict_code(region_dict, "ASIA");
    if (asia_code < 0) {
        std::cerr << "ASIA not found in region dictionary" << std::endl;
        return;
    }

    // Compute date thresholds as epoch days
    int32_t date_1994_01_01 = compute_epoch_days(1994, 1, 1);
    int32_t date_1995_01_01 = compute_epoch_days(1995, 1, 1);

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load all required columns via mmap
    MmapArray<int32_t> region_regionkey(gendb_dir + "/region/r_regionkey.bin");
    MmapArray<int32_t> region_name(gendb_dir + "/region/r_name.bin");

    MmapArray<int32_t> nation_nationkey(gendb_dir + "/nation/n_nationkey.bin");
    MmapArray<int32_t> nation_name(gendb_dir + "/nation/n_name.bin");
    MmapArray<int32_t> nation_regionkey(gendb_dir + "/nation/n_regionkey.bin");

    MmapArray<int32_t> orders_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MmapArray<int32_t> orders_custkey(gendb_dir + "/orders/o_custkey.bin");
    MmapArray<int32_t> orders_orderdate(gendb_dir + "/orders/o_orderdate.bin");

    MmapArray<int32_t> lineitem_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapArray<int32_t> lineitem_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
    MmapArray<int64_t> lineitem_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapArray<int64_t> lineitem_discount(gendb_dir + "/lineitem/l_discount.bin");

    MmapArray<int32_t> supplier_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
    MmapArray<int32_t> supplier_nationkey(gendb_dir + "/supplier/s_nationkey.bin");

    MmapArray<int32_t> customer_custkey(gendb_dir + "/customer/c_custkey.bin");
    MmapArray<int32_t> customer_nationkey(gendb_dir + "/customer/c_nationkey.bin");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", load_ms);
#endif

    // STEP 1: Filter region by r_name = 'ASIA'
#ifdef GENDB_PROFILE
    auto t_region_start = std::chrono::high_resolution_clock::now();
#endif
    int32_t target_regionkey = -1;
    for (size_t i = 0; i < region_regionkey.size; i++) {
        if (region_name[i] == asia_code) {
            target_regionkey = region_regionkey[i];
            break;
        }
    }
    if (target_regionkey < 0) {
        std::cerr << "No region found with name ASIA" << std::endl;
        return;
    }
#ifdef GENDB_PROFILE
    auto t_region_end = std::chrono::high_resolution_clock::now();
    double region_ms = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
    printf("[TIMING] filter_region: %.2f ms\n", region_ms);
#endif

    // STEP 2: Build nation lookup array: nation_lookup[nationkey] = {name_code, regionkey}
#ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif
    struct NationInfo { int32_t name_code; int32_t regionkey; bool in_asia; };
    NationInfo nation_lookup[25] = {};

    for (size_t i = 0; i < nation_nationkey.size; i++) {
        int32_t nk = nation_nationkey[i];
        int32_t rk = nation_regionkey[i];
        nation_lookup[nk] = {nation_name[i], rk, (rk == target_regionkey)};
    }
#ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    double nation_ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] build_nation_lookup: %.2f ms\n", nation_ms);
#endif

    // STEP 3: Filter orders by orderdate (parallel), build hash map orderkey → custkey
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel filtering with thread-local buffers to avoid critical section overhead
    std::vector<std::vector<uint32_t>> thread_indices(omp_get_max_threads());
    for (int t = 0; t < (int)thread_indices.size(); t++) {
        thread_indices[t].reserve(100000);
    }

#pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        std::vector<uint32_t>& local_indices = thread_indices[thread_id];

#pragma omp for schedule(static)
        for (size_t i = 0; i < orders_orderkey.size; i++) {
            int32_t odate = orders_orderdate[i];
            if (odate >= date_1994_01_01 && odate < date_1995_01_01) {
                local_indices.push_back(i);
            }
        }
    }

    // Merge thread-local results and collect unique custkeys
    size_t total_filtered = 0;
    std::unordered_set<int32_t> needed_custkeys;
    for (const auto& vec : thread_indices) total_filtered += vec.size();

    std::vector<uint32_t> filtered_indices;
    filtered_indices.reserve(total_filtered);
    for (const auto& vec : thread_indices) {
        for (uint32_t idx : vec) {
            filtered_indices.push_back(idx);
            needed_custkeys.insert(orders_custkey[idx]);
        }
    }

    // Build compact hash table from filtered orders
    CompactHashTable<int32_t, int32_t> orders_map(filtered_indices.size());
    for (uint32_t idx : filtered_indices) {
        orders_map.insert(orders_orderkey[idx], orders_custkey[idx]);
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms\n", orders_ms);
#endif

    // STEP 4: Build supplier hash map: suppkey → nationkey (parallel)
#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif
    CompactHashTable<int32_t, int32_t> supplier_map(supplier_suppkey.size);

    // Parallel build with per-thread tables (supplier is small, so single-threaded is acceptable)
    for (size_t i = 0; i < supplier_suppkey.size; i++) {
        supplier_map.insert(supplier_suppkey[i], supplier_nationkey[i]);
    }

#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double supplier_ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] build_supplier_map: %.2f ms\n", supplier_ms);
#endif

    // STEP 5: Build customer hash map: custkey → nationkey (selective - only needed keys)
#ifdef GENDB_PROFILE
    auto t_customer_start = std::chrono::high_resolution_clock::now();
#endif
    CompactHashTable<int32_t, int32_t> customer_map(needed_custkeys.size() + 100);

    // Scan customer table, but only insert rows for needed custkeys
    for (size_t i = 0; i < customer_custkey.size; i++) {
        int32_t ck = customer_custkey[i];
        if (needed_custkeys.count(ck)) {
            customer_map.insert(ck, customer_nationkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_customer_end = std::chrono::high_resolution_clock::now();
    double customer_ms = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    printf("[TIMING] build_customer_map: %.2f ms\n", customer_ms);
#endif

    // STEP 6: Scan lineitem and join with all other tables (with thread-local aggregation)
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local aggregation: each thread maintains its own revenue array
    const int max_threads = omp_get_max_threads();
    std::vector<std::vector<double>> thread_revenue(max_threads, std::vector<double>(25, 0.0));
    std::vector<std::vector<int64_t>> thread_counts(max_threads, std::vector<int64_t>(25, 0));

    // Precompute constants for revenue calculation to avoid repeated division
    const double price_discount_scale = 10000.0;  // 100.0^2

    // Parallel scan and join with NO synchronization in the hot loop
#pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < lineitem_orderkey.size; i++) {
        int thread_id = omp_get_thread_num();
        int32_t l_orderkey = lineitem_orderkey[i];
        int32_t l_suppkey = lineitem_suppkey[i];
        int64_t l_extendedprice = lineitem_extendedprice[i];
        int64_t l_discount = lineitem_discount[i];

        // Join with orders
        int32_t* o_custkey_ptr = orders_map.find(l_orderkey);
        if (!o_custkey_ptr) continue;
        int32_t o_custkey = *o_custkey_ptr;

        // Join with supplier
        int32_t* s_nationkey_ptr = supplier_map.find(l_suppkey);
        if (!s_nationkey_ptr) continue;
        int32_t s_nationkey = *s_nationkey_ptr;

        // Join with customer
        int32_t* c_nationkey_ptr = customer_map.find(o_custkey);
        if (!c_nationkey_ptr) continue;
        int32_t c_nationkey = *c_nationkey_ptr;

        // Filter: c_nationkey = s_nationkey AND both in ASIA region
        if (c_nationkey != s_nationkey) continue;

        if (!nation_lookup[c_nationkey].in_asia) continue;

        // Compute revenue: l_extendedprice * (1 - l_discount)
        // l_extendedprice and l_discount are int64_t with scale_factor=2 (meaning 10^2 = 100)
        // So: 19.99 stored as 1999, 0.10 stored as 10, 1.00 stored as 100
        // (1 - l_discount) in scaled form: (100 - l_discount) where 100 represents 1.00
        // revenue = l_extendedprice * (100 - l_discount) / 100
        // Convert to double for precision, but divide by 100 to unscale
        int64_t discount_complement = 100 - l_discount;
        double revenue_double = ((double)l_extendedprice * (double)discount_complement) / price_discount_scale;

        int32_t nation_code = nation_lookup[c_nationkey].name_code;

        // Update thread-local aggregation (NO LOCK)
        thread_revenue[thread_id][nation_code] += revenue_double;
        thread_counts[thread_id][nation_code]++;
    }

    // Merge thread-local results
    double revenue_by_nation[25] = {};
    int64_t count_by_nation[25] = {};
    for (int t = 0; t < max_threads; t++) {
        for (int nc = 0; nc < 25; nc++) {
            revenue_by_nation[nc] += thread_revenue[t][nc];
            count_by_nation[nc] += thread_counts[t][nc];
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_and_aggregate: %.2f ms\n", join_ms);
#endif

    // STEP 7: Build result and sort
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    for (int nc = 0; nc < 25; nc++) {
        if (count_by_nation[nc] > 0) {
            results.push_back({(int32_t)nc, revenue_by_nation[nc]});
        }
    }

    // Sort by revenue DESC
    std::sort(results.begin(), results.end(),
        [](const ResultRow& a, const ResultRow& b) { return a.revenue > b.revenue; });

    // Write results to CSV
    std::string output_path = results_dir + "/Q5.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "Error opening output file: " << output_path << std::endl;
        return;
    }

    // CSV header
    out << "n_name,revenue\n";

    // CSV rows
    for (const auto& row : results) {
        int32_t nc = row.nation_code;
        std::string nation_name = nation_dict.count(nc) ? nation_dict[nc] : "UNKNOWN";

        // Revenue is already in actual decimal value
        out << nation_name << "," << std::fixed << std::setprecision(4) << row.revenue << "\n";
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

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    run_q5(gendb_dir, results_dir);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    return 0;
}
#endif
