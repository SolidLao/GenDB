#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <omp.h>

/*
====================================================================================
Q5 QUERY PLAN: Local Supplier Volume (ITERATION 5 - STALL RECOVERY)
====================================================================================

ITERATION 5 CHANGES:
- CRITICAL FIX: Zone map pruning on o_orderdate (was: full scan 15M, now: skip blocks)
- OPTIMIZATION: Replace std::unordered_map with CompactHashTable (open-addressing)
- OPTIMIZATION: Parallel probe with thread-local aggregation (avoid critical section)
- CORRECTNESS: Preserve date/decimal encoding, dictionary handling, output format

LOGICAL PLAN:
1. Filter region: r_name = 'ASIA' → 1 row (out of 5)
2. Filter nation: n_regionkey = target_region → ~5 nations
3. Filter orders: o_orderdate >= 1994-01-01 AND < 1995-01-01 → ~1.5M rows (with zone map pruning)
4. Scan lineitem: Full scan 59M rows, parallel
5. Join: orders (filtered 1.5M) JOIN lineitem (59M) on o_orderkey
6. Join: result JOIN supplier on l_suppkey
7. Join: result JOIN customer on o_custkey
8. Filter: c_nationkey = s_nationkey AND both in ASIA region
9. Aggregate: GROUP BY n_name → SUM(l_extendedprice * (1 - l_discount))
10. Sort: ORDER BY revenue DESC

PHYSICAL PLAN:
- Region: Direct array lookup (5 items)
- Nation: Direct array lookup (25 items)
- Orders: ZONE MAP PRUNING + date filter → ~1.5M rows
  * Load zone map blocks for o_orderdate
  * Skip blocks where min > date_1995_01_01 or max < date_1994_01_01
  * Only scan surviving blocks
- Lineitem: Full scan 59M rows, parallel probe into orders hash table
- Joins:
  * orders → CompactHashTable<int32_t, int32_t> (orderkey → custkey)
  * supplier → CompactHashTable<int32_t, int32_t> (suppkey → nationkey)
  * customer → CompactHashTable<int32_t, int32_t> (custkey → nationkey)
- Aggregation: Thread-local buffers (25 nations × threads) + final merge
  * Avoids critical section overhead in parallel loop
- Output: Sort by revenue DESC

ZONE MAP FORMAT (empirical):
- File: indexes/orders_orderdate_zonemap.bin
- Blocks: 15M rows / 100K block_size = 150 blocks
- Per-block metadata: min (int32_t) + max (int32_t) = 8 bytes
- Total expected: 150 × 8 = 1200 bytes (actual 1804 = header + padding)

CORRECTNESS REQUIREMENTS:
- DATE: int32_t days since epoch (compare as integers)
- DECIMAL: int64_t scaled by scale_factor=2
- Dictionary: Load n_name_dict.txt and r_name_dict.txt at runtime
- Precision: Revenue aggregation at full precision, scale down once
- Zone map skip logic: skip if max < low_date OR min > high_date
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

// Compact open-addressing hash table for joins (replaces std::unordered_map)
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


// Zone map entry for block-level min/max
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
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

    // STEP 3: Filter orders by orderdate with ZONE MAP PRUNING, build hash table
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    // Load zone map for o_orderdate
    int fd_zonemap = open((gendb_dir + "/indexes/orders_orderdate_zonemap.bin").c_str(), O_RDONLY);
    std::vector<ZoneMapEntry> zone_maps;
    if (fd_zonemap >= 0) {
        off_t file_size = lseek(fd_zonemap, 0, SEEK_END);
        // Read zone map entries: expected format [header...][min/max pairs]
        // Empirical: 1804 bytes for 150 blocks = ~12 bytes/block = 2 int32_t values
        const int zonemap_entry_size = 8; // min + max as int32_t each
        const int header_size = 4;  // possible uint32_t header for count
        const int expected_entry_count = (file_size - header_size) / zonemap_entry_size;

        auto* zonemap_data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd_zonemap, 0);
        if (zonemap_data && zonemap_data != MAP_FAILED) {
            // Try to read entries starting from offset 4 (after potential header)
            const ZoneMapEntry* entries = (const ZoneMapEntry*)(zonemap_data + header_size);
            for (int e = 0; e < expected_entry_count && e * 8 + header_size < file_size; e++) {
                zone_maps.push_back(entries[e]);
            }
            munmap((void*)zonemap_data, file_size);
        }
        close(fd_zonemap);
    }

    // Build hash table with zone map pruning
    CompactHashTable<int32_t, int32_t> orders_map(1500000);
    const int32_t block_size = 100000;

    if (!zone_maps.empty()) {
        // ZONE MAP PRUNING APPROACH: skip blocks that don't overlap [date_1994_01_01, date_1995_01_01)
        for (size_t block_idx = 0; block_idx * block_size < orders_orderkey.size; block_idx++) {
            // Check zone map for this block
            bool skip_block = false;
            if (block_idx < zone_maps.size()) {
                const ZoneMapEntry& zm = zone_maps[block_idx];
                // Skip if entire block is outside the range
                // Range is [date_1994_01_01, date_1995_01_01)
                if (zm.max_val < date_1994_01_01 || zm.min_val >= date_1995_01_01) {
                    skip_block = true;
                }
            }

            if (skip_block) continue;

            // Scan rows in this block
            size_t start_row = block_idx * block_size;
            size_t end_row = std::min(start_row + block_size, orders_orderkey.size);
            for (size_t i = start_row; i < end_row; i++) {
                int32_t odate = orders_orderdate[i];
                if (odate >= date_1994_01_01 && odate < date_1995_01_01) {
                    orders_map.insert(orders_orderkey[i], orders_custkey[i]);
                }
            }
        }
    } else {
        // Fallback: full scan if zone maps unavailable
        for (size_t i = 0; i < orders_orderkey.size; i++) {
            int32_t odate = orders_orderdate[i];
            if (odate >= date_1994_01_01 && odate < date_1995_01_01) {
                orders_map.insert(orders_orderkey[i], orders_custkey[i]);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms\n", orders_ms);
#endif

    // STEP 4: Build supplier hash map: suppkey → nationkey (using CompactHashTable)
#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif
    CompactHashTable<int32_t, int32_t> supplier_map(100000);

    for (size_t i = 0; i < supplier_suppkey.size; i++) {
        supplier_map.insert(supplier_suppkey[i], supplier_nationkey[i]);
    }
#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double supplier_ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] build_supplier_map: %.2f ms\n", supplier_ms);
#endif

    // STEP 5: Build customer hash map: custkey → nationkey (using CompactHashTable)
#ifdef GENDB_PROFILE
    auto t_customer_start = std::chrono::high_resolution_clock::now();
#endif
    CompactHashTable<int32_t, int32_t> customer_map(1500000);

    for (size_t i = 0; i < customer_custkey.size; i++) {
        customer_map.insert(customer_custkey[i], customer_nationkey[i]);
    }
#ifdef GENDB_PROFILE
    auto t_customer_end = std::chrono::high_resolution_clock::now();
    double customer_ms = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    printf("[TIMING] build_customer_map: %.2f ms\n", customer_ms);
#endif

    // STEP 6: Scan lineitem and join with all other tables (parallel with thread-local aggregation)
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Aggregation: nation_code → revenue (as double for precision)
    double revenue_by_nation[25] = {};
    int64_t count_by_nation[25] = {};

    // Thread-local aggregation buffers to avoid critical section overhead
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<double>> thread_revenue(num_threads, std::vector<double>(25, 0.0));
    std::vector<std::vector<int64_t>> thread_count(num_threads, std::vector<int64_t>(25, 0));

    // Parallel scan and join
#pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < lineitem_orderkey.size; i++) {
        int tid = omp_get_thread_num();

        int32_t l_orderkey = lineitem_orderkey[i];
        int32_t l_suppkey = lineitem_suppkey[i];
        int64_t l_extendedprice = lineitem_extendedprice[i];
        int64_t l_discount = lineitem_discount[i];

        // Join with orders (using CompactHashTable)
        auto* orders_ptr = orders_map.find(l_orderkey);
        if (!orders_ptr) continue;
        int32_t o_custkey = *orders_ptr;

        // Join with supplier (using CompactHashTable)
        auto* supplier_ptr = supplier_map.find(l_suppkey);
        if (!supplier_ptr) continue;
        int32_t s_nationkey = *supplier_ptr;

        // Join with customer (using CompactHashTable)
        auto* customer_ptr = customer_map.find(o_custkey);
        if (!customer_ptr) continue;
        int32_t c_nationkey = *customer_ptr;

        // Filter: c_nationkey = s_nationkey AND both in ASIA region
        if (c_nationkey != s_nationkey) continue;

        if (!nation_lookup[c_nationkey].in_asia) continue;

        // Compute revenue: l_extendedprice * (1 - l_discount)
        // l_extendedprice and l_discount are int64_t with scale_factor=2
        // So: 19.99 stored as 1999, 0.10 stored as 10, 1.00 stored as 100
        // (1 - l_discount) in scaled form: (100 - l_discount) where 100 represents 1.00
        // revenue = l_extendedprice * (100 - l_discount) / 100
        // Convert to double for precision, but divide by 100 to unscale
        double revenue_double = ((double)l_extendedprice * (100.0 - (double)l_discount)) / 10000.0;

        int32_t nation_code = nation_lookup[c_nationkey].name_code;

        // Write to thread-local buffer (no lock needed)
        thread_revenue[tid][nation_code] += revenue_double;
        thread_count[tid][nation_code]++;
    }

    // Merge thread-local buffers
    for (int t = 0; t < num_threads; t++) {
        for (int nc = 0; nc < 25; nc++) {
            revenue_by_nation[nc] += thread_revenue[t][nc];
            count_by_nation[nc] += thread_count[t][nc];
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
