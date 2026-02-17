/*
 * ============================================================================
 * Q9: Product Type Profit Measure
 * ============================================================================
 *
 * ITERATION 5 OPTIMIZATION: Fast Year Extraction via Binary Search Lookup Table
 * -------------------------------------------------------------------------------
 *
 * KEY OPTIMIZATION:
 * The extract_year() function was called 3.26M times in the lineitem join loop,
 * with each call performing a loop over years 1970-1998 (~28 iterations avg).
 *
 * SOLUTION: Replace with O(1) binary search on a pre-computed lookup table of
 * year boundaries. For TPC-H dates (1970-1998), this is 5 binary search iterations
 * vs ~15 loop iterations, yielding ~6% improvement in lineitem_join time.
 *
 * Performance Impact (expected):
 *   lineitem_join: 331ms → 312ms (~6% improvement)
 *   total execution: ~2.8-3.0s (depends on environment)
 *
 * QUERY PLAN
 * ----------
 *
 * LOGICAL PLAN:
 *
 * Step 1: Filter single tables first
 *   - part: Filter by p_name LIKE '%green%' (all parts matching pattern)
 *   - Estimated selectivity: ~6% → ~120K rows
 *   - supplier: No single-table predicates, full scan (100K rows)
 *   - partsupp: No single-table predicates, full scan (8M rows)
 *   - lineitem: No single-table predicates, full scan (60M rows)
 *   - orders: No single-table predicates, full scan (15M rows)
 *   - nation: No single-table predicates, full scan (25 rows)
 *
 * Step 2: Join ordering (smallest filtered first)
 *   a) nation (25 rows) [full scan] → hash table H_nation[n_nationkey]
 *   b) part (120K rows) [filtered] → hash table H_part[p_partkey]
 *   c) supplier (100K rows) [full scan] → join with H_nation on s_nationkey
 *      → intermediate S (100K rows), hash table H_supp[s_suppkey]
 *   d) partsupp (8M rows) [full scan] → join with H_part and H_supp
 *      → intermediate PS (8M rows), hash table H_ps[ps_suppkey, ps_partkey]
 *   e) lineitem (60M rows) [full scan] → join with H_ps on (l_partkey, l_suppkey)
 *      and H_supp on l_suppkey → compute amount
 *   f) orders (15M rows) [full scan] → join with lineitem results on o_orderkey
 *      → extract year, GROUP BY nation + year
 *
 * Step 3: Subquery decorrelation
 *   - The subquery joins 6 tables and computes amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
 *   - No correlated subqueries; compute inline during joins
 *
 * PHYSICAL PLAN:
 *
 * Join Strategy:
 *   - H_nation: Load nation (25 rows), build hash table on n_nationkey
 *   - H_part: Load part filtered by p_name, build hash table on p_partkey
 *   - H_supp: Supplier joined with nation → hash table on s_suppkey
 *   - H_ps: PartSupp joined with part and supplier → hash table on composite key (ps_partkey, ps_suppkey)
 *   - Lineitem → probe H_ps on (l_partkey, l_suppkey), H_supp on l_suppkey → compute amount
 *   - Orders → probe lineitem results on o_orderkey
 *   - GROUP BY (nation, year) → hash aggregation (~175 groups)
 *
 * Aggregation:
 *   - destination: hash table<(nation_code, year), sum_profit>
 *   - expected groups: ~175 (25 nations × 7 years)
 *   - data structure: std::unordered_map with custom hash (for simplicity/correctness)
 *   - note: Could use flat array if year range is known (1992-1998 = 7 years)
 *
 * Scan Strategy:
 *   - Full scans for all tables (no selective filters except part)
 *   - No zone maps needed for large scans
 *
 * Parallelism:
 *   - Lineitem scan + join: OpenMP parallel for (60M rows, 64 cores)
 *   - Orders scan + join: Sequential (already filtered by lineitem join)
 *   - Aggregation: Thread-local hash maps, final merge (if parallelized)
 *   - For iteration 0: Focus on correctness; parallelization in later iterations
 *
 * ============================================================================
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include <cassert>
#include <cmath>
#include <algorithm>
#include <memory>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <omp.h>

// ============================================================================
// HELPER STRUCTURES
// ============================================================================

// Simple hash function for composite keys (nation_code, year)
struct GroupKeyHash {
    size_t operator()(const std::pair<int32_t, int32_t>& p) const {
        return ((size_t)p.first << 32) | (size_t)p.second;
    }
};

// ============================================================================
// COMPACT HASH TABLE (Open-Addressing for Better Performance)
// ============================================================================

// Compact open-addressing hash table for joins (replaces std::unordered_map)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashTable() : mask(0), count(0) {}

    explicit CompactHashTable(size_t expected_size) : count(0) {
        // Size to next power of 2, with ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    static size_t hash_key(K key) {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash_key(key) & mask;
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
        if (table.empty()) return nullptr;
        size_t idx = hash_key(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    size_t size() const { return count; }
};

// Memory-mapped file helper
struct MmapFile {
    int fd;
    void* data;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), data(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening file: " << path << std::endl;
            return;
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Error stat file: " << path << std::endl;
            close(fd);
            fd = -1;
            return;
        }
        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Error mmap file: " << path << std::endl;
            close(fd);
            fd = -1;
            data = nullptr;
        }
    }

    ~MmapFile() {
        if (data) munmap(data, size);
        if (fd >= 0) close(fd);
    }

    template<typename T>
    T* as() const { return (T*)data; }

    size_t count() const { return size / sizeof(int32_t); }
    size_t count64() const { return size / sizeof(int64_t); }
};

// Load dictionary file for string decoding
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_file) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_file);
    if (!f.is_open()) {
        std::cerr << "Error opening dictionary: " << dict_file << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// Find dictionary code by value (for filtering)
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& value) {
    for (const auto& [code, str] : dict) {
        if (str == value) return code;
    }
    return -1;
}

// Check if a string matches LIKE pattern (simplified: '%green%' means substring)
bool matches_pattern(const std::string& str, const std::string& pattern) {
    if (pattern == "%green%") {
        return str.find("green") != std::string::npos;
    }
    return true; // For other patterns, implement full LIKE matching
}

// Convert epoch days to YYYY-MM-DD string
std::string epoch_to_date(int32_t days) {
    // Epoch day 0 = 1970-01-01
    int32_t year = 1970;
    int32_t month = 1;
    int32_t day = 1;

    // Add days
    int32_t remaining = days;

    while (remaining > 0) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (remaining >= days_in_year) {
            remaining -= days_in_year;
            year++;
        } else {
            break;
        }
    }

    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) month_days[2] = 29;

    month = 1;
    while (remaining >= month_days[month]) {
        remaining -= month_days[month];
        month++;
    }
    day = remaining + 1;

    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Pre-computed cumulative days from epoch to start of each year (1970-2100)
static const int32_t year_starts[] = {
    0,     // 1970-01-01 = day 0
    365,   // 1971-01-01
    730,   // 1972-01-01
    1096,  // 1973-01-01
    1461,  // 1974-01-01
    1826,  // 1975-01-01
    2191,  // 1976-01-01
    2557,  // 1977-01-01
    2922,  // 1978-01-01
    3287,  // 1979-01-01
    3652,  // 1980-01-01
    4018,  // 1981-01-01
    4383,  // 1982-01-01
    4748,  // 1983-01-01
    5113,  // 1984-01-01
    5479,  // 1985-01-01
    5844,  // 1986-01-01
    6209,  // 1987-01-01
    6574,  // 1988-01-01
    6940,  // 1989-01-01
    7305,  // 1990-01-01
    7670,  // 1991-01-01
    8035,  // 1992-01-01
    8401,  // 1993-01-01
    8766,  // 1994-01-01
    9131,  // 1995-01-01
    9496,  // 1996-01-01
    9862,  // 1997-01-01
    10227, // 1998-01-01
    10592, // 1999-01-01
    10957, // 2000-01-01
};

// Extract year from epoch days (O(1) lookup table for TPC-H range)
static inline int32_t extract_year(int32_t days) {
    // Binary search in year_starts array (1970-2000 covered)
    int left = 0, right = 30;
    while (left < right) {
        int mid = (left + right + 1) / 2;
        if (year_starts[mid] <= days) {
            left = mid;
        } else {
            right = mid - 1;
        }
    }
    return 1970 + left;
}

// ============================================================================
// MAIN QUERY EXECUTION
// ============================================================================

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ========================================================================
    // 1. LOAD DATA
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load nation table
    MmapFile nation_nationkey(gendb_dir + "/nation/n_nationkey.bin");
    MmapFile nation_name(gendb_dir + "/nation/n_name.bin");
    int32_t* n_nationkey = nation_nationkey.as<int32_t>();
    int32_t* n_name = nation_name.as<int32_t>();
    size_t nation_rows = nation_nationkey.count();

    auto n_name_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");

    // Load part table
    MmapFile part_partkey(gendb_dir + "/part/p_partkey.bin");
    MmapFile part_name(gendb_dir + "/part/p_name.bin");
    int32_t* p_partkey = part_partkey.as<int32_t>();
    int32_t* p_name = part_name.as<int32_t>();
    size_t part_rows = part_partkey.count();

    auto p_name_dict = load_dictionary(gendb_dir + "/part/p_name_dict.txt");

    // Load supplier table
    MmapFile supplier_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
    MmapFile supplier_nationkey(gendb_dir + "/supplier/s_nationkey.bin");
    int32_t* s_suppkey = supplier_suppkey.as<int32_t>();
    int32_t* s_nationkey = supplier_nationkey.as<int32_t>();
    size_t supplier_rows = supplier_suppkey.count();

    // Load partsupp table
    MmapFile partsupp_partkey(gendb_dir + "/partsupp/ps_partkey.bin");
    MmapFile partsupp_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin");
    MmapFile partsupp_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin");
    int32_t* ps_partkey = partsupp_partkey.as<int32_t>();
    int32_t* ps_suppkey = partsupp_suppkey.as<int32_t>();
    int64_t* ps_supplycost = partsupp_supplycost.as<int64_t>();
    size_t partsupp_rows = partsupp_partkey.count();

    // Load lineitem table
    MmapFile lineitem_partkey(gendb_dir + "/lineitem/l_partkey.bin");
    MmapFile lineitem_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
    MmapFile lineitem_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapFile lineitem_quantity(gendb_dir + "/lineitem/l_quantity.bin");
    MmapFile lineitem_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile lineitem_discount(gendb_dir + "/lineitem/l_discount.bin");
    int32_t* l_partkey = lineitem_partkey.as<int32_t>();
    int32_t* l_suppkey = lineitem_suppkey.as<int32_t>();
    int32_t* l_orderkey = lineitem_orderkey.as<int32_t>();
    int64_t* l_quantity = lineitem_quantity.as<int64_t>();
    int64_t* l_extendedprice = lineitem_extendedprice.as<int64_t>();
    int64_t* l_discount = lineitem_discount.as<int64_t>();
    size_t lineitem_rows = lineitem_partkey.count();

    // Load orders table
    MmapFile orders_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MmapFile orders_orderdate(gendb_dir + "/orders/o_orderdate.bin");
    int32_t* o_orderkey = orders_orderkey.as<int32_t>();
    int32_t* o_orderdate = orders_orderdate.as<int32_t>();
    size_t orders_rows = orders_orderkey.count();

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
    #endif

    // ========================================================================
    // 2. BUILD HASH TABLES FOR DIMENSION TABLES
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_hash_build_start = std::chrono::high_resolution_clock::now();
    #endif

    // Step 2a: Nation hash table [n_nationkey -> nation_name]
    std::unordered_map<int32_t, std::string> h_nation;
    h_nation.reserve(nation_rows);
    for (size_t i = 0; i < nation_rows; i++) {
        int32_t key = n_nationkey[i];
        auto it = n_name_dict.find(n_name[i]);
        if (it != n_name_dict.end()) {
            h_nation[key] = it->second;
        }
    }

    // Step 2b: Filter part table by p_name LIKE '%green%' and build hash table [p_partkey -> 1]
    CompactHashTable<int32_t, bool> h_part(part_rows / 16);  // ~6% selectivity, size accordingly
    std::vector<int32_t> filtered_partkeys;
    for (size_t i = 0; i < part_rows; i++) {
        auto it = p_name_dict.find(p_name[i]);
        if (it != p_name_dict.end() && matches_pattern(it->second, "%green%")) {
            h_part.insert(p_partkey[i], true);
            filtered_partkeys.push_back(p_partkey[i]);
        }
    }

    // Step 2c: Supplier + Nation join: [s_suppkey -> s_nationkey]
    CompactHashTable<int32_t, int32_t> h_supp(supplier_rows); // suppkey -> nationkey
    for (size_t i = 0; i < supplier_rows; i++) {
        // All suppliers are included; nation lookup happens during lineitem processing
        h_supp.insert(s_suppkey[i], s_nationkey[i]);
    }

    #ifdef GENDB_PROFILE
    auto t_hash_build_end = std::chrono::high_resolution_clock::now();
    double ms_hash = std::chrono::duration<double, std::milli>(t_hash_build_end - t_hash_build_start).count();
    printf("[TIMING] hash_build: %.2f ms\n", ms_hash);
    printf("[TIMING] part_filtered_rows: %zu\n", filtered_partkeys.size());
    #endif

    // ========================================================================
    // NOTE: Accumulate profit at scale 10000 (price*qty) then divide at output
    // ========================================================================

    // ========================================================================
    // 3. PARTSUPP FILTERING AND HASHING
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_partsupp_start = std::chrono::high_resolution_clock::now();
    #endif

    // Build hash table: (ps_partkey, ps_suppkey) -> ps_supplycost
    // Use composite key
    struct PSKey { int32_t partkey; int32_t suppkey; };
    struct PSKeyHash {
        size_t operator()(const PSKey& k) const {
            return ((size_t)k.partkey << 32) | (size_t)k.suppkey;
        }
    };
    struct PSKeyEqual {
        bool operator()(const PSKey& a, const PSKey& b) const {
            return a.partkey == b.partkey && a.suppkey == b.suppkey;
        }
    };

    std::unordered_map<PSKey, int64_t, PSKeyHash, PSKeyEqual> h_ps;
    h_ps.reserve(partsupp_rows);
    for (size_t i = 0; i < partsupp_rows; i++) {
        // Only include (partkey, suppkey) pairs where partkey matches filtered parts
        if (h_part.find(ps_partkey[i]) != nullptr) {
            PSKey key = {ps_partkey[i], ps_suppkey[i]};
            // In case of duplicates, keep the first one (shouldn't happen in TPC-H)
            if (h_ps.find(key) == h_ps.end()) {
                h_ps[key] = ps_supplycost[i];
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_partsupp_end = std::chrono::high_resolution_clock::now();
    double ms_partsupp = std::chrono::duration<double, std::milli>(t_partsupp_end - t_partsupp_start).count();
    printf("[TIMING] partsupp_process: %.2f ms\n", ms_partsupp);
    printf("[TIMING] ps_hash_entries: %zu\n", h_ps.size());
    #endif

    // ========================================================================
    // 3.5 BUILD ORDERS HASH TABLE (COMPACT HASH TABLE FOR BETTER PERFORMANCE)
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_orders_hash_start = std::chrono::high_resolution_clock::now();
    #endif

    CompactHashTable<int32_t, int32_t> h_orders(orders_rows); // o_orderkey -> o_orderdate
    for (size_t i = 0; i < orders_rows; i++) {
        h_orders.insert(o_orderkey[i], o_orderdate[i]);
    }

    #ifdef GENDB_PROFILE
    auto t_orders_hash_end = std::chrono::high_resolution_clock::now();
    double ms_orders_hash = std::chrono::duration<double, std::milli>(t_orders_hash_end - t_orders_hash_start).count();
    printf("[TIMING] orders_hash: %.2f ms\n", ms_orders_hash);
    #endif

    // ========================================================================
    // 4. LINEITEM + JOIN + COMPUTE AMOUNT (PARALLEL)
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
    #endif

    // Join lineitem with partsupp, supplier, orders, nation
    // Compute amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
    // Group by (nation_name, year)
    // Use thread-local aggregation for parallelism

    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<std::pair<int32_t, int32_t>, int64_t, GroupKeyHash>> thread_agg(num_threads);

    // Pre-allocate thread-local aggregation maps
    for (int t = 0; t < num_threads; t++) {
        thread_agg[t].reserve(200);  // Expected ~175 groups, with overhead
    }

    size_t lineitem_matched = 0;

    #pragma omp parallel for schedule(static, 50000) reduction(+:lineitem_matched)
    for (size_t i = 0; i < lineitem_rows; i++) {
        int thread_id = omp_get_thread_num();
        int32_t l_pk = l_partkey[i];
        int32_t l_sk = l_suppkey[i];
        int32_t l_ok = l_orderkey[i];

        // Check if part is in filtered set
        if (h_part.find(l_pk) == nullptr) continue;

        // Lookup partsupp cost
        PSKey ps_key = {l_pk, l_sk};
        auto ps_it = h_ps.find(ps_key);
        if (ps_it == h_ps.end()) continue;
        int64_t ps_cost = ps_it->second;

        // Lookup supplier -> nation
        int32_t* nation_key_ptr = h_supp.find(l_sk);
        if (nation_key_ptr == nullptr) continue;
        int32_t nation_key = *nation_key_ptr;

        // Lookup nation name (we don't need the string here, just for validation)
        auto nation_it = h_nation.find(nation_key);
        if (nation_it == h_nation.end()) continue;

        // Lookup order date to extract year
        int32_t* order_date = h_orders.find(l_ok);
        if (order_date == nullptr) continue;
        int32_t o_year = extract_year(*order_date);

        // Compute amount: l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
        // Keep everything at high precision (scale 10000) during computation
        // All decimals are scaled by 100
        // l_extendedprice * (1 - l_discount):
        //   l_extendedprice is scaled (e.g., 10000 = 100.00)
        //   l_discount is scaled (e.g., 5 = 0.05)
        //   (1 - l_discount/100) = (100 - l_discount) / 100
        //   So: l_extendedprice * (100 - l_discount) / 100 = l_extendedprice * (100 - l_discount) / 100
        // ps_supplycost * l_quantity:
        //   ps_supplycost is scaled (e.g., 100 = 1.00)
        //   l_quantity is scaled (e.g., 100 = 1.00)
        //   So: ps_supplycost * l_quantity / 100
        // To avoid division loss, compute in scaled form:
        // revenue_scaled_10000 = l_extendedprice * (100 - l_discount)
        // cost_scaled_10000 = ps_supplycost * l_quantity
        // amount = (revenue - cost) / 100  (but we'll do this at output time)
        int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);  // scale 10000
        int64_t cost = ps_cost * l_quantity[i];  // scale 10000
        int64_t amount = revenue - cost;  // scale 10000, keep as-is

        // Add to thread-local aggregation (accumulated at scale 10000)
        int32_t nation_code = nation_key;
        int32_t group_year = o_year;
        std::pair<int32_t, int32_t> group_key = {nation_code, group_year};
        thread_agg[thread_id][group_key] += amount;

        lineitem_matched++;
    }

    // Merge thread-local results into global aggregation
    std::unordered_map<std::pair<int32_t, int32_t>, int64_t, GroupKeyHash> h_agg;
    h_agg.reserve(200);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [group_key, sum_profit] : thread_agg[t]) {
            h_agg[group_key] += sum_profit;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_lineitem = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] lineitem_join: %.2f ms\n", ms_lineitem);
    printf("[TIMING] lineitem_matched: %zu\n", lineitem_matched);
    printf("[TIMING] agg_groups: %zu\n", h_agg.size());
    #endif

    // ========================================================================
    // 5. OUTPUT RESULTS
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    // Collect results and sort by nation, then year DESC
    std::vector<std::tuple<std::string, int32_t, int64_t>> results;
    for (const auto& [group_key, sum_profit] : h_agg) {
        int32_t nation_code = group_key.first;
        int32_t year = group_key.second;
        auto nation_it = h_nation.find(nation_code);
        if (nation_it != h_nation.end()) {
            results.emplace_back(nation_it->second, year, sum_profit);
        }
    }

    // Sort by nation ASC, then year DESC
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (std::get<0>(a) != std::get<0>(b)) {
                return std::get<0>(a) < std::get<0>(b);
            }
            return std::get<1>(a) > std::get<1>(b);
        });

    // Write CSV
    std::string csv_file = results_dir + "/Q9.csv";
    std::ofstream out(csv_file);
    if (!out.is_open()) {
        std::cerr << "Error opening output file: " << csv_file << std::endl;
        return;
    }

    out << "nation,o_year,sum_profit\r\n";
    for (const auto& [nation, year, profit] : results) {
        // Profit is accumulated at scale 10000 (price*qty)
        // Divide by 100 to get scale 100 (final output scale)
        // Then divide by 100 again to get decimal value
        double profit_val = (double)profit / 10000.0;
        char buf[128];
        snprintf(buf, sizeof(buf), "%.4f", profit_val);
        out << nation << "," << year << "," << buf << "\r\n";
    }
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif

    printf("Query execution complete. Results written to %s\n", csv_file.c_str());
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
