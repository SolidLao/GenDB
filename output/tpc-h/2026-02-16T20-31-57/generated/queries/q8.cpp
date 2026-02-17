#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <cstdint>
#include <cmath>
#include <algorithm>
#include <cassert>
#include <immintrin.h>

namespace {

/*
=== LOGICAL QUERY PLAN ===
Q8: National Market Share (TPCH Query 8)

Tables involved: part, supplier, lineitem, orders, customer, nation (2x), region

Filtering strategy:
1. region: Filter r_name = 'AMERICA' → 1 row (domain: 5 regions)
2. nation (n1): Join region on n_regionkey → 5 rows
3. customer: Join nation (n1) on c_nationkey → ~300K rows (1.5M / 25 * 5)
4. orders: Filter o_orderdate BETWEEN 1995-01-01 AND 1996-12-31, join customer → ~600K rows
5. part: Filter p_type = 'ECONOMY ANODIZED STEEL' → ~13K rows (2M / 150)
6. supplier: No direct filter (100K rows)
7. lineitem: Filter l_shipdate BETWEEN dates, join orders+customer+part+supplier → ~180K rows
8. nation (n2): Lookup via s_nationkey → 25 rows (dimension lookup)

Join ordering (smallest filtered first):
1. region → nation (n1) [5 rows build]
2. nation (n1) → customer [5 build, 1.5M probe]
3. customer → orders [300K build, 15M probe]
4. orders → part [600K build, 2M part probe]
5. part → lineitem [13K build, 60M lineitem probe]
6. lineitem → supplier [180K joined so far, 100K supplier]
7. supplier → nation (n2) [25 dimension lookup]

Subquery decorrelation:
- Not needed; all predicates decorrelated

=== PHYSICAL QUERY PLAN ===

Scan & Filter Strategy:
- region: Full scan (5 rows), filter r_name = 'AMERICA' (direct lookup in dict)
- nation (n1): Full scan (25 rows), join region (direct array lookup: n_regionkey[i])
- customer: Full scan (1.5M rows), hash join nation (build nation result)
- orders: Full scan (15M rows), filter o_orderdate in [1995-01-01, 1996-12-31], hash join customer
- part: Full scan (2M rows), filter p_type = 'ECONOMY ANODIZED STEEL' (dict lookup)
- supplier: Full scan (100K rows)
- lineitem: Full scan (60M rows), filter l_shipdate in date range
- nation (n2): Full scan (25 rows), direct array for lookup

Join Implementation:
- region ⊲ nation (n1): Direct array lookup (n_regionkey is 0-4)
- nation (n1) ⊲ customer: Hash join (build: nation result ~5 rows, probe: customer ~1.5M)
- customer ⊲ orders: Hash join (build: customer result ~300K, probe: orders ~15M)
- orders ⊲ part: Hash join (build: part ~13K, probe: orders+customer joined ~600K)
- part ⊲ lineitem: Hash join (build: part ~13K, probe: lineitem ~60M)
- lineitem ⊲ supplier: Hash join (build: supplier ~100K, probe: lineitem joined ~180K)
- supplier ⊲ nation (n2): Direct array lookup (s_nationkey is 0-24)

Aggregation:
- GROUP BY year (2 distinct: 1995, 1996)
- Use flat array indexed by year, not hash table
- Compute SUM(volume) and SUM(CASE WHEN nation='BRAZIL' THEN volume ELSE 0)

Parallelism:
- Parallel scan of lineitem (60M rows, 64 cores) with morsel-driven approach
- Thread-local aggregation buffers per core
- Final merge of per-thread results
- Hash joins built sequentially before probe phase

Data Structures:
- nation result: small vector/array (5 rows)
- customer-join result: hash table (300K entries)
- orders-join result: hash table (600K entries)
- part: hash table (13K entries)
- supplier: hash table (100K entries)
- nation (n2): direct array (25 entries)
- Aggregation: flat array (2 entries for years 1995, 1996)
*/

// ============ Date Utilities ============

// Precomputed year lookup table (days_since_1970 → year)
// Covers epoch days 0 to ~25000 (1970-2038)
static int16_t YEAR_TABLE[35000];
static bool YEAR_TABLE_INIT = false;

void init_year_table() {
    if (YEAR_TABLE_INIT) return;

    int year = 1970;
    int day_count = 0;

    for (int d = 0; d < 35000; d++) {
        YEAR_TABLE[d] = year;
        day_count++;

        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int days_in_year = leap ? 366 : 365;

        if (day_count >= days_in_year) {
            day_count = 0;
            year++;
        }
    }

    YEAR_TABLE_INIT = true;
}

// O(1) year extraction — single array access
inline int32_t epoch_to_year(int32_t epoch_day) {
    if (epoch_day < 0 || epoch_day >= 35000) {
        // Fallback for out-of-range (shouldn't happen in practice)
        int year = 1970;
        int32_t remaining = epoch_day;
        while (remaining >= 0) {
            bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            int days_in_year = leap ? 366 : 365;
            if (remaining < days_in_year) break;
            remaining -= days_in_year;
            year++;
        }
        return year;
    }
    return YEAR_TABLE[epoch_day];
}

int32_t date_to_epoch(int year, int month, int day) {
    // Days since 1970-01-01
    // Simplified: count days in complete years, then months, then days
    int32_t days = 0;

    // Days in complete years
    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }

    // Days in complete months
    const int dim[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for (int m = 1; m < month; m++) {
        days += dim[m - 1];
        if (m == 2 && leap) days += 1;
    }

    // Days (1-indexed)
    days += (day - 1);

    return days;
}

// ============ Simple Hash Table (Open Addressing) ============

template<typename K, typename V>
struct SimpleHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied;
    };

    Entry* table;
    int64_t capacity;
    int64_t size;
    int64_t mask;

    SimpleHashTable() : table(nullptr), capacity(0), size(0), mask(0) {}

    void init(int64_t cap) {
        // Round up to power of 2
        capacity = 1;
        while (capacity < cap) capacity *= 2;
        mask = capacity - 1;

        table = (Entry*)aligned_alloc(64, capacity * sizeof(Entry));
        for (int64_t i = 0; i < capacity; i++) {
            table[i].occupied = false;
        }
        size = 0;
    }

    ~SimpleHashTable() {
        free(table);
    }

    inline int64_t hash(K key) const {
        // Fibonacci hash - excellent distribution for integers
        // Constants: (2^64 / phi) gives good distribution properties
        return ((int64_t)key * 11400714819323198485ULL) & mask;
    }

    void insert(K key, V value) {
        int64_t idx = hash(key);
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
        size++;
    }

    inline V* find(K key) {
        int64_t idx = hash(key);
        // Prefetch the first probed cache line
        __builtin_prefetch(&table[idx], 0, 3);

        while (table[idx].occupied) {
            if (table[idx].key == key) {
                return &table[idx].value;
            }
            idx = (idx + 1) & mask;
            // Prefetch next location to hide latency
            if ((idx & 3) == 0) {
                __builtin_prefetch(&table[idx], 0, 1);
            }
        }
        return nullptr;
    }
};

// ============ Memory Mapping ============

template<typename T>
T* mmap_file(const std::string& path, int64_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    count = file_size / sizeof(T);

    T* data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        close(fd);
        return nullptr;
    }

    close(fd);
    return data;
}

// ============ Dictionary Loading ============

std::unordered_map<std::string, int32_t> load_dict(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[value] = code;
        }
    }
    f.close();
    return dict;
}

// ============ Main Query Function ============

} // end anonymous namespace

void run_q8(const std::string& gendb_dir, const std::string& results_dir) {
    // Initialize year lookup table (O(1) date extraction)
    init_year_table();

    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ===== Load and Filter region =====
    #ifdef GENDB_PROFILE
    auto t_region_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t region_count;
    int32_t* r_regionkey = mmap_file<int32_t>(gendb_dir + "/region/r_regionkey.bin", region_count);
    int32_t* r_name = mmap_file<int32_t>(gendb_dir + "/region/r_name.bin", region_count);

    auto r_name_dict = load_dict(gendb_dir + "/region/r_name_dict.txt");
    int32_t america_code = r_name_dict["AMERICA"];

    // Build region result: direct array (only AMERICA region)
    int32_t america_regionkey = -1;
    for (int64_t i = 0; i < region_count; i++) {
        if (r_name[i] == america_code) {
            america_regionkey = r_regionkey[i];
            break;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_region_end = std::chrono::high_resolution_clock::now();
    double ms_region = std::chrono::duration<double, std::milli>(t_region_end - t_region_start).count();
    printf("[TIMING] region_scan: %.2f ms\n", ms_region);
    #endif

    // ===== Load and Filter nation =====
    #ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t nation_count;
    int32_t* n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    int32_t* n_regionkey = mmap_file<int32_t>(gendb_dir + "/nation/n_regionkey.bin", nation_count);
    int32_t* n_name = mmap_file<int32_t>(gendb_dir + "/nation/n_name.bin", nation_count);

    auto n_name_dict = load_dict(gendb_dir + "/nation/n_name_dict.txt");
    int32_t brazil_code = n_name_dict["BRAZIL"];

    // Build nation hash table (for n1): nationkey -> occupied
    std::vector<bool> nation_n1_occupied(25, false);
    std::vector<int32_t> nation_n1_keys;
    for (int64_t i = 0; i < nation_count; i++) {
        if (n_regionkey[i] == america_regionkey) {
            nation_n1_occupied[n_nationkey[i]] = true;
            nation_n1_keys.push_back(n_nationkey[i]);
        }
    }

    // Build nation (n2) lookup: nationkey -> code for fast string lookup
    std::vector<int32_t> nation_n2_name(25);
    for (int64_t i = 0; i < nation_count; i++) {
        nation_n2_name[n_nationkey[i]] = n_name[i];
    }

    #ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    double ms_nation = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] nation_scan: %.2f ms\n", ms_nation);
    #endif

    // ===== Load and Filter customer =====
    #ifdef GENDB_PROFILE
    auto t_customer_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t customer_count;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_nationkey = mmap_file<int32_t>(gendb_dir + "/customer/c_nationkey.bin", customer_count);

    // Get thread count upfront
    int num_threads = omp_get_max_threads();

    // Hash table: custkey -> true
    // Size to 1.5x filtered count for better load factor (lower collision rate)
    SimpleHashTable<int32_t, bool> customer_ht;
    int64_t estimated_customers = 350000; // ~300K from region filter
    customer_ht.init(estimated_customers * 2);

    // Thread-local vectors for customer filtering (to avoid contention)
    std::vector<std::vector<int32_t>> thread_customers(num_threads);

    #pragma omp parallel for schedule(static, 10000) collapse(1)
    for (int64_t i = 0; i < customer_count; i++) {
        // Direct array lookup for nationkey (fast, predictable)
        int nkey = c_nationkey[i];
        if (nkey < 25 && nation_n1_occupied[nkey]) {
            int tid = omp_get_thread_num();
            thread_customers[tid].push_back(c_custkey[i]);
        }
    }

    // Merge thread-local customers into hash table
    for (int t = 0; t < num_threads; t++) {
        for (int32_t custkey : thread_customers[t]) {
            customer_ht.insert(custkey, true);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_customer_end = std::chrono::high_resolution_clock::now();
    double ms_customer = std::chrono::duration<double, std::milli>(t_customer_end - t_customer_start).count();
    printf("[TIMING] customer_scan: %.2f ms\n", ms_customer);
    #endif

    // ===== Load and Filter orders =====
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t orders_count;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_count);

    int32_t date_1995_01_01 = date_to_epoch(1995, 1, 1);
    int32_t date_1996_12_31 = date_to_epoch(1996, 12, 31);

    // Hash table: orderkey -> true (orders matching filters)
    // Estimated: ~900K orders pass date + customer filters
    SimpleHashTable<int32_t, bool> orders_ht;
    int64_t estimated_orders = 1000000;
    orders_ht.init(estimated_orders * 2);

    int64_t orders_filtered = 0;

    // Parallel scan and filter of orders
    // Thread-local vectors to accumulate matching orders (avoid contention on hash table insertion)
    std::vector<std::vector<int32_t>> thread_orders(num_threads);

    #pragma omp parallel for reduction(+:orders_filtered) schedule(static, 10000) collapse(1)
    for (int64_t i = 0; i < orders_count; i++) {
        // Predicate order: cheapest first (date check) before expensive hash lookup
        // This reduces hash table probes from 15M to ~600K
        // Use branchless condition to help compiler vectorization
        int32_t orderdate = o_orderdate[i];
        if (orderdate >= date_1995_01_01 && orderdate <= date_1996_12_31) {
            // Only expensive hash lookup for rows passing date filter
            if (customer_ht.find(o_custkey[i]) != nullptr) {
                int tid = omp_get_thread_num();
                thread_orders[tid].push_back(o_orderkey[i]);
                orders_filtered++;
            }
        }
    }

    // Merge thread-local orders into hash table (sequential)
    for (int t = 0; t < num_threads; t++) {
        for (int32_t orderkey : thread_orders[t]) {
            orders_ht.insert(orderkey, true);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_scan_filter: %.2f ms (filtered: %ld)\n", ms_orders, orders_filtered);
    #endif

    // ===== Load and Filter part =====
    #ifdef GENDB_PROFILE
    auto t_part_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t part_count;
    int32_t* p_partkey = mmap_file<int32_t>(gendb_dir + "/part/p_partkey.bin", part_count);
    int32_t* p_type = mmap_file<int32_t>(gendb_dir + "/part/p_type.bin", part_count);

    auto p_type_dict = load_dict(gendb_dir + "/part/p_type_dict.txt");
    int32_t economy_code = p_type_dict["ECONOMY ANODIZED STEEL"];

    // Hash table: partkey -> true
    SimpleHashTable<int32_t, bool> part_ht;
    part_ht.init(part_count * 2);

    int64_t part_filtered = 0;
    std::vector<std::vector<int32_t>> thread_parts(num_threads);

    #pragma omp parallel for reduction(+:part_filtered) schedule(static, 10000) collapse(1)
    for (int64_t i = 0; i < part_count; i++) {
        // Direct dictionary code comparison (fast)
        if (p_type[i] == economy_code) {
            int tid = omp_get_thread_num();
            thread_parts[tid].push_back(p_partkey[i]);
            part_filtered++;
        }
    }

    // Merge thread-local parts into hash table
    for (int t = 0; t < num_threads; t++) {
        for (int32_t partkey : thread_parts[t]) {
            part_ht.insert(partkey, true);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_part_end = std::chrono::high_resolution_clock::now();
    double ms_part = std::chrono::duration<double, std::milli>(t_part_end - t_part_start).count();
    printf("[TIMING] part_scan_filter: %.2f ms (filtered: %ld)\n", ms_part, part_filtered);
    #endif

    // ===== Load supplier =====
    #ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t supplier_count;
    int32_t* s_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Hash table: suppkey -> nationkey
    SimpleHashTable<int32_t, int32_t> supplier_ht;
    supplier_ht.init(supplier_count * 2);

    for (int64_t i = 0; i < supplier_count; i++) {
        supplier_ht.insert(s_suppkey[i], s_nationkey[i]);
    }

    #ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    double ms_supplier = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] supplier_scan: %.2f ms\n", ms_supplier);
    #endif

    // ===== Load lineitem =====
    #ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t lineitem_count;
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    int32_t* l_partkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", lineitem_count);
    int32_t* l_suppkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);

    #ifdef GENDB_PROFILE
    auto t_lineitem_load = std::chrono::high_resolution_clock::now();
    double ms_lineitem_load = std::chrono::duration<double, std::milli>(t_lineitem_load - t_lineitem_start).count();
    printf("[TIMING] lineitem_load: %.2f ms\n", ms_lineitem_load);
    #endif

    // ===== Main aggregation: scan lineitem with parallel processing =====
    #ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    // Aggregation structure: year -> (sum_volume, sum_brazil_volume)
    struct AggResult {
        int64_t sum_volume;
        int64_t sum_brazil_volume;
    };

    // Two years: 1995 (index 0) and 1996 (index 1)
    const int32_t year_1995 = 1995;
    const int32_t year_1996 = 1996;

    // Thread-local aggregation buffers
    std::vector<std::vector<AggResult>> thread_agg(num_threads, std::vector<AggResult>(2));

    // Initialize thread-local buffers
    #pragma omp parallel for
    for (int t = 0; t < num_threads; t++) {
        thread_agg[t][0].sum_volume = 0;
        thread_agg[t][0].sum_brazil_volume = 0;
        thread_agg[t][1].sum_volume = 0;
        thread_agg[t][1].sum_brazil_volume = 0;
    }

    // Parallel scan and aggregation
    int64_t lineitem_processed = 0;

    #pragma omp parallel for reduction(+:lineitem_processed) schedule(static, 10000)
    for (int64_t i = 0; i < lineitem_count; i++) {
        // Filter: l_shipdate in range (cheapest first)
        int32_t shipdate = l_shipdate[i];
        if (shipdate < date_1995_01_01 || shipdate > date_1996_12_31) {
            continue;
        }

        // Join: orders (fast hash lookup)
        if (orders_ht.find(l_orderkey[i]) == nullptr) {
            continue;
        }

        // Join: part (fast hash lookup)
        if (part_ht.find(l_partkey[i]) == nullptr) {
            continue;
        }

        // Join: supplier to get nation
        int32_t* supp_nation = supplier_ht.find(l_suppkey[i]);
        if (supp_nation == nullptr) {
            continue;
        }

        // Compute volume first (may be needed regardless of nation filter)
        int64_t price = l_extendedprice[i];
        int64_t discount = l_discount[i];
        int64_t volume = price * (10000 - discount) / 10000;

        // Check if this row is from BRAZIL
        int32_t nation_code = nation_n2_name[*supp_nation];
        bool is_brazil = (nation_code == brazil_code);

        // Extract year from shipdate (O(1) with lookup table)
        int year = YEAR_TABLE[shipdate];
        int year_idx = (year == year_1995) ? 0 : 1;

        // Aggregate to thread-local buffer
        int tid = omp_get_thread_num();
        thread_agg[tid][year_idx].sum_volume += volume;
        if (is_brazil) {
            thread_agg[tid][year_idx].sum_brazil_volume += volume;
        }

        lineitem_processed++;
    }

    #ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms (processed: %ld)\n", ms_agg, lineitem_processed);
    #endif

    // ===== Merge thread-local results =====
    AggResult final_agg[2];
    final_agg[0].sum_volume = 0;
    final_agg[0].sum_brazil_volume = 0;
    final_agg[1].sum_volume = 0;
    final_agg[1].sum_brazil_volume = 0;

    for (int t = 0; t < num_threads; t++) {
        final_agg[0].sum_volume += thread_agg[t][0].sum_volume;
        final_agg[0].sum_brazil_volume += thread_agg[t][0].sum_brazil_volume;
        final_agg[1].sum_volume += thread_agg[t][1].sum_volume;
        final_agg[1].sum_brazil_volume += thread_agg[t][1].sum_brazil_volume;
    }

    // ===== Output =====
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q8.csv";
    std::ofstream out(output_path);

    out << "o_year,mkt_share\n";

    for (int year_idx = 0; year_idx < 2; year_idx++) {
        int32_t year = (year_idx == 0) ? year_1995 : year_1996;
        int64_t sum_vol = final_agg[year_idx].sum_volume;
        int64_t sum_brazil = final_agg[year_idx].sum_brazil_volume;

        double mkt_share = 0.0;
        if (sum_vol > 0) {
            // sum_vol is in cents (scaled by 100)
            // mkt_share = sum_brazil / sum_vol (both already scaled)
            mkt_share = static_cast<double>(sum_brazil) / static_cast<double>(sum_vol);
        }

        out << year << "," << std::fixed << std::setprecision(2) << mkt_share << "\n";
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

    std::cout << "Q8 query completed. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q8(gendb_dir, results_dir);

    return 0;
}
#endif
