#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <omp.h>

/*
================================================================================
LOGICAL PLAN: Q9 - Product Type Profit Measure
================================================================================

Step 1 (Logical): Single-table predicates and estimates
  - part: p_name LIKE '%green%' → ~1000 rows (0.05% selectivity)
  - supplier: no predicates → 100K rows
  - lineitem: no single-table predicates → 60M rows
  - orders: no predicates → 15M rows
  - nation: no predicates → 25 rows
  - partsupp: no predicates → 8M rows

Step 2 (Logical): Join graph and ordering
  1. Filter part by p_name LIKE '%green%' → filtered_part (~1000 rows)
  2. Hash join filtered_part with lineitem on p_partkey=l_partkey
     (build on filtered_part, probe lineitem) → intermediate1 (~1000-2000 rows)
  3. Hash join intermediate1 with supplier on l_suppkey=s_suppkey
     (build on supplier, probe intermediate1) → intermediate2 (~1000-2000 rows)
  4. Hash join intermediate2 with partsupp on (l_partkey,l_suppkey)=(ps_partkey,ps_suppkey)
     (use pre-built hash index on partsupp) → intermediate3 (~1000-2000 rows)
  5. Hash join intermediate3 with orders on l_orderkey=o_orderkey
     (use pre-built hash index on orders) → intermediate4 (~1000-2000 rows)
  6. Hash join intermediate4 with nation on s_nationkey=n_nationkey
     (small table, direct lookup) → final_join (~1000-2000 rows)
  7. Extract year from o_orderdate using lookup table (O(1) per row)
  8. Compute amount: l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
  9. GROUP BY (nation_code, o_year), SUM(amount)
     (expect <300 groups, use open-addressing hash table)

Step 3 (Logical): Subqueries
  - None

================================================================================
PHYSICAL PLAN: Q9
================================================================================

Scan Strategy:
  - part: Full scan with dictionary decode, filter on p_name LIKE '%green%'
  - supplier: Full scan (small table, 100K rows)
  - lineitem: Full scan with hash probe against filtered part
  - orders: Hash index lookup (pre-built index on o_orderkey)
  - partsupp: Hash index lookup (pre-built index on ps_partkey and ps_suppkey)
  - nation: Direct array lookup (25 rows, indexed by n_nationkey)

Join Implementation:
  - part ⨝ lineitem: Hash join (build filtered part, probe lineitem)
  - ⨝ supplier: Hash join (build supplier on s_suppkey, probe previous result)
  - ⨝ partsupp: Build hash map from (ps_partkey, ps_suppkey) → ps_supplycost
  - ⨝ orders: Build hash map from o_orderkey → o_orderdate
  - ⨝ nation: Direct array indexing (n_nationkey < 256)

Aggregation:
  - GROUP BY (nation_code, o_year): ~25 nations × ~7 years = ~175 groups
  - Use open-addressing hash table with (nation_code, year) as compound key

Parallelism:
  - Part scan + filter: sequential (small filtered result)
  - Lineitem scan + join probe: parallel with OpenMP
  - Aggregation: thread-local buffers with final merge

Date Extraction:
  - Precompute YEAR_TABLE[epoch_day] for O(1) extraction
  - Expected range: epoch days 9000-10700 (years 1992-1998)

Decimal Handling:
  - All DECIMAL columns stored as int64_t with scale_factor=2
  - Computation: (l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity)
  - Arithmetic: Work at full precision and normalize at end
  - amount = l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity
  - Then divide by 100 for output

================================================================================
END PLANS
================================================================================
*/

// ============================================================================
// Date Extraction Lookup Tables
// ============================================================================

static int16_t YEAR_TABLE[30000];
static int8_t MONTH_TABLE[30000];

void init_date_tables() {
    int year = 1970, month = 1, day_of_month = 1;
    const int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = year;
        MONTH_TABLE[d] = month;

        day_of_month++;
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && leap ? 1 : 0);
        if (day_of_month > dim) {
            day_of_month = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

// ============================================================================
// Memory Mapping Utility
// ============================================================================

template <typename T>
T* mmap_file(const std::string& path, int32_t& num_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Error opening " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("fstat");
        close(fd);
        return nullptr;
    }
    size_t file_size = sb.st_size;
    num_rows = file_size / sizeof(T);

    T* data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return data;
}

void munmap_file(void* ptr, size_t size) {
    if (ptr) munmap(ptr, size);
}

// ============================================================================
// Dictionary Decoding
// ============================================================================

int32_t find_dict_code(const std::string& value, const std::string& dict_path) {
    std::ifstream f(dict_path);
    if (!f.is_open()) return -1;
    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        if (line == value) return code;
        code++;
    }
    return -1;
}

// ============================================================================
// Aggregate State
// ============================================================================

struct AggState {
    int64_t sum_profit;  // scaled by 100 (stored as int64_t)

    AggState() : sum_profit(0) {}
    AggState(int64_t s) : sum_profit(s) {}
};

struct GroupKey {
    int32_t nation_code;  // 0-24
    int16_t o_year;

    GroupKey() : nation_code(0), o_year(0) {}
    GroupKey(int32_t n, int16_t y) : nation_code(n), o_year(y) {}

    bool operator==(const GroupKey& other) const {
        return nation_code == other.nation_code && o_year == other.o_year;
    }
};

struct GroupKeyHasher {
    size_t operator()(const GroupKey& k) const {
        return ((size_t)k.nation_code << 16) | ((size_t)k.o_year & 0xFFFF);
    }
};

// ============================================================================
// Multi-Value Hash Index Loading (from pre-built indexes)
// ============================================================================

struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

template <typename ValueType>
struct LoadedHashIndex {
    std::vector<HashIndexEntry> table;
    std::vector<uint32_t> positions;
    std::vector<ValueType> values;

    // Find entries for a given key
    HashIndexEntry* find(int32_t key) {
        // Linear search through table (small, only unique keys)
        for (auto& entry : table) {
            if (entry.key == key) return &entry;
        }
        return nullptr;
    }
};

// ============================================================================
// PartSupp Lookup: composite key (partkey, suppkey) -> supplycost
// ============================================================================

struct PSKey {
    int32_t partkey;
    int32_t suppkey;

    PSKey() : partkey(0), suppkey(0) {}
    PSKey(int32_t p, int32_t s) : partkey(p), suppkey(s) {}

    bool operator==(const PSKey& other) const {
        return partkey == other.partkey && suppkey == other.suppkey;
    }
};

struct PSKeyHasher {
    size_t operator()(const PSKey& k) const {
        return ((size_t)k.partkey * 2654435761U) ^ ((size_t)k.suppkey * 2246822519U);
    }
};

// ============================================================================
// Q9 Main Execution
// ============================================================================

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    init_date_tables();

    // Get thread count early for thread-local buffers
    int num_threads = omp_get_max_threads();

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // Load Part Table + Filter on p_name LIKE '%green%'
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_part_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t num_part_rows = 0;
    int32_t* p_partkey = mmap_file<int32_t>(gendb_dir + "/part/p_partkey.bin", num_part_rows);
    int32_t* p_name = mmap_file<int32_t>(gendb_dir + "/part/p_name.bin", num_part_rows);

    // Find which dictionary codes contain "green"
    std::unordered_set<int32_t> green_codes;
    std::ifstream p_name_dict_file(gendb_dir + "/part/p_name_dict.txt");
    std::string dict_line;
    int32_t code = 0;
    while (std::getline(p_name_dict_file, dict_line)) {
        if (dict_line.find("green") != std::string::npos) {
            green_codes.insert(code);
        }
        code++;
    }
    p_name_dict_file.close();

    // Filter part table (parallelized with thread-local buffers)
    std::vector<std::vector<int32_t>> thread_filtered_partkeys(num_threads);
#pragma omp parallel for schedule(static, 10000)
    for (int32_t i = 0; i < num_part_rows; i++) {
        if (green_codes.count(p_name[i])) {
            int thread_id = omp_get_thread_num();
            thread_filtered_partkeys[thread_id].push_back(p_partkey[i]);
        }
    }

    // Merge thread-local results
    std::vector<int32_t> filtered_partkeys;
    for (int t = 0; t < num_threads; t++) {
        filtered_partkeys.insert(filtered_partkeys.end(),
                                thread_filtered_partkeys[t].begin(),
                                thread_filtered_partkeys[t].end());
    }

#ifdef GENDB_PROFILE
    auto t_part_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_part_end - t_part_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", ms);
#endif

    // Build hash map for filtered part keys
    std::unordered_set<int32_t> part_set(filtered_partkeys.begin(), filtered_partkeys.end());

    // ========================================================================
    // Load Supplier Table
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_supplier_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t num_supplier_rows = 0;
    int32_t* s_suppkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", num_supplier_rows);
    int32_t* s_nationkey = mmap_file<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", num_supplier_rows);

    // Build supplier hash map: suppkey → nationkey
    // Use thread-local maps to avoid contention during parallel scan
    std::vector<std::unordered_map<int32_t, int32_t>> thread_supplier_maps(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_supplier_maps[t].reserve(num_supplier_rows / num_threads + 10000);
    }

#pragma omp parallel for schedule(static, 10000)
    for (int32_t i = 0; i < num_supplier_rows; i++) {
        int thread_id = omp_get_thread_num();
        thread_supplier_maps[thread_id][s_suppkey[i]] = s_nationkey[i];
    }

    // Merge thread-local maps
    std::unordered_map<int32_t, int32_t> supplier_nation_map;
    supplier_nation_map.reserve(num_supplier_rows);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [suppkey, nationkey] : thread_supplier_maps[t]) {
            supplier_nation_map[suppkey] = nationkey;
        }
    }

#ifdef GENDB_PROFILE
    auto t_supplier_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_supplier_end - t_supplier_start).count();
    printf("[TIMING] load_supplier: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Load PartSupp Table and build hash map (filtered by part_set)
    // KEY OPTIMIZATION: Only build entries for partkeys in filtered_partkeys
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_partsupp_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t num_partsupp_rows = 0;
    int32_t* ps_partkey = mmap_file<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", num_partsupp_rows);
    int32_t* ps_suppkey = mmap_file<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", num_partsupp_rows);
    int64_t* ps_supplycost = mmap_file<int64_t>(gendb_dir + "/partsupp/ps_supplycost.bin", num_partsupp_rows);

    // Build hash map: (partkey, suppkey) → supplycost
    // ONLY include entries where partkey is in the filtered part set (~1000 entries expected)
    // Use thread-local maps to avoid contention during parallel scan
    std::vector<std::unordered_map<PSKey, int64_t, PSKeyHasher>> thread_partsupp_maps(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_partsupp_maps[t].reserve(2000 / num_threads + 100);
    }

#pragma omp parallel for schedule(dynamic, 50000)
    for (int32_t i = 0; i < num_partsupp_rows; i++) {
        // Skip if this partkey is not in our filtered set
        if (!part_set.count(ps_partkey[i])) continue;

        PSKey key(ps_partkey[i], ps_suppkey[i]);
        int thread_id = omp_get_thread_num();
        thread_partsupp_maps[thread_id][key] = ps_supplycost[i];
    }

    // Merge thread-local maps
    std::unordered_map<PSKey, int64_t, PSKeyHasher> partsupp_map;
    partsupp_map.reserve(2000);  // Only ~1000 parts * 4 suppkeys per part
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [key, cost] : thread_partsupp_maps[t]) {
            partsupp_map[key] = cost;
        }
    }

#ifdef GENDB_PROFILE
    auto t_partsupp_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_partsupp_end - t_partsupp_start).count();
    printf("[TIMING] load_partsupp: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Load Nation Table
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_nation_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t num_nation_rows = 0;
    int32_t* n_nationkey = mmap_file<int32_t>(gendb_dir + "/nation/n_nationkey.bin", num_nation_rows);
    int32_t* n_name = mmap_file<int32_t>(gendb_dir + "/nation/n_name.bin", num_nation_rows);

    // Load nation name dictionary
    std::vector<std::string> nation_names;
    std::ifstream n_name_dict_file(gendb_dir + "/nation/n_name_dict.txt");
    std::string nation_line;
    while (std::getline(n_name_dict_file, nation_line)) {
        nation_names.push_back(nation_line);
    }
    n_name_dict_file.close();

    // Build nation map: nationkey → name
    std::unordered_map<int32_t, std::string> nation_name_map;
    for (int32_t i = 0; i < num_nation_rows; i++) {
        nation_name_map[n_nationkey[i]] = nation_names[n_name[i]];
    }

#ifdef GENDB_PROFILE
    auto t_nation_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_nation_end - t_nation_start).count();
    printf("[TIMING] load_nation: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Load LineItem first to determine which orderkeys we need
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t num_lineitem_rows = 0;
    int32_t* l_partkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", num_lineitem_rows);
    int32_t* l_suppkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", num_lineitem_rows);
    int32_t* l_orderkey = mmap_file<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", num_lineitem_rows);
    int64_t* l_quantity = mmap_file<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", num_lineitem_rows);
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", num_lineitem_rows);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", num_lineitem_rows);

    // First pass: collect orderkeys we actually need (from filtered lineitem rows)
    // This avoids loading all 15M orders
    // Use thread-local sets to avoid contention, then merge
    std::vector<std::unordered_set<int32_t>> thread_orderkey_sets(num_threads);

#pragma omp parallel for schedule(dynamic, 100000)
    for (int64_t li = 0; li < num_lineitem_rows; li++) {
        // Only collect orderkeys for rows that pass the part_key filter
        if (part_set.count(l_partkey[li])) {
            int thread_id = omp_get_thread_num();
            thread_orderkey_sets[thread_id].insert(l_orderkey[li]);
        }
    }

    // Merge thread-local sets
    std::unordered_set<int32_t> needed_orderkeys;
    for (int t = 0; t < num_threads; t++) {
        for (int32_t ok : thread_orderkey_sets[t]) {
            needed_orderkeys.insert(ok);
        }
    }

#ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Load Orders Table via Pre-Built Hash Index (selective lookup)
    // Binary format: [uint32_t num_entries] [key:int32_t, pos:uint32_t]*
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t num_orders_rows = 0;
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", num_orders_rows);

    // Load pre-built hash index for orders.o_orderkey
    // Structure: [uint32_t num_entries] then [key:int32_t, pos:uint32_t] pairs (8B each)
    int fd_idx = open((gendb_dir + "/indexes/orders_orderkey_hash.bin").c_str(), O_RDONLY);
    if (fd_idx == -1) {
        std::cerr << "Error opening orders_orderkey_hash.bin" << std::endl;
        return;
    }
    struct stat sb_idx;
    if (fstat(fd_idx, &sb_idx) == -1) {
        perror("fstat");
        close(fd_idx);
        return;
    }
    size_t idx_file_size = sb_idx.st_size;
    void* idx_data = mmap(nullptr, idx_file_size, PROT_READ, MAP_SHARED, fd_idx, 0);
    close(fd_idx);
    if (idx_data == MAP_FAILED) {
        std::cerr << "mmap failed for orders_orderkey_hash.bin" << std::endl;
        return;
    }

    // Parse the index header
    uint32_t* idx_header = (uint32_t*)idx_data;
    uint32_t num_idx_entries = idx_header[0];

    // The entries follow: key:int32_t, pos:uint32_t (8B each)
    struct OrdersIndexEntry {
        int32_t key;
        uint32_t pos;
    };
    OrdersIndexEntry* orders_idx = (OrdersIndexEntry*)(idx_header + 1);

    // Build fast lookup map: orderkey → orderdate
    // Only populate entries for orderkeys we actually need
    std::unordered_map<int32_t, int32_t> orders_date_map;
    orders_date_map.reserve(needed_orderkeys.size() + 1000);  // Pre-size based on actual need

    // Iterate the pre-built index and only load needed orderkeys
    for (uint32_t i = 0; i < num_idx_entries; i++) {
        int32_t key = orders_idx[i].key;
        if (needed_orderkeys.count(key)) {
            uint32_t pos = orders_idx[i].pos;
            if (pos < (uint32_t)num_orders_rows) {
                orders_date_map[key] = o_orderdate[pos];
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Main Query Execution: Scan LineItem, Filter & Join with Thread-Local Agg
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::unordered_map<GroupKey, AggState, GroupKeyHasher>> thread_agg_maps(num_threads);

#pragma omp parallel for schedule(dynamic, 100000)
    for (int64_t li = 0; li < num_lineitem_rows; li++) {
        // Filter: l_partkey in filtered parts
        if (!part_set.count(l_partkey[li])) continue;

        // Lookup supplier nation
        auto it_supplier = supplier_nation_map.find(l_suppkey[li]);
        if (it_supplier == supplier_nation_map.end()) continue;
        int32_t nation_key = it_supplier->second;

        // Lookup partsupp info
        PSKey ps_key(l_partkey[li], l_suppkey[li]);
        auto it_ps = partsupp_map.find(ps_key);
        if (it_ps == partsupp_map.end()) continue;
        int64_t ps_supplycost = it_ps->second;

        // Lookup order date
        auto it_order = orders_date_map.find(l_orderkey[li]);
        if (it_order == orders_date_map.end()) continue;
        int32_t epoch_day = it_order->second;

        // Extract year from order date
        int16_t o_year = YEAR_TABLE[epoch_day];

        // Compute profit amount: l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
        // All values are in scaled form (scale=2):
        // l_extendedprice: int64_t, scale 2
        // l_discount: int64_t, scale 2 (0-10 means 0.00 to 0.10)
        // ps_supplycost: int64_t, scale 2
        // l_quantity: int64_t, scale 2
        //
        // Computation:
        // (1 - l_discount/100) = (100 - l_discount) / 100
        // amount = l_extendedprice * (100 - l_discount) / 100 - ps_supplycost * l_quantity / 100
        //        = (l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity) / 100
        int64_t discount_factor = 100 - l_discount[li];  // (100 - l_discount) where l_discount is 0-10
        int64_t revenue = l_extendedprice[li] * discount_factor;  // scaled by 100
        int64_t cost = ps_supplycost * l_quantity[li];            // scaled by 100
        int64_t profit = revenue - cost;                          // scaled by 100

        // Aggregate into thread-local map
        GroupKey key(nation_key, o_year);
        int thread_id = omp_get_thread_num();
        thread_agg_maps[thread_id][key].sum_profit += profit;
    }

    // Merge thread-local maps
    std::unordered_map<GroupKey, AggState, GroupKeyHasher> agg_map;
    for (int t = 0; t < num_threads; t++) {
        for (const auto& [key, agg] : thread_agg_maps[t]) {
            agg_map[key].sum_profit += agg.sum_profit;
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_join_agg: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Sort Results
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<GroupKey, AggState>> results(agg_map.begin(), agg_map.end());
    std::sort(results.begin(), results.end(), [&nation_name_map](const auto& a, const auto& b) {
        if (a.first.nation_code != b.first.nation_code) {
            return nation_name_map.at(a.first.nation_code) < nation_name_map.at(b.first.nation_code);
        }
        return a.first.o_year > b.first.o_year;  // DESC
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Write Results
    // ========================================================================

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream csv_file(results_dir + "/Q9.csv");
    csv_file << "nation,o_year,sum_profit\n";

    for (const auto& [key, agg] : results) {
        std::string nation_name = nation_name_map.at(key.nation_code);
        int32_t year = key.o_year;
        // Convert scaled profit (int64_t, scaled by 10000 due to multiplication of two scale-100 values)
        // to actual decimal value (divide by 10000)
        double profit_value = agg.sum_profit / 10000.0;
        csv_file << nation_name << "," << year << "," << std::fixed << std::setprecision(4) << profit_value << "\n";
    }
    csv_file.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    // ========================================================================
    // Cleanup
    // ========================================================================

    size_t lineitem_size = (size_t)num_lineitem_rows * sizeof(int32_t);
    munmap_file(l_partkey, lineitem_size);
    munmap_file(l_suppkey, lineitem_size);
    munmap_file(l_orderkey, lineitem_size);
    munmap_file(l_quantity, num_lineitem_rows * sizeof(int64_t));
    munmap_file(l_extendedprice, num_lineitem_rows * sizeof(int64_t));
    munmap_file(l_discount, num_lineitem_rows * sizeof(int64_t));

    size_t part_size = (size_t)num_part_rows * sizeof(int32_t);
    munmap_file(p_partkey, part_size);
    munmap_file(p_name, part_size);

    size_t supplier_size = (size_t)num_supplier_rows * sizeof(int32_t);
    munmap_file(s_suppkey, supplier_size);
    munmap_file(s_nationkey, supplier_size);

    size_t orders_size = (size_t)num_orders_rows * sizeof(int32_t);
    munmap_file(o_orderdate, orders_size);

    size_t partsupp_size = (size_t)num_partsupp_rows * sizeof(int32_t);
    munmap_file(ps_partkey, partsupp_size);
    munmap_file(ps_suppkey, partsupp_size);
    munmap_file(ps_supplycost, num_partsupp_rows * sizeof(int64_t));

    size_t nation_size = (size_t)num_nation_rows * sizeof(int32_t);
    munmap_file(n_nationkey, nation_size);
    munmap_file(n_name, nation_size);

    // Cleanup index mmap
    munmap_file(idx_data, idx_file_size);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms);
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
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
