#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <omp.h>

/*
 * Q7: Volume Shipping
 *
 * LOGICAL PLAN:
 * 1. Filter lineitem by l_shipdate BETWEEN 1995-01-01 AND 1996-12-31 (~2.5M rows from 59M)
 * 2. Join filtered lineitem with supplier via l_suppkey (FK join)
 * 3. Join previous result with orders via l_orderkey (FK join)
 * 4. Join previous result with customer via o_custkey (FK join)
 * 5. Lookup nation names for supplier and customer via s_nationkey and c_nationkey
 * 6. Aggregate by (supp_nation, cust_nation, l_year) with SUM(volume)
 * 7. Sort by (supp_nation, cust_nation, l_year)
 *
 * PHYSICAL PLAN:
 * - Lineitem scan: full scan with date filter (integer comparison on epoch days)
 * - Joins: Use pre-built hash indexes from Storage & Index Guide for all joins
 *   - supplier_suppkey_hash: 100K entries (hash_single)
 *   - lineitem_suppkey_hash: 100K unique keys, multi-value index
 *   - orders_orderkey_hash: 15M entries (hash_single)
 *   - customer_custkey_hash: 1.5M entries (hash_single)
 *   - nation_nationkey_hash: 25 entries (hash_single)
 * - Nation dictionary lookup: Load n_name_dict.txt, find codes for 'FRANCE' and 'GERMANY'
 * - Aggregation: Open-addressing hash table (max ~2000 groups: 2 nations × ~1000 years)
 * - Sort: Map to sorted output
 */

// Date utility: compute epoch days for 1995-01-01 and 1996-12-31
// Epoch formula: sum days in complete years from 1970 to year-1, plus complete months, plus day-1
// For 1995-01-01: 25 years (1970-1994) + 0 months + 0 days
// Days in years 1970-1994 (including leap years):
// 1970,1972,1976,1980,1984,1988,1992 are leap years in that range
// Non-leap: 21 years × 365 = 7665, Leap: 6 years × 366 = 2196, Total: 9861
// For 1996-12-31: 26 years (1970-1995) + 11 months + 30 days
// 1996 is leap year (366 days) but we stop at 1995 end: 9861 + 365 = 10226
// Then add days for Jan-Nov 1996: 31+29+31+30+31+30+31+31+30+31+30 = 335, plus 31 (Dec) = 366
// Actually cleaner: 1996-12-31 epoch day = sum all days from 1970-01-01 to 1996-12-31
// 1970-1994: 9861, 1995: 365, 1996: 366, Total so far: 10592
// Wait, let me recalculate: days from 1970-01-01 to 1995-01-01 inclusive
// 1970-01-01 is day 0, 1970-01-02 is day 1, etc.
// To 1995-01-01 inclusive: 25*365 + 6 leap days = 9131 + 6 = 9137
// Hmm, different sources may vary. Let me use known values:
// 1995-01-01: epoch day 9131
// 1996-12-31: epoch day 9862
// Verify: 1996 is a leap year (divisible by 4, not by 100), so it has 366 days
// 1995-01-01 (9131) to 1996-12-31: 1 year + 365 days = 9131 + 731 = 9862 ✓
// Actually 1996-01-01 to 1996-12-31 is 366 days (leap year), so:
// 1996-01-01 = 9131 + 365 = 9496
// 1996-12-31 = 9496 + 365 = 9861 (but leap, so +366-1 = 9496 + 365 = 9861)
// Let's use safe margin and compute from first principles in code

static constexpr int32_t EPOCH_1995_01_01 = 9131;  // days since 1970-01-01
static constexpr int32_t EPOCH_1996_12_31 = 9861;  // days since 1970-01-01

// Precomputed year table for fast year extraction (O(1) lookup)
static int16_t YEAR_TABLE[30000];

void init_year_table() {
    int year = 1970, month = 1, day = 1;
    const int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int epoch_day = 0;

    while (epoch_day < 30000) {
        YEAR_TABLE[epoch_day] = year;

        epoch_day++;
        day++;
        bool is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && is_leap ? 1 : 0);

        if (day > dim) {
            day = 1;
            month++;
            if (month > 12) {
                month = 1;
                year++;
            }
        }
    }
}

inline int16_t extract_year(int32_t epoch_day) {
    if (epoch_day < 0 || epoch_day >= 30000) return 1970;
    return YEAR_TABLE[epoch_day];
}

// Mmap utility
void* mmap_file(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return nullptr;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    file_size = (size_t)size;

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// Load dictionary file and find code for a target string
int32_t find_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream f(dict_path);
    if (!f) {
        std::cerr << "Cannot open dict " << dict_path << std::endl;
        return -1;
    }

    int32_t code = 0;
    std::string line;
    while (std::getline(f, line)) {
        // Remove trailing whitespace/newlines
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r' || line.back() == ' ')) {
            line.pop_back();
        }
        if (line == target) {
            return code;
        }
        code++;
    }

    std::cerr << "Dictionary code not found for '" << target << "'" << std::endl;
    return -1;
}

// Pre-built hash index structures (from patterns/parallel-hash-join.md)
// Single-value hash index: [uint32_t num_entries] [key:int32_t, pos:uint32_t]*
// Multi-value hash index: [uint32_t num_unique] [uint32_t table_size] [key:int32_t, offset:uint32_t, count:uint32_t]* [positions_count][positions...]

struct HashIndexSingle {
    int32_t* keys;
    uint32_t* positions;
    uint32_t num_entries;

    HashIndexSingle() : keys(nullptr), positions(nullptr), num_entries(0) {}

    bool lookup(int32_t key, uint32_t& pos) const {
        // Linear scan for simplicity (100K entries is small)
        for (uint32_t i = 0; i < num_entries; i++) {
            if (keys[i] == key) {
                pos = positions[i];
                return true;
            }
        }
        return false;
    }
};

struct HashIndexMultiValue {
    int32_t* keys;
    uint32_t* offsets;
    uint32_t* counts;
    uint32_t* positions;  // flattened position array
    uint32_t num_unique;
    uint32_t table_size;

    HashIndexMultiValue() : keys(nullptr), offsets(nullptr), counts(nullptr),
                            positions(nullptr), num_unique(0), table_size(0) {}

    void lookup(int32_t key, uint32_t*& result_positions, uint32_t& result_count) const {
        // Linear scan to find key
        for (uint32_t i = 0; i < num_unique; i++) {
            if (keys[i] == key) {
                result_count = counts[i];
                result_positions = positions + offsets[i];
                return;
            }
        }
        result_count = 0;
        result_positions = nullptr;
    }
};

// Load single-value hash index from mmap
HashIndexSingle* load_hash_index_single(const std::string& path, void*& mmap_ptr, size_t& mmap_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open hash index " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    mmap_ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (mmap_ptr == MAP_FAILED) {
        std::cerr << "mmap failed for hash index " << path << std::endl;
        return nullptr;
    }

    mmap_size = file_size;

    uint32_t* data = (uint32_t*)mmap_ptr;
    uint32_t num_entries = data[0];

    HashIndexSingle* idx = new HashIndexSingle();
    idx->num_entries = num_entries;
    idx->keys = (int32_t*)(data + 1);
    idx->positions = (uint32_t*)(data + 1) + num_entries;

    return idx;
}

// Load multi-value hash index from mmap
HashIndexMultiValue* load_hash_index_multi(const std::string& path, void*& mmap_ptr, size_t& mmap_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open hash index " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    mmap_ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (mmap_ptr == MAP_FAILED) {
        std::cerr << "mmap failed for hash index " << path << std::endl;
        return nullptr;
    }

    mmap_size = file_size;

    uint32_t* data = (uint32_t*)mmap_ptr;
    uint32_t num_unique = data[0];
    uint32_t table_size = data[1];

    HashIndexMultiValue* idx = new HashIndexMultiValue();
    idx->num_unique = num_unique;
    idx->table_size = table_size;

    // Layout: [num_unique][table_size][key:int32_t, offset:uint32_t, count:uint32_t]* [positions_count][positions...]
    uint32_t* slot_ptr = data + 2;
    idx->keys = (int32_t*)slot_ptr;
    idx->offsets = (uint32_t*)(idx->keys + num_unique);
    idx->counts = idx->offsets + num_unique;

    uint32_t* positions_start = idx->counts + num_unique;
    idx->positions = positions_start + 1;  // skip positions_count

    return idx;
}

// Result aggregation structure (not used in optimized path)
struct AggResult {
    int32_t supp_nation_code;
    int32_t cust_nation_code;
    int16_t l_year;
    int64_t volume_sum;  // scaled by 2^2 = 4 due to multiplying two scaled columns
};

// Hash function for aggregation key
struct AggKeyHash {
    size_t operator()(const std::tuple<int32_t, int32_t, int16_t>& key) const {
        auto h1 = std::hash<int32_t>()(std::get<0>(key));
        auto h2 = std::hash<int32_t>()(std::get<1>(key));
        auto h3 = std::hash<int16_t>()(std::get<2>(key));
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

void run_q7(const std::string& gendb_dir, const std::string& results_dir) {
    init_year_table();

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Load lineitem columns
    size_t li_size;
    int32_t* li_suppkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_suppkey.bin", li_size);
    int32_t* li_orderkey = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_orderkey.bin", li_size);
    int32_t* li_shipdate = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", li_size);
    int64_t* li_extendedprice = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", li_size);
    int64_t* li_discount = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin", li_size);

    uint64_t li_count = li_size / sizeof(int32_t);  // All numeric columns same size

    // Load supplier columns
    size_t s_size;
    int32_t* s_suppkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_suppkey.bin", s_size);
    int32_t* s_nationkey = (int32_t*)mmap_file(gendb_dir + "/supplier/s_nationkey.bin", s_size);

    uint64_t s_count = s_size / sizeof(int32_t);

    // Load orders columns
    size_t o_size;
    int32_t* o_orderkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_orderkey.bin", o_size);
    int32_t* o_custkey = (int32_t*)mmap_file(gendb_dir + "/orders/o_custkey.bin", o_size);

    uint64_t o_count = o_size / sizeof(int32_t);

    // Load customer columns
    size_t c_size;
    int32_t* c_custkey = (int32_t*)mmap_file(gendb_dir + "/customer/c_custkey.bin", c_size);
    int32_t* c_nationkey = (int32_t*)mmap_file(gendb_dir + "/customer/c_nationkey.bin", c_size);

    uint64_t c_count = c_size / sizeof(int32_t);

    // Load nation columns
    size_t n_size;
    int32_t* n_name_codes = (int32_t*)mmap_file(gendb_dir + "/nation/n_name.bin", n_size);

    // Load nation dictionary
    int32_t france_code = find_dict_code(gendb_dir + "/nation/n_name_dict.txt", "FRANCE");
    int32_t germany_code = find_dict_code(gendb_dir + "/nation/n_name_dict.txt", "GERMANY");

    // Note: Pre-built hash indexes are available but not used in this iteration
    // (code uses std::unordered_map for simplicity in iter_0)
    // Future optimization: load and use pre-built indexes to skip hash table construction

    // Phase 1: Scan and filter lineitem by shipdate
#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<uint64_t> filtered_li_indices;
    for (uint64_t i = 0; i < li_count; i++) {
        if (li_shipdate[i] >= EPOCH_1995_01_01 && li_shipdate[i] <= EPOCH_1996_12_31) {
            filtered_li_indices.push_back(i);
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double ms_filter = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_filter);
    printf("[TIMING] filtered_rows: %zu\n", filtered_li_indices.size());
#endif

    // Phase 2: Load pre-built hash indexes and prepare lookups
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Load pre-built indexes
    void* idx_supplier_mmap = nullptr;
    size_t idx_supplier_mmap_size = 0;
    HashIndexSingle* supplier_idx = load_hash_index_single(
        gendb_dir + "/indexes/supplier_suppkey_hash.bin",
        idx_supplier_mmap, idx_supplier_mmap_size);

    void* idx_orders_mmap = nullptr;
    size_t idx_orders_mmap_size = 0;
    HashIndexSingle* orders_idx = load_hash_index_single(
        gendb_dir + "/indexes/orders_orderkey_hash.bin",
        idx_orders_mmap, idx_orders_mmap_size);

    void* idx_customer_mmap = nullptr;
    size_t idx_customer_mmap_size = 0;
    HashIndexSingle* customer_idx = load_hash_index_single(
        gendb_dir + "/indexes/customer_custkey_hash.bin",
        idx_customer_mmap, idx_customer_mmap_size);

    // Use direct array for nation lookup (only 25 entries)
    // nation: n_nationkey is 0-24, n_name_codes[i] is the i-th entry
    int32_t nation_codes[25];
    for (uint32_t i = 0; i < 25; i++) {
        nation_codes[i] = n_name_codes[i];
    }

    // Fallback hash tables if indexes failed to load
    std::unordered_map<int32_t, int32_t> supplier_ht_backup;
    std::unordered_map<int32_t, int32_t> orders_ht_backup;
    std::unordered_map<int32_t, int32_t> customer_ht_backup;

    if (!supplier_idx) {
        supplier_ht_backup.reserve(s_count);
        for (uint32_t i = 0; i < s_count; i++) {
            supplier_ht_backup[s_suppkey[i]] = s_nationkey[i];
        }
    }

    if (!orders_idx) {
        orders_ht_backup.reserve(o_count);
        for (uint32_t i = 0; i < o_count; i++) {
            orders_ht_backup[o_orderkey[i]] = o_custkey[i];
        }
    }

    if (!customer_idx) {
        customer_ht_backup.reserve(c_count);
        for (uint32_t i = 0; i < c_count; i++) {
            customer_ht_backup[c_custkey[i]] = c_nationkey[i];
        }
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hashtables: %.2f ms\n", ms_build);
#endif

    // Phase 3: Join and aggregate with filtered lineitem (parallelized)
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local aggregation tables for parallel processing
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<
        std::tuple<int32_t, int32_t, int16_t>,
        double,
        AggKeyHash
    >> thread_agg_tables(num_threads);

    for (int i = 0; i < num_threads; i++) {
        thread_agg_tables[i].reserve(100);
    }

#pragma omp parallel for schedule(static, 100000) if(filtered_li_indices.size() > 100000)
    for (size_t li_idx_pos = 0; li_idx_pos < filtered_li_indices.size(); li_idx_pos++) {
        int tid = omp_get_thread_num();
        uint64_t li_idx = filtered_li_indices[li_idx_pos];

        // Get lineitem data
        int32_t supp_key = li_suppkey[li_idx];
        int32_t order_key = li_orderkey[li_idx];
        int32_t shipdate = li_shipdate[li_idx];
        int64_t extendedprice = li_extendedprice[li_idx];  // scaled by 2
        int64_t discount = li_discount[li_idx];             // scaled by 2

        // Compute volume = extendedprice * (1 - discount)
        // scale_factor: 2 means the stored value is 100x the actual decimal value
        // So stored value 1234 = actual value 12.34
        // For price = 1234 (stored), actual = 12.34
        // For discount = 5 (stored), actual = 0.05
        // volume = price * (1 - discount) = 12.34 * (1 - 0.05) = 12.34 * 0.95
        // = (1234/100) * ((100 - 5) / 100) = 1234 * 95 / 10000
        // Accumulate in double for precision to match TPC-H results

        double volume = (double)extendedprice * (100.0 - (double)discount) / 10000.0;

        // Lookup supplier nationkey
        int32_t supp_nationkey = -1;
        if (supplier_idx) {
            uint32_t pos;
            if (supplier_idx->lookup(supp_key, pos)) {
                supp_nationkey = s_nationkey[pos];
            }
        } else {
            auto supp_it = supplier_ht_backup.find(supp_key);
            if (supp_it != supplier_ht_backup.end()) {
                supp_nationkey = supp_it->second;
            }
        }

        if (supp_nationkey == -1) continue;

        // Lookup customer key from order
        int32_t cust_key = -1;
        if (orders_idx) {
            uint32_t pos;
            if (orders_idx->lookup(order_key, pos)) {
                cust_key = o_custkey[pos];
            }
        } else {
            auto order_it = orders_ht_backup.find(order_key);
            if (order_it != orders_ht_backup.end()) {
                cust_key = order_it->second;
            }
        }

        if (cust_key == -1) continue;

        // Lookup customer nationkey
        int32_t cust_nationkey = -1;
        if (customer_idx) {
            uint32_t pos;
            if (customer_idx->lookup(cust_key, pos)) {
                cust_nationkey = c_nationkey[pos];
            }
        } else {
            auto cust_it = customer_ht_backup.find(cust_key);
            if (cust_it != customer_ht_backup.end()) {
                cust_nationkey = cust_it->second;
            }
        }

        if (cust_nationkey == -1) continue;

        // Get nation codes using direct array (nation_nationkey should be in range [0, 24])
        if (supp_nationkey < 0 || supp_nationkey >= 25) continue;
        if (cust_nationkey < 0 || cust_nationkey >= 25) continue;

        int32_t supp_nation_code = nation_codes[supp_nationkey];
        int32_t cust_nation_code = nation_codes[cust_nationkey];

        // Check nation constraint early (filter before aggregation)
        bool valid = (
            (supp_nation_code == france_code && cust_nation_code == germany_code) ||
            (supp_nation_code == germany_code && cust_nation_code == france_code)
        );
        if (!valid) continue;

        // Extract year from shipdate
        int16_t year = extract_year(shipdate);

        // Aggregate into thread-local table
        auto key = std::make_tuple(supp_nation_code, cust_nation_code, year);
        thread_agg_tables[tid][key] += volume;
    }

    // Merge thread-local aggregation tables
    std::unordered_map<
        std::tuple<int32_t, int32_t, int16_t>,
        double,
        AggKeyHash
    > agg_table;
    agg_table.reserve(100);

    for (int i = 0; i < num_threads; i++) {
        for (const auto& [key, value] : thread_agg_tables[i]) {
            agg_table[key] += value;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_join);
    printf("[TIMING] aggregate_groups: %zu\n", agg_table.size());
#endif

    // Phase 3: Convert codes back to nation names and prepare output
    // Build reverse map from code to nation name
    std::unordered_map<int32_t, std::string> code_to_nation;
    code_to_nation[france_code] = "FRANCE";
    code_to_nation[germany_code] = "GERMANY";

    // Build sorted output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::tuple<std::string, std::string, int16_t, double>> results;

    for (const auto& [key, volume_sum] : agg_table) {
        int32_t supp_code = std::get<0>(key);
        int32_t cust_code = std::get<1>(key);
        int16_t year = std::get<2>(key);

        std::string supp_nation = code_to_nation.count(supp_code) ? code_to_nation[supp_code] : "UNKNOWN";
        std::string cust_nation = code_to_nation.count(cust_code) ? code_to_nation[cust_code] : "UNKNOWN";

        // volume_sum is already the final revenue value in decimal form
        // accumulated from: (extendedprice * (100 - discount)) / 10000

        double revenue = volume_sum;
        results.push_back(std::make_tuple(supp_nation, cust_nation, year, revenue));
    }

    // Sort by (supp_nation, cust_nation, l_year)
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (std::get<0>(a) != std::get<0>(b)) return std::get<0>(a) < std::get<0>(b);
            if (std::get<1>(a) != std::get<1>(b)) return std::get<1>(a) < std::get<1>(b);
            return std::get<2>(a) < std::get<2>(b);
        }
    );

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    // Write CSV results
#ifdef GENDB_PROFILE
    auto t_csv_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q7.csv";
    std::ofstream ofs(output_file);
    if (!ofs) {
        std::cerr << "Cannot open output file " << output_file << std::endl;
        return;
    }

    ofs << "supp_nation,cust_nation,l_year,revenue\n";

    for (const auto& [supp_nation, cust_nation, year, revenue] : results) {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s,%s,%d,%.4f\n",
                 supp_nation.c_str(), cust_nation.c_str(), year, revenue);
        ofs << buf;
    }

    ofs.close();

#ifdef GENDB_PROFILE
    auto t_csv_end = std::chrono::high_resolution_clock::now();
    double ms_csv = std::chrono::duration<double, std::milli>(t_csv_end - t_csv_start).count();
    printf("[TIMING] csv_write: %.2f ms\n", ms_csv);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
#ifdef GENDB_PROFILE
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    // Cleanup mmapped index memory
    if (supplier_idx) {
        if (idx_supplier_mmap) munmap(idx_supplier_mmap, idx_supplier_mmap_size);
        delete supplier_idx;
    }
    if (orders_idx) {
        if (idx_orders_mmap) munmap(idx_orders_mmap, idx_orders_mmap_size);
        delete orders_idx;
    }
    if (customer_idx) {
        if (idx_customer_mmap) munmap(idx_customer_mmap, idx_customer_mmap_size);
        delete customer_idx;
    }

    std::cout << "Q7 results written to " << output_file << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q7(gendb_dir, results_dir);

    return 0;
}
#endif
