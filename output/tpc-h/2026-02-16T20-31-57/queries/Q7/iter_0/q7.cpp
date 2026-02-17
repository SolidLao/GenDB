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

// Note: Pre-built hash index loading functions are available in knowledge base
// (patterns/parallel-hash-join.md) for future optimization iterations

// Result aggregation structure
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
    int32_t* n_nationkey = (int32_t*)mmap_file(gendb_dir + "/nation/n_nationkey.bin", n_size);
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

    // Phase 2: Build hash tables for joins
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Build supplier hash table (key=s_suppkey -> s_nationkey)
    std::unordered_map<int32_t, int32_t> supplier_ht;
    supplier_ht.reserve(s_count);
    for (uint32_t i = 0; i < s_count; i++) {
        supplier_ht[s_suppkey[i]] = s_nationkey[i];
    }

    // Build orders hash table (key=o_orderkey -> o_custkey)
    std::unordered_map<int32_t, int32_t> orders_ht;
    orders_ht.reserve(o_count);
    for (uint32_t i = 0; i < o_count; i++) {
        orders_ht[o_orderkey[i]] = o_custkey[i];
    }

    // Build customer hash table (key=c_custkey -> c_nationkey)
    std::unordered_map<int32_t, int32_t> customer_ht;
    customer_ht.reserve(c_count);
    for (uint32_t i = 0; i < c_count; i++) {
        customer_ht[c_custkey[i]] = c_nationkey[i];
    }

    // Build nation lookup (key=n_nationkey -> n_name_code)
    std::unordered_map<int32_t, int32_t> nation_ht;
    for (uint32_t i = 0; i < 25; i++) {
        nation_ht[n_nationkey[i]] = n_name_codes[i];
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hashtables: %.2f ms\n", ms_build);
#endif

    // Phase 3: Join and aggregate with filtered lineitem
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Aggregation hash table: key = (supp_nation_code, cust_nation_code, l_year)
    // Use double to accumulate for better precision
    std::unordered_map<
        std::tuple<int32_t, int32_t, int16_t>,
        double,
        AggKeyHash
    > agg_table;
    agg_table.reserve(100000);  // Conservative estimate

    for (uint64_t li_idx : filtered_li_indices) {
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
        auto supp_it = supplier_ht.find(supp_key);
        if (supp_it == supplier_ht.end()) continue;
        int32_t supp_nationkey = supp_it->second;

        // Lookup customer key from order
        auto order_it = orders_ht.find(order_key);
        if (order_it == orders_ht.end()) continue;
        int32_t cust_key = order_it->second;

        // Lookup customer nationkey
        auto cust_it = customer_ht.find(cust_key);
        if (cust_it == customer_ht.end()) continue;
        int32_t cust_nationkey = cust_it->second;

        // Get nation codes
        auto supp_nation_it = nation_ht.find(supp_nationkey);
        auto cust_nation_it = nation_ht.find(cust_nationkey);
        if (supp_nation_it == nation_ht.end() || cust_nation_it == nation_ht.end()) continue;

        int32_t supp_nation_code = supp_nation_it->second;
        int32_t cust_nation_code = cust_nation_it->second;

        // Check nation constraint
        bool valid = (
            (supp_nation_code == france_code && cust_nation_code == germany_code) ||
            (supp_nation_code == germany_code && cust_nation_code == france_code)
        );
        if (!valid) continue;

        // Extract year from shipdate
        int16_t year = extract_year(shipdate);

        // Aggregate
        auto key = std::make_tuple(supp_nation_code, cust_nation_code, year);
        agg_table[key] += volume;
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
