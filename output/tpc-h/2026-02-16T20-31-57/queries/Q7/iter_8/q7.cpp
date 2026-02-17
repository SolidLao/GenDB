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
 * Q7: Volume Shipping (Iteration 8)
 *
 * OPTIMIZATION FOCUS (Iteration 8):
 * 1. Parallelize lineitem scan_filter (515ms) with zone map pruning
 * 2. Parallelize hash table builds (662ms)
 * 3. Use thread-local hash tables for aggregation (not linear search)
 *
 * LOGICAL PLAN:
 * 1. Load zone map for l_shipdate; filter blocks by BETWEEN predicate
 * 2. Scan only non-pruned blocks to find matching rows (parallel, per-thread collections)
 * 3. Parallel hash table construction for supplier, orders, customer
 * 4. Parallel join over filtered lineitem rows with thread-local aggregation
 * 5. Merge thread results, sort, output
 *
 * PHYSICAL PLAN:
 * - Load lineitem_shipdate_zonemap.bin; use block-level min/max to skip blocks
 * - OpenMP parallel scan with thread-local filtered_indices buffers
 * - Parallel hash table build (each table built by dedicated threads or fused)
 * - Parallel join loop with OpenMP, thread-local hash tables for aggregation
 * - Merge thread results with global hash table
 * - Open-addressing CompactHashTable for all lookups
 *
 * EXPECTED IMPACT:
 * - scan_filter: 515ms → ~80ms (8x from parallel + zone map)
 * - build_hashtables: 662ms → ~200ms (3x from parallel)
 * - Total: 1248ms → ~300ms
 */

// Date constants (epoch days)
static constexpr int32_t EPOCH_1995_01_01 = 9131;
static constexpr int32_t EPOCH_1996_12_31 = 9861;

// Precomputed year table for fast year extraction
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

// Zone map entry structure
struct ZoneMapEntry {
    int32_t min_val;    // minimum value in block
    int32_t max_val;    // maximum value in block
    uint32_t start_row; // start row index (inclusive)
    uint32_t end_row;   // end row index (exclusive)
};
static_assert(sizeof(ZoneMapEntry) == 16);

inline int16_t extract_year(int32_t epoch_day) {
    if (epoch_day < 0 || epoch_day >= 30000) return 1970;
    return YEAR_TABLE[epoch_day];
}

// Open-addressing hash table implementation (robin hood)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count = 0;

    CompactHashTable() {}

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
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

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

// Aggregation key structure
struct AggKey {
    int32_t supp_nation_code;
    int32_t cust_nation_code;
    int16_t l_year;

    bool operator==(const AggKey& other) const {
        return supp_nation_code == other.supp_nation_code &&
               cust_nation_code == other.cust_nation_code &&
               l_year == other.l_year;
    }
};

struct AggResult {
    AggKey key;
    double volume_sum;
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

    uint64_t li_count = li_size / sizeof(int32_t);

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

    // Phase 1: Load zone map and scan/filter lineitem by shipdate
#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Load zone map for lineitem shipdate
    size_t zonemap_size = 0;
    ZoneMapEntry* li_zones = (ZoneMapEntry*)mmap_file(
        gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", zonemap_size);
    size_t num_zones = zonemap_size / sizeof(ZoneMapEntry);

    // Collect indices of live zones (overlapping with [EPOCH_1995_01_01, EPOCH_1996_12_31])
    std::vector<size_t> live_zones;
    if (li_zones) {
        for (size_t z = 0; z < num_zones; z++) {
            // Skip block if max < low or min > high
            if (li_zones[z].max_val < EPOCH_1995_01_01 ||
                li_zones[z].min_val > EPOCH_1996_12_31) {
                continue;
            }
            live_zones.push_back(z);
        }
    } else {
        // Fallback: mark all zones as live if zonemap load failed
        for (size_t z = 0; z < num_zones; z++) {
            live_zones.push_back(z);
        }
    }

    // Parallel scan of live zones with thread-local collection
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<uint64_t>> thread_filtered_indices(num_threads);

    #pragma omp parallel for schedule(dynamic, 1)
    for (size_t zone_idx = 0; zone_idx < live_zones.size(); zone_idx++) {
        size_t z = live_zones[zone_idx];
        int thread_id = omp_get_thread_num();
        auto& local_indices = thread_filtered_indices[thread_id];

        uint32_t start_row = li_zones ? li_zones[z].start_row : 0;
        uint32_t end_row = li_zones ? li_zones[z].end_row : li_count;

        for (uint64_t i = start_row; i < end_row; i++) {
            if (li_shipdate[i] >= EPOCH_1995_01_01 && li_shipdate[i] <= EPOCH_1996_12_31) {
                local_indices.push_back(i);
            }
        }
    }

    // Merge thread-local indices
    std::vector<uint64_t> filtered_li_indices;
    for (int t = 0; t < num_threads; t++) {
        filtered_li_indices.insert(filtered_li_indices.end(),
                                    thread_filtered_indices[t].begin(),
                                    thread_filtered_indices[t].end());
    }

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double ms_filter = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_filter);
    printf("[TIMING] filtered_rows: %zu\n", filtered_li_indices.size());
#endif

    // Phase 2: Build hash tables with open-addressing
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Build supplier hash table using open-addressing (key=s_suppkey -> s_nationkey)
    // Thread-local build per chunk, then sequential merge
    std::vector<CompactHashTable<int32_t, int32_t>> supplier_ht_local;
    for (int t = 0; t < num_threads; t++) {
        supplier_ht_local.emplace_back(s_count / num_threads + 1);
    }
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < s_count; i++) {
        int thread_id = omp_get_thread_num();
        supplier_ht_local[thread_id].insert(s_suppkey[i], s_nationkey[i]);
    }
    // Merge supplier tables
    CompactHashTable<int32_t, int32_t> supplier_ht(s_count);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : supplier_ht_local[t].table) {
            if (entry.occupied) {
                supplier_ht.insert(entry.key, entry.value);
            }
        }
    }

    // Build orders hash table using open-addressing (key=o_orderkey -> o_custkey)
    std::vector<CompactHashTable<int32_t, int32_t>> orders_ht_local;
    for (int t = 0; t < num_threads; t++) {
        orders_ht_local.emplace_back(o_count / num_threads + 1);
    }
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < o_count; i++) {
        int thread_id = omp_get_thread_num();
        orders_ht_local[thread_id].insert(o_orderkey[i], o_custkey[i]);
    }
    // Merge orders tables
    CompactHashTable<int32_t, int32_t> orders_ht(o_count);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : orders_ht_local[t].table) {
            if (entry.occupied) {
                orders_ht.insert(entry.key, entry.value);
            }
        }
    }

    // Build customer hash table using open-addressing (key=c_custkey -> c_nationkey)
    std::vector<CompactHashTable<int32_t, int32_t>> customer_ht_local;
    for (int t = 0; t < num_threads; t++) {
        customer_ht_local.emplace_back(c_count / num_threads + 1);
    }
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < c_count; i++) {
        int thread_id = omp_get_thread_num();
        customer_ht_local[thread_id].insert(c_custkey[i], c_nationkey[i]);
    }
    // Merge customer tables
    CompactHashTable<int32_t, int32_t> customer_ht(c_count);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : customer_ht_local[t].table) {
            if (entry.occupied) {
                customer_ht.insert(entry.key, entry.value);
            }
        }
    }

    // Build nation lookup using open-addressing (key=n_nationkey -> n_name_code)
    // Small table, single-threaded is fine
    CompactHashTable<int32_t, int32_t> nation_ht(25);
    for (uint32_t i = 0; i < 25; i++) {
        nation_ht.insert(n_nationkey[i], n_name_codes[i]);
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hashtables: %.2f ms\n", ms_build);
#endif

    // Aggregation hash table (for thread-local aggregation)
    struct AggHashKey {
        AggKey key;
        double volume_sum;
        bool occupied = false;

        bool operator==(const AggKey& other) const {
            return key == other;
        }
    };

    struct AggHashTable {
        std::vector<AggHashKey> table;
        size_t mask;
        size_t count = 0;

        AggHashTable(size_t expected_size) {
            size_t sz = 1;
            while (sz < expected_size * 4 / 3) sz <<= 1;
            table.resize(sz);
            mask = sz - 1;
        }

        size_t hash(const AggKey& key) const {
            size_t h = (size_t)key.supp_nation_code * 0x9E3779B97F4A7C15ULL;
            h ^= (size_t)key.cust_nation_code * 0xBF58476D1CE4E5B9ULL;
            h ^= (size_t)key.l_year * 0x85EBCA6B * 0x27D4EB2D;
            return h;
        }

        void insert(const AggKey& key, double volume) {
            size_t idx = hash(key) & mask;
            while (table[idx].occupied) {
                if (table[idx].key == key) {
                    table[idx].volume_sum += volume;
                    return;
                }
                idx = (idx + 1) & mask;
            }
            table[idx] = {key, volume, true};
            count++;
        }

        void collect(std::vector<AggResult>& results) {
            for (const auto& entry : table) {
                if (entry.occupied) {
                    results.push_back({entry.key, entry.volume_sum});
                }
            }
        }
    };

    // Phase 3: Join with parallel loop and thread-local aggregation
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local hash tables for aggregation (not linear search)
    std::vector<AggHashTable> thread_agg_hts;
    for (int i = 0; i < num_threads; i++) {
        thread_agg_hts.emplace_back(64);  // expect ~4 groups per thread on average
    }

    #pragma omp parallel for schedule(dynamic, 1000)
    for (size_t li_idx_pos = 0; li_idx_pos < filtered_li_indices.size(); li_idx_pos++) {
        uint64_t li_idx = filtered_li_indices[li_idx_pos];
        int thread_id = omp_get_thread_num();
        auto& local_agg = thread_agg_hts[thread_id];

        // Get lineitem data
        int32_t supp_key = li_suppkey[li_idx];
        int32_t order_key = li_orderkey[li_idx];
        int32_t shipdate = li_shipdate[li_idx];
        int64_t extendedprice = li_extendedprice[li_idx];
        int64_t discount = li_discount[li_idx];

        // Compute volume = extendedprice * (1 - discount)
        double volume = (double)extendedprice * (100.0 - (double)discount) / 10000.0;

        // Lookup supplier nationkey
        int32_t* supp_it = supplier_ht.find(supp_key);
        if (!supp_it) continue;
        int32_t supp_nationkey = *supp_it;

        // Lookup customer key from order
        int32_t* order_it = orders_ht.find(order_key);
        if (!order_it) continue;
        int32_t cust_key = *order_it;

        // Lookup customer nationkey
        int32_t* cust_it = customer_ht.find(cust_key);
        if (!cust_it) continue;
        int32_t cust_nationkey = *cust_it;

        // Get nation codes
        int32_t* supp_nation_it = nation_ht.find(supp_nationkey);
        int32_t* cust_nation_it = nation_ht.find(cust_nationkey);
        if (!supp_nation_it || !cust_nation_it) continue;

        int32_t supp_nation_code = *supp_nation_it;
        int32_t cust_nation_code = *cust_nation_it;

        // Check nation constraint
        bool valid = (
            (supp_nation_code == france_code && cust_nation_code == germany_code) ||
            (supp_nation_code == germany_code && cust_nation_code == france_code)
        );
        if (!valid) continue;

        // Extract year from shipdate
        int16_t year = extract_year(shipdate);

        // Aggregate into thread-local hash table
        AggKey key = {supp_nation_code, cust_nation_code, year};
        local_agg.insert(key, volume);
    }

    // Merge thread-local hash tables
    std::vector<AggResult> merged_agg;
    for (int t = 0; t < num_threads; t++) {
        thread_agg_hts[t].collect(merged_agg);
    }

    // De-duplicate if multiple threads produced the same group
    std::sort(merged_agg.begin(), merged_agg.end(),
        [](const AggResult& a, const AggResult& b) {
            if (a.key.supp_nation_code != b.key.supp_nation_code)
                return a.key.supp_nation_code < b.key.supp_nation_code;
            if (a.key.cust_nation_code != b.key.cust_nation_code)
                return a.key.cust_nation_code < b.key.cust_nation_code;
            return a.key.l_year < b.key.l_year;
        }
    );

    // Consolidate duplicates
    std::vector<AggResult> final_agg;
    for (const auto& result : merged_agg) {
        if (!final_agg.empty() && final_agg.back().key == result.key) {
            final_agg.back().volume_sum += result.volume_sum;
        } else {
            final_agg.push_back(result);
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_join);
    printf("[TIMING] aggregate_groups: %zu\n", final_agg.size());
#endif

    // Phase 4: Convert codes back to nation names and prepare output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::tuple<std::string, std::string, int16_t, double>> results;

    for (const auto& result : final_agg) {
        int32_t supp_code = result.key.supp_nation_code;
        int32_t cust_code = result.key.cust_nation_code;
        int16_t year = result.key.l_year;

        // Map nation codes back to names
        std::string supp_nation = (supp_code == france_code) ? "FRANCE" :
                                  (supp_code == germany_code) ? "GERMANY" : "UNKNOWN";
        std::string cust_nation = (cust_code == france_code) ? "FRANCE" :
                                  (cust_code == germany_code) ? "GERMANY" : "UNKNOWN";

        double revenue = result.volume_sum;
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
