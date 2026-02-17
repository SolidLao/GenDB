/*
 * Q9: Product Type Profit Measure
 *
 * LOGICAL PLAN (OPTIMIZED - Iteration 1):
 * 1. Filter part on p_name LIKE '%green%' → ~200K green parts
 * 2. Load nation into flat array (25 rows)
 * 3. Load pre-built indexes via mmap (ZERO build time):
 *    - supplier_suppkey_hash: s_suppkey → s_nationkey
 *    - partsupp_partkey_suppkey_hash: (ps_partkey, ps_suppkey) → ps_supplycost
 *    - lineitem_partkey_suppkey_hash: (l_partkey, l_suppkey) → positions
 * 4. Build orders hash: o_orderkey → o_orderdate (15M, but needed)
 * 5. REVERSED JOIN ORDER (key optimization):
 *    - For each green_part:
 *      - For each supplier (100K suppliers):
 *        - Probe lineitem index for (partkey, suppkey) → get matching lineitem rows
 *        - For each lineitem row:
 *          - Lookup supplier → nation
 *          - Lookup partsupp → supplycost
 *          - Lookup orders → orderdate
 *          - Compute profit, extract year, aggregate
 * 6. Sort by nation ASC, year DESC
 *
 * PHYSICAL PLAN:
 * - nation: flat array (25 entries)
 * - part: filter during scan, store green_partkeys in vector
 * - supplier: PRE-BUILT INDEX (mmap, hash_single)
 * - partsupp: PRE-BUILT INDEX (mmap, hash_single composite key)
 * - lineitem: PRE-BUILT INDEX (mmap, hash_multi_value composite key)
 * - orders: open-addressing hash o_orderkey → o_orderdate
 * - aggregation: std::unordered_map (nations × years = ~175 groups)
 * - Date: O(1) year lookup table
 * - Decimals: scaled int64_t arithmetic (scale 100)
 * - Parallelism: OpenMP parallel for on green_parts × suppliers
 */

#include <iostream>
#include <fstream>
#include <iomanip>
#include <string>
#include <vector>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <cmath>
#include <omp.h>

// O(1) date lookup table for year extraction
static int16_t YEAR_TABLE[30000];

void init_date_tables() {
    int year = 1970, month = 1, day = 1;
    const int days_per_month[] = {31,28,31,30,31,30,31,31,30,31,30,31};

    for (int d = 0; d < 30000; d++) {
        YEAR_TABLE[d] = year;

        day++;
        bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int dim = days_per_month[month - 1] + (month == 2 && leap ? 1 : 0);
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

inline int extract_year(int32_t epoch_day) {
    return YEAR_TABLE[epoch_day];
}

// Open-addressing hash table for orders lookup
struct OrdersHashTable {
    struct Entry {
        int32_t key;
        int32_t value;
        bool occupied;
    };
    std::vector<Entry> table;
    size_t mask;

    OrdersHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz, {0, 0, false});
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, int32_t value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    int32_t* find(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Pre-built index structures (loaded via mmap)
struct SupplierIndex {
    struct Entry {
        int32_t key;
        uint32_t position;
    };
    uint32_t num_entries;
    uint32_t table_size;
    Entry* entries;
    size_t mask;

    int32_t* lookup_nationkey(int32_t s_suppkey, int32_t* s_nationkey_data) {
        size_t idx = ((uint64_t)s_suppkey * 0x9E3779B97F4A7C15ULL) & mask;
        while (entries[idx].position != 0 || entries[idx].key == s_suppkey) {
            if (entries[idx].key == s_suppkey) {
                return &s_nationkey_data[entries[idx].position];
            }
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

struct PartsuppIndex {
    struct Entry {
        int32_t key1;
        int32_t key2;
        uint32_t offset;
        uint32_t count;
    };
    uint32_t num_entries;
    uint32_t table_size;
    Entry* entries;
    size_t mask;

    int64_t* lookup_cost(int32_t ps_partkey, int32_t ps_suppkey, int64_t* ps_cost_data) {
        size_t idx = (((uint64_t)ps_partkey * 0x9E3779B97F4A7C15ULL) ^ ((uint64_t)ps_suppkey * 0x9E3779B185EBCA87ULL)) & mask;
        while (entries[idx].count != 0 || (entries[idx].key1 == ps_partkey && entries[idx].key2 == ps_suppkey)) {
            if (entries[idx].key1 == ps_partkey && entries[idx].key2 == ps_suppkey) {
                return &ps_cost_data[entries[idx].offset];
            }
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

struct LineitemIndex {
    struct Entry {
        int32_t key1;  // l_partkey
        int32_t key2;  // l_suppkey
        uint32_t offset;
        uint32_t count;
    };
    uint32_t num_unique;
    uint32_t table_size;
    Entry* entries;
    uint32_t* positions;
    size_t mask;

    void get_positions(int32_t l_partkey, int32_t l_suppkey, uint32_t** out_positions, uint32_t* out_count) {
        size_t idx = (((uint64_t)l_partkey * 0x9E3779B97F4A7C15ULL) ^ ((uint64_t)l_suppkey * 0x9E3779B185EBCA87ULL)) & mask;
        while (entries[idx].count != 0 || (entries[idx].key1 == l_partkey && entries[idx].key2 == l_suppkey)) {
            if (entries[idx].key1 == l_partkey && entries[idx].key2 == l_suppkey) {
                *out_positions = &positions[entries[idx].offset];
                *out_count = entries[idx].count;
                return;
            }
            idx = (idx + 1) & mask;
        }
        *out_positions = nullptr;
        *out_count = 0;
    }
};

// Custom hash for string + int pair
struct NationYearHash {
    std::size_t operator()(const std::pair<std::string, int>& p) const {
        std::size_t h1 = std::hash<std::string>{}(p.first);
        std::size_t h2 = (uint64_t)p.second * 0x9E3779B97F4A7C15ULL;
        return h1 ^ (h2 >> 32);
    }
};

// mmap helper
template<typename T>
T* mmap_binary(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    fstat(fd, &sb);
    count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return static_cast<T*>(addr);
}

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    init_date_tables();

    // Load nation (25 rows) into flat array
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t nation_rows;
    int32_t* n_nationkey = mmap_binary<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_rows);

    // Load nation names
    std::string nation_names[25];
    std::ifstream n_name_file(gendb_dir + "/nation/n_name.bin", std::ios::binary);
    for (size_t i = 0; i < nation_rows; i++) {
        uint32_t len;
        n_name_file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        nation_names[n_nationkey[i]].resize(len);
        n_name_file.read(&nation_names[n_nationkey[i]][0], len);
    }
    n_name_file.close();

    // Load part and filter on p_name LIKE '%green%'
    size_t part_rows;
    int32_t* p_partkey = mmap_binary<int32_t>(gendb_dir + "/part/p_partkey.bin", part_rows);

    std::vector<int32_t> green_partkeys;
    green_partkeys.reserve(500000);

    std::ifstream p_name_file(gendb_dir + "/part/p_name.bin", std::ios::binary);
    for (size_t i = 0; i < part_rows; i++) {
        uint32_t len;
        p_name_file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        std::string name(len, ' ');
        p_name_file.read(&name[0], len);
        if (name.find("green") != std::string::npos) {
            green_partkeys.push_back(p_partkey[i]);
        }
    }
    p_name_file.close();

    // Load supplier data (needed for index lookups)
    size_t supp_rows;
    int32_t* s_suppkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supp_rows);
    int32_t* s_nationkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supp_rows);

    // Load supplier index
    SupplierIndex supp_idx;
    int fd_supp = open((gendb_dir + "/indexes/supplier_suppkey_hash.bin").c_str(), O_RDONLY);
    if (fd_supp < 0) {
        std::cerr << "Failed to open supplier index" << std::endl;
        return;
    }
    struct stat sb_supp;
    fstat(fd_supp, &sb_supp);
    void* supp_map = mmap(nullptr, sb_supp.st_size, PROT_READ, MAP_PRIVATE, fd_supp, 0);
    close(fd_supp);
    uint32_t* supp_header = (uint32_t*)supp_map;
    supp_idx.num_entries = supp_header[0];
    supp_idx.table_size = supp_header[1];
    supp_idx.entries = (SupplierIndex::Entry*)&supp_header[2];
    supp_idx.mask = supp_idx.table_size - 1;

    // Load partsupp data (for index lookups)
    size_t ps_rows;
    int64_t* ps_supplycost = mmap_binary<int64_t>(gendb_dir + "/partsupp/ps_supplycost.bin", ps_rows);

    // Load partsupp index
    PartsuppIndex ps_idx;
    int fd_ps = open((gendb_dir + "/indexes/partsupp_partkey_suppkey_hash.bin").c_str(), O_RDONLY);
    if (fd_ps < 0) {
        std::cerr << "Failed to open partsupp index" << std::endl;
        return;
    }
    struct stat sb_ps;
    fstat(fd_ps, &sb_ps);
    void* ps_map = mmap(nullptr, sb_ps.st_size, PROT_READ, MAP_PRIVATE, fd_ps, 0);
    close(fd_ps);
    uint32_t* ps_header = (uint32_t*)ps_map;
    ps_idx.num_entries = ps_header[0];
    ps_idx.table_size = ps_header[1];
    ps_idx.entries = (PartsuppIndex::Entry*)&ps_header[2];
    ps_idx.mask = ps_idx.table_size - 1;

    // Load lineitem data
    size_t li_rows;
    int32_t* l_orderkey = mmap_binary<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", li_rows);
    int64_t* l_quantity = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", li_rows);
    int64_t* l_extendedprice = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", li_rows);
    int64_t* l_discount = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_discount.bin", li_rows);

    // Load lineitem index
    LineitemIndex li_idx;
    int fd_li = open((gendb_dir + "/indexes/lineitem_partkey_suppkey_hash.bin").c_str(), O_RDONLY);
    if (fd_li < 0) {
        std::cerr << "Failed to open lineitem index" << std::endl;
        return;
    }
    struct stat sb_li;
    fstat(fd_li, &sb_li);
    void* li_map = mmap(nullptr, sb_li.st_size, PROT_READ, MAP_PRIVATE, fd_li, 0);
    close(fd_li);
    uint32_t* li_header = (uint32_t*)li_map;
    li_idx.num_unique = li_header[0];
    li_idx.table_size = li_header[1];
    li_idx.entries = (LineitemIndex::Entry*)&li_header[2];
    size_t entries_size = li_idx.table_size * sizeof(LineitemIndex::Entry);
    uint32_t* pos_header = (uint32_t*)((char*)&li_header[2] + entries_size);
    li_idx.positions = &pos_header[1];
    li_idx.mask = li_idx.table_size - 1;

    // Build orders hash table (still needed)
    size_t ord_rows;
    int32_t* o_orderkey = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderkey.bin", ord_rows);
    int32_t* o_orderdate = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderdate.bin", ord_rows);

    OrdersHashTable order_ht(ord_rows);
    for (size_t i = 0; i < ord_rows; i++) {
        order_ht.insert(o_orderkey[i], o_orderdate[i]);
    }

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] data_load: %.2f ms\n", load_ms);
#endif

    // Join and aggregate (REVERSED JOIN ORDER)
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local aggregation maps
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<std::pair<std::string, int>, double, NationYearHash>> thread_aggs(num_threads);
    for (auto& m : thread_aggs) m.reserve(200);

    // Iterate green_parts × suppliers (reversed from lineitem scan)
    #pragma omp parallel for schedule(dynamic, 100)
    for (size_t gp_idx = 0; gp_idx < green_partkeys.size(); gp_idx++) {
        int tid = omp_get_thread_num();
        int32_t partkey = green_partkeys[gp_idx];

        // For each supplier, probe lineitem index for (partkey, suppkey)
        for (size_t s = 0; s < supp_rows; s++) {
            int32_t suppkey = s_suppkey[s];

            // Probe lineitem index
            uint32_t* li_positions = nullptr;
            uint32_t li_count = 0;
            li_idx.get_positions(partkey, suppkey, &li_positions, &li_count);

            if (li_count == 0) continue;

            // Lookup supplier nation (via index)
            int32_t* nationkey_ptr = supp_idx.lookup_nationkey(suppkey, s_nationkey);
            if (!nationkey_ptr) continue;
            int32_t nationkey = *nationkey_ptr;

            // Lookup partsupp cost (via index)
            int64_t* cost_ptr = ps_idx.lookup_cost(partkey, suppkey, ps_supplycost);
            if (!cost_ptr) continue;
            int64_t supplycost = *cost_ptr;

            // Process all matching lineitem rows
            for (uint32_t j = 0; j < li_count; j++) {
                uint32_t li_pos = li_positions[j];

                // Lookup orders
                int32_t* orderdate_ptr = order_ht.find(l_orderkey[li_pos]);
                if (!orderdate_ptr) continue;
                int32_t orderdate = *orderdate_ptr;

                // Compute profit
                int64_t revenue = l_extendedprice[li_pos] * (100 - l_discount[li_pos]);
                int64_t cost = supplycost * l_quantity[li_pos];
                int64_t profit_scaled = revenue - cost;
                double profit = profit_scaled / 10000.0;

                // Extract year
                int year = extract_year(orderdate);

                // Aggregate (thread-local)
                std::string& nation = nation_names[nationkey];
                thread_aggs[tid][{nation, year}] += profit;
            }
        }
    }

    // Merge thread-local aggregations
    std::unordered_map<std::pair<std::string, int>, double, NationYearHash> agg_map;
    agg_map.reserve(200);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& kv : thread_aggs[t]) {
            agg_map[kv.first] += kv.second;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", join_ms);
#endif

    // Sort by nation ASC, year DESC
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::tuple<std::string, int, double>> results;
    results.reserve(agg_map.size());
    for (const auto& kv : agg_map) {
        results.push_back({kv.first.first, kv.first.second, kv.second});
    }

    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (std::get<0>(a) != std::get<0>(b)) return std::get<0>(a) < std::get<0>(b);
        return std::get<1>(a) > std::get<1>(b); // year DESC
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q9.csv");
    out << "nation,o_year,sum_profit\n";
    for (const auto& r : results) {
        out << std::get<0>(r) << "," << std::get<1>(r) << ","
            << std::fixed << std::setprecision(2) << std::get<2>(r) << "\n";
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
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
