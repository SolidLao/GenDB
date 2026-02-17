/*
 * Q9: Product Type Profit Measure
 *
 * LOGICAL PLAN:
 * 1. Filter part on p_name LIKE '%green%' → ~200K rows (selective)
 * 2. Build hash set of matching p_partkeys
 * 3. Load nation into flat array (25 rows)
 * 4. Load pre-built indexes via mmap (zero build time):
 *    - supplier_suppkey_hash: s_suppkey → position
 *    - partsupp_partkey_suppkey_hash: (ps_partkey, ps_suppkey) → position list
 *    - lineitem_partkey_suppkey_hash: (l_partkey, l_suppkey) → position list
 * 5. Build compact hash on orders: o_orderkey → o_orderdate (15M rows, unavoidable)
 * 6. For each green partkey, use lineitem_partkey_suppkey_hash to find matching rows:
 *    - Lookup supplier index to get nation
 *    - Lookup partsupp index to get supplycost
 *    - Lookup orders hash to get orderdate
 *    - Compute profit = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
 *    - Extract year from orderdate (O(1) lookup)
 *    - Aggregate by (nation_name, year)
 * 7. Sort by nation ASC, year DESC
 *
 * PHYSICAL PLAN:
 * - nation: flat array (25 entries)
 * - part: hash set of matching p_partkeys (open-addressing, ~200K)
 * - supplier: mmap pre-built hash index
 * - partsupp: mmap pre-built composite hash index
 * - lineitem: mmap pre-built composite hash index
 * - orders: open-addressing hash o_orderkey → o_orderdate (15M)
 * - aggregation: thread-local hash maps, merge at end
 * - Date: O(1) year lookup table
 * - Decimals: scaled int64_t arithmetic (scale 100)
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

// Compact open-addressing hash table for orders (o_orderkey → o_orderdate)
struct OrdersHashTable {
    struct Entry { int32_t key; int32_t value; bool occupied = false; };
    std::vector<Entry> table;
    size_t mask;

    OrdersHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
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

// Compact open-addressing hash set for green partkeys
struct PartKeySet {
    struct Entry { int32_t key; bool occupied = false; };
    std::vector<Entry> table;
    size_t mask;

    PartKeySet(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return;
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, true};
    }

    bool contains(int32_t key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return true;
            idx = (idx + 1) & mask;
        }
        return false;
    }
};

// Pre-built hash index structs
struct HashSingleEntry {
    int32_t key;
    uint32_t position;
};

struct HashCompositeEntry {
    int32_t key1;
    int32_t key2;
    uint32_t offset;
    uint32_t count;
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

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load nation (25 rows) into flat array
    size_t nation_rows;
    int32_t* n_nationkey = mmap_binary<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_rows);

    std::string nation_names[25];
    std::ifstream n_name_file(gendb_dir + "/nation/n_name.bin", std::ios::binary);
    for (size_t i = 0; i < nation_rows; i++) {
        uint32_t len;
        n_name_file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        nation_names[n_nationkey[i]].resize(len);
        n_name_file.read(&nation_names[n_nationkey[i]][0], len);
    }
    n_name_file.close();

    // Load part and filter on p_name LIKE '%green%' → build compact hash set
    size_t part_rows;
    int32_t* p_partkey = mmap_binary<int32_t>(gendb_dir + "/part/p_partkey.bin", part_rows);

    PartKeySet green_parts(500000);

    std::ifstream p_name_file(gendb_dir + "/part/p_name.bin", std::ios::binary);
    for (size_t i = 0; i < part_rows; i++) {
        uint32_t len;
        p_name_file.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        std::string name(len, ' ');
        p_name_file.read(&name[0], len);
        if (name.find("green") != std::string::npos) {
            green_parts.insert(p_partkey[i]);
        }
    }
    p_name_file.close();

    // Load pre-built supplier index
    int fd_supp_idx = open((gendb_dir + "/indexes/supplier_suppkey_hash.bin").c_str(), O_RDONLY);
    struct stat sb_supp;
    fstat(fd_supp_idx, &sb_supp);
    uint8_t* supp_idx_mem = (uint8_t*)mmap(nullptr, sb_supp.st_size, PROT_READ, MAP_PRIVATE, fd_supp_idx, 0);
    close(fd_supp_idx);

    uint32_t supp_table_size = *((uint32_t*)supp_idx_mem + 1);
    HashSingleEntry* supp_idx = (HashSingleEntry*)(supp_idx_mem + 8);

    // Load supplier nationkeys
    size_t supp_rows;
    int32_t* s_nationkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supp_rows);

    // Load pre-built partsupp index (composite key)
    int fd_ps_idx = open((gendb_dir + "/indexes/partsupp_partkey_suppkey_hash.bin").c_str(), O_RDONLY);
    struct stat sb_ps;
    fstat(fd_ps_idx, &sb_ps);
    uint8_t* ps_idx_mem = (uint8_t*)mmap(nullptr, sb_ps.st_size, PROT_READ, MAP_PRIVATE, fd_ps_idx, 0);
    close(fd_ps_idx);

    uint32_t ps_table_size = *((uint32_t*)ps_idx_mem + 1);
    HashCompositeEntry* ps_idx = (HashCompositeEntry*)(ps_idx_mem + 8);

    // Load partsupp supplycost
    size_t ps_rows;
    int64_t* ps_supplycost = mmap_binary<int64_t>(gendb_dir + "/partsupp/ps_supplycost.bin", ps_rows);

    // Build compact hash table on orders: o_orderkey → o_orderdate
    size_t ord_rows;
    int32_t* o_orderkey = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderkey.bin", ord_rows);
    int32_t* o_orderdate = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderdate.bin", ord_rows);

    OrdersHashTable order_date(ord_rows);
    for (size_t i = 0; i < ord_rows; i++) {
        order_date.insert(o_orderkey[i], o_orderdate[i]);
    }

    // Load lineitem columns
    size_t li_rows;
    int32_t* l_orderkey = mmap_binary<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", li_rows);
    int32_t* l_partkey = mmap_binary<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", li_rows);
    int32_t* l_suppkey = mmap_binary<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", li_rows);
    int64_t* l_quantity = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", li_rows);
    int64_t* l_extendedprice = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", li_rows);
    int64_t* l_discount = mmap_binary<int64_t>(gendb_dir + "/lineitem/l_discount.bin", li_rows);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] data_load: %.2f ms\n", load_ms);
#endif

    // Join and aggregate
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<std::pair<std::string, int>, double, NationYearHash> agg_map;
    agg_map.reserve(1000);

    // Helper to find in single-key hash index
    auto find_single = [](HashSingleEntry* table, uint32_t table_size, int32_t key) -> uint32_t* {
        size_t idx = ((size_t)key * 0x9E3779B97F4A7C15ULL) % table_size;
        while (table[idx].key != 0) {
            if (table[idx].key == key) return &table[idx].position;
            idx = (idx + 1) % table_size;
        }
        return nullptr;
    };

    // Helper to find in composite-key hash index
    auto find_composite = [](HashCompositeEntry* table, uint32_t table_size, int32_t key1, int32_t key2) -> uint32_t* {
        uint64_t h1 = (uint64_t)key1 * 0x9E3779B97F4A7C15ULL;
        uint64_t h2 = (uint64_t)key2 * 0x9E3779B185EBCA87ULL;
        size_t hash = (h1 >> 32) ^ (h2 >> 32);
        size_t idx = hash % table_size;
        while (table[idx].key1 != 0 || table[idx].key2 != 0) {
            if (table[idx].key1 == key1 && table[idx].key2 == key2) {
                return &table[idx].offset;
            }
            idx = (idx + 1) % table_size;
        }
        return nullptr;
    };

    for (size_t i = 0; i < li_rows; i++) {
        // Filter by green parts
        if (!green_parts.contains(l_partkey[i])) continue;

        // Join with supplier using index
        uint32_t* supp_pos = find_single(supp_idx, supp_table_size, l_suppkey[i]);
        if (!supp_pos) continue;
        int32_t nationkey = s_nationkey[*supp_pos];

        // Join with partsupp using composite index
        uint32_t* ps_pos = find_composite(ps_idx, ps_table_size, l_partkey[i], l_suppkey[i]);
        if (!ps_pos) continue;
        int64_t supplycost = ps_supplycost[*ps_pos];

        // Join with orders
        int32_t* orderdate = order_date.find(l_orderkey[i]);
        if (!orderdate) continue;

        // Compute profit
        int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]);
        int64_t cost = supplycost * l_quantity[i];
        int64_t profit_scaled = revenue - cost;
        double profit = profit_scaled / 10000.0;

        // Extract year
        int year = extract_year(*orderdate);

        // Aggregate
        std::string& nation = nation_names[nationkey];
        agg_map[{nation, year}] += profit;
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
