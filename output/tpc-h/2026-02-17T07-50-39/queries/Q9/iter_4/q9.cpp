/*
 * Q9: Product Type Profit Measure - FUNDAMENTAL REWRITE (Iteration 4)
 *
 * ROOT CAUSE ANALYSIS:
 * - Previous approach: 7459ms data_load (building 4 large hash tables), 5581ms join_aggregate
 * - Architecture failures:
 *   1. Building hash on partsupp (8M rows) and orders (15M rows) from scratch
 *   2. No parallelism on 60M lineitem scan
 *   3. std::unordered_map instead of open-addressing or arrays
 *   4. Not using pre-built indexes
 *
 * NEW STRATEGY (Index-Driven + Parallel):
 * 1. Filter part on p_name LIKE '%green%' → ~200K green parts (10% selectivity)
 * 2. Build compact hash set of green_parts (200K entries)
 * 3. Load supplier into flat array s_nationkey[s_suppkey] (100K entries, dense)
 * 4. Load nation into flat array nation_names[n_nationkey] (25 entries)
 * 5. Build compact hash partsupp: (ps_partkey, ps_suppkey) → ps_supplycost (8M entries)
 * 6. Build compact hash orders: o_orderkey → o_orderdate (15M entries)
 * 7. PARALLEL SCAN lineitem (60M rows):
 *    - Thread-local aggregation: each thread maintains its own hash map
 *    - For each row: check green_parts, lookup supplier nation, partsupp cost, order date
 *    - Compute profit, aggregate by (nation, year)
 * 8. Merge thread-local results
 * 9. Sort by nation ASC, year DESC
 *
 * PHYSICAL PLAN:
 * - part filter: sequential scan (2M rows, selective predicate)
 * - green_parts: open-addressing hash set (200K entries)
 * - supplier: flat array s_nationkey[suppkey] (100K entries)
 * - nation: flat array nation_names[nationkey] (25 entries)
 * - partsupp: open-addressing hash (8M composite keys)
 * - orders: open-addressing hash (15M keys)
 * - lineitem: PARALLEL morsel-driven scan (60M rows, 64 threads, 100K morsel)
 * - aggregation: thread-local hash maps, merge at end
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
#include <thread>

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

// Fast hash for int32_t (multiply-shift, NOT identity)
inline uint64_t fast_hash_int32(int32_t key) {
    return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
}

// Fast hash for pair (composite key)
inline uint64_t fast_hash_pair(int32_t k1, int32_t k2) {
    uint64_t h1 = (uint64_t)k1 * 0x9E3779B97F4A7C15ULL;
    uint64_t h2 = (uint64_t)k2 * 0x9E3779B185EBCA87ULL;
    return h1 ^ (h2 >> 32);
}

// Open-addressing hash set for int32_t keys
struct CompactHashSet {
    std::vector<int32_t> keys;
    size_t capacity;
    int32_t empty_key = -1;

    CompactHashSet(size_t est_size) {
        capacity = 1;
        while (capacity < est_size * 2) capacity *= 2;
        keys.resize(capacity, empty_key);
    }

    void insert(int32_t key) {
        uint64_t hash = fast_hash_int32(key);
        size_t pos = hash & (capacity - 1);
        while (keys[pos] != empty_key && keys[pos] != key) {
            pos = (pos + 1) & (capacity - 1);
        }
        keys[pos] = key;
    }

    bool contains(int32_t key) const {
        uint64_t hash = fast_hash_int32(key);
        size_t pos = hash & (capacity - 1);
        while (keys[pos] != empty_key) {
            if (keys[pos] == key) return true;
            pos = (pos + 1) & (capacity - 1);
        }
        return false;
    }
};

// Open-addressing hash map for (int32_t, int32_t) → int64_t
struct CompactPairMap {
    struct Entry {
        int32_t k1, k2;
        int64_t value;
    };
    std::vector<Entry> entries;
    size_t capacity;
    int32_t empty_key = -1;

    CompactPairMap(size_t est_size) {
        capacity = 1;
        while (capacity < est_size * 2) capacity *= 2;
        entries.resize(capacity, {empty_key, empty_key, 0});
    }

    void insert(int32_t k1, int32_t k2, int64_t val) {
        uint64_t hash = fast_hash_pair(k1, k2);
        size_t pos = hash & (capacity - 1);
        while (entries[pos].k1 != empty_key && !(entries[pos].k1 == k1 && entries[pos].k2 == k2)) {
            pos = (pos + 1) & (capacity - 1);
        }
        entries[pos] = {k1, k2, val};
    }

    int64_t* find(int32_t k1, int32_t k2) {
        uint64_t hash = fast_hash_pair(k1, k2);
        size_t pos = hash & (capacity - 1);
        while (entries[pos].k1 != empty_key) {
            if (entries[pos].k1 == k1 && entries[pos].k2 == k2) {
                return &entries[pos].value;
            }
            pos = (pos + 1) & (capacity - 1);
        }
        return nullptr;
    }
};

// Open-addressing hash map for int32_t → int32_t
struct CompactIntMap {
    struct Entry {
        int32_t key;
        int32_t value;
    };
    std::vector<Entry> entries;
    size_t capacity;
    int32_t empty_key = -1;

    CompactIntMap(size_t est_size) {
        capacity = 1;
        while (capacity < est_size * 2) capacity *= 2;
        entries.resize(capacity, {empty_key, 0});
    }

    void insert(int32_t k, int32_t v) {
        uint64_t hash = fast_hash_int32(k);
        size_t pos = hash & (capacity - 1);
        while (entries[pos].key != empty_key && entries[pos].key != k) {
            pos = (pos + 1) & (capacity - 1);
        }
        entries[pos] = {k, v};
    }

    int32_t* find(int32_t k) {
        uint64_t hash = fast_hash_int32(k);
        size_t pos = hash & (capacity - 1);
        while (entries[pos].key != empty_key) {
            if (entries[pos].key == k) {
                return &entries[pos].value;
            }
            pos = (pos + 1) & (capacity - 1);
        }
        return nullptr;
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

    // Determine thread count
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;
    omp_set_num_threads(num_threads);

    // Load data
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

    // Load part and filter on p_name LIKE '%green%'
    size_t part_rows;
    int32_t* p_partkey = mmap_binary<int32_t>(gendb_dir + "/part/p_partkey.bin", part_rows);

    CompactHashSet green_parts(500000);

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

    // Load supplier: flat array s_nationkey[s_suppkey]
    size_t supp_rows;
    int32_t* s_suppkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supp_rows);
    int32_t* s_nationkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supp_rows);

    // Assuming s_suppkey is dense 1..100000, use flat array
    std::vector<int32_t> supp_nation(100001, -1);
    for (size_t i = 0; i < supp_rows; i++) {
        supp_nation[s_suppkey[i]] = s_nationkey[i];
    }

    // Load partsupp: (ps_partkey, ps_suppkey) → ps_supplycost
    size_t ps_rows;
    int32_t* ps_partkey = mmap_binary<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", ps_rows);
    int32_t* ps_suppkey = mmap_binary<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", ps_rows);
    int64_t* ps_supplycost = mmap_binary<int64_t>(gendb_dir + "/partsupp/ps_supplycost.bin", ps_rows);

    CompactPairMap partsupp_cost(ps_rows);
    for (size_t i = 0; i < ps_rows; i++) {
        partsupp_cost.insert(ps_partkey[i], ps_suppkey[i], ps_supplycost[i]);
    }

    // Load orders: o_orderkey → o_orderdate
    size_t ord_rows;
    int32_t* o_orderkey = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderkey.bin", ord_rows);
    int32_t* o_orderdate = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderdate.bin", ord_rows);

    CompactIntMap order_date(ord_rows);
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

    // Parallel join and aggregate
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Custom hash for pair in unordered_map
    struct PairHash {
        std::size_t operator()(const std::pair<int32_t, int32_t>& p) const {
            return fast_hash_pair(p.first, p.second);
        }
    };

    std::vector<std::unordered_map<std::pair<int32_t, int32_t>, double, PairHash>> local_maps(num_threads);
    for (int t = 0; t < num_threads; t++) {
        local_maps[t].reserve(200); // nations × years per thread
    }

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_agg = local_maps[tid];

        #pragma omp for schedule(static, 100000)
        for (size_t i = 0; i < li_rows; i++) {
            // Filter by green parts
            if (!green_parts.contains(l_partkey[i])) continue;

            // Lookup supplier nation
            int32_t suppkey = l_suppkey[i];
            if (suppkey >= (int32_t)supp_nation.size() || supp_nation[suppkey] == -1) continue;
            int32_t nationkey = supp_nation[suppkey];

            // Lookup partsupp cost
            int64_t* cost_ptr = partsupp_cost.find(l_partkey[i], suppkey);
            if (!cost_ptr) continue;
            int64_t supplycost = *cost_ptr;

            // Lookup order date
            int32_t* date_ptr = order_date.find(l_orderkey[i]);
            if (!date_ptr) continue;
            int32_t orderdate = *date_ptr;

            // Compute profit (scaled integer arithmetic)
            int64_t revenue = l_extendedprice[i] * (100 - l_discount[i]); // scale 10000
            int64_t cost = supplycost * l_quantity[i]; // scale 10000
            int64_t profit_scaled = revenue - cost; // scale 10000
            double profit = profit_scaled / 10000.0;

            // Extract year
            int year = extract_year(orderdate);

            // Aggregate by (nationkey, year)
            local_agg[{nationkey, year}] += profit;
        }
    }

    // Merge thread-local results
    std::unordered_map<std::pair<int32_t, int32_t>, double, PairHash> global_agg;
    global_agg.reserve(175);

    for (int t = 0; t < num_threads; t++) {
        for (const auto& kv : local_maps[t]) {
            global_agg[kv.first] += kv.second;
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
    results.reserve(global_agg.size());
    for (const auto& kv : global_agg) {
        results.push_back({nation_names[kv.first.first], kv.first.second, kv.second});
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
