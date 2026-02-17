/*
 * Q9: Product Type Profit Measure - Iteration 10
 *
 * BOTTLENECK FROM ITERATION 6: data_load 643ms (89%)
 * - Single-threaded partsupp hash table build: 8M rows (DOMINANT COST)
 * - Sequential part filtering: 2M rows
 *
 * ARCHITECTURE-LEVEL FIX:
 * 1. Load pre-built partsupp_partkey_suppkey_hash.bin index via mmap (ZERO BUILD TIME)
 * 2. Parallelize part name filtering with thread-local green_parts sets
 * 3. Use partitioned hash aggregation to avoid merge bottleneck
 *
 * PHYSICAL PLAN:
 * - Load partsupp index (mmap, pre-built): ~0ms
 * - Parallel part filter: 2M rows, 64 threads → ~10ms
 * - Parallel lineitem scan with partitioned aggregation: 60M rows → ~60ms
 * - Final merge and sort: ~5ms
 *
 * Expected: data_load <50ms, join_aggregate <100ms, total <200ms
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

// Fast hash for pair (composite key)
inline uint64_t fast_hash_pair(int32_t k1, int32_t k2) {
    uint64_t h1 = (uint64_t)k1 * 0x9E3779B97F4A7C15ULL;
    uint64_t h2 = (uint64_t)k2 * 0x9E3779B185EBCA87ULL;
    return h1 ^ (h2 >> 32);
}

// Pre-built index entry layout (from Storage Guide)
struct PartSuppIndexEntry {
    int32_t ps_partkey;
    int32_t ps_suppkey;
    uint32_t offset;
    uint32_t count;
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

    // Parallel part filtering on p_name LIKE '%green%'
    size_t part_rows;
    int32_t* p_partkey = mmap_binary<int32_t>(gendb_dir + "/part/p_partkey.bin", part_rows);

    // Thread-local green part sets
    std::vector<std::vector<int32_t>> thread_green_parts(num_threads);

    // Pre-load entire p_name file into memory for parallel access
    std::ifstream p_name_file(gendb_dir + "/part/p_name.bin", std::ios::binary);
    p_name_file.seekg(0, std::ios::end);
    size_t file_size = p_name_file.tellg();
    p_name_file.seekg(0, std::ios::beg);
    std::vector<char> p_name_data(file_size);
    p_name_file.read(p_name_data.data(), file_size);
    p_name_file.close();

    // Compute byte offsets for each part name
    std::vector<size_t> name_offsets(part_rows);
    size_t offset = 0;
    for (size_t i = 0; i < part_rows; i++) {
        name_offsets[i] = offset;
        uint32_t len = *reinterpret_cast<uint32_t*>(&p_name_data[offset]);
        offset += sizeof(uint32_t) + len;
    }

    // Parallel filter
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_green_parts[tid].reserve(part_rows / num_threads / 10 + 1000);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < part_rows; i++) {
            size_t off = name_offsets[i];
            uint32_t len = *reinterpret_cast<uint32_t*>(&p_name_data[off]);
            const char* name_ptr = &p_name_data[off + sizeof(uint32_t)];

            // Fast substring search using memmem
            if (memmem(name_ptr, len, "green", 5) != nullptr) {
                thread_green_parts[tid].push_back(p_partkey[i]);
            }
        }
    }

    // Merge thread-local results into a single hash set
    std::unordered_set<int32_t> green_parts;
    green_parts.reserve(200000);
    for (int t = 0; t < num_threads; t++) {
        for (int32_t pk : thread_green_parts[t]) {
            green_parts.insert(pk);
        }
    }

    // Load supplier s_suppkey and s_nationkey into flat arrays
    size_t supp_rows;
    int32_t* s_suppkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supp_rows);
    int32_t* s_nationkey = mmap_binary<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supp_rows);

    // Build flat array for supplier nationkey lookup (s_suppkey is dense 1..100000)
    std::vector<int32_t> supp_nation(100001, -1);
    for (size_t i = 0; i < supp_rows; i++) {
        supp_nation[s_suppkey[i]] = s_nationkey[i];
    }

    // Load pre-built partsupp index (mmap, zero build time)
    size_t ps_index_entries;
    PartSuppIndexEntry* ps_index = mmap_binary<PartSuppIndexEntry>(gendb_dir + "/indexes/partsupp_partkey_suppkey_hash.bin", ps_index_entries);

    // Skip header: [num_entries, table_size] then index entries
    uint32_t ps_table_size = *reinterpret_cast<uint32_t*>(reinterpret_cast<char*>(ps_index) + sizeof(uint32_t));
    PartSuppIndexEntry* ps_hash_table = reinterpret_cast<PartSuppIndexEntry*>(reinterpret_cast<char*>(ps_index) + 2 * sizeof(uint32_t));
    size_t ps_mask = ps_table_size - 1;

    // Load ps_supplycost column for lookups
    size_t ps_rows;
    int64_t* ps_supplycost = mmap_binary<int64_t>(gendb_dir + "/partsupp/ps_supplycost.bin", ps_rows);

    // Load orders columns
    size_t ord_rows;
    int32_t* o_orderkey = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderkey.bin", ord_rows);
    int32_t* o_orderdate = mmap_binary<int32_t>(gendb_dir + "/orders/o_orderdate.bin", ord_rows);

    // Orders: use flat array (o_orderkey is dense)
    // Find max orderkey in parallel
    int32_t max_orderkey = 0;
    #pragma omp parallel for reduction(max:max_orderkey)
    for (size_t i = 0; i < ord_rows; i++) {
        if (o_orderkey[i] > max_orderkey) max_orderkey = o_orderkey[i];
    }

    std::vector<int32_t> order_date_array(max_orderkey + 1, -1);

    // Parallel build order date array
    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < ord_rows; i++) {
        order_date_array[o_orderkey[i]] = o_orderdate[i];
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
        local_maps[t].reserve(200);
    }

    // Parallel scan lineitem with hash lookups
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_agg = local_maps[tid];

        #pragma omp for schedule(static, 100000)
        for (size_t i = 0; i < li_rows; i++) {
            // Filter by green parts
            int32_t partkey = l_partkey[i];
            if (green_parts.find(partkey) == green_parts.end()) continue;

            // Lookup supplier nation
            int32_t suppkey = l_suppkey[i];
            if (suppkey >= (int32_t)supp_nation.size() || supp_nation[suppkey] == -1) continue;
            int32_t nationkey = supp_nation[suppkey];

            // Lookup partsupp cost from pre-built index
            uint64_t hash = fast_hash_pair(partkey, suppkey);
            size_t idx = hash & ps_mask;
            bool found = false;
            int64_t supplycost = 0;
            while (ps_hash_table[idx].ps_partkey != 0 || ps_hash_table[idx].ps_suppkey != 0) {
                if (ps_hash_table[idx].ps_partkey == partkey && ps_hash_table[idx].ps_suppkey == suppkey) {
                    // Index stores offset directly (single-value hash index)
                    supplycost = ps_supplycost[ps_hash_table[idx].offset];
                    found = true;
                    break;
                }
                idx = (idx + 1) & ps_mask;
            }
            if (!found) continue;

            // Lookup order date from flat array
            int32_t orderkey = l_orderkey[i];
            if (orderkey >= (int32_t)order_date_array.size() || order_date_array[orderkey] == -1) continue;
            int32_t orderdate = order_date_array[orderkey];

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
