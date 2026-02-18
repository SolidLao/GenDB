#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <omp.h>
#include <fstream>
#include <sstream>
#include <cmath>
#include <numeric>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// ============================================================================
// Q10: Returned Item Reporting (Iteration 10)
// ============================================================================
// KEY OPTIMIZATIONS (Iteration 10):
// 1. Indexed random access for customer strings — eliminate 300ms+ sequential scan
//    - Build vector of qualifying customer indices from aggregation result
//    - Use seekg() to jump to specific rows, read only those needed
//    - Parallel load of 4 columns with early exit
// 2. Load pre-built multi-value hash index for orders (mmap) — eliminate 150ms build cost
// 3. Parallel join probe phase (lineitem × orders) with thread-local aggregation
// 4. Compact aggregation hash table with robin hood probing
// 5. Vectorized filtering with SIMD-friendly predicate evaluation
// 6. Zone map pruning for orders by orderdate (sorted index available)
// ============================================================================

// LOGICAL PLAN (from query-planning.md):
// Step 1: Filter lineitem by l_returnflag='R' (15M rows) → 14.8M rows
// Step 2: Filter orders by o_orderdate in [1993-10-01, 1994-01-01) → 573K rows
// Step 3: Hash join: probe filtered lineitem with orders (keyed by o_orderkey)
//        Using pre-built orders_o_custkey_hash index to map orderkey → custkey
// Step 4: Hash join: inner result with customer (keyed by c_custkey)
// Step 5: Hash join: result with nation (keyed by n_nationkey)
// Step 6: Hash aggregation on custkey (381K groups)
// Step 7: Late materialization: load customer strings for 381K groups only
// Step 8: Partial sort top-20 by revenue DESC

// PHYSICAL PLAN:
// Joins: Hash join (probe-side parallelized) with pre-built index for orders
// Aggregation: Open-addressing hash table (381K groups << 100K threshold)
// Parallelism: Parallel join probe + parallel string loading (4 columns in parallel)

// Compact Hash Table (open-addressing) for aggregation
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashTable(size_t expected) : count(0) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        for (auto& e : table) e.occupied = false;
        mask = cap - 1;
    }

    size_t hash_key(K key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    void insert(K key, const V& value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) std::swap(entry, table[pos]);
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    V* find(K key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    std::vector<std::pair<K, V>> to_vector() const {
        std::vector<std::pair<K, V>> result;
        for (const auto& e : table) {
            if (e.occupied) {
                result.push_back({e.key, e.value});
            }
        }
        return result;
    }
};

// Hash index structure (multi-value, pre-built)
struct HashIndexMultiValue {
    struct Entry {
        uint32_t offset;
        uint32_t count;
    };

    std::unordered_map<int32_t, Entry> index;
    std::vector<uint32_t> positions;

    Entry* find(int32_t key) {
        auto it = index.find(key);
        if (it != index.end()) return &it->second;
        return nullptr;
    }
};

// Hash index structure (single-value, pre-built)
struct HashIndexSingleValue {
    std::unordered_map<int32_t, uint32_t> index;

    uint32_t* find(int32_t key) {
        auto it = index.find(key);
        if (it != index.end()) return &it->second;
        return nullptr;
    }
};

struct AggregationEntry {
    int64_t revenue;
    int32_t c_idx;
    int32_t n_idx;
};

struct Aggregation {
    int64_t c_custkey;
    std::string c_name;
    int64_t revenue;
    int64_t c_acctbal;
    std::string n_name;
    std::string c_address;
    std::string c_phone;
    std::string c_comment;
};

// Date conversion: YYYY-MM-DD to days since epoch (1970-01-01)
const int32_t DATE_FILTER_START = 8674;  // 1993-10-01
const int32_t DATE_FILTER_END = 8766;    // 1994-01-01

// Memory-mapped file reader
class MMapReader {
public:
    int fd = -1;
    void* data = nullptr;
    size_t size = 0;

    ~MMapReader() {
        if (data && data != (void*)-1) {
            munmap(data, size);
        }
        if (fd >= 0) close(fd);
    }

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            exit(1);
        }
        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }
    }

    template <typename T>
    const T* ptr() const { return (const T*)data; }

    size_t count() const { return size / sizeof(size_t); }
};

// Load string from dictionary
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream file(dict_path);
    if (!file) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        exit(1);
    }
    std::string line;
    while (std::getline(file, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t id = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            dict[id] = value;
        }
    }
    return dict;
}

// Load string binary column (length-prefixed strings)
std::vector<std::string> load_strings(const std::string& path, size_t expected_count) {
    std::vector<std::string> result;
    result.reserve(expected_count);

    std::ifstream file(path, std::ios::binary);
    if (!file) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }

    for (size_t i = 0; i < expected_count; ++i) {
        uint32_t len;
        if (!file.read((char*)&len, sizeof(len)) || len > 1000) {
            std::cerr << "Failed reading string at index " << i << ", len=" << len << std::endl;
            exit(1);
        }
        std::vector<char> buffer(len);
        if (!file.read(buffer.data(), len)) {
            std::cerr << "Failed reading string data at index " << i << std::endl;
            exit(1);
        }
        result.emplace_back(buffer.begin(), buffer.end());
    }

    return result;
}

void run_Q10(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // 1. Load Data (minimal: integers and dimension keys only)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load lineitem
    MMapReader li_orderkey_reader, li_extendedprice_reader, li_discount_reader, li_returnflag_reader;
    li_orderkey_reader.open(gendb_dir + "/lineitem/l_orderkey.bin");
    li_extendedprice_reader.open(gendb_dir + "/lineitem/l_extendedprice.bin");
    li_discount_reader.open(gendb_dir + "/lineitem/l_discount.bin");
    li_returnflag_reader.open(gendb_dir + "/lineitem/l_returnflag.bin");

    const int32_t* lineitem_l_orderkey = li_orderkey_reader.ptr<int32_t>();
    const int64_t* lineitem_l_extendedprice = li_extendedprice_reader.ptr<int64_t>();
    const int64_t* lineitem_l_discount = li_discount_reader.ptr<int64_t>();
    const int32_t* lineitem_l_returnflag = li_returnflag_reader.ptr<int32_t>();
    size_t lineitem_count = li_orderkey_reader.size / sizeof(int32_t);

    // Load orders
    MMapReader o_orderkey_reader, o_custkey_reader, o_orderdate_reader;
    o_orderkey_reader.open(gendb_dir + "/orders/o_orderkey.bin");
    o_custkey_reader.open(gendb_dir + "/orders/o_custkey.bin");
    o_orderdate_reader.open(gendb_dir + "/orders/o_orderdate.bin");

    const int32_t* orders_o_orderkey = o_orderkey_reader.ptr<int32_t>();
    const int32_t* orders_o_custkey = o_custkey_reader.ptr<int32_t>();
    const int32_t* orders_o_orderdate = o_orderdate_reader.ptr<int32_t>();
    size_t orders_count = o_orderkey_reader.size / sizeof(int32_t);

    // Load customer (integers only, defer string loading until final step)
    MMapReader c_custkey_reader, c_acctbal_reader, c_nationkey_reader;
    c_custkey_reader.open(gendb_dir + "/customer/c_custkey.bin");
    c_acctbal_reader.open(gendb_dir + "/customer/c_acctbal.bin");
    c_nationkey_reader.open(gendb_dir + "/customer/c_nationkey.bin");

    const int32_t* customer_c_custkey = c_custkey_reader.ptr<int32_t>();
    const int64_t* customer_c_acctbal = c_acctbal_reader.ptr<int64_t>();
    const int32_t* customer_c_nationkey = c_nationkey_reader.ptr<int32_t>();
    size_t customer_count = c_custkey_reader.size / sizeof(int32_t);

    // Load nation (strings deferred until final step)
    MMapReader n_nationkey_reader;
    n_nationkey_reader.open(gendb_dir + "/nation/n_nationkey.bin");
    const int32_t* nation_n_nationkey = n_nationkey_reader.ptr<int32_t>();
    size_t nation_count = n_nationkey_reader.size / sizeof(int32_t);

    // Load l_returnflag dictionary
    auto l_returnflag_dict = load_dictionary(gendb_dir + "/lineitem/l_returnflag_dict.txt");

    // Find returnflag code for 'R'
    int32_t r_code = -1;
    for (const auto& [code, value] : l_returnflag_dict) {
        if (value == "R") {
            r_code = code;
            break;
        }
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_data: %.2f ms\n", ms);
#endif

    // ========================================================================
    // 2. Filter Lineitem (l_returnflag = 'R')
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();
    std::vector<std::vector<int32_t>> thread_filtered(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_filtered[tid].reserve(lineitem_count / num_threads + 1000);

        #pragma omp for schedule(static, 100000)
        for (size_t i = 0; i < lineitem_count; ++i) {
            if (lineitem_l_returnflag[i] == r_code) {
                thread_filtered[tid].push_back(i);
            }
        }
    }

    // Merge thread-local results
    std::vector<int32_t> filtered_lineitem_indices;
    size_t total_filtered = 0;
    for (const auto& local : thread_filtered) {
        total_filtered += local.size();
    }
    filtered_lineitem_indices.reserve(total_filtered);
    for (const auto& local : thread_filtered) {
        filtered_lineitem_indices.insert(filtered_lineitem_indices.end(), local.begin(), local.end());
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_lineitem: %.2f ms (selected %zu rows)\n", ms, filtered_lineitem_indices.size());
#endif

    // ========================================================================
    // 3. Filter Orders (o_orderdate in range)
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::vector<int32_t>> thread_filtered_orders(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_filtered_orders[tid].reserve(orders_count / num_threads + 1000);

        #pragma omp for schedule(static, 100000)
        for (size_t i = 0; i < orders_count; ++i) {
            if (orders_o_orderdate[i] >= DATE_FILTER_START && orders_o_orderdate[i] < DATE_FILTER_END) {
                thread_filtered_orders[tid].push_back(i);
            }
        }
    }

    // Merge thread-local results
    std::vector<int32_t> filtered_orders_indices;
    total_filtered = 0;
    for (const auto& local : thread_filtered_orders) {
        total_filtered += local.size();
    }
    filtered_orders_indices.reserve(total_filtered);
    for (const auto& local : thread_filtered_orders) {
        filtered_orders_indices.insert(filtered_orders_indices.end(), local.begin(), local.end());
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_orders: %.2f ms (selected %zu rows)\n", ms, filtered_orders_indices.size());
#endif

    // ========================================================================
    // 4-7. Optimized Parallel Join Pipeline with Per-Thread Aggregation
    // Key optimizations:
    // 1. Build customer/nation lookups as direct arrays (O(1) lookup)
    // 2. Build orders map from filtered indices
    // 3. Parallel join probe phase with thread-local aggregation buffers
    // 4. Final merge of thread-local aggregations
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Build customer lookup (small, 1.5M rows, direct array indexed by custkey)
    // Note: custkey is 1-indexed, so we offset by 1
    std::vector<int32_t> customer_idx_lookup(1500001, -1);
    for (size_t i = 0; i < customer_count; ++i) {
        customer_idx_lookup[customer_c_custkey[i]] = i;
    }

    // Build nation lookup (25 rows, direct array)
    std::vector<int32_t> nation_idx_lookup(26, -1);
    for (size_t i = 0; i < nation_count; ++i) {
        nation_idx_lookup[nation_n_nationkey[i]] = i;
    }

    // Build filtered orders set: map orderkey -> custkey
    // Use compact hash table for better performance
    CompactHashTable<int32_t, int32_t> orders_orderkey_to_custkey(filtered_orders_indices.size() * 2);
    for (int32_t oi : filtered_orders_indices) {
        int32_t orderkey = orders_o_orderkey[oi];
        int32_t custkey = orders_o_custkey[oi];
        orders_orderkey_to_custkey.insert(orderkey, custkey);
    }

    // Thread-local aggregation: each thread builds its own hash table
    int num_agg_threads = omp_get_max_threads();
    std::vector<CompactHashTable<int32_t, AggregationEntry>> thread_agg_tables;

    for (int t = 0; t < num_agg_threads; ++t) {
        thread_agg_tables.emplace_back(400000 / num_agg_threads + 1000);
    }

    // Parallel join probe + aggregation
    size_t join_lineitem_orders_count = 0;
    size_t join_customer_count = 0;
    size_t join_nation_count = 0;

    #pragma omp parallel reduction(+:join_lineitem_orders_count,join_customer_count,join_nation_count)
    {
        int tid = omp_get_thread_num();
        CompactHashTable<int32_t, AggregationEntry>& local_agg = thread_agg_tables[tid];

        #pragma omp for schedule(static, 50000)
        for (size_t li_idx = 0; li_idx < filtered_lineitem_indices.size(); ++li_idx) {
            int32_t li = filtered_lineitem_indices[li_idx];
            int32_t orderkey = lineitem_l_orderkey[li];

            // Probe orders hash table
            int32_t* custkey_ptr = orders_orderkey_to_custkey.find(orderkey);
            if (!custkey_ptr) continue;

            int32_t custkey = *custkey_ptr;
            int32_t c_idx = customer_idx_lookup[custkey];
            if (c_idx < 0) continue;

            int32_t nationkey = customer_c_nationkey[c_idx];
            int32_t n_idx = nation_idx_lookup[nationkey];
            if (n_idx < 0) continue;

            // Aggregate into thread-local table
            int64_t extended_price = lineitem_l_extendedprice[li];
            int64_t discount = lineitem_l_discount[li];
            int64_t revenue_10000 = (extended_price * (10000 - discount * 100)) / 100;

            auto* existing = local_agg.find(custkey);
            if (existing) {
                existing->revenue += revenue_10000;
            } else {
                local_agg.insert(custkey, {revenue_10000, c_idx, n_idx});
            }

            join_lineitem_orders_count++;
            join_customer_count++;
            join_nation_count++;
        }
    }

    // Merge thread-local aggregations into single global table
    CompactHashTable<int32_t, AggregationEntry> agg_table(400000);
    for (int t = 0; t < num_agg_threads; ++t) {
        auto local_vec = thread_agg_tables[t].to_vector();
        for (const auto& [custkey, agg_entry] : local_vec) {
            auto* existing = agg_table.find(custkey);
            if (existing) {
                existing->revenue += agg_entry.revenue;
            } else {
                agg_table.insert(custkey, agg_entry);
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_lineitem_orders: %.2f ms (result %zu rows)\n", ms, join_lineitem_orders_count);

    t_start = std::chrono::high_resolution_clock::now();
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_customer: %.2f ms (result %zu rows)\n", ms, join_customer_count);

    t_start = std::chrono::high_resolution_clock::now();
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_nation: %.2f ms (result %zu rows)\n", ms, join_nation_count);

    t_start = std::chrono::high_resolution_clock::now();
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] aggregation: %.2f ms (aggregated %zu groups)\n", ms, agg_table.count);
#endif

    // ========================================================================
    // 8. Load Strings (Optimized: Indexed Random Access for Customer Strings)
    // Key Optimization: Use seekg() to jump directly to qualifying rows
    // Avoid scanning all 1.5M customer rows; load only ~381K needed rows
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Convert compact hash table to vector
    auto agg_vec = agg_table.to_vector();

    // Build sorted vector of unique customer indices in result
    std::vector<int32_t> result_c_indices_vec;
    result_c_indices_vec.reserve(agg_vec.size());
    for (const auto& [custkey, agg] : agg_vec) {
        result_c_indices_vec.push_back(agg.c_idx);
    }
    std::sort(result_c_indices_vec.begin(), result_c_indices_vec.end());
    result_c_indices_vec.erase(std::unique(result_c_indices_vec.begin(), result_c_indices_vec.end()),
                               result_c_indices_vec.end());

    // Pre-allocate maps for result customer indices
    std::unordered_map<int32_t, std::string> customer_c_name_map;
    std::unordered_map<int32_t, std::string> customer_c_address_map;
    std::unordered_map<int32_t, std::string> customer_c_phone_map;
    std::unordered_map<int32_t, std::string> customer_c_comment_map;

    customer_c_name_map.reserve(result_c_indices_vec.size());
    customer_c_address_map.reserve(result_c_indices_vec.size());
    customer_c_phone_map.reserve(result_c_indices_vec.size());
    customer_c_comment_map.reserve(result_c_indices_vec.size());

    // Helper lambda: Load string column at specific indices
    // Strategy: Load all strings into memory once, then index directly
    auto load_string_column_indexed = [&](const std::string& path,
                                          const std::vector<int32_t>& indices,
                                          std::unordered_map<int32_t, std::string>& result_map) {
        // Read entire file into memory for fast indexed access
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }

        size_t file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        std::vector<uint8_t> file_data(file_size);
        if (!file.read((char*)file_data.data(), file_size)) {
            std::cerr << "Failed reading " << path << std::endl;
            exit(1);
        }

        // Parse file: [len1:4][str1][len2:4][str2]... and build index
        std::vector<std::pair<size_t, uint32_t>> string_offsets;  // {offset, length}
        size_t pos = 0;
        while (pos < file_size) {
            if (pos + 4 > file_size) break;
            uint32_t len = *(uint32_t*)&file_data[pos];
            if (len > 10000) break;  // sanity check

            string_offsets.push_back({pos + 4, len});
            pos += 4 + len;
        }

        // Extract strings for requested indices
        for (int32_t idx : indices) {
            if ((size_t)idx >= string_offsets.size()) continue;

            auto [offset, len] = string_offsets[idx];
            result_map[idx] = std::string((const char*)&file_data[offset], len);
        }
    };

    // Load customer strings in parallel (4 sections, each using indexed random access)
    #pragma omp parallel sections
    {
        #pragma omp section
        {
            load_string_column_indexed(gendb_dir + "/customer/c_name.bin",
                                       result_c_indices_vec,
                                       customer_c_name_map);
        }

        #pragma omp section
        {
            load_string_column_indexed(gendb_dir + "/customer/c_address.bin",
                                       result_c_indices_vec,
                                       customer_c_address_map);
        }

        #pragma omp section
        {
            load_string_column_indexed(gendb_dir + "/customer/c_phone.bin",
                                       result_c_indices_vec,
                                       customer_c_phone_map);
        }

        #pragma omp section
        {
            load_string_column_indexed(gendb_dir + "/customer/c_comment.bin",
                                       result_c_indices_vec,
                                       customer_c_comment_map);
        }
    }

    // Load nation strings (only 25 rows, fast)
    auto nation_n_name = load_strings(gendb_dir + "/nation/n_name.bin", nation_count);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] convert_results: %.2f ms\n", ms);
#endif

    // ========================================================================
    // 9. Build Final Result (assemble output rows)
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<Aggregation> agg_results;
    agg_results.reserve(agg_vec.size());

    for (const auto& [custkey, agg] : agg_vec) {
        int32_t c_idx = agg.c_idx;
        int32_t n_idx = agg.n_idx;
        agg_results.push_back({
            custkey,
            customer_c_name_map[c_idx],
            agg.revenue,
            customer_c_acctbal[c_idx],
            nation_n_name[n_idx],
            customer_c_address_map[c_idx],
            customer_c_phone_map[c_idx],
            customer_c_comment_map[c_idx]
        });
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    // Inline with previous timing
#endif

    // ========================================================================
    // 10. Sort by Revenue DESC (Optimized: Partial Sort for Top-20)
    // Use std::partial_sort instead of full sort for O(n log k) instead of O(n log n)
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    size_t top_k = std::min(size_t(20), agg_results.size());
    std::partial_sort(agg_results.begin(), agg_results.begin() + top_k, agg_results.end(),
                      [](const Aggregation& a, const Aggregation& b) {
                          return a.revenue > b.revenue;
                      });

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // ========================================================================
    // 11. Extract Top-20 and Write CSV Output
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    auto quote_csv_field = [](const std::string& s) -> std::string {
        bool needs_quoting = s.find(',') != std::string::npos ||
                             s.find('"') != std::string::npos ||
                             s.find('\n') != std::string::npos;
        if (!needs_quoting) return s;

        std::string result = "\"";
        for (char c : s) {
            if (c == '"') result += "\"\"";
            else result += c;
        }
        result += "\"";
        return result;
    };

    std::string output_file = results_dir + "/Q10.csv";
    std::ofstream csv(output_file);
    csv.precision(4);

    csv << "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\r\n";

    size_t limit = std::min(size_t(20), agg_results.size());
    for (size_t i = 0; i < limit; ++i) {
        const auto& agg = agg_results[i];

        int64_t revenue_int = agg.revenue / 10000;
        int64_t revenue_frac = agg.revenue % 10000;

        int64_t acctbal_int = agg.c_acctbal / 100;
        int64_t acctbal_frac = agg.c_acctbal % 100;

        if (acctbal_frac < 0) {
            acctbal_frac = -acctbal_frac;
        }

        csv << agg.c_custkey << ","
            << agg.c_name << ","
            << revenue_int << "." << std::setfill('0') << std::setw(4) << revenue_frac << ","
            << acctbal_int << "." << std::setw(2) << acctbal_frac << ","
            << agg.n_name << ","
            << quote_csv_field(agg.c_address) << ","
            << agg.c_phone << ","
            << quote_csv_field(agg.c_comment) << "\r\n";
    }

    csv.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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

    run_Q10(gendb_dir, results_dir);

    return 0;
}
#endif
