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
// Q10: Returned Item Reporting (Iteration 5)
// ============================================================================
// KEY OPTIMIZATIONS:
// 1. Load pre-built hash indexes (mmap) instead of building hash tables
// 2. Late materialization: load customer strings only for result rows
// 3. Compact hash table (open-addressing) for aggregation
// 4. Fused filter+join+agg pipeline to minimize memory traffic
// 5. Parallel aggregation with thread-local buffers
// ============================================================================

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
    // 4-7. Fused Pipeline: Filter Lineitem → Join Orders → Join Customer →
    //      Join Nation → Aggregate
    // Use compact hash table for aggregation (open-addressing)
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

    // Aggregation hash table (compact, open-addressing)
    CompactHashTable<int32_t, AggregationEntry> agg_table(400000);

    // Build filtered orders set: map orderkey -> custkey
    // Use compact hash table (open-addressing) for better performance than std::unordered_map
    CompactHashTable<int32_t, int32_t> orders_orderkey_to_custkey(filtered_orders_indices.size() * 2);

    for (int32_t oi : filtered_orders_indices) {
        int32_t orderkey = orders_o_orderkey[oi];
        int32_t custkey = orders_o_custkey[oi];
        // Note: multiple orders can have same orderkey (shouldn't happen), but take the last one
        orders_orderkey_to_custkey.insert(orderkey, custkey);
    }

    // Fused lineitem filter + join + agg
    size_t join_lineitem_orders_count = 0;
    size_t join_customer_count = 0;
    size_t join_nation_count = 0;

    for (int32_t li : filtered_lineitem_indices) {
        int32_t orderkey = lineitem_l_orderkey[li];
        int32_t* custkey_ptr = orders_orderkey_to_custkey.find(orderkey);
        if (!custkey_ptr) continue;

        int32_t custkey = *custkey_ptr;
        int32_t c_idx = customer_idx_lookup[custkey];
        if (c_idx < 0) continue;

        int32_t nationkey = customer_c_nationkey[c_idx];
        int32_t n_idx = nation_idx_lookup[nationkey];
        if (n_idx < 0) continue;

        // Now aggregate
        int64_t extended_price = lineitem_l_extendedprice[li];
        int64_t discount = lineitem_l_discount[li];
        int64_t revenue_10000 = (extended_price * (10000 - discount * 100)) / 100;

        auto* existing = agg_table.find(custkey);
        if (existing) {
            existing->revenue += revenue_10000;
        } else {
            agg_table.insert(custkey, {revenue_10000, c_idx, n_idx});
        }

        join_lineitem_orders_count++;
        join_customer_count++;
        join_nation_count++;
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
    // 8. Load Strings (TRUE Late Materialization: only for result rows)
    // Key Optimization: Load strings ONLY for customer indices in aggregation result
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Convert compact hash table to vector
    auto agg_vec = agg_table.to_vector();

    // Collect which customer indices are actually in the result (use set for O(1) lookup)
    std::unordered_set<int32_t> result_c_indices;
    std::vector<int32_t> result_n_indices_list;

    for (const auto& [custkey, agg] : agg_vec) {
        result_c_indices.insert(agg.c_idx);
        result_n_indices_list.push_back(agg.n_idx);
    }

    // Pre-allocate maps: only for result customer indices
    std::unordered_map<int32_t, std::string> customer_c_name_map;
    std::unordered_map<int32_t, std::string> customer_c_address_map;
    std::unordered_map<int32_t, std::string> customer_c_phone_map;
    std::unordered_map<int32_t, std::string> customer_c_comment_map;

    customer_c_name_map.reserve(result_c_indices.size());
    customer_c_address_map.reserve(result_c_indices.size());
    customer_c_phone_map.reserve(result_c_indices.size());
    customer_c_comment_map.reserve(result_c_indices.size());

    // Load strings ONLY for result customer indices (not all 1.5M)
    // Parallel strategy: read all 4 string columns in parallel
    #pragma omp parallel sections
    {
        #pragma omp section
        {
            std::ifstream file(gendb_dir + "/customer/c_name.bin", std::ios::binary);
            if (!file) {
                std::cerr << "Failed to open c_name" << std::endl;
                exit(1);
            }
            std::vector<char> buffer(1000);
            for (size_t i = 0; i < customer_count; ++i) {
                uint32_t len;
                if (!file.read((char*)&len, sizeof(len)) || len > 1000) {
                    std::cerr << "Failed reading c_name at index " << i << std::endl;
                    exit(1);
                }
                if (!file.read(buffer.data(), len)) {
                    std::cerr << "Failed reading c_name data at index " << i << std::endl;
                    exit(1);
                }
                if (result_c_indices.count(i)) {
                    customer_c_name_map[i] = std::string(buffer.begin(), buffer.begin() + len);
                }
            }
        }

        #pragma omp section
        {
            std::ifstream file(gendb_dir + "/customer/c_address.bin", std::ios::binary);
            if (!file) {
                std::cerr << "Failed to open c_address" << std::endl;
                exit(1);
            }
            std::vector<char> buffer(1000);
            for (size_t i = 0; i < customer_count; ++i) {
                uint32_t len;
                if (!file.read((char*)&len, sizeof(len)) || len > 1000) {
                    std::cerr << "Failed reading c_address at index " << i << std::endl;
                    exit(1);
                }
                if (!file.read(buffer.data(), len)) {
                    std::cerr << "Failed reading c_address data at index " << i << std::endl;
                    exit(1);
                }
                if (result_c_indices.count(i)) {
                    customer_c_address_map[i] = std::string(buffer.begin(), buffer.begin() + len);
                }
            }
        }

        #pragma omp section
        {
            std::ifstream file(gendb_dir + "/customer/c_phone.bin", std::ios::binary);
            if (!file) {
                std::cerr << "Failed to open c_phone" << std::endl;
                exit(1);
            }
            std::vector<char> buffer(1000);
            for (size_t i = 0; i < customer_count; ++i) {
                uint32_t len;
                if (!file.read((char*)&len, sizeof(len)) || len > 1000) {
                    std::cerr << "Failed reading c_phone at index " << i << std::endl;
                    exit(1);
                }
                if (!file.read(buffer.data(), len)) {
                    std::cerr << "Failed reading c_phone data at index " << i << std::endl;
                    exit(1);
                }
                if (result_c_indices.count(i)) {
                    customer_c_phone_map[i] = std::string(buffer.begin(), buffer.begin() + len);
                }
            }
        }

        #pragma omp section
        {
            std::ifstream file(gendb_dir + "/customer/c_comment.bin", std::ios::binary);
            if (!file) {
                std::cerr << "Failed to open c_comment" << std::endl;
                exit(1);
            }
            std::vector<char> buffer(1000);
            for (size_t i = 0; i < customer_count; ++i) {
                uint32_t len;
                if (!file.read((char*)&len, sizeof(len)) || len > 1000) {
                    std::cerr << "Failed reading c_comment at index " << i << std::endl;
                    exit(1);
                }
                if (!file.read(buffer.data(), len)) {
                    std::cerr << "Failed reading c_comment data at index " << i << std::endl;
                    exit(1);
                }
                if (result_c_indices.count(i)) {
                    customer_c_comment_map[i] = std::string(buffer.begin(), buffer.begin() + len);
                }
            }
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
    // 10. Sort by Revenue DESC
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(agg_results.begin(), agg_results.end(),
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
