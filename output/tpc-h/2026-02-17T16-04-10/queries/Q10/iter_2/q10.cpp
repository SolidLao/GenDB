#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <unordered_map>
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
// Q10: Returned Item Reporting (Iteration 2)
// ============================================================================
// LOGICAL PLAN:
// 1. Scan & Filter lineitem: l_returnflag = 'R' (~3% selectivity → ~1.8M rows)
// 2. Scan & Filter orders: o_orderdate in [1993-10-01, 1994-01-01) (~3% → ~450K)
// 3. Hash Join: lineitem ⋈ orders on l_orderkey = o_orderkey (build on orders, probe on lineitem)
// 4. Hash Join: result ⋈ customer on o_custkey = c_custkey (build on customer, probe with join result)
// 5. Hash Join: result ⋈ nation on c_nationkey = n_nationkey (broadcast, small table)
// 6. Parallel Aggregation: GROUP BY (c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment)
//    with SUM(l_extendedprice * (1 - l_discount))
//    Strategy: Partition aggregation by hash(c_custkey) % num_threads to avoid merging bottleneck
// 7. Order By: revenue DESC
// 8. Limit: 20
//
// PHYSICAL PLAN:
// - Parallel lineitem scan with thread-local buffering (avoids critical section)
// - Parallel orders scan with thread-local buffering
// - Single-threaded hash join (join result is ~1.1M rows, fits in memory)
// - Parallel partitioned aggregation: each thread builds local hash table for its partition
// - Single-threaded final merge and sort
// - Extract top 20 rows
//
// PERFORMANCE TARGETS:
// - Join: 400-500ms (down from 718ms, via partitioning)
// - Aggregation: 400-600ms (down from 1117ms, via thread-local partitioning)
// - Total: 2000-2300ms (down from 3340ms)
// ============================================================================

struct Aggregation {
    int64_t c_custkey;
    std::string c_name;
    int64_t revenue;  // sum of (l_extendedprice * (1 - l_discount))
    int64_t c_acctbal;
    std::string n_name;
    std::string c_address;
    std::string c_phone;
    std::string c_comment;
};

// Simple grouping key: only c_custkey for partitioned aggregation
// This allows us to partition by c_custkey % num_threads without expensive multi-column hashing
struct SimpleGroupKey {
    int64_t c_custkey;

    bool operator==(const SimpleGroupKey& other) const {
        return c_custkey == other.c_custkey;
    }
};

struct SimpleGroupKeyHash {
    size_t operator()(const SimpleGroupKey& key) const {
        return std::hash<int64_t>()(key.c_custkey);
    }
};

// Full group key for final results (when we know which customer groups we have)
struct GroupKey {
    int64_t c_custkey;
    std::string c_name;
    int64_t c_acctbal;
    std::string c_phone;
    std::string n_name;
    std::string c_address;
    std::string c_comment;

    bool operator==(const GroupKey& other) const {
        return c_custkey == other.c_custkey &&
               c_name == other.c_name &&
               c_acctbal == other.c_acctbal &&
               c_phone == other.c_phone &&
               n_name == other.n_name &&
               c_address == other.c_address &&
               c_comment == other.c_comment;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& key) const {
        size_t h = std::hash<int64_t>()(key.c_custkey);
        h ^= std::hash<std::string>()(key.c_name) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int64_t>()(key.c_acctbal) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<std::string>()(key.c_phone) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<std::string>()(key.n_name) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<std::string>()(key.c_address) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<std::string>()(key.c_comment) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// Date conversion: YYYY-MM-DD to days since epoch (1970-01-01)
// Q10 filters: o_orderdate >= 1993-10-01 and < 1994-01-01
// Using Python: (datetime(1993, 10, 1) - datetime(1970, 1, 1)).days = 8674
// Using Python: (datetime(1994, 1, 1) - datetime(1970, 1, 1)).days = 8766
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

// Load string from dictionary (format: id=value per line)
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
    // 1. Load Data
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

    // Load customer
    MMapReader c_custkey_reader, c_acctbal_reader, c_nationkey_reader;
    c_custkey_reader.open(gendb_dir + "/customer/c_custkey.bin");
    c_acctbal_reader.open(gendb_dir + "/customer/c_acctbal.bin");
    c_nationkey_reader.open(gendb_dir + "/customer/c_nationkey.bin");

    const int32_t* customer_c_custkey = c_custkey_reader.ptr<int32_t>();
    const int64_t* customer_c_acctbal = c_acctbal_reader.ptr<int64_t>();
    const int32_t* customer_c_nationkey = c_nationkey_reader.ptr<int32_t>();
    size_t customer_count = c_custkey_reader.size / sizeof(int32_t);

    // Load customer strings
    auto customer_c_name = load_strings(gendb_dir + "/customer/c_name.bin", customer_count);
    auto customer_c_address = load_strings(gendb_dir + "/customer/c_address.bin", customer_count);
    auto customer_c_phone = load_strings(gendb_dir + "/customer/c_phone.bin", customer_count);
    auto customer_c_comment = load_strings(gendb_dir + "/customer/c_comment.bin", customer_count);

    // Load nation
    MMapReader n_nationkey_reader;
    n_nationkey_reader.open(gendb_dir + "/nation/n_nationkey.bin");
    const int32_t* nation_n_nationkey = n_nationkey_reader.ptr<int32_t>();
    size_t nation_count = n_nationkey_reader.size / sizeof(int32_t);
    auto nation_n_name = load_strings(gendb_dir + "/nation/n_name.bin", nation_count);

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

    // Use thread-local buffering to avoid critical section bottleneck
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

    // Use thread-local buffering to avoid critical section bottleneck
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
    // 4. Hash Join: Lineitem ⋈ Orders (l_orderkey = o_orderkey)
    //    Build on filtered orders (smaller side), probe with lineitem
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Build phase: hash table on filtered orders
    // Structure: orderkey → {count, offset_into_positions_array}
    // This enables efficient multi-match (1:N) join
    struct OrderEntry {
        uint32_t offset;
        uint32_t count;
    };

    std::unordered_map<int32_t, OrderEntry> orders_hash;
    orders_hash.reserve(filtered_orders_indices.size() * 2);

    // Count occurrences per orderkey
    for (int32_t oi : filtered_orders_indices) {
        int32_t orderkey = orders_o_orderkey[oi];
        orders_hash[orderkey].count++;
    }

    // Compute prefix sums for offsets
    uint32_t total_positions = 0;
    std::vector<uint32_t> positions_array;
    positions_array.reserve(filtered_orders_indices.size());

    for (auto& [orderkey, entry] : orders_hash) {
        entry.offset = total_positions;
        total_positions += entry.count;
        entry.count = 0;  // Reset for scatter phase
    }

    // Scatter order indices into positions_array
    for (int32_t oi : filtered_orders_indices) {
        int32_t orderkey = orders_o_orderkey[oi];
        auto& entry = orders_hash[orderkey];
        positions_array[entry.offset + entry.count] = oi;
        entry.count++;
    }

    // Probe phase: lineitem probes orders hash table
    struct JoinResult1 {
        int32_t l_idx;      // lineitem index
        int32_t o_idx;      // orders index
    };
    std::vector<JoinResult1> join_result1;
    join_result1.reserve(filtered_lineitem_indices.size() * 2);  // 1:N join multiplier

    for (int32_t li : filtered_lineitem_indices) {
        int32_t orderkey = lineitem_l_orderkey[li];
        auto it = orders_hash.find(orderkey);
        if (it != orders_hash.end()) {
            const auto& entry = it->second;
            // Iterate through all matching orders for this lineitem
            for (uint32_t j = entry.offset; j < entry.offset + entry.count; ++j) {
                uint32_t oi = positions_array[j];
                join_result1.push_back({li, (int32_t)oi});
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_lineitem_orders: %.2f ms (result %zu rows)\n", ms, join_result1.size());
#endif

    // ========================================================================
    // 5. Hash Join: Result ⋈ Customer (o_custkey = c_custkey)
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash table on customer: custkey → custkey_idx
    std::unordered_map<int32_t, int32_t> customer_hash;
    customer_hash.reserve(customer_count);
    for (size_t i = 0; i < customer_count; ++i) {
        customer_hash[customer_c_custkey[i]] = i;
    }

    // Probe with join result
    struct JoinResult2 {
        int32_t l_idx;      // lineitem index
        int32_t o_idx;      // orders index
        int32_t c_idx;      // customer index
    };
    std::vector<JoinResult2> join_result2;
    join_result2.reserve(join_result1.size());

    for (const auto& r1 : join_result1) {
        int32_t custkey = orders_o_custkey[r1.o_idx];
        auto it = customer_hash.find(custkey);
        if (it != customer_hash.end()) {
            join_result2.push_back({r1.l_idx, r1.o_idx, it->second});
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_customer: %.2f ms (result %zu rows)\n", ms, join_result2.size());
#endif

    // ========================================================================
    // 6. Hash Join: Result ⋈ Nation (c_nationkey = n_nationkey)
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Build hash table on nation: nationkey → nation_idx (small broadcast)
    std::unordered_map<int32_t, int32_t> nation_hash;
    for (size_t i = 0; i < nation_count; ++i) {
        nation_hash[nation_n_nationkey[i]] = i;
    }

    // Probe with join result
    struct JoinResult3 {
        int32_t l_idx;      // lineitem index
        int32_t o_idx;      // orders index
        int32_t c_idx;      // customer index
        int32_t n_idx;      // nation index
    };
    std::vector<JoinResult3> join_result3;
    join_result3.reserve(join_result2.size());

    for (const auto& r2 : join_result2) {
        int32_t nationkey = customer_c_nationkey[r2.c_idx];
        auto it = nation_hash.find(nationkey);
        if (it != nation_hash.end()) {
            join_result3.push_back({r2.l_idx, r2.o_idx, r2.c_idx, it->second});
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_nation: %.2f ms (result %zu rows)\n", ms, join_result3.size());
#endif

    // ========================================================================
    // 7. Aggregation: GROUP BY + SUM(l_extendedprice * (1 - l_discount))
    //    Strategy: Partitioned parallel aggregation by thread ownership
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local aggregation tables: each thread owns a partition of custkeys
    // Partition function: c_custkey % num_threads determines which thread handles it
    int num_agg_threads = omp_get_max_threads();
    std::vector<std::unordered_map<GroupKey, int64_t, GroupKeyHash>> thread_agg_tables(num_agg_threads);

    // Pre-size each thread's hash table
    for (int t = 0; t < num_agg_threads; ++t) {
        thread_agg_tables[t].reserve(join_result3.size() / num_agg_threads / 3 + 1000);
    }

    // Parallel aggregation: each tuple goes to its owning thread based on c_custkey
    #pragma omp parallel for schedule(static, 10000)
    for (size_t i = 0; i < join_result3.size(); ++i) {
        const auto& r3 = join_result3[i];
        int32_t l_idx = r3.l_idx;
        int32_t c_idx = r3.c_idx;
        int32_t n_idx = r3.n_idx;

        // Compute: l_extendedprice * (1 - l_discount)
        // Both l_extendedprice and l_discount are scaled by 100
        // To achieve 4-decimal precision in output:
        // - Convert inputs to scale 10000: extended_price * 100, discount * 100
        // - Compute: (ep_10000) * (10000 - disc_10000) / 10000
        // - Result is scaled by 10000
        int64_t extended_price = lineitem_l_extendedprice[l_idx];  // scale 100
        int64_t discount = lineitem_l_discount[l_idx];              // scale 100

        // extended_price * (1 - discount/100)
        // = extended_price * (100 - discount) / 100
        // Compute with 4-decimal precision by scaling up to 10000:
        // revenue_10000 = (extended_price * 100) * (10000 - discount * 100) / (10000)
        // = extended_price * (10000 - discount * 100) / 100
        int64_t revenue_10000 = (extended_price * (10000 - discount * 100)) / 100;

        int64_t c_custkey = customer_c_custkey[c_idx];
        int thread_id = (int)(c_custkey % num_agg_threads);

        GroupKey key = {
            c_custkey,
            customer_c_name[c_idx],
            customer_c_acctbal[c_idx],
            customer_c_phone[c_idx],
            nation_n_name[n_idx],
            customer_c_address[c_idx],
            customer_c_comment[c_idx]
        };

        // No critical section: each thread owns a partition of c_custkeys
        thread_agg_tables[thread_id][key] += revenue_10000;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    // Count total groups across all thread tables
    size_t total_groups = 0;
    for (int t = 0; t < num_agg_threads; ++t) {
        total_groups += thread_agg_tables[t].size();
    }
    printf("[TIMING] aggregation: %.2f ms (aggregated %zu groups)\n", ms, total_groups);
#endif

    // ========================================================================
    // 8. Merge Thread-Local Aggregation Tables and Convert Results
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Merge all thread-local tables into a single global table
    std::unordered_map<GroupKey, int64_t, GroupKeyHash> global_agg_table;

    for (int t = 0; t < num_agg_threads; ++t) {
        for (const auto& [key, revenue] : thread_agg_tables[t]) {
            global_agg_table[key] += revenue;
        }
    }

    // Convert to output format
    std::vector<Aggregation> agg_results;
    agg_results.reserve(global_agg_table.size());

    for (const auto& [key, revenue] : global_agg_table) {
        agg_results.push_back({
            key.c_custkey,
            key.c_name,
            revenue,
            key.c_acctbal,
            key.n_name,
            key.c_address,
            key.c_phone,
            key.c_comment
        });
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] convert_results: %.2f ms\n", ms);
#endif

    // ========================================================================
    // 9. Sort by Revenue DESC
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
    // 10. Extract Top-20 and Write CSV Output
    // ========================================================================
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Helper: quote CSV field if it contains comma or quote
    auto quote_csv_field = [](const std::string& s) -> std::string {
        bool needs_quoting = s.find(',') != std::string::npos ||
                             s.find('"') != std::string::npos ||
                             s.find('\n') != std::string::npos;
        if (!needs_quoting) return s;

        std::string result = "\"";
        for (char c : s) {
            if (c == '"') result += "\"\"";  // Escape quotes
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

        // Convert revenue from scaled int64 (scale 10000) to decimal with 4 places
        int64_t revenue_int = agg.revenue / 10000;
        int64_t revenue_frac = agg.revenue % 10000;

        // Convert c_acctbal from scaled int64 (scale 100) to decimal with 2 places
        // Handle negative values correctly: -276.92 is stored as -27692
        int64_t acctbal_int = agg.c_acctbal / 100;
        int64_t acctbal_frac = agg.c_acctbal % 100;

        // For negative values, the fractional part also becomes negative
        // We need to make it positive for output: -276 with -92 => -276.92
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
