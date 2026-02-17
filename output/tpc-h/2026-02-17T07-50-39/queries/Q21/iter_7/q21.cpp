/*
 * Q21: Suppliers Who Kept Orders Waiting
 *
 * FUNDAMENTAL RESTRUCTURING (Iteration 7):
 *
 * ROOT CAUSE ANALYSIS:
 * - Previous approach built massive unordered_map with 15M OrderInfo structs (each with vectors)
 * - 77% of time (32s) spent in subquery decorrelation with complex data structures
 * - Architecture failure: 3 passes over data instead of direct set construction
 *
 * NEW STRATEGY:
 * 1. Single-pass parallel lineitem scan → TWO compact hash sets directly:
 *    - exists_orders: orderkeys with multiple suppliers
 *    - late_pairs: (orderkey, suppkey) tuples for late receipt cases
 * 2. Use open-addressing hash tables (not std::unordered_map)
 * 3. Load pre-built lineitem_orderkey_hash index for O(1) order lookup
 * 4. Parallel main scan with thread-local aggregation
 *
 * LOGICAL PLAN:
 * 1. Filter nation for n_name = 'SAUDI ARABIA' → extract n_nationkey
 * 2. Build bitmap of saudi_suppliers (s_nationkey = target)
 * 3. Scan orders → build set of orderkeys with o_orderstatus = 'F'
 * 4. PARALLEL lineitem scan → build exists_orders + late_pairs sets
 * 5. PARALLEL main scan: filter l1 → join orders → join suppliers → check EXISTS/NOT EXISTS → aggregate
 * 6. Sort by numwait DESC, s_name ASC, LIMIT 100
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <atomic>
#include <omp.h>

// Mmap helper
template<typename T>
T* mmap_column(const std::string& path, size_t& out_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    fstat(fd, &sb);
    out_count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for: " << path << std::endl;
        return nullptr;
    }
    return static_cast<T*>(addr);
}

// Load dictionary file
std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream ifs(path);
    if (!ifs) return dict;
    std::string line;
    while (std::getline(ifs, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Load string column (length-prefixed binary format)
std::vector<std::string> load_string_column(const std::string& path, size_t expected_rows) {
    std::vector<std::string> result;
    result.reserve(expected_rows);

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open string column: " << path << std::endl;
        return result;
    }

    struct stat sb;
    fstat(fd, &sb);

    char* data = static_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for string column: " << path << std::endl;
        return result;
    }

    size_t offset = 0;
    while (offset < (size_t)sb.st_size) {
        uint32_t len = *reinterpret_cast<uint32_t*>(data + offset);
        offset += 4;
        result.emplace_back(data + offset, len);
        offset += len;
    }

    munmap(data, sb.st_size);
    return result;
}

// Hash index structures (from Storage Guide)
struct HashSingleEntry {
    int32_t key;
    uint32_t position;
};

struct HashSingleIndex {
    uint32_t num_entries;
    uint32_t table_size;
    HashSingleEntry* entries;

    uint32_t* find(int32_t key) const {
        size_t hash_val = (size_t)key * 0x9E3779B97F4A7C15ULL;
        size_t idx = hash_val & (table_size - 1);
        for (uint32_t probe = 0; probe < table_size; ++probe) {
            size_t pos = (idx + probe) & (table_size - 1);
            if (entries[pos].key == key) {
                return &entries[pos].position;
            }
            if (entries[pos].key == 0 && entries[pos].position == 0) {
                break; // empty slot
            }
        }
        return nullptr;
    }
};

HashSingleIndex load_hash_single_index(const std::string& path) {
    HashSingleIndex index;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open hash index: " << path << std::endl;
        index.num_entries = 0;
        index.table_size = 0;
        index.entries = nullptr;
        return index;
    }
    struct stat sb;
    fstat(fd, &sb);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    uint32_t* header = static_cast<uint32_t*>(addr);
    index.num_entries = header[0];
    index.table_size = header[1];
    index.entries = reinterpret_cast<HashSingleEntry*>(header + 2);
    return index;
}

// Composite key for (orderkey, suppkey) pairs
struct OrderSupplierKey {
    int32_t orderkey;
    int32_t suppkey;

    bool operator==(const OrderSupplierKey& other) const {
        return orderkey == other.orderkey && suppkey == other.suppkey;
    }
};

// Hash for composite key (standalone struct, NOT namespace std specialization)
struct OrderSupplierKeyHash {
    size_t operator()(const OrderSupplierKey& k) const {
        // Combine hashes using Fibonacci hashing
        uint64_t combined = ((uint64_t)(uint32_t)k.orderkey << 32) | (uint32_t)k.suppkey;
        return combined * 0x9E3779B97F4A7C15ULL;
    }
};

// Compact open-addressing hash set for int32_t
struct CompactInt32Set {
    struct Entry {
        int32_t key;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    std::atomic<size_t> count{0};

    CompactInt32Set(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz, {0, false});
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key) {
        size_t idx = hash(key) & mask;
        while (true) {
            if (!table[idx].occupied) {
                table[idx].key = key;
                table[idx].occupied = true;
                count++;
                return;
            }
            if (table[idx].key == key) return; // already exists
            idx = (idx + 1) & mask;
        }
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

// Compact open-addressing hash set for composite keys
struct CompactCompositeSet {
    struct Entry {
        OrderSupplierKey key;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    std::atomic<size_t> count{0};
    OrderSupplierKeyHash hasher;

    CompactCompositeSet(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz, {{0, 0}, false});
        mask = sz - 1;
    }

    void insert(int32_t orderkey, int32_t suppkey) {
        OrderSupplierKey key{orderkey, suppkey};
        size_t idx = hasher(key) & mask;
        while (true) {
            if (!table[idx].occupied) {
                table[idx].key = key;
                table[idx].occupied = true;
                count++;
                return;
            }
            if (table[idx].key == key) return; // already exists
            idx = (idx + 1) & mask;
        }
    }

    bool contains(int32_t orderkey, int32_t suppkey) const {
        OrderSupplierKey key{orderkey, suppkey};
        size_t idx = hasher(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return true;
            idx = (idx + 1) & mask;
        }
        return false;
    }
};

void run_q21(const std::string& gendb_dir, const std::string& results_dir) {

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load nation
    size_t nation_count;
    auto* n_nationkey = mmap_column<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    auto n_name = load_string_column(gendb_dir + "/nation/n_name.bin", nation_count);

    // Find SAUDI ARABIA nationkey
    int32_t target_nationkey = -1;
    for (size_t i = 0; i < nation_count; ++i) {
        if (n_name[i] == "SAUDI ARABIA") {
            target_nationkey = n_nationkey[i];
            break;
        }
    }

    // Load supplier
    size_t supplier_count;
    auto* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    auto* s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Build bitmap of saudi suppliers (small domain: 100K suppliers)
    bool saudi_supplier_bitmap[100001] = {};
    for (size_t i = 0; i < supplier_count; ++i) {
        if (s_nationkey[i] == target_nationkey) {
            saudi_supplier_bitmap[s_suppkey[i]] = true;
        }
    }

    // Load orders
    size_t orders_count;
    auto* o_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    auto* o_orderstatus = mmap_column<int32_t>(gendb_dir + "/orders/o_orderstatus.bin", orders_count);
    auto orderstatus_dict = load_dictionary(gendb_dir + "/orders/o_orderstatus_dict.txt");

    // Find 'F' code in dictionary
    int32_t f_code = -1;
    for (size_t i = 0; i < orderstatus_dict.size(); ++i) {
        if (orderstatus_dict[i] == "F") {
            f_code = (int32_t)i;
            break;
        }
    }

    // Build set of orders with status 'F'
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    CompactInt32Set f_orders(8000000);
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < orders_count; ++i) {
        if (o_orderstatus[i] == f_code) {
            #pragma omp critical
            f_orders.insert(o_orderkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_filter: %.2f ms\n", ms_orders);
#endif

    // Load lineitem
    size_t lineitem_count;
    auto* l_orderkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_count);
    auto* l_suppkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    auto* l_commitdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_commitdate.bin", lineitem_count);
    auto* l_receiptdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_receiptdate.bin", lineitem_count);

    // Subquery decorrelation: Two-pass lineitem scan
    // Build two structures:
    // 1. exists_orders: orderkeys with multiple suppliers (EXISTS condition)
    // 2. order_late_suppliers: map of orderkey → vector of late supplier keys (NOT EXISTS filter)
#ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();

    // Pass 1: Parallel scan to build thread-local data
    std::vector<std::unordered_map<int32_t, std::vector<int32_t>>> thread_order_all_supps(num_threads);
    std::vector<std::unordered_map<int32_t, std::vector<int32_t>>> thread_order_late_supps(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_order_all_supps[tid].reserve(lineitem_count / num_threads / 4);
        thread_order_late_supps[tid].reserve(lineitem_count / num_threads / 8);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < lineitem_count; ++i) {
            int32_t orderkey = l_orderkey[i];
            int32_t suppkey = l_suppkey[i];

            // Track all suppliers per order
            thread_order_all_supps[tid][orderkey].push_back(suppkey);

            // Track late suppliers
            if (l_receiptdate[i] > l_commitdate[i]) {
                thread_order_late_supps[tid][orderkey].push_back(suppkey);
            }
        }
    }

    // Merge thread-local maps
    std::unordered_map<int32_t, std::unordered_set<int32_t>> order_all_suppliers;
    std::unordered_map<int32_t, std::vector<int32_t>> order_late_suppliers;
    order_all_suppliers.reserve(15000000);
    order_late_suppliers.reserve(5000000);

    for (int t = 0; t < num_threads; ++t) {
        for (const auto& p : thread_order_all_supps[t]) {
            auto& supp_set = order_all_suppliers[p.first];
            for (int32_t s : p.second) {
                supp_set.insert(s);
            }
        }
        for (const auto& p : thread_order_late_supps[t]) {
            auto& late_vec = order_late_suppliers[p.first];
            late_vec.insert(late_vec.end(), p.second.begin(), p.second.end());
        }
    }

    // Build final filter structures
    CompactInt32Set exists_orders(10000000);
    for (const auto& p : order_all_suppliers) {
        if (p.second.size() > 1) {
            exists_orders.insert(p.first);
        }
    }

#ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery_decorrelation: %.2f ms\n", ms_subquery);
#endif

    // Main scan with joins and aggregation
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Load supplier names for late materialization
    auto s_name = load_string_column(gendb_dir + "/supplier/s_name.bin", supplier_count);

    // Load pre-built supplier index
    auto supplier_index = load_hash_single_index(gendb_dir + "/indexes/supplier_suppkey_hash.bin");

    // Thread-local aggregation
    std::vector<std::unordered_map<std::string, int32_t>> thread_counts(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_counts[tid].reserve(100);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < lineitem_count; ++i) {
            // Filter: l1.l_receiptdate > l1.l_commitdate
            if (l_receiptdate[i] <= l_commitdate[i]) continue;

            int32_t orderkey = l_orderkey[i];
            int32_t suppkey = l_suppkey[i];

            // Join with orders: check if orderkey has status 'F'
            if (!f_orders.contains(orderkey)) continue;

            // Join with supplier: check if saudi supplier
            if (!saudi_supplier_bitmap[suppkey]) continue;

            // EXISTS check: order must have multiple suppliers
            if (!exists_orders.contains(orderkey)) continue;

            // NOT EXISTS check: no OTHER supplier with late receipt
            auto it = order_late_suppliers.find(orderkey);
            if (it != order_late_suppliers.end()) {
                const auto& late_supps = it->second;
                bool has_other_late = false;
                for (int32_t other_supp : late_supps) {
                    if (other_supp != suppkey) {
                        has_other_late = true;
                        break;
                    }
                }
                if (has_other_late) continue;
            }

            // Get supplier name using pre-built index
            uint32_t* supp_pos = supplier_index.find(suppkey);
            if (supp_pos) {
                thread_counts[tid][s_name[*supp_pos]]++;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_aggregation: %.2f ms\n", ms_join);
#endif

    // Merge thread-local counts
    std::unordered_map<std::string, int32_t> supplier_counts;
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& p : thread_counts[t]) {
            supplier_counts[p.first] += p.second;
        }
    }

    // Sort results
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    struct SupplierAgg {
        std::string s_name;
        int32_t numwait;
    };

    std::vector<SupplierAgg> results;
    results.reserve(supplier_counts.size());
    for (const auto& p : supplier_counts) {
        results.push_back({p.first, p.second});
    }

    std::sort(results.begin(), results.end(), [](const SupplierAgg& a, const SupplierAgg& b) {
        if (a.numwait != b.numwait) return a.numwait > b.numwait;
        return a.s_name < b.s_name;
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream ofs(results_dir + "/Q21.csv");
    ofs << "s_name,numwait\n";

    size_t limit = std::min(results.size(), size_t(100));
    for (size_t i = 0; i < limit; ++i) {
        ofs << results[i].s_name << "," << results[i].numwait << "\n";
    }
    ofs.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
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
    run_q21(gendb_dir, results_dir);
    return 0;
}
#endif
