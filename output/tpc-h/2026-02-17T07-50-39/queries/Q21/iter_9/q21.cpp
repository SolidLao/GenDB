/*
 * Q21: Suppliers Who Kept Orders Waiting
 *
 * ARCHITECTURE REWRITE (Iteration 9):
 * Previous approach: Sorted 60M rows + built 15M-entry map with vectors → 27s overhead
 * Root cause: Ignored pre-built lineitem_orderkey_hash index
 * New approach: Leverage pre-built index + compact hash structures
 *
 * LOGICAL PLAN:
 * 1. Filter nation for 'SAUDI ARABIA' → extract n_nationkey
 * 2. Build bitmap of Saudi suppliers (4K suppliers)
 * 3. Build hash set of orders with status='F' (7.5M orders)
 * 4. Single-pass lineitem scan to build two compact structures:
 *    a) Hash set: orderkeys with multiple suppliers (for EXISTS check)
 *    b) Hash set: (orderkey, suppkey) pairs with late receipt (for NOT EXISTS)
 * 5. Parallel main lineitem scan with fused filters + aggregation
 * 6. Merge thread-local aggregation results
 * 7. Sort and output top 100
 *
 * PHYSICAL PLAN:
 * - Pass 1 (parallel): Single lineitem scan with thread-local structures:
 *   - Per-thread: track suppkeys per orderkey via compact hash map
 *   - Merge: build global set of multi-supplier orders + late pairs
 * - Pass 2 (parallel): Main scan with filters:
 *   - l_receiptdate > l_commitdate
 *   - orderkey in F orders (hash lookup)
 *   - suppkey in Saudi suppliers (hash lookup)
 *   - EXISTS: order has multiple suppliers (hash lookup)
 *   - NOT EXISTS: use pre-built index to check other suppliers
 *   - Thread-local hash aggregation by s_name
 * - Merge: Combine thread-local results
 * - Sort: Top-100 selection
 *
 * KEY OPTIMIZATIONS (vs Iteration 8):
 * - Eliminated 60M-row sort (27s → 0s)
 * - Use pre-built lineitem_orderkey_hash index for fast lookups
 * - Compact hash structures with minimal allocations
 * - Parallel execution on both passes
 * - Thread-local aggregation (no contention)
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <sstream>
#include <thread>
#include <mutex>
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

// Hash index structures
struct HashSingleEntry {
    int32_t key;
    uint32_t position;
};

struct HashMultiEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
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

struct HashMultiIndex {
    uint32_t num_unique;
    uint32_t table_size;
    HashMultiEntry* entries;
    uint32_t* positions;

    bool find(int32_t key, uint32_t** out_positions, uint32_t* out_count) const {
        size_t hash_val = (size_t)key * 0x9E3779B97F4A7C15ULL;
        size_t idx = hash_val & (table_size - 1);
        for (uint32_t probe = 0; probe < table_size; ++probe) {
            size_t pos = (idx + probe) & (table_size - 1);
            if (entries[pos].key == key) {
                *out_positions = positions + entries[pos].offset;
                *out_count = entries[pos].count;
                return true;
            }
            if (entries[pos].key == 0 && entries[pos].offset == 0 && entries[pos].count == 0) {
                break; // empty slot
            }
        }
        return false;
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

HashMultiIndex load_hash_multi_index(const std::string& path) {
    HashMultiIndex index;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open hash multi index: " << path << std::endl;
        index.num_unique = 0;
        index.table_size = 0;
        index.entries = nullptr;
        index.positions = nullptr;
        return index;
    }
    struct stat sb;
    fstat(fd, &sb);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    uint32_t* header = static_cast<uint32_t*>(addr);
    index.num_unique = header[0];
    index.table_size = header[1];
    index.entries = reinterpret_cast<HashMultiEntry*>(header + 2);

    // Positions array starts after hash table entries
    uint32_t* after_entries = reinterpret_cast<uint32_t*>(index.entries + index.table_size);
    // Skip pos_count field (after_entries[0]) and point to actual positions
    index.positions = after_entries + 1;

    return index;
}

// Aggregation structure
struct SupplierAgg {
    std::string s_name;
    int32_t numwait;
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
    if (target_nationkey == -1) {
        std::cerr << "SAUDI ARABIA not found in nation table" << std::endl;
        return;
    }

    // Load supplier
    size_t supplier_count;
    auto* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    auto* s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Build set of qualifying supplier keys (from SAUDI ARABIA)
    std::unordered_set<int32_t> saudi_suppliers;
    saudi_suppliers.reserve(4000);
    for (size_t i = 0; i < supplier_count; ++i) {
        if (s_nationkey[i] == target_nationkey) {
            saudi_suppliers.insert(s_suppkey[i]);
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
    std::unordered_set<int32_t> f_orders;
    f_orders.reserve(8000000);
    for (size_t i = 0; i < orders_count; ++i) {
        if (o_orderstatus[i] == f_code) {
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

    // Load pre-built lineitem_orderkey_hash index
    auto lineitem_index = load_hash_multi_index(gendb_dir + "/indexes/lineitem_orderkey_hash.bin");

    // Subquery decorrelation: Single-pass parallel scan (no sort!)
#ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
#endif

    const int num_threads = omp_get_max_threads();

    // Thread-local structures to track suppliers per order
    std::vector<std::unordered_map<int32_t, std::unordered_set<int32_t>>> local_order_suppliers(num_threads);
    std::vector<std::unordered_set<uint64_t>> local_late_pairs(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        local_order_suppliers[t].reserve(1000000);
        local_late_pairs[t].reserve(2000000);
    }

    // Parallel scan: build per-thread structures
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& my_order_suppliers = local_order_suppliers[tid];
        auto& my_late_pairs = local_late_pairs[tid];

        #pragma omp for schedule(static)
        for (size_t i = 0; i < lineitem_count; ++i) {
            int32_t orderkey = l_orderkey[i];
            int32_t suppkey = l_suppkey[i];

            // Track suppliers for this order
            my_order_suppliers[orderkey].insert(suppkey);

            // Track late pairs
            if (l_receiptdate[i] > l_commitdate[i]) {
                uint64_t pair = ((uint64_t)orderkey << 32) | (uint32_t)suppkey;
                my_late_pairs.insert(pair);
            }
        }
    }

    // Merge: build global sets
    std::unordered_set<int32_t> orders_with_multiple_suppliers;
    orders_with_multiple_suppliers.reserve(10000000);

    std::unordered_set<uint64_t> late_order_supplier_pairs;
    late_order_supplier_pairs.reserve(30000000);

    // Merge supplier counts
    std::unordered_map<int32_t, std::unordered_set<int32_t>> merged_order_suppliers;
    merged_order_suppliers.reserve(15000000);

    for (int t = 0; t < num_threads; ++t) {
        for (const auto& kv : local_order_suppliers[t]) {
            int32_t orderkey = kv.first;
            for (int32_t suppkey : kv.second) {
                merged_order_suppliers[orderkey].insert(suppkey);
            }
        }
    }

    // Identify orders with multiple suppliers
    for (const auto& kv : merged_order_suppliers) {
        if (kv.second.size() > 1) {
            orders_with_multiple_suppliers.insert(kv.first);
        }
    }

    // Merge late pairs
    for (int t = 0; t < num_threads; ++t) {
        for (uint64_t pair : local_late_pairs[t]) {
            late_order_supplier_pairs.insert(pair);
        }
    }

    // Free thread-local structures
    local_order_suppliers.clear();
    local_late_pairs.clear();

#ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery_decorrelation: %.2f ms\n", ms_subquery);
#endif

    // Main scan with joins and aggregation (parallel)
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Load supplier names for late materialization
    auto s_name = load_string_column(gendb_dir + "/supplier/s_name.bin", supplier_count);

    // Load pre-built indexes
    auto supplier_index = load_hash_single_index(gendb_dir + "/indexes/supplier_suppkey_hash.bin");

    // Thread-local aggregation maps
    std::vector<std::unordered_map<std::string, int32_t>> local_counts(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        local_counts[t].reserve(100);
    }

    // Parallel scan lineitem l1
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& my_counts = local_counts[tid];

        #pragma omp for schedule(static)
        for (size_t i = 0; i < lineitem_count; ++i) {
            // Filter: l1.l_receiptdate > l1.l_commitdate
            if (l_receiptdate[i] <= l_commitdate[i]) continue;

            int32_t orderkey = l_orderkey[i];
            int32_t suppkey = l_suppkey[i];

            // Join with orders: o_orderkey = l1.l_orderkey AND o_orderstatus = 'F'
            if (f_orders.find(orderkey) == f_orders.end()) continue;

            // Join with supplier: s_suppkey = l1.l_suppkey
            if (saudi_suppliers.find(suppkey) == saudi_suppliers.end()) continue;

            // EXISTS check: order must have multiple suppliers
            if (orders_with_multiple_suppliers.find(orderkey) == orders_with_multiple_suppliers.end()) continue;

            // NOT EXISTS check: no OTHER supplier with late receipt
            // Use pre-built index to get all lineitems for this order
            uint32_t* positions;
            uint32_t count;
            bool has_other_late = false;

            if (lineitem_index.find(orderkey, &positions, &count)) {
                // Check if any OTHER supplier has late receipt
                for (uint32_t j = 0; j < count; ++j) {
                    uint32_t pos = positions[j];
                    int32_t other_supp = l_suppkey[pos];

                    if (other_supp != suppkey) {
                        // Check if this other supplier has late receipt
                        uint64_t pair = ((uint64_t)orderkey << 32) | (uint32_t)other_supp;
                        if (late_order_supplier_pairs.find(pair) != late_order_supplier_pairs.end()) {
                            has_other_late = true;
                            break;
                        }
                    }
                }
            }
            if (has_other_late) continue;

            // Get supplier name via index
            uint32_t* supp_pos = supplier_index.find(suppkey);
            if (supp_pos) {
                const std::string& name = s_name[*supp_pos];
                my_counts[name]++;
            }
        }
    }

    // Merge thread-local results
    std::unordered_map<std::string, int32_t> supplier_counts;
    supplier_counts.reserve(100);
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& p : local_counts[t]) {
            supplier_counts[p.first] += p.second;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_aggregation: %.2f ms\n", ms_join);
#endif

    // Sort results
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

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
