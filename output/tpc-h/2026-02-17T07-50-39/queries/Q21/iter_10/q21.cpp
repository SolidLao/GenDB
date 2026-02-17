/*
 * Q21: Suppliers Who Kept Orders Waiting
 *
 * ITERATION 10 - REWRITE using pre-built lineitem_orderkey_hash index
 *
 * Previous bottleneck: 27s sorting/grouping 60M lineitem rows
 * Root cause: NOT using pre-built lineitem_orderkey_hash index
 *
 * LOGICAL PLAN:
 * 1. Filter nation → 'SAUDI ARABIA' nationkey
 * 2. Build bitmap of Saudi suppliers (~4K)
 * 3. Build hash set of 'F' orders (~7.5M)
 * 4. Load pre-built lineitem_orderkey_hash (instant grouping by orderkey)
 * 5. Single-pass lineitem scan using index to answer subqueries:
 *    - For each qualifying l1 row, use index to get all lineitems for that order
 *    - EXISTS: check if >1 distinct supplier (cache per orderkey)
 *    - NOT EXISTS: check if other suppliers have late receipts (cache per orderkey)
 * 6. Parallel aggregation by supplier name
 * 7. Sort and output top 100
 *
 * PHYSICAL PLAN:
 * - Pre-filters: nation→nationkey, supplier bitmap, orders→F set
 * - Main scan (parallel): For each lineitem l1:
 *   1. Check l1.receiptdate > l1.commitdate
 *   2. Probe F orders set
 *   3. Probe Saudi suppliers bitmap
 *   4. Use pre-built index to get all lineitems for l1.orderkey
 *   5. Check EXISTS (>1 supplier) - cache result per orderkey
 *   6. Check NOT EXISTS (no other late supplier) - cache result per orderkey
 *   7. Aggregate by supplier name (thread-local)
 * - Merge thread-local results
 * - Sort by numwait DESC, s_name ASC
 *
 * KEY OPTIMIZATIONS:
 * - Use pre-built lineitem_orderkey_hash → eliminates 27s sort/grouping
 * - Cache subquery results per orderkey (millions of orders reused)
 * - Parallel main scan with thread-local aggregation
 * - Late materialization: load s_name only for qualifying rows
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
    // after_entries[0] is pos_count (not needed for access)
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
#ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
#endif

    auto lineitem_orderkey_index = load_hash_multi_index(gendb_dir + "/indexes/lineitem_orderkey_hash.bin");

    // Per-orderkey cached subquery results (thread-safe with partitioning)
    const int num_threads = omp_get_max_threads();

    // Partition orderkey space for lock-free caching
    const size_t NUM_PARTITIONS = 1024;
    std::vector<std::unordered_map<int32_t, uint8_t>> exists_cache(NUM_PARTITIONS);
    std::vector<std::unordered_map<int32_t, uint8_t>> not_exists_cache(NUM_PARTITIONS);
    std::vector<std::mutex> partition_locks(NUM_PARTITIONS);

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

    // Lambda: Check EXISTS - order has multiple distinct suppliers
    auto check_exists = [&](int32_t orderkey) -> bool {
        size_t partition = ((size_t)orderkey * 0x9E3779B9) % NUM_PARTITIONS;

        // Check cache first
        {
            auto& cache = exists_cache[partition];
            auto it = cache.find(orderkey);
            if (it != cache.end()) {
                return it->second == 1;
            }
        }

        // Compute and cache
        uint32_t* positions;
        uint32_t count;
        bool has_multiple = false;

        if (lineitem_orderkey_index.find(orderkey, &positions, &count)) {
            // Count distinct suppliers
            int32_t first_supp = -1;
            for (uint32_t j = 0; j < count; ++j) {
                int32_t supp = l_suppkey[positions[j]];
                if (first_supp == -1) {
                    first_supp = supp;
                } else if (supp != first_supp) {
                    has_multiple = true;
                    break;
                }
            }
        }

        // Cache result
        std::lock_guard<std::mutex> lock(partition_locks[partition]);
        exists_cache[partition][orderkey] = has_multiple ? 1 : 0;
        return has_multiple;
    };

    // Lambda: Check NOT EXISTS - no other supplier with late receipt
    auto check_not_exists = [&](int32_t orderkey, int32_t my_suppkey) -> bool {
        size_t partition = ((size_t)orderkey * 0x9E3779B9) % NUM_PARTITIONS;

        // Check cache first
        {
            auto& cache = not_exists_cache[partition];
            auto it = cache.find(orderkey);
            if (it != cache.end()) {
                // Cache stores: 0=has other late, 1=no other late
                return it->second == 1;
            }
        }

        // Compute: check if any OTHER supplier has late receipt
        uint32_t* positions;
        uint32_t count;
        bool has_other_late = false;

        if (lineitem_orderkey_index.find(orderkey, &positions, &count)) {
            for (uint32_t j = 0; j < count; ++j) {
                uint32_t pos = positions[j];
                int32_t other_supp = l_suppkey[pos];
                if (other_supp != my_suppkey && l_receiptdate[pos] > l_commitdate[pos]) {
                    has_other_late = true;
                    break;
                }
            }
        }

        // Cache result
        std::lock_guard<std::mutex> lock(partition_locks[partition]);
        not_exists_cache[partition][orderkey] = has_other_late ? 0 : 1;
        return !has_other_late;
    };

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

            // EXISTS check: order must have multiple suppliers (cached)
            if (!check_exists(orderkey)) continue;

            // NOT EXISTS check: no OTHER supplier with late receipt (cached)
            if (!check_not_exists(orderkey, suppkey)) continue;

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
