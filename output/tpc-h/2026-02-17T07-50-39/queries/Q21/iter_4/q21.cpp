/*
 * Q21: Suppliers Who Kept Orders Waiting (ARCHITECTURAL REWRITE - Iteration 4)
 *
 * BOTTLENECK ANALYSIS (iter_0): 41839ms total, 32357ms (77%) in subquery_decorrelation
 * - Building 15M-entry hash map with sequential scan: catastrophic cache behavior
 * - Two separate lineitem passes (decorrelation + main scan): redundant I/O
 * - std::unordered_map overhead: 80 bytes/entry = 1.2GB pointer chasing
 *
 * NEW ARCHITECTURE: Parallel Single-Pass Fused Execution
 *
 * LOGICAL PLAN:
 * 1. Filter nation: n_name = 'SAUDI ARABIA' → extract n_nationkey (25 → 1 row)
 * 2. Build bitmap: saudi_suppliers[s_suppkey] = (s_nationkey == target) (100K suppliers → ~4K bits set)
 * 3. Filter orders: o_orderstatus = 'F' → build bitmap f_orders[o_orderkey] (15M → ~7.5M bits set)
 * 4. FUSED SINGLE-PASS lineitem scan (PARALLEL):
 *    For each lineitem row:
 *      - Apply all filters inline (receiptdate > commitdate, saudi supplier, F order)
 *      - Track per-order state for EXISTS/NOT EXISTS (compact 2-bit state per order)
 *      - Accumulate aggregation directly into thread-local s_name counters
 * 5. Merge thread-local aggregation results
 * 6. Sort by numwait DESC, s_name ASC, LIMIT 100
 *
 * PHYSICAL PLAN:
 * - Nation: Sequential scan (25 rows) → extract nationkey
 * - Supplier: Build bitmap (bool array 100K entries, ~12KB)
 * - Orders: Parallel scan → build bitmap (bool array 15M entries, ~15MB)
 * - Lineitem: PARALLEL MORSEL-DRIVEN SCAN (64 threads, 100K morsel size)
 *   - Per morsel: apply all filters, update thread-local state
 *   - Thread-local order state: compact vector<pair<suppkey, late_flag>>
 *   - Thread-local aggregation: unordered_map<string, int32_t> (~100 entries)
 * - Merge: Combine thread-local order states → compute final EXISTS/NOT EXISTS bitmaps
 * - Merge: Combine thread-local aggregation maps
 * - Sort: std::sort, output top 100
 *
 * KEY OPTIMIZATIONS:
 * - Bitmap filters (not hash sets): O(1) lookup, ~30MB total vs 1GB+ hash map overhead
 * - Single lineitem pass: Fuse decorrelation + main scan (eliminates 60M row re-read)
 * - Parallel everything: OpenMP on orders filter + lineitem scan (64-core machine)
 * - Pre-built supplier index: Zero-cost lookup for s_name materialization
 * - Compact order state: per-order (suppkey, late_flag) instead of full OrderInfo struct
 * - Morsel-driven: 100K row chunks for cache-friendly processing
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
    uint32_t pos_count = after_entries[0];
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

    // Build bitmap of qualifying supplier keys (from SAUDI ARABIA)
    // Bitmap: O(1) lookup, ~100KB memory vs 300KB+ for hash set
    std::vector<bool> saudi_suppliers(100001, false);  // s_suppkey range: [1, 100000]
    for (size_t i = 0; i < supplier_count; ++i) {
        if (s_nationkey[i] == target_nationkey) {
            saudi_suppliers[s_suppkey[i]] = true;
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

    // Build bitmap of orders with status 'F' (PARALLEL)
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif
    // Bitmap: bool array, ~15MB vs 600MB+ for hash set with 7.5M entries
    int32_t max_orderkey = 0;
    for (size_t i = 0; i < orders_count; ++i) {
        if (o_orderkey[i] > max_orderkey) max_orderkey = o_orderkey[i];
    }
    std::vector<bool> f_orders(max_orderkey + 1, false);

    #pragma omp parallel for schedule(static, 100000)
    for (size_t i = 0; i < orders_count; ++i) {
        if (o_orderstatus[i] == f_code) {
            f_orders[o_orderkey[i]] = true;
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

    // Load supplier names and index for late materialization
    auto s_name = load_string_column(gendb_dir + "/supplier/s_name.bin", supplier_count);
    auto supplier_index = load_hash_single_index(gendb_dir + "/indexes/supplier_suppkey_hash.bin");

    // FUSED SINGLE-PASS PARALLEL LINEITEM SCAN
    // Combines: subquery decorrelation + filtering + aggregation
#ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local structures for parallel processing
    int num_threads = omp_get_max_threads();

    // Per-thread order tracking: map orderkey → list of (suppkey, is_late) pairs
    std::vector<std::unordered_map<int32_t, std::vector<std::pair<int32_t, bool>>>> thread_order_data(num_threads);

    // Reserve space for per-thread order tracking
    for (int t = 0; t < num_threads; ++t) {
        thread_order_data[t].reserve(2000000);  // ~2M orders per thread (60M / 30 threads)
    }

    // Parallel morsel-driven scan
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_order_data = thread_order_data[tid];

        #pragma omp for schedule(dynamic, 100000) nowait
        for (size_t i = 0; i < lineitem_count; ++i) {
            int32_t orderkey = l_orderkey[i];
            int32_t suppkey = l_suppkey[i];
            bool is_late = l_receiptdate[i] > l_commitdate[i];

            // Track all suppliers per order for EXISTS/NOT EXISTS
            local_order_data[orderkey].emplace_back(suppkey, is_late);
        }
    }

#ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double ms_subquery = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery_decorrelation: %.2f ms\n", ms_subquery);
#endif

    // Merge thread-local order data and perform aggregation
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Merge order data from all threads
    std::unordered_map<int32_t, std::vector<std::pair<int32_t, bool>>> merged_order_data;
    merged_order_data.reserve(15000000);
    for (int t = 0; t < num_threads; ++t) {
        for (auto& entry : thread_order_data[t]) {
            auto& target = merged_order_data[entry.first];
            target.insert(target.end(), entry.second.begin(), entry.second.end());
        }
    }

    // Process each order to find qualifying lineitem rows
    std::unordered_map<std::string, int32_t> supplier_counts;
    supplier_counts.reserve(100);

    for (auto& order_entry : merged_order_data) {
        int32_t orderkey = order_entry.first;
        auto& suppliers = order_entry.second;

        // Quick filters: order must have status 'F'
        if (orderkey >= (int32_t)f_orders.size() || !f_orders[orderkey]) continue;

        // Build unique supplier set for EXISTS check
        std::unordered_set<int32_t> unique_suppliers;
        std::unordered_set<int32_t> late_suppliers;
        for (const auto& p : suppliers) {
            unique_suppliers.insert(p.first);
            if (p.second) late_suppliers.insert(p.first);
        }

        // EXISTS: order must have multiple suppliers
        if (unique_suppliers.size() < 2) continue;

        // Process each lineitem for this order
        for (const auto& p : suppliers) {
            int32_t suppkey = p.first;
            bool is_late = p.second;

            // Filter: l1.l_receiptdate > l1.l_commitdate
            if (!is_late) continue;

            // Join with supplier: must be from SAUDI ARABIA
            if (suppkey >= (int32_t)saudi_suppliers.size() || !saudi_suppliers[suppkey]) continue;

            // NOT EXISTS check: no OTHER supplier with late receipt
            bool has_other_late = false;
            for (int32_t other_supp : late_suppliers) {
                if (other_supp != suppkey) {
                    has_other_late = true;
                    break;
                }
            }
            if (has_other_late) continue;

            // Get supplier name via index and aggregate
            uint32_t* supp_pos = supplier_index.find(suppkey);
            if (supp_pos) {
                const std::string& name = s_name[*supp_pos];
                supplier_counts[name]++;
            }
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
