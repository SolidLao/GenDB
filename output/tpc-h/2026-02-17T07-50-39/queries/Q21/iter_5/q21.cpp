/*
 * Q21: Suppliers Who Kept Orders Waiting
 *
 * LOGICAL PLAN (FUNDAMENTAL RESTRUCTURING - Iteration 5):
 * 1. Filter nation for n_name = 'SAUDI ARABIA' → bitmap[25]
 * 2. Filter supplier: s_nationkey in bitmap → ~4K suppliers
 * 3. Filter orders: o_orderstatus = 'F' → ~7.5M orders → bitmap for fast lookup
 * 4. FUSED SINGLE-PASS LINEITEM SCAN:
 *    - For each lineitem row with late receipt (l_receiptdate > l_commitdate):
 *      a) Check if order is 'F' status (bitmap)
 *      b) Check if supplier is from SAUDI ARABIA (bitmap)
 *      c) Track per-order: suppkeys (for EXISTS) and late suppkeys (for NOT EXISTS)
 *      d) If all conditions pass, accumulate into aggregation
 *    - Eliminates separate subquery decorrelation phase
 * 5. Apply EXISTS/NOT EXISTS filters during aggregation
 * 6. Sort by numwait DESC, s_name ASC, LIMIT 100
 *
 * PHYSICAL PLAN:
 * - Nation filter: O(25) scan → single nationkey
 * - Supplier filter: O(100K) scan → bitmap[100001] for O(1) lookup
 * - Orders filter: O(15M) parallel scan → bitmap[max_orderkey+1] for O(1) lookup
 * - FUSED lineitem scan: O(60M) parallel scan with:
 *   - Pre-built lineitem_orderkey_hash index to get all lineitems per order
 *   - Per-order structures built on-the-fly during main scan
 *   - Thread-local aggregation to avoid contention
 * - Aggregation: Small groups (~100) → concurrent hash map or merge thread-local
 * - Sort: std::sort on final vector
 *
 * OPTIMIZATION TECHNIQUES (ADDRESSING STALL):
 * - Eliminate separate 15M-entry hash table build (32 seconds bottleneck)
 * - Use bitmaps for O(1) filtering instead of hash sets
 * - Fuse subquery computation into main scan
 * - Open-addressing hash for aggregation
 * - Parallel scan with OpenMP
 * - Load pre-built indexes via mmap (zero build time)
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
    // after_entries[0] contains pos_count, positions start at after_entries[1]
    index.positions = after_entries + 1;

    return index;
}

// Aggregation structure
struct SupplierAgg {
    std::string s_name;
    int32_t numwait;
};

// Open-addressing hash table for aggregation
struct AggEntry {
    uint32_t supp_idx;
    int32_t count;
    bool occupied;
};

struct CompactAggTable {
    std::vector<AggEntry> table;
    size_t mask;

    CompactAggTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz, {0, 0, false});
        mask = sz - 1;
    }

    void add(uint32_t supp_idx, int32_t delta) {
        size_t idx = ((size_t)supp_idx * 0x9E3779B97F4A7C15ULL) & mask;
        while (true) {
            if (!table[idx].occupied) {
                table[idx] = {supp_idx, delta, true};
                return;
            }
            if (table[idx].supp_idx == supp_idx) {
                table[idx].count += delta;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    void merge_from(const CompactAggTable& other) {
        for (const auto& entry : other.table) {
            if (entry.occupied) {
                add(entry.supp_idx, entry.count);
            }
        }
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
    if (target_nationkey == -1) {
        std::cerr << "SAUDI ARABIA not found in nation table" << std::endl;
        return;
    }

    // Load supplier
    size_t supplier_count;
    auto* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    auto* s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);

    // Build bitmap of qualifying suppliers (SAUDI ARABIA) - O(1) lookup
    int32_t max_suppkey = 0;
    for (size_t i = 0; i < supplier_count; ++i) {
        if (s_suppkey[i] > max_suppkey) max_suppkey = s_suppkey[i];
    }
    std::vector<uint8_t> saudi_supplier_bitmap(max_suppkey + 1, 0);
    std::vector<uint32_t> saudi_supplier_positions;
    saudi_supplier_positions.reserve(4000);
    for (size_t i = 0; i < supplier_count; ++i) {
        if (s_nationkey[i] == target_nationkey) {
            saudi_supplier_bitmap[s_suppkey[i]] = 1;
            saudi_supplier_positions.push_back(i);
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

    // Build bitmap of orders with status 'F' - O(1) lookup
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif
    int32_t max_orderkey = 0;
    for (size_t i = 0; i < orders_count; ++i) {
        if (o_orderkey[i] > max_orderkey) max_orderkey = o_orderkey[i];
    }
    std::vector<uint8_t> f_orders_bitmap(max_orderkey + 1, 0);
    for (size_t i = 0; i < orders_count; ++i) {
        if (o_orderstatus[i] == f_code) {
            f_orders_bitmap[o_orderkey[i]] = 1;
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
    auto lineitem_orderkey_index = load_hash_multi_index(gendb_dir + "/indexes/lineitem_orderkey_hash.bin");

    // Load supplier names for late materialization
    auto s_name = load_string_column(gendb_dir + "/supplier/s_name.bin", supplier_count);

    // FUSED: Subquery decorrelation + Main scan + Aggregation in single pass
#ifdef GENDB_PROFILE
    auto t_fused_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread count
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;

    // Thread-local aggregation tables
    std::vector<CompactAggTable> thread_agg(num_threads, CompactAggTable(200));

    // Parallel scan of lineitem
    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();

        #pragma omp for schedule(dynamic, 10000)
        for (size_t i = 0; i < lineitem_count; ++i) {
            // Filter: l1.l_receiptdate > l1.l_commitdate (late receipt)
            if (l_receiptdate[i] <= l_commitdate[i]) continue;

            int32_t orderkey = l_orderkey[i];
            int32_t suppkey = l_suppkey[i];

            // Filter: o_orderstatus = 'F' (bitmap lookup)
            if (orderkey > max_orderkey || !f_orders_bitmap[orderkey]) continue;

            // Filter: supplier from SAUDI ARABIA (bitmap lookup)
            if (suppkey > max_suppkey || !saudi_supplier_bitmap[suppkey]) continue;

            // EXISTS check: order must have multiple suppliers
            // Get all lineitems for this order from pre-built index
            uint32_t* li_positions;
            uint32_t li_count;
            if (!lineitem_orderkey_index.find(orderkey, &li_positions, &li_count)) continue;

            // Check if order has multiple distinct suppliers
            bool has_multiple_suppliers = false;
            for (uint32_t j = 0; j < li_count; ++j) {
                if (l_suppkey[li_positions[j]] != suppkey) {
                    has_multiple_suppliers = true;
                    break;
                }
            }
            if (!has_multiple_suppliers) continue;

            // NOT EXISTS check: no OTHER supplier with late receipt
            bool has_other_late = false;
            for (uint32_t j = 0; j < li_count; ++j) {
                uint32_t pos = li_positions[j];
                if (l_suppkey[pos] != suppkey &&
                    l_receiptdate[pos] > l_commitdate[pos]) {
                    has_other_late = true;
                    break;
                }
            }
            if (has_other_late) continue;

            // Find supplier position in supplier table
            // Binary search in saudi_supplier_positions (small array ~4K)
            uint32_t supp_pos = 0;
            for (uint32_t sp : saudi_supplier_positions) {
                if (s_suppkey[sp] == suppkey) {
                    supp_pos = sp;
                    break;
                }
            }

            // Aggregate by supplier position (not name yet - defer string ops)
            thread_agg[tid].add(supp_pos, 1);
        }
    }

    // Merge thread-local aggregation tables
    CompactAggTable final_agg(200);
    for (int t = 0; t < num_threads; ++t) {
        final_agg.merge_from(thread_agg[t]);
    }

#ifdef GENDB_PROFILE
    auto t_fused_end = std::chrono::high_resolution_clock::now();
    double ms_fused = std::chrono::duration<double, std::milli>(t_fused_end - t_fused_start).count();
    printf("[TIMING] fused_scan_aggregation: %.2f ms\n", ms_fused);
#endif

    // Materialize supplier names and sort results
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<SupplierAgg> results;
    results.reserve(200);
    for (const auto& entry : final_agg.table) {
        if (entry.occupied) {
            results.push_back({s_name[entry.supp_idx], entry.count});
        }
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
