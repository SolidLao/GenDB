/*
 * Q21: Suppliers Who Kept Orders Waiting
 *
 * LOGICAL PLAN (REWRITTEN FOR ITERATION 6 - STALL RECOVERY):
 * 1. Filter nation for n_name = 'SAUDI ARABIA' → extract target_nationkey
 * 2. Build hash set of SAUDI suppliers (s_nationkey = target_nationkey) → ~4K suppliers
 * 3. Filter orders for o_orderstatus = 'F' → build hash set → ~7.5M orderkeys
 * 4. Single-pass FUSED lineitem scan with inline EXISTS/NOT EXISTS evaluation:
 *    - For each lineitem l1 where l_receiptdate > l_commitdate:
 *      - Check l1.l_suppkey in SAUDI suppliers → skip if not
 *      - Check l1.l_orderkey in 'F' orders → skip if not
 *      - Use pre-built lineitem_orderkey_hash to get ALL lineitems for this order
 *      - Inline check: EXISTS(other supplier) AND NOT EXISTS(other late supplier)
 *      - If passes: aggregate directly into partitioned hash tables
 * 5. Merge partition results, sort, output top 100
 *
 * PHYSICAL PLAN (NEW ARCHITECTURE):
 * - Filters: bitmap for SAUDI suppliers, hash set for 'F' orders
 * - Lineitem scan: PARALLEL with morsel-driven chunking (10K rows per morsel)
 * - EXISTS/NOT EXISTS: INLINE evaluation using pre-built lineitem_orderkey_hash
 *   - No intermediate OrderInfo structures
 *   - Direct check per qualifying l1: iterate order's lineitems once
 * - Aggregation: PARTITIONED by s_name hash → 64 partitions, no merge lock contention
 * - Supplier name lookup: late materialization via pre-built supplier_suppkey_hash
 *
 * KEY DIFFERENCES FROM PREVIOUS ITERATIONS:
 * 1. NO separate subquery decorrelation phase (eliminates 32s bottleneck)
 * 2. NO OrderInfo map with 15M entries
 * 3. NO second lineitem scan
 * 4. Parallel processing with partitioned aggregation (no atomic CAS needed)
 * 5. Inline EXISTS/NOT EXISTS checks using index lookup (fast random access)
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
    // pos_count = after_entries[0]; // Not needed for lookup
    index.positions = after_entries + 1;

    return index;
}

// Aggregation structure
struct SupplierAgg {
    std::string s_name;
    int32_t numwait;
};

// Compact hash table for aggregation (open-addressing, better cache locality than std::unordered_map)
struct AggregationEntry {
    std::string key;
    int32_t count;
    bool occupied;
};

struct AggregationTable {
    std::vector<AggregationEntry> entries;
    size_t capacity;
    size_t size;

    AggregationTable(size_t cap = 128) : capacity(cap), size(0) {
        entries.resize(capacity);
        for (auto& e : entries) e.occupied = false;
    }

    void insert_or_increment(const std::string& key) {
        size_t h = std::hash<std::string>{}(key);
        size_t idx = h & (capacity - 1);

        for (size_t probe = 0; probe < capacity; ++probe) {
            size_t pos = (idx + probe) & (capacity - 1);
            if (!entries[pos].occupied) {
                entries[pos].key = key;
                entries[pos].count = 1;
                entries[pos].occupied = true;
                size++;
                return;
            }
            if (entries[pos].key == key) {
                entries[pos].count++;
                return;
            }
        }
    }

    void merge(const AggregationTable& other) {
        for (const auto& e : other.entries) {
            if (e.occupied) {
                insert_or_increment(e.key);
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

    // Build bitmap for qualifying supplier keys (SAUDI ARABIA suppliers)
    // Supplier keys range 1-100000, use bitmap for O(1) lookup
    constexpr size_t MAX_SUPPKEY = 100001;
    std::vector<bool> is_saudi_supplier(MAX_SUPPKEY, false);
    for (size_t i = 0; i < supplier_count; ++i) {
        if (s_nationkey[i] == target_nationkey) {
            is_saudi_supplier[s_suppkey[i]] = true;
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

    // Load pre-built indexes (need these for inline EXISTS/NOT EXISTS checks)
    auto lineitem_orderkey_index = load_hash_multi_index(gendb_dir + "/indexes/lineitem_orderkey_hash.bin");
    auto supplier_index = load_hash_single_index(gendb_dir + "/indexes/supplier_suppkey_hash.bin");

    // Load supplier names for late materialization
    auto s_name = load_string_column(gendb_dir + "/supplier/s_name.bin", supplier_count);

    // Single-pass fused scan with inline EXISTS/NOT EXISTS evaluation
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Partitioned aggregation: 64 partitions to avoid lock contention
    constexpr int NUM_PARTITIONS = 64;
    std::vector<AggregationTable> partitions(NUM_PARTITIONS);

    // Parallel scan with morsel-driven approach
    const size_t morsel_size = 10000;
    std::atomic<size_t> next_morsel{0};
    const size_t num_morsels = (lineitem_count + morsel_size - 1) / morsel_size;

    #pragma omp parallel
    {
        AggregationTable local_partitions[NUM_PARTITIONS];
        for (int p = 0; p < NUM_PARTITIONS; ++p) {
            local_partitions[p] = AggregationTable(128);
        }

        while (true) {
            size_t morsel_id = next_morsel.fetch_add(1, std::memory_order_relaxed);
            if (morsel_id >= num_morsels) break;

            size_t start = morsel_id * morsel_size;
            size_t end = std::min(start + morsel_size, lineitem_count);

            for (size_t i = start; i < end; ++i) {
                // Filter: l1.l_receiptdate > l1.l_commitdate
                if (l_receiptdate[i] <= l_commitdate[i]) continue;

                int32_t orderkey = l_orderkey[i];
                int32_t suppkey = l_suppkey[i];

                // Early filter: supplier must be from SAUDI ARABIA
                if (suppkey < 0 || (size_t)suppkey >= MAX_SUPPKEY || !is_saudi_supplier[suppkey]) continue;

                // Early filter: order must have status 'F'
                if (f_orders.find(orderkey) == f_orders.end()) continue;

                // Inline EXISTS check: order must have multiple suppliers
                // Get all lineitems for this order using pre-built index
                uint32_t* order_positions = nullptr;
                uint32_t order_count = 0;
                if (!lineitem_orderkey_index.find(orderkey, &order_positions, &order_count)) continue;

                // Check if there's another supplier in this order
                bool has_other_supplier = false;
                bool has_other_late_supplier = false;

                for (uint32_t j = 0; j < order_count; ++j) {
                    uint32_t pos = order_positions[j];
                    int32_t other_suppkey = l_suppkey[pos];

                    if (other_suppkey != suppkey) {
                        has_other_supplier = true;

                        // Check if this other supplier also has late receipt
                        if (l_receiptdate[pos] > l_commitdate[pos]) {
                            has_other_late_supplier = true;
                            break; // Early exit: NOT EXISTS fails
                        }
                    }
                }

                // EXISTS check
                if (!has_other_supplier) continue;

                // NOT EXISTS check
                if (has_other_late_supplier) continue;

                // Get supplier name via index and aggregate
                uint32_t* supp_pos = supplier_index.find(suppkey);
                if (supp_pos) {
                    const std::string& name = s_name[*supp_pos];
                    // Partition by hash of supplier name
                    size_t h = std::hash<std::string>{}(name);
                    int partition_id = h & (NUM_PARTITIONS - 1);
                    local_partitions[partition_id].insert_or_increment(name);
                }
            }
        }

        // Merge local partitions into global partitions
        #pragma omp critical
        {
            for (int p = 0; p < NUM_PARTITIONS; ++p) {
                partitions[p].merge(local_partitions[p]);
            }
        }
    }

    // Collect results from all partitions
    std::unordered_map<std::string, int32_t> supplier_counts;
    supplier_counts.reserve(100);
    for (int p = 0; p < NUM_PARTITIONS; ++p) {
        for (const auto& e : partitions[p].entries) {
            if (e.occupied) {
                supplier_counts[e.key] += e.count;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] fused_scan_aggregation: %.2f ms\n", ms_scan);
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
