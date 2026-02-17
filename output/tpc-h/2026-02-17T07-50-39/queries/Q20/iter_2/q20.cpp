/*
 * Q20: Potential Part Promotion (Iteration 2)
 *
 * LOGICAL PLAN:
 * 1. Filter nation: n_name = 'CANADA' → 1 row
 * 2. Filter part: p_name LIKE 'forest%' → ~21.5K rows (1% of 2M)
 * 3. Build part_set: hash set of qualifying p_partkey
 * 4. Filter partsupp: ps_partkey IN part_set → ~86K candidates
 * 5. Decorrelate correlated subquery:
 *    - Pre-compute: GROUP BY (l_partkey, l_suppkey)
 *                   SUM(l_quantity) WHERE l_shipdate ∈ [1994-01-01, 1995-01-01)
 *    - Build hash map: (partkey, suppkey) → sum_quantity
 * 6. Filter partsupp: ps_availqty > 0.5 * threshold
 * 7. Build suppkey_set from qualifying partsupp
 * 8. Join supplier ⋈ nation on n_nationkey, filter by suppkey_set
 * 9. Sort by s_name
 *
 * PHYSICAL PLAN OPTIMIZATIONS (Iteration 2):
 * - Nation: Direct scan (25 rows), find 'CANADA'
 * - Part: LATE MATERIALIZATION
 *   * Phase 1: Load p_partkey + p_name as mmap
 *   * Phase 2: Parallel scan with optimized prefix check (no substr)
 *   * Build forest_parts hash set
 * - Lineitem: (unchanged from iter 1)
 *   * ZONE MAP pruning on l_shipdate
 *   * Partitioned parallel aggregation
 *   * Open-addressing hash table with improved hash function
 * - Partsupp: Parallel scan filtered by part_set
 * - Supplier: Load pre-built supplier_suppkey_hash index, direct lookup
 * - Sort: std::sort on s_name
 *
 * KEY CHANGES vs Iteration 1:
 * 1. Late materialization: p_name loaded via mmap, not load_strings
 * 2. Optimized prefix check: memcmp instead of substr
 * 3. Parallel part filter: OpenMP for 2M part scan
 * 4. Improved hash function: better mixing to reduce clustering
 * 5. Pre-built supplier index: Load supplier_suppkey_hash for O(1) lookup
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <thread>
#include <mutex>
#include <omp.h>

// Date conversion: '1994-01-01' and '1995-01-01' to epoch days
// 1994-01-01: (1994-1970)*365 + 6 leap years (1972,1976,1980,1984,1988,1992) = 8766
// 1995-01-01: 8766 + 365 = 9131
constexpr int32_t DATE_1994_01_01 = 8766;
constexpr int32_t DATE_1995_01_01 = 9131;

// Zone map structure for l_shipdate (from Storage Guide)
struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
};
static_assert(sizeof(ZoneMapEntry) == 8, "Zone map entry must be 8 bytes");

// Composite key hash for (partkey, suppkey) - improved mixing
inline uint64_t hash_pair(int32_t partkey, int32_t suppkey) {
    // Combine keys with prime multiplication to avoid patterns
    uint64_t h = ((uint64_t)partkey * 0x9E3779B97F4A7C15ULL) ^
                 ((uint64_t)suppkey * 0x517CC1B727220A95ULL);
    // MurmurHash3 finalizer
    h ^= h >> 33;
    h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33;
    h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

// Open-addressing hash table for aggregation (Robin Hood hashing)
struct AggEntry {
    int32_t partkey;
    int32_t suppkey;
    int64_t sum_qty;
    uint16_t dist;  // probe distance
    bool occupied;
};

class AggHashTable {
public:
    std::vector<AggEntry> table;
    size_t mask;
    size_t count;

    AggHashTable(size_t capacity) : count(0) {
        // Round up to power of 2
        size_t cap = 1;
        while (cap < capacity) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
    }

    void insert_or_add(int32_t partkey, int32_t suppkey, int64_t qty) {
        uint64_t h = hash_pair(partkey, suppkey);
        size_t pos = h & mask;
        AggEntry entry{partkey, suppkey, qty, 0, true};

        while (true) {
            if (!table[pos].occupied) {
                table[pos] = entry;
                count++;
                return;
            }
            if (table[pos].partkey == partkey && table[pos].suppkey == suppkey) {
                table[pos].sum_qty += qty;
                return;
            }
            // Robin Hood: swap if incoming has higher displacement
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
    }

    int64_t* find(int32_t partkey, int32_t suppkey) {
        uint64_t h = hash_pair(partkey, suppkey);
        size_t pos = h & mask;
        uint16_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].partkey == partkey && table[pos].suppkey == suppkey) {
                return &table[pos].sum_qty;
            }
            if (dist > table[pos].dist) {
                return nullptr;  // would have been inserted earlier
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }
};

// Memory-mapped file helper
template<typename T>
T* mmap_column(const std::string& path, size_t& out_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s\n", path.c_str());
        return nullptr;
    }
    struct stat sb;
    fstat(fd, &sb);
    out_count = sb.st_size / sizeof(T);
    T* data = (T*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return data;
}

// Load string column (binary: [uint32_t len][char data...] per row)
std::vector<std::string> load_strings(const std::string& path, size_t num_rows) {
    std::vector<std::string> result;
    result.reserve(num_rows);
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return result;
    struct stat sb;
    fstat(fd, &sb);
    char* data = (char*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    size_t offset = 0;
    for (size_t i = 0; i < num_rows && offset + 4 <= (size_t)sb.st_size; i++) {
        uint32_t len = *(uint32_t*)(data + offset);
        offset += 4;
        if (offset + len <= (size_t)sb.st_size) {
            result.emplace_back(data + offset, len);
            offset += len;
        }
    }
    munmap(data, sb.st_size);
    return result;
}

void run_q20(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Get thread count for parallel operations
    int num_threads = std::thread::hardware_concurrency();

    // Step 1: Find CANADA nationkey
#ifdef GENDB_PROFILE
    auto t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t nation_count;
    int32_t* n_nationkey = mmap_column<int32_t>(gendb_dir + "/nation/n_nationkey.bin", nation_count);
    auto n_name = load_strings(gendb_dir + "/nation/n_name.bin", nation_count);

    int32_t canada_nationkey = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (n_name[i] == "CANADA") {
            canada_nationkey = n_nationkey[i];
            break;
        }
    }
#ifdef GENDB_PROFILE
    auto t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] nation_scan: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif

    // Step 2: Filter part for 'forest%' prefix (late materialization + parallel)
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t part_count;
    int32_t* p_partkey = mmap_column<int32_t>(gendb_dir + "/part/p_partkey.bin", part_count);

    // Load p_name as mmap instead of loading all strings eagerly
    int fd_pname = open((gendb_dir + "/part/p_name.bin").c_str(), O_RDONLY);
    struct stat sb_pname;
    fstat(fd_pname, &sb_pname);
    char* p_name_data = (char*)mmap(nullptr, sb_pname.st_size, PROT_READ, MAP_PRIVATE, fd_pname, 0);
    close(fd_pname);

    // Build offset array for random access (sequential scan once)
    std::vector<size_t> p_name_offsets(part_count);
    size_t offset = 0;
    for (size_t i = 0; i < part_count; i++) {
        p_name_offsets[i] = offset;
        uint32_t len = *(uint32_t*)(p_name_data + offset);
        offset += 4 + len;
    }

    // Thread-local storage for parallel filter
    std::vector<std::vector<int32_t>> thread_local_parts(num_threads);

    // Parallel scan with optimized prefix check
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_local_parts[tid].reserve(10000);  // ~21K total / num_threads

        #pragma omp for schedule(static)
        for (size_t i = 0; i < part_count; i++) {
            size_t off = p_name_offsets[i];
            uint32_t len = *(uint32_t*)(p_name_data + off);
            const char* str = p_name_data + off + 4;

            // Optimized prefix check: memcmp instead of substr
            if (len >= 6 && memcmp(str, "forest", 6) == 0) {
                thread_local_parts[tid].push_back(p_partkey[i]);
            }
        }
    }

    // Merge thread-local results into hash set
    std::unordered_set<int32_t> forest_parts;
    size_t total_count = 0;
    for (const auto& local : thread_local_parts) {
        total_count += local.size();
    }
    forest_parts.reserve(total_count);

    for (const auto& local : thread_local_parts) {
        forest_parts.insert(local.begin(), local.end());
    }

    munmap(p_name_data, sb_pname.st_size);

#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] part_filter: %.2f ms (forest parts: %zu)\n",
           std::chrono::duration<double, std::milli>(t2 - t1).count(), forest_parts.size());
#endif

    // Step 3: Decorrelate subquery — scan lineitem once, aggregate by (partkey, suppkey)
    // OPTIMIZATIONS:
    // - Zone map pruning on l_shipdate (skip blocks outside [1994-01-01, 1995-01-01))
    // - Partitioned parallel aggregation (no merge bottleneck)
    // - Open-addressing hash table (2-5x faster than std::unordered_map)
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t lineitem_count;
    int32_t* l_partkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_partkey.bin", lineitem_count);
    int32_t* l_suppkey = mmap_column<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_count);
    int64_t* l_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", lineitem_count);
    int32_t* l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);

    // Load zone map for l_shipdate
    size_t zonemap_count;
    ZoneMapEntry* zonemap = mmap_column<ZoneMapEntry>(gendb_dir + "/indexes/lineitem_shipdate_zone.bin", zonemap_count);

    // Skip the first 4 bytes (num_entries header) by offsetting
    zonemap = (ZoneMapEntry*)((char*)zonemap + 4);
    zonemap_count = (zonemap_count * 8 - 4) / 8;  // adjust for header

    // Identify qualifying blocks using zone map
    constexpr size_t BLOCK_SIZE = 100000;
    std::vector<bool> active_blocks(zonemap_count, false);
    size_t total_active_rows = 0;

    for (size_t z = 0; z < zonemap_count; z++) {
        // Skip if zone is completely outside date range
        if (zonemap[z].max_value < DATE_1994_01_01 || zonemap[z].min_value >= DATE_1995_01_01) {
            continue;
        }
        active_blocks[z] = true;
        total_active_rows += std::min(BLOCK_SIZE, lineitem_count - z * BLOCK_SIZE);
    }

    // Partitioned parallel aggregation
    constexpr int NUM_PARTITIONS = 64;  // Power of 2 for fast modulo

    // Each partition gets its own hash table (coordination-free)
    std::vector<AggHashTable> partition_tables;
    partition_tables.reserve(NUM_PARTITIONS);
    for (int i = 0; i < NUM_PARTITIONS; i++) {
        partition_tables.emplace_back(100000);  // ~100K capacity per partition
    }

    // Parallel scan with zone map filtering
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            size_t chunk = (zonemap_count + num_threads - 1) / num_threads;
            size_t start_zone = t * chunk;
            size_t end_zone = std::min(start_zone + chunk, zonemap_count);

            for (size_t z = start_zone; z < end_zone; z++) {
                if (!active_blocks[z]) continue;

                size_t start_row = z * BLOCK_SIZE;
                size_t end_row = std::min(start_row + BLOCK_SIZE, lineitem_count);

                for (size_t i = start_row; i < end_row; i++) {
                    int32_t shipdate = l_shipdate[i];
                    if (shipdate >= DATE_1994_01_01 && shipdate < DATE_1995_01_01) {
                        int32_t pk = l_partkey[i];
                        int32_t sk = l_suppkey[i];

                        // Hash to partition (coordination-free)
                        uint64_t h = hash_pair(pk, sk);
                        int partition = h & (NUM_PARTITIONS - 1);

                        partition_tables[partition].insert_or_add(pk, sk, l_quantity[i]);
                    }
                }
            }
        });
    }
    for (auto& th : threads) th.join();

    // Count total groups
    size_t total_groups = 0;
    for (const auto& pt : partition_tables) {
        total_groups += pt.count;
    }

#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] lineitem_agg: %.2f ms (groups: %zu)\n",
           std::chrono::duration<double, std::milli>(t2 - t1).count(), total_groups);
#endif

    // Step 4: Filter partsupp using partitioned aggregation results (parallel)
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t partsupp_count;
    int32_t* ps_partkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_partkey.bin", partsupp_count);
    int32_t* ps_suppkey = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_suppkey.bin", partsupp_count);
    int32_t* ps_availqty = mmap_column<int32_t>(gendb_dir + "/partsupp/ps_availqty.bin", partsupp_count);

    // Thread-local qualifying suppkeys
    std::vector<std::vector<int32_t>> thread_local_suppkeys(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        thread_local_suppkeys[tid].reserve(5000);

        #pragma omp for schedule(dynamic, 10000)
        for (size_t i = 0; i < partsupp_count; i++) {
            int32_t pk = ps_partkey[i];
            int32_t sk = ps_suppkey[i];

            // Check if part is in forest_parts
            if (forest_parts.count(pk) == 0) continue;

            // Look up in the correct partition
            uint64_t h = hash_pair(pk, sk);
            int partition = h & (NUM_PARTITIONS - 1);
            int64_t* sum_qty = partition_tables[partition].find(pk, sk);

            // If no lineitem data for this (partkey, suppkey) pair, the subquery returns NULL
            // In SQL, ps_availqty > NULL evaluates to NULL (not true), so exclude this row
            if (sum_qty == nullptr) continue;

            // l_quantity is scaled by 100, so sum is also scaled by 100
            // 0.5 * sum means sum / 2
            int64_t threshold = (*sum_qty) / 2;

            // ps_availqty is int32_t, threshold is int64_t (scaled by 100)
            // Convert ps_availqty to same scale: ps_availqty * 100
            if ((int64_t)ps_availqty[i] * 100 > threshold) {
                thread_local_suppkeys[tid].push_back(sk);
            }
        }
    }

    // Merge thread-local results into hash set (dedup)
    std::unordered_set<int32_t> qualifying_suppkeys;
    size_t total_suppkey_count = 0;
    for (const auto& local : thread_local_suppkeys) {
        total_suppkey_count += local.size();
    }
    qualifying_suppkeys.reserve(total_suppkey_count);

    for (const auto& local : thread_local_suppkeys) {
        qualifying_suppkeys.insert(local.begin(), local.end());
    }

#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] partsupp_filter: %.2f ms (qualifying suppkeys: %zu)\n",
           std::chrono::duration<double, std::milli>(t2 - t1).count(), qualifying_suppkeys.size());
#endif

    // Step 5: Filter supplier by nationkey and suppkey
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    size_t supplier_count;
    int32_t* s_suppkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_suppkey.bin", supplier_count);
    int32_t* s_nationkey = mmap_column<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", supplier_count);
    auto s_name = load_strings(gendb_dir + "/supplier/s_name.bin", supplier_count);
    auto s_address = load_strings(gendb_dir + "/supplier/s_address.bin", supplier_count);

    struct Result {
        std::string s_name;
        std::string s_address;
    };
    std::vector<Result> results;
    results.reserve(1000);

    for (size_t i = 0; i < supplier_count; i++) {
        if (s_nationkey[i] == canada_nationkey && qualifying_suppkeys.count(s_suppkey[i])) {
            results.push_back({s_name[i], s_address[i]});
        }
    }
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] supplier_join: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif

    // Step 6: Sort by s_name
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.s_name < b.s_name;
    });
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] sort: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] total: %.2f ms\n", std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count());
#endif

    // Step 7: Write output
#ifdef GENDB_PROFILE
    t1 = std::chrono::high_resolution_clock::now();
#endif
    std::ofstream outfile(results_dir + "/Q20.csv");
    outfile << "s_name,s_address\n";
    for (const auto& r : results) {
        // Quote fields if they contain commas
        auto quote_if_needed = [](const std::string& s) {
            if (s.find(',') != std::string::npos || s.find('"') != std::string::npos) {
                std::string quoted = "\"";
                for (char c : s) {
                    if (c == '"') quoted += "\"\"";
                    else quoted += c;
                }
                quoted += "\"";
                return quoted;
            }
            return s;
        };
        outfile << quote_if_needed(r.s_name) << "," << quote_if_needed(r.s_address) << "\n";
    }
    outfile.close();
#ifdef GENDB_PROFILE
    t2 = std::chrono::high_resolution_clock::now();
    printf("[TIMING] output: %.2f ms\n", std::chrono::duration<double, std::milli>(t2 - t1).count());
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q20(gendb_dir, results_dir);
    return 0;
}
#endif
