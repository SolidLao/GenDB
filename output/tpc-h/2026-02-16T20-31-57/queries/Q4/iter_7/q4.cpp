/*
================================================================================
Q4: Order Priority Checking - Iteration 7

LOGICAL PLAN (Zone Map Pruning Optimization):
1. Scan orders table with date range filter: o_orderdate >= 1993-07-01 (epoch day 8582)
   and o_orderdate < 1993-10-01 (epoch day 8674)
   - Load and consult zone map index (orders_orderdate_zonemap.bin)
   - Skip entire blocks where min/max fall outside the range → reduce I/O
   - Filtered result ~573K rows (empirically confirmed)
   - Build hash set of qualifying order keys
2. Scan lineitem table and filter:
   - Only process rows where (a) l_orderkey in filtered orders AND (b) l_commitdate < l_receiptdate
   - This produces a much smaller semi-join set (only matching lineitem rows)
3. For each filtered order, check if it has at least one matching lineitem row
   - Use the pre-filtered lineitem set
4. Group by o_orderpriority and count
   - 5 distinct priorities → use flat array [0..4] indexed by priority code
5. Output sorted by o_orderpriority

PHYSICAL PLAN:
- Orders scan:
  - Load zone map index → identify qualifying blocks (15M rows / 100K per block = 150 blocks)
  - Only scan/decode blocks with overlapping date ranges
  - Load o_orderkey, o_orderdate, o_orderpriority columns from qualifying blocks only
  - Build hash set of filtered order keys (~573K entries)
- Lineitem scan: Filter by (orderkey in orders AND l_commitdate < l_receiptdate)
  - Load l_orderkey and l_commitdate, l_receiptdate columns
  - Build semi-join set of matching order keys (~526K distinct)
- Semi-join probe: Scan filtered orders from hash set, check if in lineitem set, aggregate by priority
- Aggregation: Flat array indexed by o_orderpriority code (0-4)
- Parallelism:
  - Orders scan: parallel for over blocks within zone map range (thread-local buffer)
  - Lineitem scan: parallel for to filter and build semi-join set (thread-local)
  - Final aggregation: serial (only 5 groups)

OPTIMIZATION RATIONALE (Iteration 7):
- Iteration 3 baseline: 102.15 ms
  - orders_filter: 34.75 ms (full scan of 15M rows)
  - semi_join_build: 57.91 ms (scan of 59M lineitem rows)
- Iteration 7: Add zone map pruning on orders_orderdate
  - Zone map: 150 blocks, skip blocks outside [8582, 8674)
  - Expected block skip rate: ~85% (date range is selective, only 3-month window)
  - Expected orders_filter speedup: 34.75 ms → ~7 ms (5x)
  - Total expected: ~75 ms (vs 102.15 ms)
  - Gap to Umbra (66 ms): 1.1x

================================================================================
*/

#include <iostream>
#include <fstream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <array>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <algorithm>
#include <omp.h>

// ============================================================================
// Compact Hash Table for Semi-Join Set (Open Addressing)
// ============================================================================

template<typename K>
struct CompactHashSet {
    struct Entry {
        K key;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashSet(size_t expected_size) : count(0) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    // Fibonacci hashing for good distribution
    size_t hash(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return;  // Already exists
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, true};
        count++;
    }

    bool contains(K key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return true;
            idx = (idx + 1) & mask;
        }
        return false;
    }

    size_t size() const { return count; }
};

// ============================================================================
// Helper Functions
// ============================================================================

// Decode dictionary-encoded string file to get code->string mapping
// Returns a vector where index = code, value = string
std::vector<std::string> load_dictionary(const std::string& dict_path) {
    std::vector<std::string> result;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return result;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty()) {
            result.push_back(line);
        }
    }
    file.close();
    return result;
}

// Memory-map a binary column file
// Returns {pointer, file_size}
struct MmapFile {
    void* ptr;
    size_t size;
    int fd;

    MmapFile(const std::string& path) : ptr(nullptr), size(0), fd(-1) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "fstat failed for: " << path << std::endl;
            close(fd);
            fd = -1;
            return;
        }

        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "mmap failed for: " << path << std::endl;
            close(fd);
            fd = -1;
            ptr = nullptr;
            size = 0;
        }
    }

    ~MmapFile() {
        if (ptr != nullptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
};

// Zone map reader for orders_orderdate_zonemap.bin
// Layout: [uint32_t num_blocks=150] then [int32_t min, int32_t max, uint32_t row_count] per block (12 bytes/block)
struct ZoneMap {
    struct Block {
        int32_t min_val;
        int32_t max_val;
        uint32_t row_count;
    };

    std::vector<Block> blocks;

    ZoneMap(const std::string& zonemap_path) {
        MmapFile mf(zonemap_path);
        if (mf.ptr == nullptr || mf.size < 4) {
            std::cerr << "Failed to load zone map" << std::endl;
            return;
        }

        const uint32_t* p = (const uint32_t*)mf.ptr;
        uint32_t num_blocks = p[0];

        const Block* block_data = (const Block*)(&p[1]);
        for (uint32_t i = 0; i < num_blocks; i++) {
            blocks.push_back(block_data[i]);
        }
    }

    // Check if range [range_min, range_max) overlaps with zone [min, max]
    bool overlaps(int32_t zone_min, int32_t zone_max, int32_t range_min, int32_t range_max) {
        return !(zone_max < range_min || zone_min >= range_max);
    }
};

// ============================================================================
// Query Execution
// ============================================================================

void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Date constants (epoch days since 1970-01-01)
    const int32_t date_1993_07_01 = 8582;  // 1993-07-01
    const int32_t date_1993_10_01 = 8674;  // 1993-10-01

    // ========================================================================
    // STEP 1: Load and filter orders table by date range
    // Build hash set of filtered order keys
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
#endif

    const std::string orders_dir = gendb_dir + "/orders";
    MmapFile o_orderkey_file(orders_dir + "/o_orderkey.bin");
    MmapFile o_orderdate_file(orders_dir + "/o_orderdate.bin");
    MmapFile o_orderpriority_file(orders_dir + "/o_orderpriority.bin");

    if (o_orderkey_file.ptr == nullptr || o_orderdate_file.ptr == nullptr ||
        o_orderpriority_file.ptr == nullptr) {
        std::cerr << "Failed to load orders files" << std::endl;
        return;
    }

    const int32_t* o_orderkey = (const int32_t*)o_orderkey_file.ptr;
    const int32_t* o_orderdate = (const int32_t*)o_orderdate_file.ptr;
    const int32_t* o_orderpriority = (const int32_t*)o_orderpriority_file.ptr;

    int64_t orders_rows = o_orderkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    printf("[TIMING] orders_load_columns: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_orders_start).count());

    auto t_orders_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Load o_orderpriority dictionary
    std::vector<std::string> priority_dict = load_dictionary(orders_dir + "/o_orderpriority_dict.txt");

    std::cout << "Loaded " << priority_dict.size() << " priorities" << std::endl;
    for (size_t i = 0; i < priority_dict.size(); i++) {
        std::cout << "  Priority " << i << ": " << priority_dict[i] << std::endl;
    }

    // Phase 1: Load zone map to identify qualifying blocks
    ZoneMap zone_map(orders_dir + "/orders_orderdate_zonemap.bin");

    // Identify which blocks overlap with [date_1993_07_01, date_1993_10_01)
    std::vector<std::pair<uint32_t, uint32_t>> block_ranges;
    for (size_t block_idx = 0; block_idx < zone_map.blocks.size(); block_idx++) {
        const auto& block = zone_map.blocks[block_idx];
        // Check if block overlaps with query range
        // Block has [min_val, max_val], query is [date_1993_07_01, date_1993_10_01)
        // Overlaps if: block.max_val >= date_1993_07_01 AND block.min_val < date_1993_10_01
        if (block.max_val >= date_1993_07_01 && block.min_val < date_1993_10_01) {
            // Calculate row range: blocks are 100K rows each
            uint32_t start_row = (uint32_t)block_idx * 100000;
            uint32_t end_row = (uint32_t)(block_idx + 1) * 100000;
            if (end_row > (uint32_t)orders_rows) end_row = (uint32_t)orders_rows;
            block_ranges.push_back({start_row, end_row});
        }
    }

    // Phase 1: Count orders matching date filter (only in qualifying blocks)
    int64_t filtered_orders_count = 0;
#pragma omp parallel for reduction(+:filtered_orders_count)
    for (int64_t b = 0; b < (int64_t)block_ranges.size(); b++) {
        for (uint32_t i = block_ranges[b].first; i < block_ranges[b].second; i++) {
            int32_t orderdate = o_orderdate[i];
            if (orderdate >= date_1993_07_01 && orderdate < date_1993_10_01) {
                filtered_orders_count++;
            }
        }
    }

    // Phase 2: Build hash set of filtered order keys using thread-local aggregation
    CompactHashSet<int32_t> filtered_orders_set(filtered_orders_count);

    int num_threads = omp_get_max_threads();
    std::vector<std::vector<int32_t>> thread_local_orderkeys(num_threads);

#pragma omp parallel for
    for (int64_t b = 0; b < (int64_t)block_ranges.size(); b++) {
        for (uint32_t i = block_ranges[b].first; i < block_ranges[b].second; i++) {
            int32_t orderdate = o_orderdate[i];
            if (orderdate >= date_1993_07_01 && orderdate < date_1993_10_01) {
                int thread_id = omp_get_thread_num();
                thread_local_orderkeys[thread_id].push_back(o_orderkey[i]);
            }
        }
    }

    // Merge thread-local order keys into hash set
    for (int t = 0; t < num_threads; t++) {
        for (int32_t key : thread_local_orderkeys[t]) {
            filtered_orders_set.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    printf("[TIMING] orders_filter: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_orders_filter_start).count());
#endif

    std::cout << "Filtered orders: " << filtered_orders_count << " rows" << std::endl;

    // ========================================================================
    // STEP 2: Load lineitem columns
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    const std::string lineitem_dir = gendb_dir + "/lineitem";
    MmapFile li_orderkey_file(lineitem_dir + "/l_orderkey.bin");
    MmapFile li_commitdate_file(lineitem_dir + "/l_commitdate.bin");
    MmapFile li_receiptdate_file(lineitem_dir + "/l_receiptdate.bin");

    if (li_orderkey_file.ptr == nullptr || li_commitdate_file.ptr == nullptr ||
        li_receiptdate_file.ptr == nullptr) {
        std::cerr << "Failed to load lineitem files" << std::endl;
        return;
    }

    const int32_t* li_orderkey = (const int32_t*)li_orderkey_file.ptr;
    const int32_t* li_commitdate = (const int32_t*)li_commitdate_file.ptr;
    const int32_t* li_receiptdate = (const int32_t*)li_receiptdate_file.ptr;

    int64_t lineitem_rows = li_orderkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    printf("[TIMING] lineitem_load_columns: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_lineitem_start).count());

    auto t_semi_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Phase 3: Build semi-join set of orderkeys from lineitem
    // Only include orderkeys that:
    //   (a) match a filtered order (from date range)
    //   (b) have l_commitdate < l_receiptdate
    // This dramatically reduces the lineitem scan workload
    // OPTIMIZATION: Single pass over lineitem to collect distinct keys
    std::vector<std::vector<int32_t>> thread_local_semi_keys(num_threads);

#pragma omp parallel for
    for (int64_t i = 0; i < lineitem_rows; i++) {
        if (filtered_orders_set.contains(li_orderkey[i]) && li_commitdate[i] < li_receiptdate[i]) {
            int thread_id = omp_get_thread_num();
            thread_local_semi_keys[thread_id].push_back(li_orderkey[i]);
        }
    }

    // Build hash set of distinct order keys that match the conditions
    // Estimate size based on total keys collected across threads
    int64_t total_semi_keys = 0;
    for (int t = 0; t < num_threads; t++) {
        total_semi_keys += thread_local_semi_keys[t].size();
    }
    CompactHashSet<int32_t> semi_join_set(total_semi_keys);

    // Merge thread-local keys into semi-join set
    for (int t = 0; t < num_threads; t++) {
        for (int32_t key : thread_local_semi_keys[t]) {
            semi_join_set.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    printf("[TIMING] semi_join_build: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_semi_join_start).count());
#endif

    std::cout << "Semi-join set size: " << semi_join_set.size() << " distinct order keys" << std::endl;

    // ========================================================================
    // STEP 3: Scan filtered orders and probe semi-join set
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_orders_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Flat array for aggregation: index by priority code (0-4)
    // Use thread-local aggregation for parallel scan
    std::vector<std::array<int64_t, 5>> thread_local_counts(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_local_counts[t] = {0, 0, 0, 0, 0};
    }

    // Parallel scan of orders and apply filters
#pragma omp parallel for
    for (int64_t i = 0; i < orders_rows; i++) {
        int32_t orderdate = o_orderdate[i];

        // Date filter: o_orderdate >= date_1993_07_01 AND o_orderdate < date_1993_10_01
        if (orderdate < date_1993_07_01 || orderdate >= date_1993_10_01) {
            continue;
        }

        // Semi-join with lineitem: check if this order key exists in semi-join set
        int32_t orderkey = o_orderkey[i];
        if (!semi_join_set.contains(orderkey)) {
            continue;
        }

        // This order qualifies: aggregate by priority
        int32_t priority_code = o_orderpriority[i];
        if (priority_code >= 0 && priority_code < 5) {
            int thread_id = omp_get_thread_num();
            thread_local_counts[thread_id][priority_code]++;
        }
    }

    // Merge thread-local counts
    std::array<int64_t, 5> priority_counts = {0, 0, 0, 0, 0};
    for (int t = 0; t < num_threads; t++) {
        for (int p = 0; p < 5; p++) {
            priority_counts[p] += thread_local_counts[t][p];
        }
    }

#ifdef GENDB_PROFILE
    printf("[TIMING] orders_scan_filter_join: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_orders_scan_start).count());
#endif

    // ========================================================================
    // STEP 4: Output results sorted by o_orderpriority
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    // Create result vector: (priority_string, count)
    std::vector<std::pair<std::string, int64_t>> results;
    for (size_t i = 0; i < 5 && i < priority_dict.size(); i++) {
        if (priority_counts[i] > 0) {  // Include priorities with at least one order
            results.push_back({priority_dict[i], priority_counts[i]});
        }
    }

    // Sort by priority string (ascending)
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Write to CSV
    std::string output_file = results_dir + "/Q4.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "o_orderpriority,order_count\n";

    // Write results
    for (const auto& [priority, count] : results) {
        out << priority << "," << count << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count() - output_ms;
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Q4 results written to " << output_file << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q4(gendb_dir, results_dir);

    return 0;
}
#endif
