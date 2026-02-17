/*
================================================================================
Q4: Order Priority Checking - Iteration 0

LOGICAL PLAN:
1. Scan lineitem table and build hash set of l_orderkey where l_commitdate < l_receiptdate
   - This creates a semi-join set for the EXISTS subquery
   - All lineitem rows checked (59M); filter is selective
2. Scan orders table with date range filter: o_orderdate >= 1993-07-01 (epoch day 8582)
   and o_orderdate < 1993-10-01 (epoch day 8674)
   - Use zone map to skip blocks outside the range
   - Filtered result ~1.6M rows (based on workload analysis)
3. Semi-join: probe orders with lineitem semi-join set
   - Keep only orders that have at least one matching lineitem with l_commitdate < l_receiptdate
4. Group by o_orderpriority and count
   - 5 distinct priorities → use flat array [0..4] indexed by priority code
5. Output sorted by o_orderpriority (natural order from dictionary)

PHYSICAL PLAN:
- Lineitem scan: Load l_orderkey and l_commitdate, l_receiptdate columns via mmap
  - No zone map needed (full scan to build semi-join set)
  - Build open-addressing hash table with l_orderkey values where predicate true
- Orders scan: Use zone map on o_orderdate to skip non-matching blocks
  - Load o_orderkey, o_orderdate, o_orderpriority columns
  - Apply date range filter after zone map pruning
  - Probe with lineitem semi-join set
- Aggregation: Flat array indexed by o_orderpriority code (0-4)
  - Priority codes from dictionary: 0=5-LOW, 1=1-URGENT, 2=4-NOT SPECIFIED, 3=2-HIGH, 4=3-MEDIUM
- Join strategy: Hash semi-join (pre-build set from smaller filtered lineitem, probe orders)
- Parallelism:
  - Lineitem scan with OpenMP parallel for to build semi-join set (thread-local aggregation)
  - Orders scan sequential (zone map logic not easily parallelizable)
  - Aggregation single-threaded (only 5 groups)

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
    // STEP 1: Load lineitem columns and build semi-join set
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

    // Build hash set of distinct orderkey values where l_commitdate < l_receiptdate
    // Use thread-local sets to avoid contention
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_set<int32_t>> thread_local_sets(num_threads);

#pragma omp parallel for
    for (int64_t i = 0; i < lineitem_rows; i++) {
        if (li_commitdate[i] < li_receiptdate[i]) {
            int thread_id = omp_get_thread_num();
            thread_local_sets[thread_id].insert(li_orderkey[i]);
        }
    }

    // Merge thread-local sets into single set
    std::unordered_set<int32_t> lineitem_orderkey_set;
    for (int t = 0; t < num_threads; t++) {
        for (int32_t key : thread_local_sets[t]) {
            lineitem_orderkey_set.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    printf("[TIMING] semi_join_build: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_semi_join_start).count());
#endif

    std::cout << "Semi-join set size: " << lineitem_orderkey_set.size() << " distinct order keys" << std::endl;

    // ========================================================================
    // STEP 2: Load zone map for orders table
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
#endif

    ZoneMap orders_zonemap(gendb_dir + "/indexes/orders_orderdate_zonemap.bin");

#ifdef GENDB_PROFILE
    printf("[TIMING] zonemap_load: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_zonemap_start).count());
#endif

    // ========================================================================
    // STEP 3: Load orders columns
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
#endif

    // Load o_orderpriority dictionary
    std::vector<std::string> priority_dict = load_dictionary(orders_dir + "/o_orderpriority_dict.txt");

    std::cout << "Loaded " << priority_dict.size() << " priorities" << std::endl;
    for (size_t i = 0; i < priority_dict.size(); i++) {
        std::cout << "  Priority " << i << ": " << priority_dict[i] << std::endl;
    }

    // ========================================================================
    // STEP 4: Scan orders with zone map pruning and date filtering
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_orders_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Zone map is available but we use a simple date filter for correctness
    // (Zone map could be used to skip some blocks, but the filter is still needed)

    // Flat array for aggregation: index by priority code (0-4)
    std::array<int64_t, 5> priority_counts = {0, 0, 0, 0, 0};

    // Scan orders and apply filters
    for (int64_t i = 0; i < orders_rows; i++) {
        int32_t orderdate = o_orderdate[i];

        // Date filter: o_orderdate >= date_1993_07_01 AND o_orderdate < date_1993_10_01
        if (orderdate < date_1993_07_01 || orderdate >= date_1993_10_01) {
            continue;
        }

        // Semi-join with lineitem: check if this order key exists in lineitem set
        int32_t orderkey = o_orderkey[i];
        if (lineitem_orderkey_set.count(orderkey) == 0) {
            continue;
        }

        // This order qualifies: aggregate by priority
        int32_t priority_code = o_orderpriority[i];
        if (priority_code >= 0 && priority_code < 5) {
            priority_counts[priority_code]++;
        }
    }

#ifdef GENDB_PROFILE
    printf("[TIMING] orders_scan_filter_join: %.2f ms\n",
           std::chrono::duration<double, std::milli>(
               std::chrono::high_resolution_clock::now() - t_orders_scan_start).count());
#endif

    // ========================================================================
    // STEP 5: Output results sorted by o_orderpriority
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
