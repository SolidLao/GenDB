#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <omp.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <immintrin.h>

// ============================================================================
// Q4: Order Priority Checking
//
// Query:
//   SELECT o_orderpriority, COUNT(*) AS order_count
//   FROM orders
//   WHERE o_orderdate >= DATE '1993-07-01'
//     AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
//     AND EXISTS (SELECT * FROM lineitem
//                 WHERE l_orderkey = o_orderkey
//                   AND l_commitdate < l_receiptdate)
//   GROUP BY o_orderpriority
//   ORDER BY o_orderpriority;
// ============================================================================

// Memory-mapped file helper
class MmapFile {
public:
    MmapFile(const std::string& path, size_t expected_size = 0)
        : fd(-1), data(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << ": " << strerror(errno) << std::endl;
            return;
        }

        size_t file_size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);

        if (expected_size > 0 && file_size != expected_size) {
            std::cerr << "File size mismatch for " << path
                      << ": expected " << expected_size
                      << ", got " << file_size << std::endl;
        }

        size = file_size;
        data = (uint8_t*)mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);

        if (data == MAP_FAILED) {
            std::cerr << "mmap failed for " << path << ": " << strerror(errno) << std::endl;
            data = nullptr;
            close(fd);
            fd = -1;
        }
    }

    ~MmapFile() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    template<typename T>
    const T* as() const { return reinterpret_cast<const T*>(data); }

    size_t get_size() const { return size; }
    bool is_valid() const { return data != nullptr && fd >= 0; }

private:
    int fd;
    uint8_t* data;
    size_t size;
};

// Load dictionary mapping codes to strings
std::unordered_map<int8_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int8_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int8_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// ============================================================================
// Open-addressing hash table for semi-join set (compact, O(1) lookup)
// ============================================================================
template<typename K>
struct CompactHashSet {
    struct Entry { K key; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;
    size_t element_count = 0;

    CompactHashSet(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    inline size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return;  // Already exists
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, true};
        element_count++;
    }

    bool contains(K key) const {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return true;
            idx = (idx + 1) & mask;
        }
        return false;
    }

    size_t size() const { return element_count; }
};

void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========================================================================
    // LOAD DATA - Orders table
    // ========================================================================
#ifdef GENDB_PROFILE
    auto load_start = std::chrono::high_resolution_clock::now();
#endif

    const int32_t ORDERS_ROW_COUNT = 15000000;
    const int32_t DATE_START = 8582;  // 1993-07-01 in epoch days
    const int32_t DATE_END = 8674;    // 1993-10-01 in epoch days

    std::string orders_dir = gendb_dir + "/orders";
    std::string lineitem_dir = gendb_dir + "/lineitem";

    // Load orders columns
    MmapFile orders_orderkey(orders_dir + "/o_orderkey.bin", ORDERS_ROW_COUNT * 4);
    MmapFile orders_orderdate(orders_dir + "/o_orderdate.bin", ORDERS_ROW_COUNT * 4);
    MmapFile orders_orderpriority(orders_dir + "/o_orderpriority.bin", ORDERS_ROW_COUNT * 1);

    if (!orders_orderkey.is_valid() || !orders_orderdate.is_valid() ||
        !orders_orderpriority.is_valid()) {
        std::cerr << "Failed to load orders data" << std::endl;
        return;
    }

    const int32_t* o_orderkey = orders_orderkey.as<int32_t>();
    const int32_t* o_orderdate = orders_orderdate.as<int32_t>();
    const int8_t* o_orderpriority_codes = orders_orderpriority.as<int8_t>();

    // Load orders priority dictionary
    std::unordered_map<int8_t, std::string> priority_dict =
        load_dictionary(orders_dir + "/o_orderpriority_dict.txt");

    if (priority_dict.empty()) {
        std::cerr << "Failed to load priority dictionary" << std::endl;
        return;
    }


#ifdef GENDB_PROFILE
    auto load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(load_end - load_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", load_ms);
#endif

    // ========================================================================
    // LOAD DATA - Lineitem table and build semi-join set
    // ========================================================================
#ifdef GENDB_PROFILE
    auto build_start = std::chrono::high_resolution_clock::now();
#endif

    const int32_t LINEITEM_ROW_COUNT = 59986052;

    MmapFile lineitem_orderkey(lineitem_dir + "/l_orderkey.bin", LINEITEM_ROW_COUNT * 4);
    MmapFile lineitem_commitdate(lineitem_dir + "/l_commitdate.bin", LINEITEM_ROW_COUNT * 4);
    MmapFile lineitem_receiptdate(lineitem_dir + "/l_receiptdate.bin", LINEITEM_ROW_COUNT * 4);

    if (!lineitem_orderkey.is_valid() || !lineitem_commitdate.is_valid() ||
        !lineitem_receiptdate.is_valid()) {
        std::cerr << "Failed to load lineitem data" << std::endl;
        return;
    }

    const int32_t* l_orderkey = lineitem_orderkey.as<int32_t>();
    const int32_t* l_commitdate = lineitem_commitdate.as<int32_t>();
    const int32_t* l_receiptdate = lineitem_receiptdate.as<int32_t>();

    // Build semi-join set: collect all distinct orderkeys where l_commitdate < l_receiptdate
    // Optimization 1: Single-pass construction (no counting phase)
    // Optimization 2: Use open-addressing hash set for O(1) lookups
    int num_threads = omp_get_max_threads();

    // Pre-allocate hash set (estimate ~22M distinct keys with ~40% match rate)
    CompactHashSet<int32_t> valid_orderkeys_impl(LINEITEM_ROW_COUNT * 2 / 5);

    // Thread-local storage for batch merging (avoid lock contention)
    std::vector<std::vector<int32_t>> partition_keys(num_threads);

    // Pre-allocate all vectors in sequential region
    for (int t = 0; t < num_threads; ++t) {
        partition_keys[t].reserve(LINEITEM_ROW_COUNT / num_threads / 2 + 10000);
    }

#pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        std::vector<int32_t>& local_keys = partition_keys[thread_id];

        // Single-pass: check condition and collect in one loop
#pragma omp for nowait schedule(static, 100000)
        for (int32_t i = 0; i < LINEITEM_ROW_COUNT; ++i) {
            if (l_commitdate[i] < l_receiptdate[i]) {
                local_keys.push_back(l_orderkey[i]);
            }
        }
    }

    // Phase 2: Merge thread-local keys into global set (deduplicated)
    // Sequential merge to avoid lock contention
    for (int p = 0; p < num_threads; ++p) {
        for (int32_t key : partition_keys[p]) {
            valid_orderkeys_impl.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    auto build_end = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(build_end - build_start).count();
    printf("[TIMING] build_semijoin_set: %.2f ms\n", build_ms);
    printf("[TIMING] semijoin_set_size: %zu\n", valid_orderkeys_impl.size());
#endif

    // ========================================================================
    // FILTER & GROUP BY (with Zone Map Pruning)
    // ========================================================================
#ifdef GENDB_PROFILE
    auto filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Load zone map for orders_o_orderdate to skip non-matching blocks
    // Layout: [uint32_t num_zones] then [min_value:int32_t, max_value:int32_t, row_count:uint32_t] per zone
    struct ZoneMapEntry {
        int32_t min_val;
        int32_t max_val;
        uint32_t row_count;
    };
    static_assert(sizeof(ZoneMapEntry) == 12);

    std::vector<ZoneMapEntry> zones;
    std::string zonemap_path = orders_dir + "/../indexes/orders_o_orderdate_zone.bin";
    int zm_fd = open(zonemap_path.c_str(), O_RDONLY);
    if (zm_fd >= 0) {
        uint32_t num_zones;
        ssize_t nbytes = read(zm_fd, &num_zones, sizeof(uint32_t));
        if (nbytes == (ssize_t)sizeof(uint32_t)) {
            zones.resize(num_zones);
            ssize_t data_bytes = read(zm_fd, zones.data(), num_zones * sizeof(ZoneMapEntry));
            (void)data_bytes;  // Suppress unused result warning
        }
        close(zm_fd);
    }

    // Group by o_orderpriority with low cardinality (5 groups)
    // Use a flat array indexed by priority code (0-4) for better performance
    // Priority codes are: 0=1-URGENT, 1=2-HIGH, 2=3-MEDIUM, 3=4-NOT SPECIFIED, 4=5-LOW
    const int32_t MAX_PRIORITY_CODE = 5;
    std::vector<int32_t> count_by_priority(MAX_PRIORITY_CODE, 0);

    // Build block ranges (zone map blocks)
    std::vector<std::pair<uint32_t, uint32_t>> block_ranges;
    if (!zones.empty()) {
        // Construct block ranges from zone map
        uint32_t row_offset = 0;
        for (const auto& zone : zones) {
            // Zone map pruning: skip blocks that don't overlap with [DATE_START, DATE_END)
            if (zone.max_val < DATE_START || zone.min_val >= DATE_END) {
                row_offset += zone.row_count;
                continue;  // Skip this block
            }
            uint32_t block_end = row_offset + zone.row_count;
            block_ranges.push_back({row_offset, block_end});
            row_offset = block_end;
        }
    } else {
        // No zone map available, process entire table
        block_ranges.push_back({0, ORDERS_ROW_COUNT});
    }

#pragma omp parallel
    {
        // Thread-local flat array for counts
        std::vector<int32_t> local_counts(MAX_PRIORITY_CODE, 0);

#pragma omp for nowait schedule(dynamic)
        for (size_t b = 0; b < block_ranges.size(); ++b) {
            uint32_t block_start = block_ranges[b].first;
            uint32_t block_end = block_ranges[b].second;

            for (uint32_t i = block_start; i < block_end; ++i) {
                // Check date range (zone map pruned blocks, but double-check for safety)
                if (o_orderdate[i] >= DATE_START && o_orderdate[i] < DATE_END) {
                    // Check if orderkey exists in semi-join set
                    if (valid_orderkeys_impl.contains(o_orderkey[i])) {
                        // Increment count for this priority
                        int8_t priority_code = o_orderpriority_codes[i];
                        if (priority_code >= 0 && priority_code < MAX_PRIORITY_CODE) {
                            local_counts[priority_code]++;
                        }
                    }
                }
            }
        }

        // Merge thread-local counts
#pragma omp critical
        {
            for (int32_t code = 0; code < MAX_PRIORITY_CODE; ++code) {
                count_by_priority[code] += local_counts[code];
            }
        }
    }

#ifdef GENDB_PROFILE
    auto filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(filter_end - filter_start).count();
    printf("[TIMING] filter_and_group: %.2f ms\n", filter_ms);
#endif

    // ========================================================================
    // OUTPUT
    // ========================================================================
#ifdef GENDB_PROFILE
    auto output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q4.csv";
    std::ofstream out(output_path);

    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "o_orderpriority,order_count\n";

    // Collect results for sorting
    std::vector<std::pair<std::string, int32_t>> results;
    for (int8_t code = 0; code < MAX_PRIORITY_CODE; ++code) {
        if (count_by_priority[code] > 0) {
            auto it = priority_dict.find(code);
            if (it != priority_dict.end()) {
                results.push_back({it->second, count_by_priority[code]});
            }
        }
    }

    // Sort by priority string (alphabetically)
    std::sort(results.begin(), results.end());

    // Write results
    for (const auto& [priority, count] : results) {
        out << priority << "," << count << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(output_end - output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // ========================================================================
    // TOTAL TIMING
    // ========================================================================
#ifdef GENDB_PROFILE
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Q4 execution completed. Results written to: " << output_path << std::endl;
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
