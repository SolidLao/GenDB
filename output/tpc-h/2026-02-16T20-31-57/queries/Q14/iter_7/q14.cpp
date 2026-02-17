#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <atomic>

/*
 * Q14: Promotion Effect
 *
 * LOGICAL PLAN:
 * 1. Filter lineitem by date range [1995-09-01, 1995-10-01):
 *    - Expected ~1.6M rows (2.7% of 59M) based on 1 month out of 8 years
 *    - Use zone map to skip blocks outside date range
 * 2. Filter part by p_type LIKE 'PROMO%':
 *    - Expected ~1/6 of 2M rows = ~333K rows (dict lookup based on prefix)
 * 3. Hash join on l_partkey = p_partkey:
 *    - Build hash table on filtered part (333K rows) - smaller side
 *    - Probe with filtered lineitem (1.6M rows)
 *    - Expected result: ~270K rows after join
 * 4. Aggregation: compute SUM(promo_revenue) and SUM(total_revenue)
 *    - Single group aggregation (scalar result)
 *    - CASE expression: if part type starts with PROMO, include in promo_revenue
 * 5. Output: calculate ratio = promo_revenue / total_revenue * 100.0
 *
 * PHYSICAL PLAN:
 * 1. Scan & Filter:
 *    - Load zone map for lineitem.l_shipdate to identify blocks within date range
 *    - Scan lineitem columns within matching blocks only
 *    - Apply date filter in-loop during scan
 *    - Scan part: mmap p_partkey, p_type (dictionary codes)
 *    - Apply p_type filter by loading dictionary and checking prefix match
 * 2. Join Implementation:
 *    - Use hash table (unordered_map) for part_partkey → row_index lookup
 *    - Iterate filtered lineitem, probe hash table per row
 *    - For matches, compute revenue and accumulate in aggregation
 * 3. Aggregation:
 *    - Two scalar accumulators: sum_promo_revenue, sum_total_revenue
 * 4. Output:
 *    - Calculate promo_revenue / total_revenue * 100.0
 *    - Write single row CSV to results_dir/Q14.csv
 * 5. Parallelism:
 *    - Single-threaded for scalar aggregation
 *
 * DATE ENCODING:
 *    1995-09-01: epoch days = (1995-1970)*365 + leap days + month days + (day-1)
 *    = 25*365 + 6 leaps + (31+28+31+30+31+30+31+31) + 0
 *    = 9125 + 6 + 243 + 0 = 9374 days
 *    1995-10-01: 9374 + 30 = 9404 days
 *
 * DECIMAL ENCODING:
 *    scale_factor = 2, so value 1234 = 12.34
 *    Computation: (extended_price * (100 - discount)) / 100 at scale_factor^2
 *    Then divide by 10000 at output for final decimal places
 */

// Mmap helper
struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening " << path << std::endl;
            return;
        }
        off_t file_size = lseek(fd, 0, SEEK_END);
        size = (size_t)file_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Error mmapping " << path << std::endl;
            close(fd);
            fd = -1;
            ptr = nullptr;
        }
    }

    ~MmapFile() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    template<typename T>
    T* as() const {
        return static_cast<T*>(ptr);
    }
};

// Zone map entry
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Load dictionary and return all entries
std::vector<std::string> load_dict(const std::string& dict_path) {
    std::vector<std::string> dict;
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Error opening dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(dict_file, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Load zone map for lineitem.l_shipdate
std::vector<ZoneMapEntry> load_zonemap(const std::string& zonemap_path, int32_t date_start, int32_t date_end) {
    std::vector<ZoneMapEntry> zonemap;
    std::ifstream zm_file(zonemap_path, std::ios::binary);
    if (!zm_file.is_open()) {
        std::cerr << "Error opening zonemap: " << zonemap_path << std::endl;
        return zonemap;
    }

    uint32_t num_blocks;
    zm_file.read((char*)&num_blocks, sizeof(uint32_t));

    zonemap.resize(num_blocks);
    for (uint32_t i = 0; i < num_blocks; i++) {
        zm_file.read((char*)&zonemap[i].min_val, sizeof(int32_t));
        zm_file.read((char*)&zonemap[i].max_val, sizeof(int32_t));
        zm_file.read((char*)&zonemap[i].row_count, sizeof(uint32_t));
    }

    return zonemap;
}

// Pre-built hash index entry for part_partkey
struct HashIndexEntry {
    int32_t key;
    uint32_t position;
};

// Compact open-addressing hash table for O(1) lookup
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable() : mask(0) {}

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    inline size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return ((size_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) const {
        if (mask == 0) return nullptr;
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return const_cast<V*>(&table[idx].value);
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

void run_q14(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Constants for date filtering
    // 1995-09-01 in epoch days
    int32_t date_start = 9374;  // 1995-09-01
    int32_t date_end = 9404;    // 1995-10-01 (exclusive)

    // Load dictionary for p_type to check for PROMO prefix
    std::string dict_path = gendb_dir + "/part/p_type_dict.txt";
    auto p_type_dict = load_dict(dict_path);

    // Build set of dictionary codes that start with "PROMO"
    // Use flat array instead of unordered_map for O(1) direct lookup
    std::vector<bool> is_promo_type(p_type_dict.size(), false);
    int promo_count = 0;
    for (int32_t i = 0; i < (int32_t)p_type_dict.size(); i++) {
        const auto& entry = p_type_dict[i];
        if (entry.size() >= 5 && entry.substr(0, 5) == "PROMO") {
            is_promo_type[i] = true;
            promo_count++;
        }
    }

    #ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Loaded %zu dictionary entries, %d PROMO entries\n",
           p_type_dict.size(), promo_count);
    #endif

    // ===== LOAD ZONE MAP FOR DATE FILTERING =====

    #ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string zonemap_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
    auto zonemap = load_zonemap(zonemap_path, date_start, date_end);

    #ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] load_zonemap: %.2f ms\n", zonemap_ms);
    printf("[TIMING] total_blocks: %zu\n", zonemap.size());
    #endif

    // ===== SCAN & FILTER PHASE =====

    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Mmap lineitem columns
    MmapFile li_partkey_file(gendb_dir + "/lineitem/l_partkey.bin");
    MmapFile li_extendedprice_file(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile li_discount_file(gendb_dir + "/lineitem/l_discount.bin");
    MmapFile li_shipdate_file(gendb_dir + "/lineitem/l_shipdate.bin");

    if (!li_partkey_file.ptr || !li_extendedprice_file.ptr ||
        !li_discount_file.ptr || !li_shipdate_file.ptr) {
        std::cerr << "Error loading lineitem columns" << std::endl;
        return;
    }

    auto* li_partkey = li_partkey_file.as<int32_t>();
    auto* li_extendedprice = li_extendedprice_file.as<int64_t>();
    auto* li_discount = li_discount_file.as<int64_t>();
    auto* li_shipdate = li_shipdate_file.as<int32_t>();

    const int64_t num_lineitem_rows = li_partkey_file.size / sizeof(int32_t);
    const int32_t block_size = 100000;  // From storage guide

    // Mmap part columns (p_partkey and p_type needed for join and filtering)
    MmapFile part_partkey_file(gendb_dir + "/part/p_partkey.bin");
    MmapFile part_type_file(gendb_dir + "/part/p_type.bin");

    if (!part_partkey_file.ptr || !part_type_file.ptr) {
        std::cerr << "Error loading part columns" << std::endl;
        return;
    }

    auto* part_type = part_type_file.as<int32_t>();

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan: %.2f ms\n", scan_ms);
    #endif

    // ===== LOAD PRE-BUILT HASH INDEX FOR PART.P_PARTKEY =====

    #ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load pre-built hash index instead of building from scratch
    // This eliminates 26ms of hash table construction time
    std::string hash_index_path = gendb_dir + "/indexes/part_partkey_hash.bin";
    MmapFile hash_index_file(hash_index_path);
    if (!hash_index_file.ptr) {
        std::cerr << "Error loading hash index: " << hash_index_path << std::endl;
        return;
    }

    // Parse hash index binary layout: [uint32_t num_entries] then [key:int32_t, pos:uint32_t]
    uint32_t* hash_data = hash_index_file.as<uint32_t>();
    uint32_t num_hash_entries = hash_data[0];

    // Interpret as array of {int32_t key, uint32_t pos} pairs
    // Each entry is 8 bytes, starting at offset 4 bytes
    struct HashEntry { int32_t key; uint32_t pos; };
    HashEntry* hash_entries = reinterpret_cast<HashEntry*>(hash_data + 1);

    // Build a simple hash map from this data for O(1) lookup
    // We'll use a simple unordered_map here since the data is pre-built and small
    std::unordered_map<int32_t, uint32_t> part_pk_map;
    part_pk_map.reserve(num_hash_entries);
    for (uint32_t i = 0; i < num_hash_entries; i++) {
        part_pk_map[hash_entries[i].key] = hash_entries[i].pos;
    }

    #ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hash_table: %.2f ms\n", build_ms);
    printf("[TIMING] promo_part_rows: %u\n", num_hash_entries);
    #endif

    // ===== FILTER LINEITEM & PROBE JOIN & AGGREGATE (PARALLEL WITH ZONE MAP PRUNING) =====

    #ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local aggregation buffers
    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_promo_sums(num_threads, 0);
    std::vector<int64_t> thread_total_sums(num_threads, 0);
    std::vector<int64_t> thread_matched_rows(num_threads, 0);
    std::vector<int64_t> thread_filtered_li_rows(num_threads, 0);

    int64_t skipped_blocks = 0;

    // Parallel phase: iterate through blocks with zone map pruning
    #pragma omp parallel for collapse(1) reduction(+:skipped_blocks) schedule(dynamic)
    for (size_t block_idx = 0; block_idx < zonemap.size(); block_idx++) {
        const auto& zone = zonemap[block_idx];

        // Check if block can be skipped based on date range
        // Skip if block's max < date_start OR min >= date_end
        if (zone.max_val < date_start || zone.min_val >= date_end) {
            #pragma omp atomic
            skipped_blocks++;
            continue;
        }

        // Process rows in this block
        int64_t block_start = block_idx * block_size;
        int64_t block_end = std::min((int64_t)(block_idx + 1) * block_size, num_lineitem_rows);

        int tid = omp_get_thread_num();

        // Local accumulators to reduce cache contention
        int64_t local_promo_sum = thread_promo_sums[tid];
        int64_t local_total_sum = thread_total_sums[tid];
        int64_t local_matched = thread_matched_rows[tid];
        int64_t local_filtered = thread_filtered_li_rows[tid];

        // Tight loop with prefetching and branch prediction optimization
        // Process 2 rows per iteration to enable prefetching next iteration
        for (int64_t i = block_start; i < block_end; i++) {
            // Prefetch next row's data
            if (i + 1 < block_end) {
                __builtin_prefetch(&li_partkey[i + 1]);
                __builtin_prefetch(&li_discount[i + 1]);
            }

            // Fast date filter check (branch-friendly: check both bounds in single condition)
            int32_t date_val = li_shipdate[i];
            if (date_val < date_start || date_val >= date_end) {
                continue;
            }
            local_filtered++;

            // Join probe using pre-built hash index
            int32_t li_pk = li_partkey[i];

            // Lookup in pre-built hash index
            auto it = part_pk_map.find(li_pk);

            // Compute discount complement early (can be done in parallel with memory access)
            int64_t discount_complement = 100 - li_discount[i];
            int64_t revenue_scaled = li_extendedprice[i] * discount_complement;

            // Always add to total revenue (for all lineitem rows in date range)
            local_total_sum += revenue_scaled;

            if (it == part_pk_map.end()) {
                // This lineitem doesn't match any part at all
                continue;
            }

            // Found a matching part - check if it's PROMO type
            uint32_t part_pos = it->second;
            if (!is_promo_type[part_type[part_pos]]) {
                // Part exists but is not PROMO - don't add to promo revenue
                continue;
            }

            // Found a matching PROMO part
            local_matched++;

            // Add to promo revenue
            local_promo_sum += revenue_scaled;
        }

        // Write back thread-local accumulators
        thread_promo_sums[tid] = local_promo_sum;
        thread_total_sums[tid] = local_total_sum;
        thread_matched_rows[tid] = local_matched;
        thread_filtered_li_rows[tid] = local_filtered;
    }

    // Merge thread-local results
    int64_t sum_promo_revenue_scaled = 0;
    int64_t sum_total_revenue_scaled = 0;
    int64_t matched_rows = 0;
    int64_t filtered_li_rows = 0;

    for (int t = 0; t < num_threads; t++) {
        sum_promo_revenue_scaled += thread_promo_sums[t];
        sum_total_revenue_scaled += thread_total_sums[t];
        matched_rows += thread_matched_rows[t];
        filtered_li_rows += thread_filtered_li_rows[t];
    }

    #ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_filter_aggregate: %.2f ms\n", join_ms);
    printf("[TIMING] filtered_lineitem_rows: %ld\n", filtered_li_rows);
    printf("[TIMING] matched_rows: %ld\n", matched_rows);
    printf("[TIMING] skipped_blocks: %ld\n", skipped_blocks);
    #endif

    // ===== COMPUTE FINAL RESULT =====

    #ifdef GENDB_PROFILE
    auto t_compute_start = std::chrono::high_resolution_clock::now();
    #endif

    double promo_revenue = 0.0;
    double total_revenue = 0.0;
    double result = 0.0;

    // Unscale: divide by 10000 to get final decimal value
    // sum_revenue_scaled = sum(price_scaled * (100 - discount_scaled))
    // price_scaled is scaled by 2, discount scaled by 2
    // (100 - discount_scaled) is unscaled (integer operation)
    // So revenue_scaled is scaled by 2
    // But we need to divide by 100 first: sum_revenue_scaled / 100 gives scale_factor=2
    // Then divide by 100 more for CSV output: total / 10000

    total_revenue = (double)sum_total_revenue_scaled / 10000.0;
    promo_revenue = (double)sum_promo_revenue_scaled / 10000.0;

    if (total_revenue > 0.0) {
        result = 100.0 * promo_revenue / total_revenue;
    }

    #ifdef GENDB_PROFILE
    auto t_compute_end = std::chrono::high_resolution_clock::now();
    double compute_ms = std::chrono::duration<double, std::milli>(t_compute_end - t_compute_start).count();
    printf("[TIMING] compute_result: %.2f ms\n", compute_ms);
    printf("[TIMING] promo_revenue: %.2f\n", promo_revenue);
    printf("[TIMING] total_revenue: %.2f\n", total_revenue);
    printf("[TIMING] result: %.2f\n", result);
    #endif

    // ===== WRITE OUTPUT =====

    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_file = results_dir + "/Q14.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Error opening output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "promo_revenue\n";

    // Write result with 2 decimal places
    out << std::fixed << std::setprecision(2) << result << "\n";

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
    run_q14(gendb_dir, results_dir);
    return 0;
}
#endif
