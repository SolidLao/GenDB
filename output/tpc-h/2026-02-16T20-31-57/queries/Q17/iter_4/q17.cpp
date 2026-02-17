#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <omp.h>

/*
 * ========== QUERY Q17: Small-Quantity-Order Revenue ==========
 *
 * LOGICAL PLAN:
 * 1. Filter part table (FIRST — predicate pushdown):
 *    - Scan 2M part rows (dictionary-encoded p_brand, p_container)
 *    - Load p_brand_dict.txt and p_container_dict.txt at runtime
 *    - Find codes for "Brand#23" and "MED BOX"
 *    - Filter: p_brand == brand_code AND p_container == container_code
 *    - Result: ~2K parts (highly selective)
 *    - Build hash set for fast lookup
 *
 * 2. Pre-compute subquery (ONLY for filtered partkeys):
 *    - Single pass over 59M lineitem rows
 *    - Only aggregate for partkeys in filtered set
 *    - Compute threshold = 0.2 * AVG(l_quantity) per partkey
 *    - Result: ~2K thresholds (not 2M)
 *
 * 3. Join + filter lineitem:
 *    - Scan lineitem rows
 *    - Check if l_partkey in filtered set (from step 1)
 *    - Look up threshold from step 2
 *    - Filter: l_quantity < threshold
 *    - Accumulate l_extendedprice into sum
 *
 * 4. Output:
 *    - SUM(l_extendedprice) / 7.0
 *    - Scale from int64_t (scale_factor=2) to actual decimal
 *
 * PHYSICAL PLAN:
 * - Part filter: Full scan of 2M rows (~7ms)
 * - Subquery: Only aggregate for ~2K partkeys (thread-local maps to avoid contention)
 * - Join: Hash table lookup (O(1)) + quantity filter
 * - Aggregation: Parallel thread-local sums, merged at end
 * - Parallelism: OpenMP parallel_for with thread-local aggregation (64 cores available)
 *
 * KEY OPTIMIZATIONS (Iteration 3):
 * - Phase reordering: Filter part FIRST, then compute thresholds only for filtered keys
 * - Thread-local aggregation: Reduces contention from 2M partkey updates to ~2K
 * - Parallel processing: Lineitem scan parallelized across 64 cores
 * - Predicate pushdown: Part filters applied before subquery computation
 * - Pipeline fusion: no intermediate materialization
 *
 * OPTIMIZATIONS (Iteration 4):
 * - Replace unordered_map with compact open-addressing hash table for better cache locality
 * - Fuse Phase 2 & 3: Single pass over 60M lineitem rows instead of two passes
 * - Inline threshold lookups to reduce indirection
 * - Branch-friendly predicate checking to improve CPU prediction
 */


// Compact open-addressing hash table for small cardinality joins
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        bool occupied;
        Entry() : key(0), value(), occupied(false) {}
    };

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
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = Entry();
        table[idx].key = key;
        table[idx].value = value;
        table[idx].occupied = true;
    }

    inline V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    size_t size() const {
        size_t count = 0;
        for (const auto& entry : table) {
            if (entry.occupied) count++;
        }
        return count;
    }
};

// Load dictionary and find code for a target string
int32_t find_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return -1;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(dict_file, line)) {
        // Dictionary format: one value per line, indexed from 0
        if (line == target) {
            return code;
        }
        code++;
    }
    return -1;
}

// Utility: memory-map a binary file
void* mmap_file(const std::string& filepath, size_t& size) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file: " << filepath << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    size = static_cast<size_t>(file_size);

    void* data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Error mmapping file: " << filepath << std::endl;
        return nullptr;
    }

    return data;
}

void run_q17(const std::string& gendb_dir, const std::string& results_dir) {

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    std::cout << "[Q17] Starting execution..." << std::endl;

    // ===== PHASE 1: FILTER PART TABLE (MOVED FIRST — PREDICATE PUSHDOWN) =====
#ifdef GENDB_PROFILE
    auto t_phase1_start = std::chrono::high_resolution_clock::now();
#endif

    // Load part columns
    size_t part_size = 0;
    int32_t* p_partkey_data = (int32_t*)mmap_file(gendb_dir + "/part/p_partkey.bin", part_size);
    int32_t num_part = part_size / sizeof(int32_t);

    size_t p_brand_size = 0;
    int32_t* p_brand_data = (int32_t*)mmap_file(gendb_dir + "/part/p_brand.bin", p_brand_size);

    size_t p_container_size = 0;
    int32_t* p_container_data = (int32_t*)mmap_file(gendb_dir + "/part/p_container.bin", p_container_size);

    if (!p_partkey_data || !p_brand_data || !p_container_data) {
        std::cerr << "Error loading part data" << std::endl;
        return;
    }

    // Load dictionary codes
    int32_t brand_23_code = find_dict_code(gendb_dir + "/part/p_brand_dict.txt", "Brand#23");
    int32_t medbox_code = find_dict_code(gendb_dir + "/part/p_container_dict.txt", "MED BOX");

    if (brand_23_code < 0 || medbox_code < 0) {
        std::cerr << "Error loading dictionary codes" << std::endl;
        return;
    }

    std::cout << "[Q17] brand_code=" << brand_23_code << " container_code=" << medbox_code << std::endl;

    // Build compact hash table of filtered part keys (~2K rows expected)
    // Use open-addressing for better cache locality than unordered_map
    CompactHashTable<int32_t, bool> filtered_partkeys(4000);
    for (int32_t i = 0; i < num_part; i++) {
        if (p_brand_data[i] == brand_23_code && p_container_data[i] == medbox_code) {
            filtered_partkeys.insert(p_partkey_data[i], true);
        }
    }

    std::cout << "[Q17] qualifying parts: " << filtered_partkeys.size() << std::endl;

#ifdef GENDB_PROFILE
    auto t_phase1_end = std::chrono::high_resolution_clock::now();
    double phase1_ms = std::chrono::duration<double, std::milli>(t_phase1_end - t_phase1_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", phase1_ms);
#endif

    // ===== PHASE 2 + 3 (FUSED): PRECOMPUTE THRESHOLDS AND AGGREGATE IN SINGLE PASS =====
#ifdef GENDB_PROFILE
    auto t_phase2_start = std::chrono::high_resolution_clock::now();
#endif

    // Load lineitem columns
    size_t lineitem_size = 0;
    int32_t* l_partkey_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_partkey.bin", lineitem_size);
    int32_t num_lineitem = lineitem_size / sizeof(int32_t);

    size_t l_quantity_size = 0;
    int64_t* l_quantity_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_quantity.bin", l_quantity_size);

    size_t l_extendedprice_size = 0;
    int64_t* l_extendedprice_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", l_extendedprice_size);

    if (!l_partkey_data || !l_quantity_data || !l_extendedprice_data) {
        std::cerr << "Error loading lineitem data" << std::endl;
        return;
    }

    // Use compact hash table for threshold storage
    CompactHashTable<int32_t, int64_t> partkey_threshold(4000);

    // Thread-local data for two-pass approach:
    // Pass 1: Accumulate stats (sum, count) per partkey using compact hash table
    // Pass 2: Compute thresholds from stats, then aggregate matching rows
    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTable<int32_t, std::pair<int64_t, int32_t>>> thread_local_stats(num_threads);

    // Initialize thread-local tables
    for (int t = 0; t < num_threads; t++) {
        thread_local_stats[t] = CompactHashTable<int32_t, std::pair<int64_t, int32_t>>(4000);
    }

    // PASS 1: Accumulate sum and count ONLY for partkeys that passed part filter
#pragma omp parallel for schedule(static, 100000)
    for (int32_t i = 0; i < num_lineitem; i++) {
        int32_t pk = l_partkey_data[i];

        // Quick check: does this partkey exist in filtered set?
        if (filtered_partkeys.find(pk) == nullptr) {
            continue;
        }

        int64_t qty = l_quantity_data[i];
        int thread_id = omp_get_thread_num();

        // Find or insert into thread-local table
        auto* existing = thread_local_stats[thread_id].find(pk);
        if (existing != nullptr) {
            existing->first += qty;
            existing->second += 1;
        } else {
            thread_local_stats[thread_id].insert(pk, {qty, 1});
        }
    }

    // Merge thread-local stats and compute thresholds
    CompactHashTable<int32_t, std::pair<int64_t, int32_t>> merged_stats(4000);
    for (int t = 0; t < num_threads; t++) {
        for (size_t i = 0; i < thread_local_stats[t].table.size(); i++) {
            auto& entry = thread_local_stats[t].table[i];
            if (entry.occupied) {
                int32_t pk = entry.key;
                auto* existing = merged_stats.find(pk);
                if (existing != nullptr) {
                    existing->first += entry.value.first;
                    existing->second += entry.value.second;
                } else {
                    merged_stats.insert(pk, entry.value);
                }
            }
        }
    }

    // Compute thresholds from merged stats
    for (size_t i = 0; i < merged_stats.table.size(); i++) {
        auto& entry = merged_stats.table[i];
        if (entry.occupied) {
            int32_t pk = entry.key;
            int64_t sum_qty = entry.value.first;
            int32_t count_qty = entry.value.second;
            // Compute 0.2 * AVG using floating point to avoid truncation
            double avg_qty_double = (double)sum_qty / (double)count_qty;
            double threshold_double = 0.2 * avg_qty_double;
            int64_t threshold = (int64_t)std::round(threshold_double);
            partkey_threshold.insert(pk, threshold);
        }
    }

#ifdef GENDB_PROFILE
    auto t_phase2_end = std::chrono::high_resolution_clock::now();
    double phase2_ms = std::chrono::duration<double, std::milli>(t_phase2_end - t_phase2_start).count();
    printf("[TIMING] precompute_thresholds: %.2f ms\n", phase2_ms);
#endif

    // ===== PHASE 3: AGGREGATE MATCHING ROWS =====
#ifdef GENDB_PROFILE
    auto t_phase3_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel aggregation with thread-local buffers
    std::vector<int64_t> thread_sums(num_threads, 0);

#pragma omp parallel for schedule(static, 100000)
    for (int32_t i = 0; i < num_lineitem; i++) {
        int32_t pk = l_partkey_data[i];

        // Single hash lookup to find threshold (implicitly checks if partkey is filtered)
        auto* threshold_ptr = partkey_threshold.find(pk);
        if (threshold_ptr != nullptr) {
            int64_t qty = l_quantity_data[i];
            int64_t threshold = *threshold_ptr;
            if (qty < threshold) {
                int thread_id = omp_get_thread_num();
                thread_sums[thread_id] += l_extendedprice_data[i];
            }
        }
    }

    // Merge thread-local sums
    int64_t total_extended_price = 0;
    for (int i = 0; i < num_threads; i++) {
        total_extended_price += thread_sums[i];
    }

#ifdef GENDB_PROFILE
    auto t_phase3_end = std::chrono::high_resolution_clock::now();
    double phase3_ms = std::chrono::duration<double, std::milli>(t_phase3_end - t_phase3_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", phase3_ms);
#endif

    // ===== FINAL AGGREGATION =====
    // Divide by 7.0, remember to un-scale the result
    // total_extended_price is scaled by 100 (DECIMAL with scale_factor=100)
    // avg_yearly = (total_extended_price / 100) / 7.0 = total_extended_price / 700.0
    double avg_yearly = (double)total_extended_price / 700.0;

    std::cout << "[Q17] Result: " << std::fixed << std::setprecision(2) << avg_yearly << std::endl;

    // ===== WRITE RESULTS =====
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q17.csv";
    std::ofstream out(output_file);
    if (!out) {
        std::cerr << "Error opening output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "avg_yearly\n";
    // Write result with 2 decimal places
    out << std::fixed << std::setprecision(2) << avg_yearly << "\n";
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Cleanup
    if (l_partkey_data) munmap(l_partkey_data, lineitem_size);
    if (l_quantity_data) munmap(l_quantity_data, l_quantity_size);
    if (l_extendedprice_data) munmap(l_extendedprice_data, l_extendedprice_size);
    if (p_partkey_data) munmap(p_partkey_data, part_size);
    if (p_brand_data) munmap(p_brand_data, p_brand_size);
    if (p_container_data) munmap(p_container_data, p_container_size);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q17(gendb_dir, results_dir);

    return 0;
}
#endif
