#include <iostream>
#include <fstream>
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
#include <thread>
#include <omp.h>

/*
 * ========== QUERY Q17: Small-Quantity-Order Revenue ==========
 *
 * LOGICAL PLAN:
 * 1. Pre-compute subquery: For each l_partkey, compute 0.2 * AVG(l_quantity)
 *    - Single pass over 59M lineitem rows
 *    - Aggregate into hash map: partkey -> (sum_qty, count)
 *    - Compute threshold = 0.2 * sum / count
 *
 * 2. Filter part table:
 *    - Scan 2M part rows (dictionary-encoded p_brand, p_container)
 *    - Load p_brand_dict.txt and p_container_dict.txt at runtime
 *    - Find codes for "Brand#23" and "MED BOX"
 *    - Filter: p_brand == brand_code AND p_container == container_code
 *    - Result: ~1-10 parts (highly selective)
 *
 * 3. Join + filter lineitem:
 *    - For each qualifying part p:
 *      - Look up p.p_partkey in subquery results -> get threshold
 *      - Scan all lineitem rows with l_partkey == p.p_partkey
 *      - Filter: l_quantity < threshold
 *      - Accumulate l_extendedprice into sum
 *
 * 4. Output:
 *    - SUM(l_extendedprice) / 7.0
 *    - Scale from int64_t (scale_factor=2) to actual decimal
 *
 * PHYSICAL PLAN:
 * - Subquery: Open-addressing hash table, 2M slots (pre-sized)
 * - Part filter: Full scan with early exit after few matches (branch-friendly)
 * - Join: Hash table lookup (O(1)) then linear scan of lineitem bucket
 * - Aggregation: Single int64_t accumulator at full precision (scale_factor²)
 * - Parallelism: Single-threaded (small result set), focus on cache efficiency
 *
 * KEY OPTIMIZATIONS:
 * - Correlated subquery decorrelated into pre-computed hash map
 * - Dictionary string lookups done once at start, not per row
 * - Predicate pushdown: part filters applied before join
 * - Pipeline fusion: no intermediate materialization
 * - Late materialization for part table (load only needed columns for filtering)
 */

struct QuantityThreshold {
    int64_t sum_qty;
    int64_t count;
    int64_t threshold_scaled;  // Precomputed: 0.2 * sum_qty / count (avoids per-row division)

    void compute_threshold() {
        if (count == 0) {
            threshold_scaled = 0;
        } else {
            // scale_factor = 2 means scaled by 10^2 = 100
            // stored_value = actual_value * 100
            // AVG(actual) = sum_stored / 100 / count = sum_stored / (100 * count)
            // threshold = 0.2 * AVG(actual) = 0.2 * sum_stored / (100 * count)
            // To compare with stored values: stored < threshold * 100 = 0.2 * sum_stored / count
            threshold_scaled = sum_qty / (5 * count);
        }
    }
};

// Simple mmap helper
struct MmapFile {
    int fd;
    void* data;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), data(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return;
        }
        off_t file_size = lseek(fd, 0, SEEK_END);
        if (file_size < 0) {
            close(fd);
            fd = -1;
            return;
        }
        size = file_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "mmap failed for " << path << std::endl;
            close(fd);
            fd = -1;
            data = nullptr;
        }
    }

    ~MmapFile() {
        if (data != nullptr) munmap(data, size);
        if (fd >= 0) close(fd);
    }

    bool is_valid() const { return data != nullptr; }
};

// Load dictionary and find code for a target string
int32_t find_dict_code(const std::string& dict_path, const std::string& target) {
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return -1;
    }

    std::string line;
    while (std::getline(dict_file, line)) {
        // Dictionary format: one value per line
        if (line == target) {
            // The dictionary is 0-indexed by line number
            static std::ifstream dict_for_count(dict_path);
            dict_for_count.clear();
            dict_for_count.seekg(0);
            int32_t code = 0;
            std::string temp;
            while (std::getline(dict_for_count, temp)) {
                if (temp == target) break;
                code++;
            }
            return code;
        }
    }
    return -1;
}

// Load dictionary and find code (optimized version)
int32_t find_dict_code_optimized(const std::string& dict_path, const std::string& target) {
    std::ifstream dict_file(dict_path);
    if (!dict_file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return -1;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(dict_file, line)) {
        if (line == target) {
            return code;
        }
        code++;
    }
    return -1;
}

void run_q17(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    std::cout << "[Q17] Starting execution..." << std::endl;

    // ============ STEP 1: Load dictionaries and find target codes ============
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t brand_code = find_dict_code_optimized(
        gendb_dir + "/part/p_brand_dict.txt", "Brand#23"
    );
    int32_t container_code = find_dict_code_optimized(
        gendb_dir + "/part/p_container_dict.txt", "MED BOX"
    );

    std::cout << "[Q17] brand_code=" << brand_code << " container_code=" << container_code << std::endl;

    if (brand_code < 0 || container_code < 0) {
        std::cerr << "[Q17] ERROR: Failed to find dictionary codes" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] dict_load: %.2f ms\n", dict_ms);
#endif

    // ============ STEP 2: Pre-compute subquery (0.2 * AVG(l_quantity) per partkey) ============
#ifdef GENDB_PROFILE
    auto t_subquery_start = std::chrono::high_resolution_clock::now();
#endif

    // Load lineitem columns
    MmapFile l_partkey_file(gendb_dir + "/lineitem/l_partkey.bin");
    MmapFile l_quantity_file(gendb_dir + "/lineitem/l_quantity.bin");

    if (!l_partkey_file.is_valid() || !l_quantity_file.is_valid()) {
        std::cerr << "[Q17] ERROR: Failed to load lineitem files" << std::endl;
        return;
    }

    const int32_t* l_partkey = static_cast<const int32_t*>(l_partkey_file.data);
    const int64_t* l_quantity = static_cast<const int64_t*>(l_quantity_file.data);

    int64_t lineitem_rows = l_partkey_file.size / sizeof(int32_t);
    std::cout << "[Q17] lineitem rows: " << lineitem_rows << std::endl;

    // Direct array: partkey -> (sum_qty, count, threshold_scaled)
    // Part table has p_partkey in range [1, 2000000]
    // Allocate array large enough to hold all possible partkeys
    std::vector<QuantityThreshold> subquery_results(2000001);  // Index 0 unused, indices 1..2000000

    // Parallel subquery computation with thread-local aggregation
    // Use OpenMP to parallelize the 59M row scan
    const int num_threads = std::min(64, (int)std::thread::hardware_concurrency());
    std::vector<std::vector<QuantityThreshold>> local_buffers(num_threads);
    for (int t = 0; t < num_threads; t++) {
        local_buffers[t].resize(2000001);
    }

#pragma omp parallel for schedule(static) num_threads(num_threads)
    for (int64_t i = 0; i < lineitem_rows; i++) {
        int thread_id = omp_get_thread_num();
        int32_t partkey = l_partkey[i];
        int64_t qty = l_quantity[i];

        if (partkey >= 1 && partkey <= 2000000) {
            local_buffers[thread_id][partkey].sum_qty += qty;
            local_buffers[thread_id][partkey].count++;
        }
    }

    // Merge thread-local buffers into final result
    for (int32_t partkey = 1; partkey <= 2000000; partkey++) {
        int64_t total_sum = 0;
        int64_t total_count = 0;
        for (int t = 0; t < num_threads; t++) {
            total_sum += local_buffers[t][partkey].sum_qty;
            total_count += local_buffers[t][partkey].count;
        }
        subquery_results[partkey].sum_qty = total_sum;
        subquery_results[partkey].count = total_count;
        subquery_results[partkey].compute_threshold();  // Precompute threshold once
    }

    // Count distinct partkeys for logging
    int64_t distinct_count = 0;
    for (int32_t i = 1; i <= 2000000; i++) {
        if (subquery_results[i].count > 0) {
            distinct_count++;
        }
    }

    std::cout << "[Q17] subquery: " << distinct_count << " distinct partkeys" << std::endl;

#ifdef GENDB_PROFILE
    auto t_subquery_end = std::chrono::high_resolution_clock::now();
    double subquery_ms = std::chrono::duration<double, std::milli>(t_subquery_end - t_subquery_start).count();
    printf("[TIMING] subquery: %.2f ms\n", subquery_ms);
#endif

    // ============ STEP 3: Filter part table ============
#ifdef GENDB_PROFILE
    auto t_part_filter_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile p_partkey_file(gendb_dir + "/part/p_partkey.bin");
    MmapFile p_brand_file(gendb_dir + "/part/p_brand.bin");
    MmapFile p_container_file(gendb_dir + "/part/p_container.bin");

    if (!p_partkey_file.is_valid() || !p_brand_file.is_valid() || !p_container_file.is_valid()) {
        std::cerr << "[Q17] ERROR: Failed to load part files" << std::endl;
        return;
    }

    const int32_t* p_partkey = static_cast<const int32_t*>(p_partkey_file.data);
    const int32_t* p_brand = static_cast<const int32_t*>(p_brand_file.data);
    const int32_t* p_container = static_cast<const int32_t*>(p_container_file.data);

    int64_t part_rows = p_partkey_file.size / sizeof(int32_t);
    std::cout << "[Q17] part rows: " << part_rows << std::endl;

    // Collect qualifying parts
    std::vector<int32_t> qualifying_partkeys;
    qualifying_partkeys.reserve(100);

    for (int64_t i = 0; i < part_rows; i++) {
        if (p_brand[i] == brand_code && p_container[i] == container_code) {
            qualifying_partkeys.push_back(p_partkey[i]);
        }
    }

    std::cout << "[Q17] qualifying parts: " << qualifying_partkeys.size() << std::endl;

#ifdef GENDB_PROFILE
    auto t_part_filter_end = std::chrono::high_resolution_clock::now();
    double part_filter_ms = std::chrono::duration<double, std::milli>(t_part_filter_end - t_part_filter_start).count();
    printf("[TIMING] part_filter: %.2f ms\n", part_filter_ms);
#endif

    // ============ STEP 4: Join + aggregate ============
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile l_extendedprice_file(gendb_dir + "/lineitem/l_extendedprice.bin");
    if (!l_extendedprice_file.is_valid()) {
        std::cerr << "[Q17] ERROR: Failed to load l_extendedprice" << std::endl;
        return;
    }

    const int64_t* l_extendedprice = static_cast<const int64_t*>(l_extendedprice_file.data);

    // Build a set of qualifying partkeys for fast lookup
    // Use a small array for direct indexing since only ~2K parts can qualify
    // Create a boolean array for O(1) lookup
    std::vector<bool> qualifying_array(2000001, false);
    for (int32_t partkey : qualifying_partkeys) {
        qualifying_array[partkey] = true;
    }

    int64_t sum_extended_price = 0;  // scaled by scale_factor^2 = 4

    // Parallel pass over lineitem with thread-local aggregation
#pragma omp parallel for schedule(static) num_threads(num_threads) reduction(+:sum_extended_price)
    for (int64_t i = 0; i < lineitem_rows; i++) {
        int32_t partkey = l_partkey[i];

        // Check if this partkey qualifies from part table
        if (partkey < 1 || partkey > 2000000 || !qualifying_array[partkey]) {
            continue;  // This partkey doesn't match part filters or is invalid
        }

        const QuantityThreshold& threshold = subquery_results[partkey];
        if (threshold.count == 0) {
            // No lineitem rows for this part
            continue;
        }

        // Use precomputed threshold_scaled for integer comparison (avoids per-row division)
        // threshold_scaled = 0.2 * sum_qty / count
        // l_quantity < threshold_scaled
        if (l_quantity[i] < threshold.threshold_scaled) {
            sum_extended_price += l_extendedprice[i];
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join_filter_aggregate: %.2f ms\n", join_ms);
#endif

    // ============ STEP 5: Final computation and output ============
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    // sum_extended_price is the sum of scaled l_extendedprice values (scale_factor=2)
    // scale_factor=2 means scaled by 10^2 = 100
    // To get the actual sum: sum_extended_price / 100
    // Result = (sum_extended_price / 100) / 7.0 = sum_extended_price / 700.0

    double avg_yearly = static_cast<double>(sum_extended_price) / 700.0;

    std::cout << "[Q17] Result: " << std::fixed << std::setprecision(2) << avg_yearly << std::endl;

    // Write CSV output
    std::ofstream output_file(results_dir + "/Q17.csv");
    if (!output_file.is_open()) {
        std::cerr << "[Q17] ERROR: Failed to open output file" << std::endl;
        return;
    }

    output_file << "avg_yearly\n";
    output_file << std::fixed << std::setprecision(2) << avg_yearly << "\n";
    output_file.close();

    std::cout << "[Q17] Output written to " << results_dir << "/Q17.csv" << std::endl;

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

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

    run_q17(gendb_dir, results_dir);

    return 0;
}
#endif
