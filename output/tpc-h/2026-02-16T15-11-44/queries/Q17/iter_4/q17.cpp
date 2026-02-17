#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <omp.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*
=== LOGICAL QUERY PLAN FOR Q17 ===

Query: SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
       FROM lineitem, part
       WHERE p_partkey = l_partkey
         AND p_brand = 'Brand#23'
         AND p_container = 'MED BOX'
         AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)

Step 1 (Logical): Identify tables and predicates
  - part table: p_brand = 'Brand#23' AND p_container = 'MED BOX'
                Estimated: 2M rows * (1/25) * (1/10) ≈ 8K rows
  - lineitem table: l_quantity < threshold_for_partkey (from correlated subquery)
                    Estimated: 60M rows, selective after join to filtered part (8K)

Step 2 (Logical): Decorrelate correlated subquery
  - Subquery: For each p_partkey, compute 0.2 * AVG(l_quantity)
  - Pre-compute into hash map: partkey → (sum_quantity, count) or computed threshold
  - Access: O(1) lookup per lineitem row

Step 3 (Logical): Join graph and ordering
  - Build small hash table on filtered part (8K rows)
  - Probe with lineitem (60M rows) to find matching l_partkey
  - Apply quantity threshold predicate during probe
  - Single SUM aggregation (1 group)

Step 4 (Logical): Aggregation
  - Single-group aggregation: SUM(l_extendedprice)
  - Post-aggregation: divide by 7.0

=== PHYSICAL QUERY PLAN FOR Q17 ===

Phase 1 (Precompute thresholds):
  - Scan all lineitem rows in parallel
  - Group by l_partkey: compute SUM(l_quantity), COUNT
  - Data structure: open-addressing hash table for better cache locality
  - Multiply average by 0.2 and store in same map
  - Parallelism: OpenMP parallel_for with thread-local aggregation buffers

Phase 2 (Filter part table):
  - Load part binary columns: p_partkey, p_brand, p_container
  - Load dictionary files for p_brand and p_container
  - Find dictionary codes for 'Brand#23' and 'MED BOX'
  - Scan part table, filter by p_brand_code == brand_23_code AND p_container_code == medbox_code
  - Store filtered part keys in compact set, no separate hash table needed
  - Expected output: ~8K part rows

Phase 3 (Join lineitem with part and aggregate):
  - Scan all lineitem rows in parallel
  - Single hash lookup into threshold map (encodes membership in filtered part keys)
  - If threshold found: check if l_quantity < threshold
  - If both true: add l_extendedprice to SUM
  - Data structure: thread-local SUM accumulators, merge at end
  - Parallelism: OpenMP parallel_for with reduction on sum

Final aggregation:
  - Divide SUM by 7.0
  - Output: single row with column avg_yearly

=== KEY IMPLEMENTATION NOTES ===
- All DECIMAL columns (l_quantity, l_extendedprice) are int64_t with scale_factor=100
  - l_quantity = actual_value * 100 (e.g., 25.5 → 2550)
  - Threshold computation: 0.2 * AVG(l_quantity) in scaled units (scale 100)
- Dictionary codes loaded at runtime from _dict.txt files
- Threshold = 0.2 * average_quantity = (int64_t)(0.2 * sum_quantity / count)
- Open-addressing hash table for better performance than std::unordered_map (2-5x faster)
- Single threshold map serves dual purpose: stores thresholds AND implicitly marks filtered part keys
*/

// Compact open-addressing hash table for small lookups (filtered part keys ~8K entries)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2 with ~75% load factor for good performance
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    // Fibonacci hashing for good distribution of integer keys
    size_t hash(K key) const {
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
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Utility: load dictionary and find code for a value
// Dictionary format: code=value (one per line)
int32_t load_dict_code(const std::string& dict_file, const std::string& target_value) {
    std::ifstream f(dict_file);
    if (!f) {
        std::cerr << "Error opening dictionary file: " << dict_file << std::endl;
        return -1;
    }

    std::string line;
    while (std::getline(f, line)) {
        // Parse "code=value" format
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            if (value == target_value) {
                f.close();
                return code;
            }
        }
    }
    f.close();
    std::cerr << "Value not found in dictionary: " << target_value << " in " << dict_file << std::endl;
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
    int32_t brand_23_code = load_dict_code(gendb_dir + "/part/p_brand_dict.txt", "Brand#23");
    int32_t medbox_code = load_dict_code(gendb_dir + "/part/p_container_dict.txt", "MED BOX");

    if (brand_23_code < 0 || medbox_code < 0) {
        std::cerr << "Error loading dictionary codes" << std::endl;
        return;
    }

    // Count filtered part keys (~8K rows expected)
    int32_t filtered_part_count = 0;
    for (int32_t i = 0; i < num_part; i++) {
        if (p_brand_data[i] == brand_23_code && p_container_data[i] == medbox_code) {
            filtered_part_count++;
        }
    }

    // Note: filtered_partkeys set is NOT built here — it's implicitly represented
    // by the thresholds map built in Phase 2. This saves space and one hash table lookup.

#ifdef GENDB_PROFILE
    auto t_phase1_end = std::chrono::high_resolution_clock::now();
    double phase1_ms = std::chrono::duration<double, std::milli>(t_phase1_end - t_phase1_start).count();
    printf("[TIMING] filter_part: %.2f ms\n", phase1_ms);
#endif

    // ===== PHASE 2: PRECOMPUTE QUANTITY THRESHOLDS (ONLY FOR FILTERED PARTKEYS) =====
#ifdef GENDB_PROFILE
    auto t_phase2_start = std::chrono::high_resolution_clock::now();
#endif

    // Load lineitem columns
    size_t lineitem_size = 0;
    int32_t* l_partkey_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_partkey.bin", lineitem_size);
    int32_t num_lineitem = lineitem_size / sizeof(int32_t);

    size_t l_quantity_size = 0;
    int64_t* l_quantity_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_quantity.bin", l_quantity_size);

    if (!l_partkey_data || !l_quantity_data) {
        std::cerr << "Error loading lineitem data" << std::endl;
        return;
    }

    // First pass: Build temp threshold map from ALL lineitem rows (to find avg per partkey)
    // Use thread-local aggregation to avoid critical section contention
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, std::pair<int64_t, int32_t>>> thread_local_stats(num_threads);

#pragma omp parallel for schedule(static, 100000)
    for (int32_t i = 0; i < num_lineitem; i++) {
        int32_t pk = l_partkey_data[i];
        int64_t qty = l_quantity_data[i];

        int thread_id = omp_get_thread_num();
        auto& stats = thread_local_stats[thread_id][pk];
        stats.first += qty;
        stats.second += 1;
    }

    // Merge thread-local aggregates into final threshold map
    std::unordered_map<int32_t, std::pair<int64_t, int32_t>> partkey_stats;
    for (int t = 0; t < num_threads; t++) {
        for (auto& [pk, stats] : thread_local_stats[t]) {
            auto& merged = partkey_stats[pk];
            merged.first += stats.first;
            merged.second += stats.second;
        }
    }

    // Compute thresholds ONLY for filtered part keys, use open-addressing hash table
    // This single map serves dual purpose: (1) stores thresholds, (2) marks which part keys are filtered
    CompactHashTable<int32_t, int64_t> partkey_threshold(filtered_part_count);

    // Rebuild filtered part set and compute thresholds in one pass
    for (int32_t i = 0; i < num_part; i++) {
        if (p_brand_data[i] == brand_23_code && p_container_data[i] == medbox_code) {
            int32_t pk = p_partkey_data[i];

            // Look up stats for this partkey
            auto it = partkey_stats.find(pk);
            if (it != partkey_stats.end()) {
                int64_t sum_qty = it->second.first;
                int32_t count_qty = it->second.second;

                // Compute 0.2 * AVG using floating point to avoid truncation
                // avg_qty_scaled = sum_qty / count_qty
                // threshold_scaled = 0.2 * avg_qty_scaled = 0.2 * sum_qty / count_qty
                double avg_qty_double = (double)sum_qty / (double)count_qty;
                double threshold_double = 0.2 * avg_qty_double;
                int64_t threshold = (int64_t)std::round(threshold_double);

                partkey_threshold.insert(pk, threshold);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_phase2_end = std::chrono::high_resolution_clock::now();
    double phase2_ms = std::chrono::duration<double, std::milli>(t_phase2_end - t_phase2_start).count();
    printf("[TIMING] precompute_thresholds: %.2f ms\n", phase2_ms);
#endif

    // ===== PHASE 3: JOIN LINEITEM WITH PART AND AGGREGATE =====
#ifdef GENDB_PROFILE
    auto t_phase3_start = std::chrono::high_resolution_clock::now();
#endif

    // Load l_extendedprice
    size_t l_extendedprice_size = 0;
    int64_t* l_extendedprice_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", l_extendedprice_size);

    if (!l_extendedprice_data) {
        std::cerr << "Error loading l_extendedprice data" << std::endl;
        return;
    }

    // Parallel aggregation with thread-local buffers
    std::vector<int64_t> thread_sums(num_threads, 0);

#pragma omp parallel for schedule(static, 100000)
    for (int32_t i = 0; i < num_lineitem; i++) {
        int32_t pk = l_partkey_data[i];

        // Single hash lookup into compact open-addressing table to find threshold
        // (implicitly checks if partkey is filtered in the part table)
        int64_t* threshold_ptr = partkey_threshold.find(pk);
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
    out << "avg_yearly\r\n";
    // Write result with 2 decimal places
    out << std::fixed << std::setprecision(2) << avg_yearly << "\r\n";
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
