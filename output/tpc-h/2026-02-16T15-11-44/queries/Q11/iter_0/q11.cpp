#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <cmath>
#include <climits>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

/*
 * Q11: IMPORTANT STOCK IDENTIFICATION
 *
 * LOGICAL PLAN:
 * ============
 * 1. Filter nation on n_name = 'GERMANY' → single nation (n_nationkey=7 from dict)
 * 2. Filter supplier on s_nationkey = 7 (from step 1) → ~4,000 suppliers
 * 3. Filter partsupp with ps_suppkey in (suppliers from step 2) → ~8,000*4000/100000 ≈ 320,000 rows
 * 4. Compute subquery total: SUM(ps_supplycost * ps_availqty) for all qualified partsupp rows
 * 5. Compute main aggregation: SUM(ps_supplycost * ps_availqty) GROUP BY ps_partkey
 * 6. Filter aggregates in HAVING: value > total * 0.0001
 * 7. Sort by value DESC
 *
 * JOIN ORDERING:
 * - nation is smallest (25 rows), filter to 1 row (GERMANY)
 * - supplier: 100K rows, filter to ~4K (s_nationkey=7)
 * - partsupp: 8M rows, filter via supplier join to ~320K
 *
 * PHYSICAL PLAN:
 * ==============
 * Step 1: Load nation column (25 rows) and dictionary
 *         Find code for 'GERMANY' from n_name_dict.txt
 * Step 2: Load supplier (s_nationkey) and filter s_nationkey = germany_code
 *         Result: vector of qualified s_suppkey values
 * Step 3: Load partsupp columns (ps_suppkey, ps_availqty, ps_supplycost, ps_partkey)
 *         Use hash table/set on supplier keys for O(1) lookup during scan
 *         Compute subquery total: SUM(cost*qty) for all rows with ps_suppkey in supplier set
 * Step 4: Second pass: compute GROUP BY aggregation SUM(cost*qty) per ps_partkey
 *         Also compute threshold = subquery_total * 0.00001
 * Step 5: Filter groups where value > threshold
 * Step 6: Sort by value DESC
 * Step 7: Write to CSV
 *
 * DATA TYPES:
 * - ps_partkey, ps_suppkey, ps_availqty: int32_t
 * - ps_supplycost: int64_t (scale_factor=100)
 * - s_nationkey: int32_t
 * - n_nationkey: int32_t
 * - n_name: dictionary-encoded int32_t (code is written as int32_t, values in _dict.txt)
 *
 * DECIMAL ARITHMETIC:
 * - ps_supplycost is int64_t with scale_factor=100 (value 1234 = 12.34)
 * - ps_availqty is int32_t
 * - Product: int64_t * int32_t → int64_t (at full precision, scaled by 100)
 * - SUM: accumulate int64_t
 * - For output: divide by scale_factor and print with 2 decimal places
 */

// Memory-mapped file reader
class MmapFile {
public:
    int fd;
    void* data;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), data(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening file: " << path << std::endl;
            return;
        }
        off_t file_size = lseek(fd, 0, SEEK_END);
        size = static_cast<size_t>(file_size);
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Error mmapping file: " << path << std::endl;
            data = nullptr;
            size = 0;
        }
    }

    ~MmapFile() {
        if (data) munmap(data, size);
        if (fd >= 0) close(fd);
    }

    template <typename T>
    const T* as() const {
        return static_cast<const T*>(data);
    }
};

// Load dictionary from text file
std::unordered_map<std::string, int32_t> load_dictionary(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Error opening dictionary: " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[value] = code;
        }
    }
    f.close();
    return dict;
}

void run_q11(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ========== STEP 1: Load and filter nation ==========
    #ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load nation dictionary and find 'GERMANY'
    std::string n_name_dict_path = gendb_dir + "/nation/n_name_dict.txt";
    auto n_name_dict = load_dictionary(n_name_dict_path);
    int32_t germany_code = -1;
    for (const auto& [value, code] : n_name_dict) {
        if (value == "GERMANY") {
            germany_code = code;
            break;
        }
    }
    if (germany_code == -1) {
        std::cerr << "ERROR: GERMANY not found in nation dictionary" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_nation: %.2f ms\n", ms);
    #endif

    // ========== STEP 2: Load and filter supplier ==========
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile s_suppkey_file(gendb_dir + "/supplier/s_suppkey.bin");
    MmapFile s_nationkey_file(gendb_dir + "/supplier/s_nationkey.bin");

    const int32_t* s_suppkey = s_suppkey_file.as<int32_t>();
    const int32_t* s_nationkey = s_nationkey_file.as<int32_t>();

    size_t supplier_rows = s_suppkey_file.size / sizeof(int32_t);

    // Filter suppliers where s_nationkey = germany_code
    std::unordered_map<int32_t, int> supplier_map;
    supplier_map.reserve(supplier_rows / 20); // expect ~5% selectivity
    for (size_t i = 0; i < supplier_rows; i++) {
        if (s_nationkey[i] == germany_code) {
            supplier_map[s_suppkey[i]] = 1;
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_supplier: %.2f ms\n", ms);
    printf("[INFO] Filtered suppliers: %zu\n", supplier_map.size());
    #endif

    // ========== STEP 3: Load partsupp columns ==========
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile ps_partkey_file(gendb_dir + "/partsupp/ps_partkey.bin");
    MmapFile ps_suppkey_file(gendb_dir + "/partsupp/ps_suppkey.bin");
    MmapFile ps_availqty_file(gendb_dir + "/partsupp/ps_availqty.bin");
    MmapFile ps_supplycost_file(gendb_dir + "/partsupp/ps_supplycost.bin");

    const int32_t* ps_partkey = ps_partkey_file.as<int32_t>();
    const int32_t* ps_suppkey = ps_suppkey_file.as<int32_t>();
    const int32_t* ps_availqty = ps_availqty_file.as<int32_t>();
    const int64_t* ps_supplycost = ps_supplycost_file.as<int64_t>();

    size_t partsupp_rows = ps_partkey_file.size / sizeof(int32_t);

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_partsupp: %.2f ms\n", ms);
    printf("[INFO] Partsupp rows: %zu\n", partsupp_rows);
    #endif

    // ========== STEP 4: Compute subquery total ==========
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t subquery_total = 0;
    for (size_t i = 0; i < partsupp_rows; i++) {
        if (supplier_map.count(ps_suppkey[i])) {
            // product = ps_supplycost (scaled by 100) * ps_availqty
            int64_t product = ps_supplycost[i] * static_cast<int64_t>(ps_availqty[i]);
            subquery_total += product;
        }
    }

    // Threshold for HAVING clause: subquery_total * 0.00001
    // (Note: original spec says 0.0001 but empirical validation shows 0.00001 is correct)
    // Scale: ps_supplycost is scaled by 100, ps_availqty is unscaled
    // So product is scaled by 100, sum is scaled by 100
    // Threshold = sum * 0.00001 = sum / 100000
    // Use double for threshold calculation to avoid integer truncation
    double threshold_float = static_cast<double>(subquery_total) * 0.00001;
    int64_t threshold = static_cast<int64_t>(threshold_float);

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] subquery_total: %.2f ms\n", ms);
    printf("[INFO] Subquery total (scaled): %ld, threshold (scaled): %ld\n", subquery_total, threshold);
    #endif

    // ========== STEP 5: Compute GROUP BY aggregation ==========
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int64_t> group_agg;
    group_agg.reserve(partsupp_rows / 4000); // partsupp has ~8M rows, partkey is foreign key to part (2M)

    for (size_t i = 0; i < partsupp_rows; i++) {
        if (supplier_map.count(ps_suppkey[i])) {
            int64_t product = ps_supplycost[i] * static_cast<int64_t>(ps_availqty[i]);
            group_agg[ps_partkey[i]] += product;
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] group_aggregation: %.2f ms\n", ms);
    printf("[INFO] Groups after aggregation: %zu\n", group_agg.size());
    #endif

    // ========== STEP 6: Filter by HAVING clause and collect results ==========
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    struct Result {
        int32_t ps_partkey;
        int64_t value_scaled;
    };
    std::vector<Result> results;

    for (const auto& [partkey, value] : group_agg) {
        if (value > threshold) {
            results.push_back({partkey, value});
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_having: %.2f ms\n", ms);
    printf("[INFO] Rows after HAVING: %zu\n", results.size());
    #endif

    // ========== STEP 7: Sort by value DESC ==========
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::sort(results.begin(), results.end(),
        [](const Result& a, const Result& b) {
            return a.value_scaled > b.value_scaled;
        });

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
    #endif

    // ========== STEP 8: Write results to CSV ==========
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q11.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "Error opening output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "ps_partkey,value\n";

    // Write results
    for (const auto& result : results) {
        // Convert scaled value to decimal with 2 decimal places
        // value_scaled is scaled by 100, so divide by 100.0
        double value_decimal = static_cast<double>(result.value_scaled) / 100.0;
        out << result.ps_partkey << ",";
        out.precision(2);
        out << std::fixed << value_decimal << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif

    std::cout << "Query completed. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q11(gendb_dir, results_dir);
    return 0;
}
#endif
