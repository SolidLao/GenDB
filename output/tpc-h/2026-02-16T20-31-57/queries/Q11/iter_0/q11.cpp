#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

// ============================================================================
// LOGICAL PLAN:
// 1. Load nation table, find GERMANY's nationkey via dictionary lookup
// 2. Build hash table on supplier.s_suppkey -> (s_nationkey) for fast lookup
// 3. TWO PASSES OVER PARTSUPP:
//    - Pass 1: Calculate total SUM(ps_supplycost * ps_availqty) for GERMANY suppliers
//    - Pass 2: Group by ps_partkey, aggregate SUM(ps_supplycost * ps_availqty),
//      filter with HAVING, build result set
// 4. Sort results by value DESC
// 5. Output to CSV
//
// PHYSICAL PLAN:
// - Nation lookup: Direct array since only 25 entries
// - Supplier hash table: std::unordered_map<int32_t, int32_t> for s_suppkey->s_nationkey
// - Partsupp aggregation: std::unordered_map<int32_t, int64_t> for ps_partkey->sum
// - Joins: Hash probe on supplier hash table during partsupp scan
// - Decimal arithmetic: scale_factor=2, accumulate as int64_t
// ============================================================================

struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile() : fd(-1), ptr(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            std::cerr << "fstat failed for " << path << std::endl;
            close();
            return false;
        }

        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "mmap failed for " << path << std::endl;
            close();
            return false;
        }

        return true;
    }

    void close() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
            ptr = nullptr;
        }
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

// Load dictionary file: returns code -> string mapping
// Dictionary format: each line is a value, line number (0-indexed) is the code
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(f, line)) {
        if (line.empty()) {
            code++;
            continue;
        }
        // Try parsing as "code=value" format first
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
            code++;
        } else {
            // Simple line-based format: code is line number
            dict[code] = line;
            code++;
        }
    }
    f.close();
    return dict;
}

// Find which code maps to a specific string value
int32_t find_dict_code(const std::unordered_map<int32_t, std::string>& dict, const std::string& target) {
    for (const auto& [code, value] : dict) {
        if (value == target) return code;
    }
    return -1;  // Not found
}

void run_q11(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ========================================================================
    // 1. Load NATION table and find GERMANY
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile nation_nationkey_file, nation_name_file;
    if (!nation_nationkey_file.open(gendb_dir + "/nation/n_nationkey.bin")) return;
    if (!nation_name_file.open(gendb_dir + "/nation/n_name.bin")) return;

    int32_t* nation_nationkey = (int32_t*)nation_nationkey_file.ptr;
    int32_t* nation_name = (int32_t*)nation_name_file.ptr;

    // Load dictionary for nation names
    auto n_name_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");

    // Find GERMANY's nation key
    int32_t germany_code = find_dict_code(n_name_dict, "GERMANY");
    int32_t germany_nationkey = -1;

    for (size_t i = 0; i < 25; i++) {
        if (nation_name[i] == germany_code) {
            germany_nationkey = nation_nationkey[i];
            break;
        }
    }

    if (germany_nationkey == -1) {
        std::cerr << "GERMANY not found in nation table" << std::endl;
        return;
    }


    #ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_nation: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 2. Load SUPPLIER table and build hash table
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile supplier_suppkey_file, supplier_nationkey_file;
    if (!supplier_suppkey_file.open(gendb_dir + "/supplier/s_suppkey.bin")) return;
    if (!supplier_nationkey_file.open(gendb_dir + "/supplier/s_nationkey.bin")) return;

    int32_t* supplier_suppkey = (int32_t*)supplier_suppkey_file.ptr;
    int32_t* supplier_nationkey = (int32_t*)supplier_nationkey_file.ptr;

    const int32_t num_suppliers = 100000;

    // Build hash table: suppkey -> nationkey
    std::unordered_map<int32_t, int32_t> supplier_ht;
    supplier_ht.reserve(num_suppliers);

    for (int32_t i = 0; i < num_suppliers; i++) {
        supplier_ht[supplier_suppkey[i]] = supplier_nationkey[i];
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_supplier_ht: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 3. Load PARTSUPP table
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile partsupp_partkey_file, partsupp_suppkey_file, partsupp_supplycost_file, partsupp_availqty_file;
    if (!partsupp_partkey_file.open(gendb_dir + "/partsupp/ps_partkey.bin")) return;
    if (!partsupp_suppkey_file.open(gendb_dir + "/partsupp/ps_suppkey.bin")) return;
    if (!partsupp_supplycost_file.open(gendb_dir + "/partsupp/ps_supplycost.bin")) return;
    if (!partsupp_availqty_file.open(gendb_dir + "/partsupp/ps_availqty.bin")) return;

    int32_t* partsupp_partkey = (int32_t*)partsupp_partkey_file.ptr;
    int32_t* partsupp_suppkey = (int32_t*)partsupp_suppkey_file.ptr;
    int64_t* partsupp_supplycost = (int64_t*)partsupp_supplycost_file.ptr;
    int32_t* partsupp_availqty = (int32_t*)partsupp_availqty_file.ptr;

    const int32_t num_partsupp = 8000000;

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_partsupp: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 4. PASS 1: Calculate total sum for GERMANY suppliers
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t total_sum = 0;
    int64_t germany_count = 0;

    for (int32_t i = 0; i < num_partsupp; i++) {
        int32_t suppkey = partsupp_suppkey[i];
        auto it = supplier_ht.find(suppkey);
        if (it != supplier_ht.end() && it->second == germany_nationkey) {
            germany_count++;
            // Calculate ps_supplycost * ps_availqty
            // ps_supplycost is stored as int64_t with scale_factor=2 (i.e., scaled by 100)
            // ps_availqty is int32_t (unscaled quantity)
            // Product = (supplycost_scaled) * availqty, scaled by 100
            int64_t product = partsupp_supplycost[i] * partsupp_availqty[i];
            total_sum += product;
        }
    }


    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] pass1_total_sum: %.2f ms\n", ms);
    #endif

    // Calculate threshold: 0.0001 * total_sum
    // ps_supplycost is scaled by 100, so the product (supplycost * availqty) is also scaled by 100
    // Therefore, the accumulated total_sum is 100x the semantic value
    // To get the correct 0.0001 factor, we divide by 100000 (100 * 10000)
    int64_t threshold = total_sum / 100000;

    // ========================================================================
    // 5. PASS 2: Aggregate SUM(ps_supplycost * ps_availqty) by ps_partkey
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int64_t> aggregation;
    aggregation.reserve(2000000);  // Estimated number of distinct partkeys

    for (int32_t i = 0; i < num_partsupp; i++) {
        int32_t suppkey = partsupp_suppkey[i];
        auto it = supplier_ht.find(suppkey);
        if (it != supplier_ht.end() && it->second == germany_nationkey) {
            int32_t partkey = partsupp_partkey[i];
            // Product = (supplycost_scaled) * availqty, scaled by 100
            int64_t product = partsupp_supplycost[i] * partsupp_availqty[i];
            aggregation[partkey] += product;
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 6. Apply HAVING filter and collect results
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::pair<int32_t, int64_t>> results;
    results.reserve(aggregation.size());

    for (const auto& [partkey, sum_val] : aggregation) {
        if (sum_val > threshold) {
            results.push_back({partkey, sum_val});
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] having_filter: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 7. Sort by value DESC
    // ========================================================================
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::sort(results.begin(), results.end(),
              [](const std::pair<int32_t, int64_t>& a, const std::pair<int32_t, int64_t>& b) {
                  return a.second > b.second;
              });

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // 8. Write CSV output
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q11.csv");
    out << "ps_partkey,value\n";

    // Scale factor is 2 for ps_supplycost, so result is scaled by 2
    // We need to output with 2 decimal places: divide by 100 (scale_factor * 50)
    // Actually: value is sum of (supplycost*availqty) where supplycost is scaled by 2
    // So result is scaled by 2. To output as decimal, divide by 2 to get back to unscaled,
    // then multiply by scale_factor=2 and divide by 100 for 2 decimal places.
    // Result: divide by 2 for CSV output with proper decimal formatting

    for (const auto& [partkey, sum_val] : results) {
        // sum_val is scaled by 100 (scale_factor=2 for decimal)
        // Output with 2 decimal places: divide by 100
        double value = sum_val / 100.0;
        out << partkey << "," << std::fixed << std::setprecision(2) << value << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
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

    run_q11(gendb_dir, results_dir);

    return 0;
}
#endif
