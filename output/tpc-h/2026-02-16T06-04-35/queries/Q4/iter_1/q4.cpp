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
    // Use a vector of unordered_sets per thread, then merge
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_set<int32_t>> thread_sets(num_threads);

#pragma omp parallel for
    for (int32_t i = 0; i < LINEITEM_ROW_COUNT; ++i) {
        if (l_commitdate[i] < l_receiptdate[i]) {
            int tid = omp_get_thread_num();
            thread_sets[tid].insert(l_orderkey[i]);
        }
    }

    // Merge thread-local sets
    std::unordered_set<int32_t> valid_orderkeys;
    for (int t = 0; t < num_threads; ++t) {
        for (int32_t key : thread_sets[t]) {
            valid_orderkeys.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    auto build_end = std::chrono::high_resolution_clock::now();
    double build_ms = std::chrono::duration<double, std::milli>(build_end - build_start).count();
    printf("[TIMING] build_semijoin_set: %.2f ms\n", build_ms);
    printf("[TIMING] semijoin_set_size: %zu\n", valid_orderkeys.size());
#endif

    // ========================================================================
    // FILTER & GROUP BY
    // ========================================================================
#ifdef GENDB_PROFILE
    auto filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Group by o_orderpriority with low cardinality (5 groups)
    // Use an unordered_map for thread-safe counting (safer than flat array with potential code issues)
    std::unordered_map<int8_t, int32_t> count_by_priority;

#pragma omp parallel
    {
        // Thread-local map
        std::unordered_map<int8_t, int32_t> local_counts;

#pragma omp for nowait
        for (int32_t i = 0; i < ORDERS_ROW_COUNT; ++i) {
            // Check date range
            if (o_orderdate[i] >= DATE_START && o_orderdate[i] < DATE_END) {
                // Check if orderkey exists in semi-join set
                if (valid_orderkeys.count(o_orderkey[i])) {
                    // Increment count for this priority
                    int8_t priority_code = o_orderpriority_codes[i];
                    local_counts[priority_code]++;
                }
            }
        }

        // Merge thread-local counts
#pragma omp critical
        {
            for (const auto& [code, count] : local_counts) {
                count_by_priority[code] += count;
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
    for (const auto& [code, count] : count_by_priority) {
        auto it = priority_dict.find(code);
        if (it != priority_dict.end()) {
            results.push_back({it->second, count});
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
