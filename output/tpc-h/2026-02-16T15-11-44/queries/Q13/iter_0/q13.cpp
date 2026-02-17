#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>

/*
================================================================================
Q13 LOGICAL & PHYSICAL PLAN
================================================================================

LOGICAL PLAN:
1. Table customer (1.5M rows): No filters. Full scan to get c_custkey.
2. Table orders (15M rows): One filter: o_comment NOT LIKE '%special%requests%'
   - Estimated selectivity: ~99% (most comments don't match pattern)
   - Estimated rows after filter: ~14.85M

3. LEFT OUTER JOIN on c_custkey = o_custkey (with comment filter applied to orders)
   - All customers appear in result (even with 0 orders)
   - Smaller side (customer) has 1.5M rows, larger side (orders filtered) has ~14.85M

4. First aggregation: GROUP BY c_custkey, COUNT(o_orderkey)
   - Output: (c_custkey, order_count) for all 1.5M customers
   - Customers with no matching orders get count=0

5. Second aggregation: GROUP BY c_count, COUNT(*)
   - Input: (c_custkey, order_count) from first agg
   - Output: (count_value, frequency) where frequency = how many customers have that count

6. Final sort: ORDER BY custdist DESC, c_count DESC

PHYSICAL PLAN:
1. SCAN: customer.c_custkey via mmap
2. SCAN + FILTER: orders (o_orderkey, o_custkey, o_comment)
   - Load o_comment dictionary
   - Apply pattern filter: NOT (contains "special" AND contains "requests")
   - Result: ~14.85M rows

3. HASH JOIN:
   - Build: hash table on customer.c_custkey (1.5M keys)
   - Probe: filtered orders with o_custkey lookup
   - Parallelized probe with thread-local aggregation buffers

4. AGGREGATION 1: Hash map<c_custkey, count>
   - Merge thread-local buffers into final map
   - Add missing customers (count=0)

5. AGGREGATION 2: Flat array/map for count → frequency
   - Low cardinality (count values: 0-45)
   - Use std::map<count, frequency> for automatic sorting

6. SORT: std::vector with custom comparator
   - PRIMARY: custdist DESC
   - SECONDARY: c_count DESC

================================================================================
*/

// Mmap helper for loading binary files
class MmapFile {
public:
    int fd;
    void* addr;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), addr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Cannot open " + path);
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            close(fd);
            throw std::runtime_error("Cannot stat " + path);
        }
        size = sb.st_size;
        addr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("Cannot mmap " + path);
        }
    }

    ~MmapFile() {
        if (addr) munmap(addr, size);
        if (fd >= 0) close(fd);
    }

    template <typename T>
    const T* data() const {
        return static_cast<const T*>(addr);
    }

    size_t count() const {
        return size / sizeof(int32_t);
    }
};

// Load dictionary file (format: "code=value\n")
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        throw std::runtime_error("Cannot open " + dict_path);
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int32_t code = std::stoi(line.substr(0, eq));
        std::string value = line.substr(eq + 1);
        dict[code] = value;
    }
    return dict;
}

// Pattern matching: check if string matches '%special%requests%'
// This means "special" appears before "requests" in the string
bool matches_special_requests_pattern(const std::string& s) {
    size_t pos_special = s.find("special");
    if (pos_special == std::string::npos) {
        return false; // "special" not found
    }
    // Look for "requests" after "special"
    size_t pos_requests = s.find("requests", pos_special + 7); // +7 is length of "special"
    return (pos_requests != std::string::npos);
}

void run_q13(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load customer data
    #ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile customer_custkey_file(gendb_dir + "/customer/c_custkey.bin");
    const int32_t* customer_custkey = customer_custkey_file.data<int32_t>();
    size_t num_customers = customer_custkey_file.count();

    #ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_customer_data: %.2f ms\n", ms);
    #endif

    // Load orders data
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile orders_custkey_file(gendb_dir + "/orders/o_custkey.bin");
    MmapFile orders_comment_file(gendb_dir + "/orders/o_comment.bin");

    const int32_t* orders_custkey = orders_custkey_file.data<int32_t>();
    const int32_t* orders_comment = orders_comment_file.data<int32_t>();
    size_t num_orders = orders_custkey_file.count();

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders_data: %.2f ms\n", ms);
    #endif

    // Load o_comment dictionary
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    auto comment_dict = load_dictionary(gendb_dir + "/orders/o_comment_dict.txt");

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_dictionary: %.2f ms\n", ms);
    #endif

    // Build hash map on customer custkey
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, size_t> customer_map;
    customer_map.reserve(num_customers);
    for (size_t i = 0; i < num_customers; ++i) {
        customer_map[customer_custkey[i]] = i;
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_customer_hash: %.2f ms\n", ms);
    #endif

    // Scan and filter orders, apply LEFT JOIN with customer
    // Group by c_custkey and count matching orders
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local aggregation for first aggregation (GROUP BY c_custkey, COUNT(o_orderkey))
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, int32_t>> thread_local_agg(num_threads);

    // Parallel scan + filter + join probe
    #pragma omp parallel for schedule(dynamic, 65536)
    for (int64_t i = 0; i < (int64_t)num_orders; ++i) {
        // Check filter: o_comment NOT LIKE '%special%requests%'
        int32_t comment_code = orders_comment[i];
        const auto& it = comment_dict.find(comment_code);
        if (it == comment_dict.end()) {
            continue; // Code not found, skip (shouldn't happen)
        }

        const std::string& comment_str = it->second;
        bool matches_pattern = matches_special_requests_pattern(comment_str);

        // Filter: NOT LIKE '%special%requests%'
        if (matches_pattern) {
            continue; // Skip this order
        }

        // This order passes the filter, join with customer
        int32_t o_custkey = orders_custkey[i];

        // Increment count for this customer
        int tid = omp_get_thread_num();
        thread_local_agg[tid][o_custkey]++;
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] scan_filter_join_probe: %.2f ms\n", ms);
    #endif

    // Merge thread-local aggregations
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int32_t> customer_order_count;
    for (int tid = 0; tid < num_threads; ++tid) {
        for (auto& [custkey, count] : thread_local_agg[tid]) {
            customer_order_count[custkey] += count;
        }
    }

    // Add missing customers (LEFT JOIN semantics): customers with 0 orders
    for (size_t i = 0; i < num_customers; ++i) {
        int32_t custkey = customer_custkey[i];
        if (customer_order_count.find(custkey) == customer_order_count.end()) {
            customer_order_count[custkey] = 0;
        }
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] merge_aggregation: %.2f ms\n", ms);
    #endif

    // Second aggregation: GROUP BY c_count (order count), COUNT(*)
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::map<int32_t, int32_t> count_frequency; // count_value -> frequency
    for (auto& [custkey, count] : customer_order_count) {
        count_frequency[count]++;
    }

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] second_aggregation: %.2f ms\n", ms);
    #endif

    // Prepare results for sorting (collect into vector)
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::pair<int32_t, int32_t>> results;
    for (auto& [count, freq] : count_frequency) {
        results.push_back({count, freq});
    }

    // Sort: PRIMARY custdist DESC, SECONDARY c_count DESC
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second; // custdist DESC
            }
            return a.first > b.first; // c_count DESC
        }
    );

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
    #endif

    // Write results to CSV
    #ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_file = results_dir + "/Q13.csv";
    std::ofstream outfile(output_file);
    outfile << "c_count,custdist\n";
    for (auto& [c_count, custdist] : results) {
        outfile << c_count << "," << custdist << "\n";
    }
    outfile.close();

    #ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
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
    run_q13(gendb_dir, results_dir);
    return 0;
}
#endif
