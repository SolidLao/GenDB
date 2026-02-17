#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <omp.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*
================================================================================
LOGICAL PLAN for Q13
================================================================================

Query Structure:
  SELECT c_count, COUNT(*) AS custdist
  FROM (
    SELECT c_custkey, COUNT(o_orderkey) AS c_count
    FROM customer LEFT OUTER JOIN orders ON
      c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
  ) AS c_orders
  GROUP BY c_count
  ORDER BY custdist DESC, c_count DESC;

Execution Plan:

Step 1 (Logical - Filtering):
  - Load o_comment dictionary to identify codes matching "special%requests" pattern
  - During orders scan, build set of codes to EXCLUDE (those matching the pattern)
  - Load all customer c_custkey values (1.5M rows)
  - Load all orders (o_custkey, o_comment) data (15M rows)

Step 2 (Logical - Join + Aggregation Level 1):
  - For each customer c_custkey:
    - Count matching orders (LEFT OUTER JOIN semantics - all customers kept)
    - Only count orders where o_comment NOT in special_requests_codes set
    - Result: c_custkey → c_count (order count per customer)
  - Cardinality: 1.5M rows (one per customer)

Step 3 (Logical - Aggregation Level 2):
  - Group by c_count (order counts): low cardinality (~25 distinct values)
  - Count customers per order count
  - Result: c_count → custdist (customer count per order count)

Step 4 (Logical - Sorting):
  - Sort by custdist DESC, then c_count DESC
  - Final output: ~25 rows

Join Strategy:
  - Since this is a LEFT OUTER JOIN, all customers are kept regardless of matches
  - We need to count matching orders for each customer
  - Simplest approach: scan all customers, for each count matching orders
  - Alternative: build hash map from orders first, then iterate customers

Implementation Choice:
  - Build multimap from orders: cust_key → count of non-matching orders
  - Iterate all customers: look up count in multimap, default to 0 if not found
  - More cache-efficient than hash index probing with wrong hash function

================================================================================
PHYSICAL PLAN for Q13
================================================================================

Step 1 (Physical - Data Loading):
  - Load customer.c_custkey via mmap (1.5M rows)
  - Load orders.o_custkey, o_comment via mmap (15M rows)

Step 2 (Physical - Dictionary Filtering):
  - Load o_comment_dict.txt
  - Dictionary format: one value per line, line number is the code
  - Find all codes where the string contains "special...requests"
  - Build unordered_set<int32_t> of codes to filter out

Step 3 (Physical - Build Orders Aggregation Map):
  - Iterate all 15M orders
  - For each order:
    - If o_comment NOT in special_requests_codes:
      - Increment count for o_custkey in hash map
  - Result: unordered_map<int32_t, int32_t> mapping c_custkey → filtered order count
  - This is O(15M) with low constant factor

Step 4 (Physical - First Aggregation):
  - Create hash map: c_custkey → order_count
  - Initialize all customers with count from Step 3 (default 0)
  - Result: 1.5M entries

Step 5 (Physical - Second Aggregation):
  - Create second hash map: c_count → custdist
  - Iterate first aggregation result (1.5M entries)
  - For each (c_custkey, c_count) pair, increment count for that c_count
  - This will have ~25 distinct values

Step 6 (Physical - Sorting & Output):
  - Convert aggregation map to vector of (c_count, custdist) pairs
  - Sort by custdist DESC, then c_count DESC
  - Write to CSV

Parallelism Strategy:
  - Dictionary filtering: single-threaded (small data, I/O bound)
  - Orders aggregation: single-threaded (15M rows, but hash map insertion is sequential)
    - Could use thread-local buffers + merge for parallelism, but not critical
  - Second aggregation: single-threaded (small data)
  - Sorting: std::sort (single-threaded, small data ~25 rows)

Data Structure Choices:
  - First level: std::unordered_map<int32_t, int32_t> (c_custkey → c_count)
  - Second level: std::unordered_map<int32_t, int32_t> (c_count → custdist)
  - Dictionary codes: std::unordered_set<int32_t> for O(1) lookup

Index Usage:
  - NOT using pre-built hash index (requires correct hash function)
  - Direct scan is simpler and just as efficient for this query

================================================================================
*/

// Memory-mapped file helper
class MappedFile {
public:
    int fd;
    void* ptr;
    size_t size;

    MappedFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return;
        }
        off_t sz = lseek(fd, 0, SEEK_END);
        size = sz;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            ptr = nullptr;
        }
    }

    ~MappedFile() {
        if (ptr && ptr != MAP_FAILED) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }

    template<typename T>
    T* as() { return static_cast<T*>(ptr); }

    bool is_valid() { return ptr && ptr != MAP_FAILED; }
};

void run_Q13(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    std::string customer_dir = gendb_dir + "/customer";
    std::string orders_dir = gendb_dir + "/orders";

    // ========== Load Data ==========
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load customer custkey
    MappedFile customer_custkey_file(customer_dir + "/c_custkey.bin");
    if (!customer_custkey_file.is_valid()) {
        std::cerr << "Failed to load customer custkey" << std::endl;
        return;
    }
    int32_t* customer_custkey = customer_custkey_file.as<int32_t>();
    int32_t num_customers = customer_custkey_file.size / sizeof(int32_t);

    // Load orders columns
    MappedFile orders_custkey_file(orders_dir + "/o_custkey.bin");
    MappedFile orders_comment_file(orders_dir + "/o_comment.bin");

    if (!orders_custkey_file.is_valid() || !orders_comment_file.is_valid()) {
        std::cerr << "Failed to load orders data" << std::endl;
        return;
    }

    int32_t* orders_custkey = orders_custkey_file.as<int32_t>();
    int32_t* orders_comment = orders_comment_file.as<int32_t>();
    int32_t num_orders = orders_custkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    // ========== Load and Parse Dictionary ==========
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    // Load o_comment dictionary to find codes matching "special%requests"
    // Pattern: "special%requests" where % is a wildcard
    // This means: contains "special" followed (eventually) by "requests"
    // Dictionary format: one value per line, line number (0-indexed) is the code

    // First pass: count total lines to pre-allocate vector
    std::vector<std::string> dict_lines;
    {
        std::ifstream dict_file(orders_dir + "/o_comment_dict.txt");
        if (!dict_file.good()) {
            std::cerr << "Failed to load o_comment_dict.txt" << std::endl;
            return;
        }

        std::string line;
        while (std::getline(dict_file, line)) {
            dict_lines.push_back(line);
        }
    }

    // Parallel dictionary filtering with thread-local sets
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_set<int32_t>> thread_local_codes(num_threads);

    #pragma omp parallel for schedule(dynamic, 10000)
    for (int32_t code = 0; code < (int32_t)dict_lines.size(); ++code) {
        const std::string& line = dict_lines[code];
        // Check if value contains "special" followed by "requests"
        size_t special_pos = line.find("special");
        if (special_pos != std::string::npos) {
            // Look for "requests" after "special"
            size_t requests_pos = line.find("requests", special_pos + 7);  // +7 to skip "special"
            if (requests_pos != std::string::npos) {
                int tid = omp_get_thread_num();
                thread_local_codes[tid].insert(code);
            }
        }
    }

    // Merge thread-local sets into single set
    std::unordered_set<int32_t> special_requests_codes;
    for (int t = 0; t < num_threads; ++t) {
        for (int32_t code : thread_local_codes[t]) {
            special_requests_codes.insert(code);
        }
    }

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double ms_dict = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] dictionary_filter: %.2f ms (found %zu codes)\n", ms_dict, special_requests_codes.size());
#endif

    // ========== Build First Aggregation: Count Filtered Orders per Customer ==========
#ifdef GENDB_PROFILE
    auto t_agg1_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel phase: scan all orders with thread-local hash tables
    int num_threads_agg = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, int32_t>> thread_local_counts(num_threads_agg);

    // Pre-allocate thread-local maps
    for (int t = 0; t < num_threads_agg; ++t) {
        thread_local_counts[t].reserve(num_customers / num_threads_agg + 1000);
    }

    // Parallel scan: each thread maintains its own aggregation table
    #pragma omp parallel for schedule(static)
    for (int32_t i = 0; i < num_orders; ++i) {
        int32_t cust_key = orders_custkey[i];
        int32_t comment_code = orders_comment[i];

        // If comment code NOT in special_requests set, count it
        if (special_requests_codes.find(comment_code) == special_requests_codes.end()) {
            int tid = omp_get_thread_num();
            thread_local_counts[tid][cust_key]++;
        }
    }

    // Merge phase: combine thread-local results
    std::unordered_map<int32_t, int32_t> cust_filtered_order_count;
    cust_filtered_order_count.reserve(num_customers);

    for (int t = 0; t < num_threads_agg; ++t) {
        for (const auto& [cust_key, count] : thread_local_counts[t]) {
            cust_filtered_order_count[cust_key] += count;
        }
    }

    // Initialize all customers (including those with 0 orders)
    std::unordered_map<int32_t, int32_t> cust_order_count;
    cust_order_count.reserve(num_customers);

    for (int32_t i = 0; i < num_customers; ++i) {
        int32_t cust_key = customer_custkey[i];
        // Default to 0 if not in the filtered map
        if (cust_filtered_order_count.find(cust_key) != cust_filtered_order_count.end()) {
            cust_order_count[cust_key] = cust_filtered_order_count[cust_key];
        } else {
            cust_order_count[cust_key] = 0;
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg1_end = std::chrono::high_resolution_clock::now();
    double ms_agg1 = std::chrono::duration<double, std::milli>(t_agg1_end - t_agg1_start).count();
    printf("[TIMING] first_aggregation: %.2f ms\n", ms_agg1);
#endif

    // ========== Second Aggregation: Count Customers per Order Count ==========
#ifdef GENDB_PROFILE
    auto t_agg2_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> count_dist;
    for (const auto& [cust_key, order_count] : cust_order_count) {
        count_dist[order_count]++;
    }

#ifdef GENDB_PROFILE
    auto t_agg2_end = std::chrono::high_resolution_clock::now();
    double ms_agg2 = std::chrono::duration<double, std::milli>(t_agg2_end - t_agg2_start).count();
    printf("[TIMING] second_aggregation: %.2f ms\n", ms_agg2);
#endif

    // ========== Sorting ==========
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    // Convert to vector for sorting
    std::vector<std::pair<int32_t, int32_t>> result_rows;
    for (const auto& [c_count, custdist] : count_dist) {
        result_rows.push_back({c_count, custdist});
    }

    // Sort by custdist DESC, then c_count DESC
    std::sort(result_rows.begin(), result_rows.end(),
        [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second;  // custdist DESC
            }
            return a.first > b.first;  // c_count DESC
        });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

    // ========== Write Output ==========
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q13.csv";
    std::ofstream output_file(output_path);

    if (!output_file.good()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    output_file << "c_count,custdist\n";

    // Write results
    for (const auto& [c_count, custdist] : result_rows) {
        output_file << c_count << "," << custdist << "\n";
    }

    output_file.close();

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

    std::cout << "Q13 completed. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_Q13(gendb_dir, results_dir);
    return 0;
}
#endif
