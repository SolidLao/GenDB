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
#include <atomic>
#include <omp.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*
================================================================================
LOGICAL PLAN for Q13 (ITERATION 7 - PARALLELIZED)
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

================================================================================
PHYSICAL PLAN for Q13 (ITERATION 7 - PARALLELIZED)
================================================================================

Step 1 (Physical - Data Loading):
  - Load customer.c_custkey via mmap (1.5M rows)
  - Load orders.o_custkey, o_comment via mmap (15M rows)

Step 2 (Physical - Dictionary Filtering):
  - Load o_comment_dict.txt (sequential, small data)
  - Find all codes where the string contains "special...requests"
  - Build unordered_set<int32_t> of codes to filter out

Step 3 (Physical - Parallel Orders Aggregation):
  - Initialize array for all customers: cust_order_count[1.5M] = 0
  - OpenMP parallel for over 15M orders, partitioned by thread
  - Each thread scans its chunk of orders:
    - If o_comment NOT in special_requests_codes:
      - Atomically increment cust_order_count[o_custkey]
  - Synchronization via OpenMP barrier at end (implicit in parallel for)
  - Result: array with order counts for all customers

Step 4 (Physical - Second Aggregation):
  - Create second hash map: c_count → custdist
  - Iterate all customers (1.5M entries)
  - For each customer with order count c_count, increment count_dist[c_count]
  - This will have ~25 distinct values

Step 5 (Physical - Sorting & Output):
  - Convert aggregation map to vector of (c_count, custdist) pairs
  - Sort by custdist DESC, then c_count DESC
  - Write to CSV

Parallelism Strategy:
  - Dictionary filtering: single-threaded (small data)
  - Orders aggregation: PARALLELIZED with OpenMP (15M rows across 64 cores)
    - Using atomic increments to avoid locks
    - OR: use thread-local buffers + merge (faster but requires more code)
  - Second aggregation: single-threaded (low cardinality)
  - Sorting: std::sort (small data ~25 rows)

Data Structure Choices:
  - First level: Array<int32_t, 1.5M> for customer order counts (indexed by relative position)
    - Actually: hash map (int32_t→int32_t) keyed by c_custkey for compatibility
    - USING ATOMIC INCREMENTS for thread-safe updates
  - Second level: std::unordered_map<int32_t, int32_t> (c_count → custdist)
  - Dictionary codes: std::unordered_set<int32_t> for O(1) lookup

Key Optimizations:
  - Parallel orders scan: ~20-40x expected speedup on 64 cores
  - Single aggregation pass (no second lookup phase)
  - Atomic counter pattern avoids locks

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
    std::unordered_set<int32_t> special_requests_codes;
    {
        std::ifstream dict_file(orders_dir + "/o_comment_dict.txt");
        if (!dict_file.good()) {
            std::cerr << "Failed to load o_comment_dict.txt" << std::endl;
            return;
        }

        std::string line;
        int32_t code = 0;
        while (std::getline(dict_file, line)) {
            // Check if value contains "special" followed by "requests"
            // Pattern: "special%requests" means both must appear with special before requests
            size_t special_pos = line.find("special");
            if (special_pos != std::string::npos) {
                // Look for "requests" after "special"
                size_t requests_pos = line.find("requests", special_pos + 7);  // +7 to skip "special"
                if (requests_pos != std::string::npos) {
                    special_requests_codes.insert(code);
                }
            }
            code++;
        }
    }

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double ms_dict = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] dictionary_filter: %.2f ms (found %zu codes)\n", ms_dict, special_requests_codes.size());
#endif

    // ========== Build First Aggregation: Count Filtered Orders per Customer (PARALLELIZED) ==========
#ifdef GENDB_PROFILE
    auto t_agg1_start = std::chrono::high_resolution_clock::now();
#endif

    // Use a hash map to store customer order counts (thread-safe via OpenMP reduction or atomics)
    // For correctness and simplicity, we'll use thread-local maps + merge pattern
    std::unordered_map<int32_t, int32_t> cust_order_count;
    cust_order_count.reserve(num_customers);

    // Initialize all customers with 0 count (LEFT OUTER JOIN semantics)
    for (int32_t i = 0; i < num_customers; ++i) {
        cust_order_count[customer_custkey[i]] = 0;
    }

    // Parallel scan of orders with thread-local aggregation
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, int32_t>> thread_local_counts(num_threads);

#pragma omp parallel for num_threads(num_threads) schedule(static)
    for (int32_t i = 0; i < num_orders; ++i) {
        int thread_id = omp_get_thread_num();
        int32_t cust_key = orders_custkey[i];
        int32_t comment_code = orders_comment[i];

        // If comment code NOT in special_requests set, count it
        if (special_requests_codes.find(comment_code) == special_requests_codes.end()) {
            thread_local_counts[thread_id][cust_key]++;
        }
    }

    // Merge thread-local counts into global map
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& [cust_key, count] : thread_local_counts[t]) {
            cust_order_count[cust_key] += count;
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
