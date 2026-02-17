#include <algorithm>
#include <chrono>
#include <cstring>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <omp.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>

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

// Fast open-addressing hash table with linear probing
template<typename K, typename V>
class FastHashTable {
public:
    struct Entry {
        K key;
        V value;
        bool occupied;

        Entry() : key(0), value(0), occupied(false) {}
    };

    std::vector<Entry> table;
    size_t size;

    FastHashTable(size_t capacity) : size(0) {
        // Size to next power of 2 at 1.3x capacity for ~77% load factor and efficient masking
        size_t actual_cap = (size_t)(capacity * 1.3);
        size_t pow2_cap = 1;
        while (pow2_cap < actual_cap) pow2_cap <<= 1;
        table.resize(pow2_cap);
    }

    inline size_t hash(K key) const {
        // Fibonacci hash with bit shift for better distribution and modulo-free computation
        // Assumes table size is power of 2 (will be set in constructor)
        return ((size_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    V& operator[](K key) {
        size_t mask = table.size() - 1;  // Power-of-2 masking
        size_t idx = hash(key) & mask;
        size_t start_idx = idx;

        while (table[idx].occupied && table[idx].key != key) {
            idx = (idx + 1) & mask;
            if (idx == start_idx) {
                // Table full - should not happen with 1.3x sizing
                throw std::runtime_error("Hash table full");
            }
        }

        if (!table[idx].occupied) {
            table[idx].key = key;
            table[idx].value = V(0);
            table[idx].occupied = true;
        }

        return table[idx].value;
    }

    V* find(K key) {
        size_t mask = table.size() - 1;  // Power-of-2 masking
        size_t idx = hash(key) & mask;
        size_t start_idx = idx;

        while (table[idx].occupied) {
            if (table[idx].key == key) {
                return &table[idx].value;
            }
            idx = (idx + 1) & mask;
            if (idx == start_idx) break;
        }

        return nullptr;
    }
};

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

    // First pass: count dictionary entries
    int32_t num_dict_entries = 0;
    {
        std::ifstream dict_file(orders_dir + "/o_comment_dict.txt");
        if (!dict_file.good()) {
            std::cerr << "Failed to load o_comment_dict.txt" << std::endl;
            return;
        }
        std::string line;
        while (std::getline(dict_file, line)) {
            num_dict_entries++;
        }
    }

    // Second pass: parse with correct size
    std::vector<bool> is_special_code(num_dict_entries, false);
    {
        std::ifstream dict_file(orders_dir + "/o_comment_dict.txt");
        if (!dict_file.good()) {
            std::cerr << "Failed to load o_comment_dict.txt (second pass)" << std::endl;
            return;
        }

        std::string line;
        line.reserve(256);  // Pre-allocate string buffer
        int32_t code = 0;

        while (std::getline(dict_file, line)) {
            // Check if value contains "special" followed by "requests"
            // Inline the pattern match for speed
            const char* str = line.c_str();
            const char* special_pos = std::strstr(str, "special");
            if (special_pos != nullptr) {
                // Look for "requests" after "special" (offset by length of "special")
                if (std::strstr(special_pos + 7, "requests") != nullptr) {
                    is_special_code[code] = true;
                }
            }
            code++;
            line.clear();
        }
    }

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double ms_dict = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    size_t num_special = 0;
    for (bool b : is_special_code) if (b) num_special++;
    printf("[TIMING] dictionary_filter: %.2f ms (found %zu codes)\n", ms_dict, num_special);
#endif

    // ========== Build First Aggregation: Count Filtered Orders per Customer ==========
#ifdef GENDB_PROFILE
    auto t_agg1_start = std::chrono::high_resolution_clock::now();
#endif

    // Use open-addressing hash table instead of unordered_map
    FastHashTable<int32_t, int32_t> cust_filtered_order_count(num_customers);

    // Check if is_special_code is empty and extend if needed
    int32_t max_comment_code = 0;
    for (int32_t i = 0; i < num_orders; ++i) {
        if (orders_comment[i] > max_comment_code) {
            max_comment_code = orders_comment[i];
        }
    }
    if ((size_t)max_comment_code >= is_special_code.size()) {
        is_special_code.resize(max_comment_code + 1, false);
    }

    // Scan all orders and count non-matching ones per customer (parallel)
    // Use limited thread-local buffers to avoid excessive memory and merge overhead
    // Rule: limit to CPU core count for better cache locality during merge
    int num_threads = std::min(16, (int)std::thread::hardware_concurrency());
    if (num_threads <= 0) num_threads = 1;

    std::vector<FastHashTable<int32_t, int32_t>> thread_local_tables;
    for (int t = 0; t < num_threads; ++t) {
        thread_local_tables.emplace_back(num_customers);
    }

    #pragma omp parallel for schedule(static, 100000) num_threads(16)
    for (int32_t i = 0; i < num_orders; ++i) {
        int32_t cust_key = orders_custkey[i];
        int32_t comment_code = orders_comment[i];

        // Check if comment_code is in valid range and if it's special
        bool is_special = false;
        if ((size_t)comment_code < is_special_code.size()) {
            is_special = is_special_code[comment_code];
        }

        // If comment code NOT special, count it
        if (!is_special) {
            int tid = omp_get_thread_num();
            thread_local_tables[tid][cust_key]++;
        }
    }

    // Merge thread-local tables into main table (sequential, cache-friendly merge)
    for (int t = 0; t < num_threads; ++t) {
        // Pre-reserve space in the main table to avoid rehashes during merge
        for (const auto& entry : thread_local_tables[t].table) {
            if (entry.occupied) {
                cust_filtered_order_count[entry.key] += entry.value;
            }
        }
    }

    // Clear thread-local tables to free memory
    thread_local_tables.clear();

    // Initialize all customers (including those with 0 orders)
    FastHashTable<int32_t, int32_t> cust_order_count(num_customers);

    for (int32_t i = 0; i < num_customers; ++i) {
        int32_t cust_key = customer_custkey[i];
        // Look up in filtered map; default to 0 if not found
        int32_t* count_ptr = cust_filtered_order_count.find(cust_key);
        cust_order_count[cust_key] = count_ptr ? *count_ptr : 0;
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

    // Use open-addressing hash table for second aggregation as well (small cardinality but consistent approach)
    FastHashTable<int32_t, int32_t> count_dist(256);  // Small cardinality (~25 distinct values)
    for (const auto& entry : cust_order_count.table) {
        if (entry.occupied) {
            count_dist[entry.value]++;
        }
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
    for (const auto& entry : count_dist.table) {
        if (entry.occupied) {
            result_rows.push_back({entry.key, entry.value});
        }
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
