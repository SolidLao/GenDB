/*
 * Q13: Customer Distribution
 *
 * LOGICAL PLAN:
 * 1. Tables: customer (1.5M rows), orders (15M rows)
 * 2. LEFT OUTER JOIN: customer LEFT JOIN orders ON c_custkey = o_custkey
 *    AND o_comment NOT LIKE '%special%requests%'
 * 3. First aggregation: COUNT(o_orderkey) per c_custkey → (c_custkey, c_count)
 * 4. Second aggregation: COUNT(*) per c_count → (c_count, custdist)
 * 5. Sort: ORDER BY custdist DESC, c_count DESC
 *
 * PHYSICAL PLAN:
 * 1. Load customer.c_custkey (1.5M int32_t values)
 * 2. Load orders.o_custkey, orders.o_comment (15M rows)
 * 3. Build hash map: custkey → count of matching orders (apply filter during build)
 * 4. For each customer:
 *    - Look up count in hash map (default to 0 if not found - LEFT OUTER JOIN semantics)
 * 5. Second-level aggregation: hash table mapping c_count → custdist
 * 6. Sort results by custdist DESC, c_count DESC
 * 7. Parallelism: Parallel orders scan for building hash map, parallel customer scan
 *
 * PATTERN MATCHING:
 * "NOT LIKE '%special%requests%'" means:
 * - Find "special" substring in o_comment
 * - If found, check if "requests" appears anywhere after it
 * - If both found in sequence, EXCLUDE this order
 * - Otherwise, INCLUDE this order in the count
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>

// Helper function to mmap a binary column file
template<typename T>
T* mmap_column(const std::string& path, size_t expected_rows, size_t& actual_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat " << path << std::endl;
        close(fd);
        exit(1);
    }
    actual_rows = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        close(fd);
        exit(1);
    }
    close(fd);
    return static_cast<T*>(addr);
}

// Load strings from binary column (length-prefixed format)
std::vector<std::string> load_strings(const std::string& path, size_t expected_rows) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    std::vector<std::string> result;
    result.reserve(expected_rows);

    while (ifs) {
        uint32_t len;
        ifs.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!ifs) break;
        std::string s(len, '\0');
        ifs.read(&s[0], len);
        if (!ifs) break;
        result.push_back(std::move(s));
    }
    return result;
}

// Check if comment contains "special" followed by "requests"
// Pattern: %special%requests% means "special" appears before "requests"
bool contains_special_requests(const std::string& comment) {
    size_t pos_special = comment.find("special");
    if (pos_special == std::string::npos) return false;

    size_t pos_requests = comment.find("requests", pos_special + 7); // search after "special"
    return pos_requests != std::string::npos;
}

void run_q13(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // 1. Load customer data
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    size_t num_customers = 0;
    int32_t* c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", 1500000, num_customers);
#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms_load);
#endif

    // 2. Load orders data
#ifdef GENDB_PROFILE
    t_load_start = std::chrono::high_resolution_clock::now();
#endif
    size_t num_orders = 0;
    int32_t* o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", 15000000, num_orders);
    std::vector<std::string> o_comment = load_strings(gendb_dir + "/orders/o_comment.bin", num_orders);
#ifdef GENDB_PROFILE
    t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load_orders = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms_load_orders);
#endif

    // 3. Build hash map: custkey → count of matching orders
    //    Use parallel aggregation with thread-local hash maps
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, int32_t>> local_counts(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_orders; i++) {
            if (!contains_special_requests(o_comment[i])) {
                local_counts[tid][o_custkey[i]]++;
            }
        }
    }

    // Merge thread-local maps
    std::unordered_map<int32_t, int32_t> customer_order_counts;
    customer_order_counts.reserve(1000000); // expect ~1M unique customers
    for (auto& local_map : local_counts) {
        for (auto& [custkey, count] : local_map) {
            customer_order_counts[custkey] += count;
        }
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hash_map: %.2f ms\n", ms_build);
#endif

    // 4. Second aggregation: count customers by order count
    //    For LEFT OUTER JOIN: customers not in hash map have count = 0
#ifdef GENDB_PROFILE
    auto t_agg2_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::unordered_map<int32_t, int64_t>> local_dist(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_customers; i++) {
            int32_t custkey = c_custkey[i];
            auto it = customer_order_counts.find(custkey);
            int32_t c_count = (it != customer_order_counts.end()) ? it->second : 0;
            local_dist[tid][c_count]++;
        }
    }

    // Merge thread-local distribution maps
    std::unordered_map<int32_t, int64_t> final_counts;
    for (auto& local_map : local_dist) {
        for (auto& [c_count, custdist] : local_map) {
            final_counts[c_count] += custdist;
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg2_end = std::chrono::high_resolution_clock::now();
    double ms_agg2 = std::chrono::duration<double, std::milli>(t_agg2_end - t_agg2_start).count();
    printf("[TIMING] aggregation_level2: %.2f ms\n", ms_agg2);
#endif

    // 5. Sort results: ORDER BY custdist DESC, c_count DESC
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<int32_t, int64_t>> results;
    results.reserve(final_counts.size());
    for (auto& [c_count, custdist] : final_counts) {
        results.push_back({c_count, custdist});
    }

    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second; // custdist DESC
        return a.first > b.first; // c_count DESC
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    // 6. Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out(results_dir + "/Q13.csv");
    out << "c_count,custdist\n";
    for (auto& [c_count, custdist] : results) {
        out << c_count << "," << custdist << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    // Cleanup
    munmap(c_custkey, num_customers * sizeof(int32_t));
    munmap(o_custkey, num_orders * sizeof(int32_t));
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
