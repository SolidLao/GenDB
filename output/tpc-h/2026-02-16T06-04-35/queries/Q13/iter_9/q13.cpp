#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <omp.h>

// Helper function to read binary column via mmap
template <typename T>
std::vector<T> load_column(const std::string& filepath, size_t num_rows) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    size_t bytes_needed = num_rows * sizeof(T);
    void* mapped = mmap(NULL, bytes_needed, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed for: " + filepath);
    }

    std::vector<T> result(num_rows);
    std::memcpy(result.data(), mapped, bytes_needed);
    munmap(mapped, bytes_needed);
    close(fd);
    return result;
}

// Helper function to load VARCHAR column as strings
// Format: [uint32_t count] [uint32_t offsets[count]] [string_data...]
std::vector<std::string> load_varchar_column(const std::string& filepath, size_t num_rows) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    // Map entire file
    void* mapped = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap failed for: " + filepath);
    }

    uint8_t* data = static_cast<uint8_t*>(mapped);
    size_t offset = 0;

    // Read count
    uint32_t count = *reinterpret_cast<const uint32_t*>(data + offset);
    offset += sizeof(uint32_t);

    // Read offsets
    std::vector<uint32_t> offsets(count);
    std::memcpy(offsets.data(), data + offset, count * sizeof(uint32_t));
    offset += count * sizeof(uint32_t);

    // String data starts at 'offset'
    const uint8_t* string_data = data + offset;

    std::vector<std::string> result;
    result.reserve(num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        uint32_t start = offsets[i];
        uint32_t end = (i + 1 < num_rows) ? offsets[i + 1] : (file_size - offset);
        uint32_t len = end - start;

        std::string str(reinterpret_cast<const char*>(string_data + start), len);
        result.push_back(str);
    }

    munmap(mapped, file_size);
    close(fd);
    return result;
}

// Check if a string matches the LIKE pattern '%special%requests%'
// Pattern: must contain 'special', then later must contain 'requests'
// Optimized: use C-style strstr which is often better optimized than std::string::find
inline bool matches_pattern(const std::string& s) {
    // Minimum length: "special" (7) + at least one char between + "requests" (8) = 16
    size_t len = s.length();
    if (len < 16) {
        return false;
    }

    // Use C-style strstr for faster substring search (often SIMD-accelerated in libc)
    const char* pos_special = std::strstr(s.c_str(), "special");
    if (!pos_special) {
        return false;
    }

    // Check if there's room for "requests" after "special"
    size_t remaining = len - (pos_special - s.c_str()) - 7;
    if (remaining < 8) {
        return false;
    }

    // Search for "requests" starting right after "special"
    return std::strstr(pos_special + 7, "requests") != nullptr;
}

void run_q13(const std::string& gendb_dir, const std::string& results_dir) {
    const size_t num_customers = 1500000;
    const size_t num_orders = 15000000;

    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load customer data
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<int32_t> customer_custkey = load_column<int32_t>(
        gendb_dir + "/customer/c_custkey.bin", num_customers);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", load_ms);
    #endif

    // Load order data
    #ifdef GENDB_PROFILE
    auto t_load_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<int32_t> order_custkey = load_column<int32_t>(
        gendb_dir + "/orders/o_custkey.bin", num_orders);

    std::vector<std::string> order_comment = load_varchar_column(
        gendb_dir + "/orders/o_comment.bin", num_orders);

    #ifdef GENDB_PROFILE
    auto t_load_orders_end = std::chrono::high_resolution_clock::now();
    double load_orders_ms = std::chrono::duration<double, std::milli>(t_load_orders_end - t_load_orders_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", load_orders_ms);
    #endif

    // First aggregation: GROUP BY c_custkey, COUNT(o_orderkey) where comment NOT LIKE '%special%requests%'
    // Result: map from c_custkey to count
    #ifdef GENDB_PROFILE
    auto t_agg1_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int32_t> custkey_to_count;

    // Pre-size the hash table to estimated capacity (1.5M unique customers)
    // 75% load factor is typical for good performance
    custkey_to_count.reserve(1500000);

    // Build a customer lookup set: customer_custkey[i] -> true
    // This enables efficient O(1) lookup during order processing
    std::unordered_set<int32_t> customer_keys;
    customer_keys.reserve(1500000);
    for (size_t i = 0; i < num_customers; ++i) {
        customer_keys.insert(customer_custkey[i]);
    }

    // Process orders in parallel, collecting local aggregates
    int num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, int32_t>> thread_local_counts(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        thread_local_counts[t].reserve(1500000 / num_threads + 10000);
    }

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        std::unordered_map<int32_t, int32_t>& local_counts = thread_local_counts[thread_id];

        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_orders; ++i) {
            // Check LIKE pattern: NOT LIKE '%special%requests%'
            if (!matches_pattern(order_comment[i])) {
                int32_t cust_key = order_custkey[i];
                local_counts[cust_key]++;
            }
        }
    }

    // Merge all thread-local counts into global map (no lock needed after parallel region)
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& p : thread_local_counts[t]) {
            custkey_to_count[p.first] += p.second;
        }
    }

    // Add all customers with 0 orders (LEFT OUTER JOIN semantics)
    for (int32_t cust_key : customer_keys) {
        if (custkey_to_count.find(cust_key) == custkey_to_count.end()) {
            custkey_to_count[cust_key] = 0;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_agg1_end = std::chrono::high_resolution_clock::now();
    double agg1_ms = std::chrono::duration<double, std::milli>(t_agg1_end - t_agg1_start).count();
    printf("[TIMING] aggregation_1: %.2f ms\n", agg1_ms);
    #endif

    // Second aggregation: GROUP BY c_count, COUNT(*) customers
    #ifdef GENDB_PROFILE
    auto t_agg2_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<int32_t, int32_t> count_to_custdist;
    for (const auto& p : custkey_to_count) {
        int32_t c_count = p.second;
        count_to_custdist[c_count]++;
    }

    #ifdef GENDB_PROFILE
    auto t_agg2_end = std::chrono::high_resolution_clock::now();
    double agg2_ms = std::chrono::duration<double, std::milli>(t_agg2_end - t_agg2_start).count();
    printf("[TIMING] aggregation_2: %.2f ms\n", agg2_ms);
    #endif

    // Sort results: ORDER BY custdist DESC, c_count DESC
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::pair<int32_t, int32_t>> results;
    for (const auto& p : count_to_custdist) {
        results.push_back({p.first, p.second});
    }

    std::sort(results.begin(), results.end(),
        [](const std::pair<int32_t, int32_t>& a, const std::pair<int32_t, int32_t>& b) {
            if (a.second != b.second) {
                return a.second > b.second;  // custdist DESC
            }
            return a.first > b.first;  // c_count DESC
        });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
    #endif

    // Write results to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q13.csv";
    std::ofstream out(output_path);
    if (!out) {
        throw std::runtime_error("Cannot open output file: " + output_path);
    }

    out << "c_count,custdist\n";
    for (const auto& p : results) {
        out << p.first << "," << p.second << "\n";
    }
    out.close();

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

    try {
        run_q13(gendb_dir, results_dir);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#endif
