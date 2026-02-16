#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
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
// Optimized: inline and with early exit on short strings
inline bool matches_pattern(const std::string& s) {
    // Early exit: minimum length is 15 (special=7, requests=8, at least 1 char each between/after)
    if (s.length() < 15) {
        return false;
    }
    size_t pos_special = s.find("special");
    if (pos_special == std::string::npos) {
        return false;
    }
    // Search for "requests" starting right after "special"
    return s.find("requests", pos_special + 7) != std::string::npos;
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

    // Create a set of all customer keys for O(1) lookup
    std::unordered_map<int32_t, int32_t> customer_set;
    for (size_t i = 0; i < num_customers; ++i) {
        customer_set[customer_custkey[i]] = 0;
    }

    // Process orders in parallel, collecting local aggregates
    #pragma omp parallel
    {
        std::unordered_map<int32_t, int32_t> local_counts;

        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_orders; ++i) {
            // Check LIKE pattern: NOT LIKE '%special%requests%'
            if (!matches_pattern(order_comment[i])) {
                int32_t cust_key = order_custkey[i];
                local_counts[cust_key]++;
            }
        }

        // Merge local counts into global map
        #pragma omp critical
        {
            for (const auto& p : local_counts) {
                custkey_to_count[p.first] += p.second;
            }
        }
    }

    // Add all customers with 0 orders (LEFT OUTER JOIN semantics)
    for (const auto& p : customer_set) {
        if (custkey_to_count.find(p.first) == custkey_to_count.end()) {
            custkey_to_count[p.first] = 0;
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
