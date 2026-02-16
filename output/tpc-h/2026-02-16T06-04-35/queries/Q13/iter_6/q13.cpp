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

// Compact open-addressing hash table for aggregations
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    void iterate(std::function<void(K, V&)> fn) {
        for (auto& entry : table) {
            if (entry.occupied) {
                fn(entry.key, entry.value);
            }
        }
    }
};

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
// Optimized using memmem for faster substring search
inline bool matches_pattern(const std::string& s) {
    // Minimum length: "special" (7) + at least one char between + "requests" (8) = 16
    size_t len = s.length();
    if (len < 16) {
        return false;
    }

    const char* str_data = s.data();

    // Find "special" substring using memmem
    const void* special_pos = memmem(str_data, len, "special", 7);
    if (!special_pos) {
        return false;
    }

    // Calculate remaining length after "special"
    const char* special_ptr = static_cast<const char*>(special_pos);
    size_t after_special = len - (special_ptr - str_data + 7);

    // Need at least 8 bytes for "requests"
    if (after_special < 8) {
        return false;
    }

    // Search for "requests" after "special"
    const void* requests_pos = memmem(special_ptr + 7, after_special, "requests", 8);
    return requests_pos != nullptr;
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

    CompactHashTable<int32_t, int32_t> custkey_to_count(1500000);

    // Create a set of all customer keys for O(1) lookup
    CompactHashTable<int32_t, int32_t> customer_set(1500000);
    for (size_t i = 0; i < num_customers; ++i) {
        customer_set.insert(customer_custkey[i], 0);
    }

    // Process orders in parallel, collecting local aggregates
    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTable<int32_t, int32_t>*> thread_local_counts(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        thread_local_counts[t] = new CompactHashTable<int32_t, int32_t>(1500000 / num_threads + 10000);
    }

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        CompactHashTable<int32_t, int32_t>& local_counts = *thread_local_counts[thread_id];

        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_orders; ++i) {
            // Check LIKE pattern: NOT LIKE '%special%requests%'
            if (!matches_pattern(order_comment[i])) {
                int32_t cust_key = order_custkey[i];
                int32_t* existing = local_counts.find(cust_key);
                if (existing) {
                    (*existing)++;
                } else {
                    local_counts.insert(cust_key, 1);
                }
            }
        }
    }

    // Merge all thread-local counts into global map (no lock needed after parallel region)
    for (int t = 0; t < num_threads; ++t) {
        thread_local_counts[t]->iterate([&](int32_t key, int32_t& val) {
            int32_t* existing = custkey_to_count.find(key);
            if (existing) {
                (*existing) += val;
            } else {
                custkey_to_count.insert(key, val);
            }
        });
        delete thread_local_counts[t];
    }

    // Add all customers with 0 orders (LEFT OUTER JOIN semantics)
    customer_set.iterate([&](int32_t key, int32_t&) {
        if (!custkey_to_count.find(key)) {
            custkey_to_count.insert(key, 0);
        }
    });

    #ifdef GENDB_PROFILE
    auto t_agg1_end = std::chrono::high_resolution_clock::now();
    double agg1_ms = std::chrono::duration<double, std::milli>(t_agg1_end - t_agg1_start).count();
    printf("[TIMING] aggregation_1: %.2f ms\n", agg1_ms);
    #endif

    // Second aggregation: GROUP BY c_count, COUNT(*) customers
    #ifdef GENDB_PROFILE
    auto t_agg2_start = std::chrono::high_resolution_clock::now();
    #endif

    CompactHashTable<int32_t, int32_t> count_to_custdist(256);  // Low cardinality: order counts
    custkey_to_count.iterate([&](int32_t key, int32_t& count_val) {
        int32_t c_count = count_val;
        int32_t* existing = count_to_custdist.find(c_count);
        if (existing) {
            (*existing)++;
        } else {
            count_to_custdist.insert(c_count, 1);
        }
    });

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
    count_to_custdist.iterate([&](int32_t key, int32_t& val) {
        results.push_back({key, val});
    });

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
