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
 * PHYSICAL PLAN (OPTIMIZED):
 * 1. Load pre-built hash index: orders_custkey_hash.bin (0ms build time)
 * 2. Load customer.c_custkey (1.5M int32_t values)
 * 3. For each unique custkey in index:
 *    - Load o_comment only for positions belonging to this custkey
 *    - Count orders where o_comment NOT LIKE '%special%requests%'
 * 4. For each customer: look up count (default 0 for LEFT JOIN)
 * 5. Second-level aggregation using open-addressing hash table
 * 6. Sort results by custdist DESC, c_count DESC
 * 7. Parallelism: Parallel customer processing
 *
 * KEY OPTIMIZATIONS:
 * - Load pre-built index (eliminates 4358ms build_hash_map)
 * - Late materialization: load o_comment only for positions we need
 * - Open-addressing hash tables (2-5x faster than std::unordered_map)
 * - Fused filter + aggregation
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
#include <cstdint>

// Compact open-addressing hash table (2-5x faster than std::unordered_map)
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; uint8_t dist; bool occupied; };
    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
    }

    size_t hash_key(K key) const {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL >> 32;
    }

    void insert(K key, V value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) { table[pos].value += value; return; }
            if (entry.dist > table[pos].dist) std::swap(entry, table[pos]);
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
    }

    V* find(K key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    // Iterator support for final extraction
    template<typename Func>
    void for_each(Func&& func) {
        for (auto& entry : table) {
            if (entry.occupied) func(entry.key, entry.value);
        }
    }
};

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

// Load a single string at a specific position (late materialization)
std::string load_string_at_position(const char* file_base, size_t position) {
    const char* ptr = file_base;
    // Skip to the target position
    for (size_t i = 0; i < position; i++) {
        uint32_t len;
        memcpy(&len, ptr, sizeof(len));
        ptr += sizeof(uint32_t) + len;
    }
    // Read the target string
    uint32_t len;
    memcpy(&len, ptr, sizeof(len));
    ptr += sizeof(uint32_t);
    return std::string(ptr, len);
}

// Memory-map string column file for random access
const char* mmap_string_file(const std::string& path, size_t& file_size) {
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
    file_size = sb.st_size;
    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        close(fd);
        exit(1);
    }
    close(fd);
    return static_cast<const char*>(addr);
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

    // 1. Load pre-built hash index for orders.o_custkey
#ifdef GENDB_PROFILE
    auto t_load_idx_start = std::chrono::high_resolution_clock::now();
#endif

    std::string index_path = gendb_dir + "/indexes/orders_custkey_hash.bin";
    int fd = open(index_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open index " << index_path << std::endl;
        exit(1);
    }

    struct stat sb;
    fstat(fd, &sb);
    const char* idx_data = static_cast<const char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    // Parse index header
    uint32_t num_unique, table_size;
    memcpy(&num_unique, idx_data, sizeof(uint32_t));
    memcpy(&table_size, idx_data + 4, sizeof(uint32_t));

    const char* hash_table_base = idx_data + 8;
    const char* positions_base = hash_table_base + table_size * 12; // 12 bytes per slot

#ifdef GENDB_PROFILE
    auto t_load_idx_end = std::chrono::high_resolution_clock::now();
    double ms_load_idx = std::chrono::duration<double, std::milli>(t_load_idx_end - t_load_idx_start).count();
    printf("[TIMING] load_index: %.2f ms\n", ms_load_idx);
#endif

    // 2. Load customer data
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

    // 3. Memory-map o_comment for late materialization
#ifdef GENDB_PROFILE
    auto t_load_comment_start = std::chrono::high_resolution_clock::now();
#endif
    size_t comment_file_size = 0;
    const char* o_comment_base = mmap_string_file(gendb_dir + "/orders/o_comment.bin", comment_file_size);
#ifdef GENDB_PROFILE
    auto t_load_comment_end = std::chrono::high_resolution_clock::now();
    double ms_load_comment = std::chrono::duration<double, std::milli>(t_load_comment_end - t_load_comment_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms_load_comment);
#endif

    // 4. Build custkey → count map using the pre-built index
    //    For each unique custkey, count orders without "special%requests" pattern
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    CompactHashTable<int32_t, int32_t> customer_order_counts(num_unique);

    // Process each unique custkey from the index
    #pragma omp parallel
    {
        // Thread-local hash table
        CompactHashTable<int32_t, int32_t> local_counts(num_unique / omp_get_num_threads() + 1000);

        #pragma omp for schedule(dynamic, 100)
        for (uint32_t slot = 0; slot < table_size; slot++) {
            const char* slot_ptr = hash_table_base + slot * 12;
            int32_t key;
            uint32_t offset, count;
            memcpy(&key, slot_ptr, sizeof(int32_t));
            memcpy(&offset, slot_ptr + 4, sizeof(uint32_t));
            memcpy(&count, slot_ptr + 8, sizeof(uint32_t));

            if (count == 0) continue; // empty slot

            // Read positions for this custkey
            const char* pos_ptr = positions_base + offset * sizeof(uint32_t);
            uint32_t positions_count;
            memcpy(&positions_count, pos_ptr, sizeof(uint32_t));
            pos_ptr += sizeof(uint32_t);

            // Count orders without "special%requests" pattern
            int32_t valid_count = 0;
            for (uint32_t i = 0; i < positions_count; i++) {
                uint32_t pos;
                memcpy(&pos, pos_ptr, sizeof(uint32_t));
                pos_ptr += sizeof(uint32_t);

                // Late materialization: load o_comment only for this position
                std::string comment = load_string_at_position(o_comment_base, pos);
                if (!contains_special_requests(comment)) {
                    valid_count++;
                }
            }

            if (valid_count > 0) {
                local_counts.insert(key, valid_count);
            }
        }

        // Merge thread-local results into global table
        #pragma omp critical
        {
            local_counts.for_each([&](int32_t key, int32_t count) {
                customer_order_counts.insert(key, count);
            });
        }
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hash_map: %.2f ms\n", ms_build);
#endif

    // 5. Second aggregation: count customers by order count
    //    For LEFT OUTER JOIN: customers not in hash map have count = 0
#ifdef GENDB_PROFILE
    auto t_agg2_start = std::chrono::high_resolution_clock::now();
#endif

    CompactHashTable<int32_t, int64_t> final_counts(100); // small number of distinct c_count values

    #pragma omp parallel
    {
        CompactHashTable<int32_t, int64_t> local_dist(100);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_customers; i++) {
            int32_t custkey = c_custkey[i];
            int32_t* count_ptr = customer_order_counts.find(custkey);
            int32_t c_count = count_ptr ? *count_ptr : 0;
            local_dist.insert(c_count, 1);
        }

        #pragma omp critical
        {
            local_dist.for_each([&](int32_t c_count, int64_t custdist) {
                final_counts.insert(c_count, custdist);
            });
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg2_end = std::chrono::high_resolution_clock::now();
    double ms_agg2 = std::chrono::duration<double, std::milli>(t_agg2_end - t_agg2_start).count();
    printf("[TIMING] aggregation_level2: %.2f ms\n", ms_agg2);
#endif

    // 6. Sort results: ORDER BY custdist DESC, c_count DESC
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<int32_t, int64_t>> results;
    results.reserve(100); // small number of distinct c_count values

    final_counts.for_each([&](int32_t c_count, int64_t custdist) {
        results.push_back({c_count, custdist});
    });

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

    // 7. Write output
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
    munmap((void*)o_comment_base, comment_file_size);
    munmap((void*)idx_data, sb.st_size);
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
