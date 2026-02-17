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
#include <atomic>
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

// Check if comment contains "special" followed by "requests"
// Pattern: %special%requests% means "special" appears before "requests"
// Optimized for speed: use strstr() which is typically faster than std::string::find()
inline bool contains_special_requests(const char* comment, size_t len) {
    // Fast path: check minimum length
    if (len < 15) return false; // "special" (7) + "requests" (8) = 15 minimum

    // Find "special" using strstr
    const char* pos_special = strstr(comment, "special");
    if (!pos_special) return false;

    // Find "requests" after "special"
    const char* pos_requests = strstr(pos_special + 7, "requests");
    return pos_requests != nullptr;
}

// Compact hash table for aggregation (open addressing, Robin Hood hashing)
// 2-5x faster than std::unordered_map for large tables
template<typename K, typename V>
struct CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t dist;
        bool occupied;
    };
    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashTable(size_t expected) : count(0) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
    }

    inline size_t hash_key(K key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    void insert_or_add(K key, V value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value += value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
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

    // Iterator for range-based for loop
    struct Iterator {
        Entry* ptr;
        Entry* end;
        Iterator(Entry* p, Entry* e) : ptr(p), end(e) {
            while (ptr != end && !ptr->occupied) ++ptr;
        }
        bool operator!=(const Iterator& other) const { return ptr != other.ptr; }
        void operator++() {
            do { ++ptr; } while (ptr != end && !ptr->occupied);
        }
        Entry& operator*() { return *ptr; }
    };

    Iterator begin() { return Iterator(table.data(), table.data() + table.size()); }
    Iterator end() { return Iterator(table.data() + table.size(), table.data() + table.size()); }
};

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

    // 2. Load orders data (mmap custkey, stream comments)
#ifdef GENDB_PROFILE
    t_load_start = std::chrono::high_resolution_clock::now();
#endif
    size_t num_orders = 0;
    int32_t* o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", 15000000, num_orders);

    // Open comment file for streaming (don't load all strings into memory)
    std::string comment_path = gendb_dir + "/orders/o_comment.bin";
    int comment_fd = open(comment_path.c_str(), O_RDONLY);
    if (comment_fd < 0) {
        std::cerr << "Failed to open " << comment_path << std::endl;
        exit(1);
    }
    struct stat comment_stat;
    if (fstat(comment_fd, &comment_stat) < 0) {
        std::cerr << "Failed to stat " << comment_path << std::endl;
        close(comment_fd);
        exit(1);
    }
    size_t comment_file_size = comment_stat.st_size;
    char* comment_data = (char*)mmap(nullptr, comment_file_size, PROT_READ, MAP_PRIVATE, comment_fd, 0);
    if (comment_data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << comment_path << std::endl;
        close(comment_fd);
        exit(1);
    }
    close(comment_fd);

#ifdef GENDB_PROFILE
    t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load_orders = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms_load_orders);
#endif

    // 3. Build hash map: custkey → count of matching orders
    //    Parallel scan with partitioned hash tables (avoid merge bottleneck)
#ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
#endif

    // Pre-compute string offsets for parallel access
    std::vector<size_t> comment_offsets(num_orders + 1);
    size_t offset = 0;
    for (size_t i = 0; i < num_orders; i++) {
        comment_offsets[i] = offset;
        uint32_t len;
        memcpy(&len, comment_data + offset, sizeof(len));
        offset += sizeof(len) + len;
    }
    comment_offsets[num_orders] = offset;

    int num_threads = omp_get_max_threads();

    // Use thread-local hash tables, then merge
    std::vector<CompactHashTable<int32_t, int32_t>> thread_local_counts;
    for (int i = 0; i < num_threads; i++) {
        thread_local_counts.emplace_back(1000000 / num_threads);
    }

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        #pragma omp for schedule(static)
        for (size_t i = 0; i < num_orders; i++) {
            size_t off = comment_offsets[i];
            uint32_t len;
            memcpy(&len, comment_data + off, sizeof(len));
            const char* comment_str = comment_data + off + sizeof(len);

            if (!contains_special_requests(comment_str, len)) {
                int32_t custkey = o_custkey[i];
                thread_local_counts[tid].insert_or_add(custkey, 1);
            }
        }
    }

    // Merge thread-local hash tables
    CompactHashTable<int32_t, int32_t> customer_order_counts(1000000);
    for (auto& local_table : thread_local_counts) {
        for (auto& entry : local_table) {
            customer_order_counts.insert_or_add(entry.key, entry.value);
        }
    }

#ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_hash_map: %.2f ms\n", ms_build);
#endif

    // Cleanup mmap
    munmap(comment_data, comment_file_size);

    // 4. Second aggregation: count customers by order count
    //    For LEFT OUTER JOIN: customers not in hash map have count = 0
    //    Use flat array since c_count is typically small (0-50)
#ifdef GENDB_PROFILE
    auto t_agg2_start = std::chrono::high_resolution_clock::now();
#endif

    // Use atomic array for small cardinality aggregation (c_count typically 0-50)
    constexpr int MAX_COUNT = 256;
    std::vector<std::atomic<int64_t>> count_distribution(MAX_COUNT);
    for (int i = 0; i < MAX_COUNT; i++) {
        count_distribution[i] = 0;
    }

    // Overflow map for rare large counts
    CompactHashTable<int32_t, int64_t> overflow_counts(1000);

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < num_customers; i++) {
        int32_t custkey = c_custkey[i];
        int32_t* count_ptr = customer_order_counts.find(custkey);
        int32_t c_count = count_ptr ? *count_ptr : 0;

        if (c_count < MAX_COUNT) {
            count_distribution[c_count].fetch_add(1, std::memory_order_relaxed);
        } else {
            #pragma omp critical(overflow)
            {
                overflow_counts.insert_or_add(c_count, 1);
            }
        }
    }

    // Collect results
    std::unordered_map<int32_t, int64_t> final_counts;
    for (int i = 0; i < MAX_COUNT; i++) {
        int64_t val = count_distribution[i].load(std::memory_order_relaxed);
        if (val > 0) {
            final_counts[i] = val;
        }
    }
    for (auto& entry : overflow_counts) {
        final_counts[entry.key] = entry.value;
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
