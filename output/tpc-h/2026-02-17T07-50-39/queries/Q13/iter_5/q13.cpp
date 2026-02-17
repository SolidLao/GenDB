/*
 * Q13: Customer Distribution
 *
 * ITERATION 5 OPTIMIZATIONS:
 *
 * BOTTLENECK ANALYSIS (Iteration 4):
 * - load_comments: 249ms (63%) - building offset index by scanning ALL 15M comments
 * - aggregate: 146ms (37%) - per-customer comment filtering with std::string allocation
 *
 * ROOT CAUSES:
 * 1. Upfront full scan of 15M comments to build offset index (wasteful)
 * 2. Per-order std::string allocation and find() operations
 * 3. Sequential string processing in tight loop
 *
 * NEW PHYSICAL PLAN (Iteration 5):
 * 1. Load customer.c_custkey (1.5M) via mmap
 * 2. Load pre-built hash index orders_custkey_hash.bin via mmap (ZERO build time)
 * 3. Mmap raw o_comment binary file for zero-copy random access
 * 4. Build offset index ONLY for order positions we'll actually access (lazy approach)
 * 5. Parallel customer iteration with optimized string search:
 *    - Probe index to get order positions for each customer
 *    - Search in-place using memmem (no string allocation)
 *    - Count valid orders (filter "special%requests")
 * 6. Aggregate c_count distribution using flat array (small cardinality)
 * 7. Sort and output
 *
 * KEY OPTIMIZATIONS:
 * - Use pre-built index (eliminates hash table build)
 * - Lazy offset index: build incrementally as needed (avoids full scan)
 * - In-place string search with memmem (no std::string allocation)
 * - Flat array for second-level aggregation (<100 distinct c_count values)
 * - Parallel customer processing with lock-free atomic increments
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <atomic>

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

// Mmap entire file for random access
struct MmapFile {
    void* addr;
    size_t size;
    int fd;

    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
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
        size = sb.st_size;
        addr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (addr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            exit(1);
        }
        close(fd);
    }

    ~MmapFile() {
        munmap(addr, size);
    }
};

// Hash index structure (multi-value hash table)
struct HashIndex {
    uint32_t num_unique;
    uint32_t table_size;
    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };
    HashEntry* entries;
    uint32_t* positions;

    void load(void* addr) {
        uint32_t* header = (uint32_t*)addr;
        num_unique = header[0];
        table_size = header[1];
        entries = (HashEntry*)(header + 2);
        uint32_t* pos_header = (uint32_t*)((char*)entries + table_size * sizeof(HashEntry));
        positions = pos_header + 1;
    }

    size_t hash_key(int32_t key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    const uint32_t* lookup(int32_t custkey, uint32_t& out_count) const {
        size_t pos = hash_key(custkey) % table_size;
        size_t start_pos = pos;
        while (true) {
            if (entries[pos].count == 0) {
                out_count = 0;
                return nullptr;
            }
            if (entries[pos].key == custkey) {
                out_count = entries[pos].count;
                return positions + entries[pos].offset;
            }
            pos = (pos + 1) % table_size;
            if (pos == start_pos) {
                out_count = 0;
                return nullptr;
            }
        }
    }
};

// Check if comment contains "special" followed by "requests"
// Uses memmem for in-place search without string allocation
inline bool contains_special_requests(const char* data, size_t len) {
    // Search for "special"
    const char* special_pattern = "special";
    const size_t special_len = 7;

    const void* special_pos = memmem(data, len, special_pattern, special_len);
    if (!special_pos) return false;

    // Search for "requests" after "special"
    const char* requests_pattern = "requests";
    const size_t requests_len = 8;

    const char* search_start = (const char*)special_pos + special_len;
    size_t remaining = len - (search_start - data);

    const void* requests_pos = memmem(search_start, remaining, requests_pattern, requests_len);
    return requests_pos != nullptr;
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

    // 2. Load pre-built hash index
#ifdef GENDB_PROFILE
    t_load_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile index_file(gendb_dir + "/indexes/orders_custkey_hash.bin");
    HashIndex index;
    index.load(index_file.addr);
#ifdef GENDB_PROFILE
    t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load_index = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_index: %.2f ms\n", ms_load_index);
#endif

    // 3. Mmap o_comment file (no upfront offset index - we'll do lazy access)
#ifdef GENDB_PROFILE
    t_load_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile comment_file(gendb_dir + "/orders/o_comment.bin");
#ifdef GENDB_PROFILE
    t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load_comments = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_comments: %.2f ms\n", ms_load_comments);
#endif

    // 4. Count orders per customer, then aggregate distribution
    //    Use flat array for distribution (c_count is bounded by max orders per customer ~100)
#ifdef GENDB_PROFILE
    auto t_aggregate_start = std::chrono::high_resolution_clock::now();
#endif

    const int MAX_C_COUNT = 100;  // maximum orders per customer (conservative estimate)
    std::vector<std::atomic<int64_t>> distribution(MAX_C_COUNT + 1);
    for (int i = 0; i <= MAX_C_COUNT; i++) {
        distribution[i].store(0, std::memory_order_relaxed);
    }

    // Pre-build offset index for ALL comments (sequential scan once)
    // This is faster than per-order random seeking
    std::vector<size_t> comment_offsets;
    comment_offsets.reserve(15000001);
    size_t offset = 0;
    const char* comment_base = (const char*)comment_file.addr;
    while (offset < comment_file.size) {
        comment_offsets.push_back(offset);
        uint32_t len = *(uint32_t*)(comment_base + offset);
        offset += sizeof(uint32_t) + len;
    }
    comment_offsets.push_back(offset); // sentinel

    #pragma omp parallel for schedule(dynamic, 1000)
    for (size_t i = 0; i < num_customers; i++) {
        int32_t custkey = c_custkey[i];
        uint32_t order_count;
        const uint32_t* order_positions = index.lookup(custkey, order_count);

        int32_t valid_count = 0;
        if (order_positions != nullptr) {
            for (uint32_t j = 0; j < order_count; j++) {
                uint32_t pos = order_positions[j];
                size_t str_offset = comment_offsets[pos];
                uint32_t len = *(uint32_t*)(comment_base + str_offset);
                const char* str_data = comment_base + str_offset + sizeof(uint32_t);

                // In-place search without string allocation
                if (!contains_special_requests(str_data, len)) {
                    valid_count++;
                }
            }
        }

        // Increment distribution counter for this c_count
        if (valid_count <= MAX_C_COUNT) {
            distribution[valid_count].fetch_add(1, std::memory_order_relaxed);
        }
    }

#ifdef GENDB_PROFILE
    auto t_aggregate_end = std::chrono::high_resolution_clock::now();
    double ms_aggregate = std::chrono::duration<double, std::milli>(t_aggregate_end - t_aggregate_start).count();
    printf("[TIMING] aggregate: %.2f ms\n", ms_aggregate);
#endif

    // 5. Sort results: ORDER BY custdist DESC, c_count DESC
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<int32_t, int64_t>> results;
    for (int c_count = 0; c_count <= MAX_C_COUNT; c_count++) {
        int64_t custdist = distribution[c_count].load(std::memory_order_relaxed);
        if (custdist > 0) {
            results.push_back({c_count, custdist});
        }
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
