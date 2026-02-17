/*
 * Q13: Customer Distribution
 *
 * ITERATION 8 OPTIMIZATIONS:
 *
 * BOTTLENECK ANALYSIS (Iteration 5):
 * - aggregate: 372ms (100%) - dominated by sequential offset index build for 15M comments
 *
 * ROOT CAUSES:
 * 1. Sequential scan of ALL 15M comments to build offset index (lines 227-238)
 * 2. memmem() string search overhead
 * 3. Atomic array has false sharing on hot counters
 *
 * NEW PHYSICAL PLAN (Iteration 8):
 * 1. Load customer.c_custkey (1.5M) via mmap
 * 2. Load pre-built hash index orders_custkey_hash.bin via mmap
 * 3. Mmap o_comment binary for zero-copy access
 * 4. PARALLEL offset index build: partition 15M comments across threads
 *    - Each thread scans its partition to build local offset array
 *    - Merge offset arrays (simple concatenation)
 * 5. Parallel customer iteration with optimized string search:
 *    - Replace memmem with specialized pattern matcher (faster)
 *    - Search for "special" then "requests" in remainder
 * 6. Thread-local distribution arrays (no atomics, no false sharing)
 *    - Each thread maintains private histogram
 *    - Merge histograms after parallel phase
 * 7. Sort and output
 *
 * KEY OPTIMIZATIONS:
 * - Parallel offset index build (was sequential bottleneck)
 * - Specialized string pattern matcher (faster than memmem)
 * - Thread-local aggregation (eliminates atomic contention)
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

// Optimized pattern matcher for "special" followed by "requests"
// Faster than memmem - uses single pass with state machine
inline bool contains_special_requests(const char* data, size_t len) {
    if (len < 15) return false; // "special" + "requests" = 15 chars minimum

    // Boyer-Moore-style: search for 's' of "special" first
    for (size_t i = 0; i < len - 14; i++) {
        if (data[i] == 's' || data[i] == 'S') {
            // Check for "special" (case-insensitive not needed per TPC-H spec, but safe)
            if (data[i+1] == 'p' && data[i+2] == 'e' && data[i+3] == 'c' &&
                data[i+4] == 'i' && data[i+5] == 'a' && data[i+6] == 'l') {
                // Found "special", now search for "requests" in remainder
                size_t start = i + 7;
                for (size_t j = start; j < len - 7; j++) {
                    if (data[j] == 'r' || data[j] == 'R') {
                        if (data[j+1] == 'e' && data[j+2] == 'q' && data[j+3] == 'u' &&
                            data[j+4] == 'e' && data[j+5] == 's' && data[j+6] == 't' &&
                            data[j+7] == 's') {
                            return true;
                        }
                    }
                }
                // No need to continue searching for more "special" - pattern is "%special%requests%"
                return false;
            }
        }
    }
    return false;
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

    // 4. Build offset index in parallel (partition comments across threads)
#ifdef GENDB_PROFILE
    auto t_aggregate_start = std::chrono::high_resolution_clock::now();
#endif

    const char* comment_base = (const char*)comment_file.addr;
    const int num_threads = omp_get_max_threads();

    // Estimate number of orders (15M) for partitioning
    const size_t est_orders = 15000000;
    std::vector<std::vector<size_t>> thread_offsets(num_threads);

    // Parallel offset index build: each thread scans its byte range
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        size_t chunk_size = comment_file.size / num_threads;
        size_t start_byte = tid * chunk_size;
        size_t end_byte = (tid == num_threads - 1) ? comment_file.size : start_byte + chunk_size;

        // Align to record boundary: scan forward to find next record start
        if (tid > 0) {
            while (start_byte < end_byte) {
                uint32_t len = *(uint32_t*)(comment_base + start_byte);
                if (len > 0 && len < 80 && start_byte + sizeof(uint32_t) + len <= comment_file.size) {
                    break; // found valid record
                }
                start_byte++;
            }
        }

        thread_offsets[tid].reserve(est_orders / num_threads + 1000);
        size_t offset = start_byte;
        while (offset < end_byte) {
            thread_offsets[tid].push_back(offset);
            uint32_t len = *(uint32_t*)(comment_base + offset);
            offset += sizeof(uint32_t) + len;
        }
    }

    // Merge offset arrays
    std::vector<size_t> comment_offsets;
    comment_offsets.reserve(est_orders + 1);
    for (int t = 0; t < num_threads; t++) {
        comment_offsets.insert(comment_offsets.end(),
                               thread_offsets[t].begin(),
                               thread_offsets[t].end());
    }

    // 5. Count orders per customer with thread-local aggregation (no atomic contention)
    const int MAX_C_COUNT = 100;
    std::vector<std::vector<int64_t>> thread_distributions(num_threads, std::vector<int64_t>(MAX_C_COUNT + 1, 0));

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_dist = thread_distributions[tid];

        #pragma omp for schedule(dynamic, 1000) nowait
        for (size_t i = 0; i < num_customers; i++) {
            int32_t custkey = c_custkey[i];
            uint32_t order_count;
            const uint32_t* order_positions = index.lookup(custkey, order_count);

            int32_t valid_count = 0;
            if (order_positions != nullptr) {
                for (uint32_t j = 0; j < order_count; j++) {
                    uint32_t pos = order_positions[j];
                    if (pos < comment_offsets.size()) {
                        size_t str_offset = comment_offsets[pos];
                        uint32_t len = *(uint32_t*)(comment_base + str_offset);
                        const char* str_data = comment_base + str_offset + sizeof(uint32_t);

                        // In-place search without string allocation
                        if (!contains_special_requests(str_data, len)) {
                            valid_count++;
                        }
                    }
                }
            }

            // Increment thread-local counter
            if (valid_count <= MAX_C_COUNT) {
                local_dist[valid_count]++;
            }
        }
    }

    // Merge thread-local distributions
    std::vector<int64_t> distribution(MAX_C_COUNT + 1, 0);
    for (int t = 0; t < num_threads; t++) {
        for (int c = 0; c <= MAX_C_COUNT; c++) {
            distribution[c] += thread_distributions[t][c];
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
        int64_t custdist = distribution[c_count];
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
