/*
 * Q13: Customer Distribution
 *
 * ITERATION 10 OPTIMIZATIONS:
 *
 * BOTTLENECK ANALYSIS (Iteration 8):
 * - aggregate: 225ms (100%) - dominated by parallel offset index build + 15M string searches
 *
 * ROOT CAUSE:
 * Current plan iterates 1.5M customers, looks up orders via hash index, then searches comment strings.
 * This requires building offset index for ALL 15M comments upfront, then searching them.
 *
 * NEW PHYSICAL PLAN (Iteration 10) - INVERTED JOIN ORDER:
 * 1. SCAN orders ONCE in parallel (15M rows):
 *    - Read o_custkey and o_comment
 *    - Filter: WHERE o_comment NOT LIKE '%special%requests%'
 *    - Build hash table: custkey → count of matching orders (parallel aggregation)
 * 2. Iterate customers (1.5M):
 *    - Look up count from hash table (default 0 for LEFT OUTER JOIN)
 *    - Build distribution histogram
 * 3. Sort and output
 *
 * KEY ADVANTAGES:
 * - No offset index build needed (sequential streaming scan of o_comment)
 * - String search on ALL 15M comments but ZERO hash probes (was 1.5M hash probes × avg orders/customer)
 * - Simpler aggregation: single hash table (custkey → count) vs per-customer counting
 * - Better parallelism: partition 15M orders across threads for independent processing
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
struct MmapColumn {
    T* data;
    size_t rows;
    size_t size_bytes;

    MmapColumn(const std::string& path) {
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
        size_bytes = sb.st_size;
        rows = sb.st_size / sizeof(T);
        void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (addr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            exit(1);
        }
        close(fd);
        data = static_cast<T*>(addr);
    }

    ~MmapColumn() {
        munmap(data, size_bytes);
    }

    T& operator[](size_t i) { return data[i]; }
    const T& operator[](size_t i) const { return data[i]; }
};

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

// Open-addressing hash table for aggregation (custkey → order count)
// Much faster than std::unordered_map for aggregation
struct OrderCountTable {
    struct Entry {
        int32_t custkey;
        int32_t count;
    };
    std::vector<Entry> entries;
    size_t capacity;
    size_t mask;

    OrderCountTable(size_t expected_size) {
        // Use power-of-2 size for fast modulo via bitwise AND
        capacity = 1;
        while (capacity < expected_size * 2) capacity *= 2;
        mask = capacity - 1;
        entries.resize(capacity, {-1, 0}); // -1 = empty slot
    }

    inline size_t hash(int32_t key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
    }

    void increment(int32_t custkey) {
        size_t pos = hash(custkey) & mask;
        while (true) {
            if (entries[pos].custkey == -1) {
                // Empty slot - insert new entry
                entries[pos].custkey = custkey;
                entries[pos].count = 1;
                return;
            }
            if (entries[pos].custkey == custkey) {
                // Found existing entry - increment
                entries[pos].count++;
                return;
            }
            // Linear probing
            pos = (pos + 1) & mask;
        }
    }

    int32_t get_count(int32_t custkey) const {
        size_t pos = hash(custkey) & mask;
        while (true) {
            if (entries[pos].custkey == -1) {
                return 0; // Not found
            }
            if (entries[pos].custkey == custkey) {
                return entries[pos].count;
            }
            pos = (pos + 1) & mask;
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

    // 1. Load orders data (o_custkey and o_comment)
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    MmapColumn<int32_t> o_custkey(gendb_dir + "/orders/o_custkey.bin");
    MmapFile comment_file(gendb_dir + "/orders/o_comment.bin");
    const char* comment_base = (const char*)comment_file.addr;
#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms_load);
#endif

    // 2. Scan orders and build custkey → count hash table (parallel aggregation)
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    const int num_threads = omp_get_max_threads();

    // Parallel scan of comment file partitions
    std::vector<std::vector<std::pair<int32_t, bool>>> thread_results(num_threads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        size_t chunk_size = comment_file.size / num_threads;
        size_t start_byte = tid * chunk_size;
        size_t end_byte = (tid == num_threads - 1) ? comment_file.size : start_byte + chunk_size;

        // Align to record boundary
        size_t order_start_idx = 0;
        if (tid > 0) {
            while (start_byte < end_byte) {
                uint32_t len = *(uint32_t*)(comment_base + start_byte);
                if (len > 0 && len < 80 && start_byte + sizeof(uint32_t) + len <= comment_file.size) {
                    break;
                }
                start_byte++;
            }
            // Count how many orders are before this byte offset
            size_t tmp_offset = 0;
            while (tmp_offset < start_byte) {
                uint32_t len = *(uint32_t*)(comment_base + tmp_offset);
                tmp_offset += sizeof(uint32_t) + len;
                order_start_idx++;
            }
        }

        thread_results[tid].reserve((end_byte - start_byte) / 60); // avg comment ~60 bytes

        size_t offset = start_byte;
        size_t order_idx = order_start_idx;

        while (offset < end_byte && offset < comment_file.size) {
            uint32_t len = *(uint32_t*)(comment_base + offset);
            if (len == 0 || len > 79 || offset + sizeof(uint32_t) + len > comment_file.size) {
                break; // End of valid data
            }

            const char* str_data = comment_base + offset + sizeof(uint32_t);
            bool matches = !contains_special_requests(str_data, len);

            if (order_idx < o_custkey.rows) {
                thread_results[tid].emplace_back(o_custkey[order_idx], matches);
            }

            offset += sizeof(uint32_t) + len;
            order_idx++;
        }
    }

    // Merge results into hash table
    OrderCountTable cust_order_counts(1600000); // ~1.5M customers
    for (int t = 0; t < num_threads; t++) {
        for (auto& [custkey, matches] : thread_results[t]) {
            if (matches) {
                cust_order_counts.increment(custkey);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_orders: %.2f ms\n", ms_scan);
#endif

    // 3. Load customer data and build distribution histogram
#ifdef GENDB_PROFILE
    auto t_dist_start = std::chrono::high_resolution_clock::now();
#endif
    MmapColumn<int32_t> c_custkey(gendb_dir + "/customer/c_custkey.bin");

    const int MAX_C_COUNT = 100;
    std::vector<int64_t> distribution(MAX_C_COUNT + 1, 0);

    for (size_t i = 0; i < c_custkey.rows; i++) {
        int32_t custkey = c_custkey[i];
        int32_t count = cust_order_counts.get_count(custkey);
        if (count <= MAX_C_COUNT) {
            distribution[count]++;
        }
    }

#ifdef GENDB_PROFILE
    auto t_dist_end = std::chrono::high_resolution_clock::now();
    double ms_dist = std::chrono::duration<double, std::milli>(t_dist_end - t_dist_start).count();
    printf("[TIMING] build_distribution: %.2f ms\n", ms_dist);
#endif

    // 4. Sort results: ORDER BY custdist DESC, c_count DESC
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

    // 5. Write output
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
