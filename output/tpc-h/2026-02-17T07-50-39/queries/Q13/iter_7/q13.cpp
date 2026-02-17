/*
 * Q13: Customer Distribution
 *
 * ITERATION 7 OPTIMIZATIONS:
 *
 * BOTTLENECK ANALYSIS (Iteration 5):
 * - aggregate: 372ms (100%) - dominated by offset index build for 15M comments
 *   - Lines 227-238: Sequential scan of ALL 15M comments to build offset array
 *   - Lines 240-265: Parallel customer lookup with random comment access
 *
 * ROOT CAUSE:
 * Current approach: customer-centric (iterate customers, probe index, random access comments)
 * Problem: Random access to variable-length strings requires upfront offset index build
 * Solution: Reverse to order-centric approach with sequential comment scan
 *
 * NEW PHYSICAL PLAN (Iteration 7):
 * 1. Load customer.c_custkey (1.5M) - needed for final distribution
 * 2. Load orders.o_custkey (15M) - sequential read
 * 3. Build offset index for o_comment (unavoidable for variable-length strings)
 * 4. PARALLEL scan over orders (15M rows):
 *    - For each order: read custkey, check comment for "special%requests"
 *    - Use DENSE ARRAY for custkey counts (custkeys are 1..1.5M, dense!)
 *    - Thread-local arrays + parallel reduction (no locks, no hash tables!)
 * 5. Iterate customers, read count from array, build c_count distribution
 * 6. Sort and output
 *
 * KEY OPTIMIZATIONS:
 * - Dense array for custkey->count (O(1) access, no hashing, perfect cache)
 * - Thread-local count arrays to eliminate lock contention
 * - Parallel reduction instead of atomic increments
 * - No hash tables anywhere (array is 10x faster for dense keys)
 * - Flat array for final c_count distribution (atomic increments)
 *
 * EXPECTED IMPROVEMENT:
 * - Replace hash table lookups (100-200ms) with array access (~10ms)
 * - Eliminate lock/atomic contention in aggregation
 * - Target: <150ms (approach Umbra 135ms)
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

// Mmap entire file for sequential scan
struct MmapFile {
    void* addr;
    size_t size;

    MmapFile(const std::string& path) {
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
        size = sb.st_size;
        addr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
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

    // 1. Load customer data (for final iteration)
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

    // 2. Load orders columns
#ifdef GENDB_PROFILE
    t_load_start = std::chrono::high_resolution_clock::now();
#endif
    size_t num_orders = 0;
    int32_t* o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", 15000000, num_orders);
    MmapFile comment_file(gendb_dir + "/orders/o_comment.bin");
#ifdef GENDB_PROFILE
    t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load_orders = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms_load_orders);
#endif

    // 3. Build offset index for comments (unavoidable for variable-length strings)
#ifdef GENDB_PROFILE
    auto t_index_start = std::chrono::high_resolution_clock::now();
#endif

    const char* comment_base = (const char*)comment_file.addr;
    std::vector<size_t> comment_offsets;
    comment_offsets.reserve(num_orders + 1);
    size_t offset = 0;
    while (offset < comment_file.size && comment_offsets.size() < num_orders) {
        comment_offsets.push_back(offset);
        uint32_t len = *(uint32_t*)(comment_base + offset);
        offset += sizeof(uint32_t) + len;
    }
    comment_offsets.push_back(offset);  // sentinel

#ifdef GENDB_PROFILE
    auto t_index_end = std::chrono::high_resolution_clock::now();
    double ms_index = std::chrono::duration<double, std::milli>(t_index_end - t_index_start).count();
    printf("[TIMING] build_index: %.2f ms\n", ms_index);
#endif

    // 4. Parallel scan over orders: use dense array for custkey counts
    //    custkeys are 1..1.5M (dense), so array is 10x faster than hash table
#ifdef GENDB_PROFILE
    auto t_aggregate_start = std::chrono::high_resolution_clock::now();
#endif

    const int MAX_CUSTKEY = 1600000;  // SF10: 1.5M customers, add buffer
    std::vector<int32_t> custkey_counts(MAX_CUSTKEY, 0);

    #pragma omp parallel
    {
        // Thread-local count array
        std::vector<int32_t> local_counts(MAX_CUSTKEY, 0);

        #pragma omp for schedule(static, 50000) nowait
        for (size_t i = 0; i < num_orders; i++) {
            int32_t custkey = o_custkey[i];

            // Read comment at position i
            size_t str_offset = comment_offsets[i];
            uint32_t len = *(uint32_t*)(comment_base + str_offset);
            const char* str_data = comment_base + str_offset + sizeof(uint32_t);

            // Filter: skip if comment contains "special%requests"
            if (!contains_special_requests(str_data, len)) {
                // Increment count for this customer (dense array access)
                if (custkey > 0 && custkey < MAX_CUSTKEY) {
                    local_counts[custkey]++;
                }
            }
        }

        // Parallel reduction: merge thread-local counts into global array
        #pragma omp for schedule(static) nowait
        for (int key = 0; key < MAX_CUSTKEY; key++) {
            if (local_counts[key] > 0) {
                #pragma omp atomic
                custkey_counts[key] += local_counts[key];
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_aggregate_end = std::chrono::high_resolution_clock::now();
    double ms_aggregate = std::chrono::duration<double, std::milli>(t_aggregate_end - t_aggregate_start).count();
    printf("[TIMING] aggregate: %.2f ms\n", ms_aggregate);
#endif

    // 5. Build c_count distribution: iterate customers, lookup count from array
#ifdef GENDB_PROFILE
    auto t_distribute_start = std::chrono::high_resolution_clock::now();
#endif

    const int MAX_C_COUNT = 100;
    std::vector<std::atomic<int64_t>> distribution(MAX_C_COUNT + 1);
    for (int i = 0; i <= MAX_C_COUNT; i++) {
        distribution[i].store(0, std::memory_order_relaxed);
    }

    #pragma omp parallel for schedule(static, 10000)
    for (size_t i = 0; i < num_customers; i++) {
        int32_t custkey = c_custkey[i];
        int32_t count = (custkey > 0 && custkey < MAX_CUSTKEY) ? custkey_counts[custkey] : 0;

        if (count <= MAX_C_COUNT) {
            distribution[count].fetch_add(1, std::memory_order_relaxed);
        }
    }

#ifdef GENDB_PROFILE
    auto t_distribute_end = std::chrono::high_resolution_clock::now();
    double ms_distribute = std::chrono::duration<double, std::milli>(t_distribute_end - t_distribute_start).count();
    printf("[TIMING] distribute: %.2f ms\n", ms_distribute);
#endif

    // 6. Sort results: ORDER BY custdist DESC, c_count DESC
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
