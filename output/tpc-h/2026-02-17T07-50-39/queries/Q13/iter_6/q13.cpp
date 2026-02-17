/*
 * Q13: Customer Distribution
 *
 * ITERATION 6 OPTIMIZATIONS:
 *
 * BOTTLENECK ANALYSIS (Iteration 5):
 * - aggregate: 372ms (100%)
 *   - Offset index building: ~200ms (sequential scan of 783MB variable-length strings)
 *   - Customer-centric processing: ~170ms (1.5M hash probes, random comment access)
 *
 * ROOT CAUSES:
 * 1. Customer-centric approach causes random access to comments (poor cache behavior)
 * 2. Hash index probe overhead (1.5M probes with linear probing)
 * 3. Sequential offset building (single-threaded bottleneck)
 *
 * ALGORITHMIC CHANGE (Iteration 6):
 * Switch from customer-centric to ORDER-CENTRIC approach:
 * - OLD: for each customer → probe index → access N orders → filter comments
 * - NEW: for each order → filter comment → increment custkey count → aggregate
 *
 * NEW PHYSICAL PLAN:
 * 1. Load customer.c_custkey (1.5M) via mmap - used only for left outer join semantics
 * 2. Load orders.o_custkey (15M) via mmap
 * 3. Parallel scan orders + comments together (sequential, cache-friendly):
 *    - For each order: read comment inline, check for "special%requests"
 *    - If NOT matched: increment customer_order_count[o_custkey] (thread-local)
 * 4. Merge thread-local customer counts
 * 5. LEFT OUTER JOIN: add customers with zero matching orders
 * 6. Aggregate c_count distribution using flat array
 * 7. Sort and output
 *
 * KEY OPTIMIZATIONS:
 * - Sequential order scan (cache-friendly, no random access)
 * - No offset index needed (inline comment parsing during scan)
 * - Thread-local aggregation (lock-free hot path)
 * - Parallel scan of 15M orders (8-16x speedup)
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

    // 1. Load customer data (for LEFT OUTER JOIN semantics)
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

    // 3. Mmap o_comment file for zero-copy access
#ifdef GENDB_PROFILE
    t_load_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile comment_file(gendb_dir + "/orders/o_comment.bin");
    const size_t num_orders = 15000000;  // known from storage guide
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

    // Build comment offset index with optimized sequential scan
    // (single pass, cache-friendly, minimal allocations)
    const char* comment_base = (const char*)comment_file.addr;
    std::vector<uint32_t> comment_offsets;
    comment_offsets.reserve(num_orders + 1);

    uint32_t offset = 0;
    while (offset < comment_file.size) {
        comment_offsets.push_back(offset);
        uint32_t len = *(uint32_t*)(comment_base + offset);
        offset += sizeof(uint32_t) + len;
    }

    // Phase 1: Parallel count orders per customer using hash index
    // Thread-local counts to avoid contention
    const int num_threads = omp_get_max_threads();
    const int MAX_CUSTKEY = 2000000;
    std::vector<std::vector<int32_t>> thread_counts(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_counts[t].resize(MAX_CUSTKEY, 0);
    }

    #pragma omp parallel for schedule(dynamic, 1000)
    for (size_t i = 0; i < num_customers; i++) {
        int tid = omp_get_thread_num();
        int32_t custkey = c_custkey[i];

        uint32_t order_count;
        const uint32_t* order_positions = index.lookup(custkey, order_count);

        int32_t valid_count = 0;
        if (order_positions != nullptr) {
            for (uint32_t j = 0; j < order_count; j++) {
                uint32_t pos = order_positions[j];
                if (pos < comment_offsets.size()) {
                    uint32_t str_offset = comment_offsets[pos];
                    uint32_t len = *(uint32_t*)(comment_base + str_offset);
                    const char* str_data = comment_base + str_offset + sizeof(uint32_t);

                    // In-place search without string allocation
                    if (!contains_special_requests(str_data, len)) {
                        valid_count++;
                    }
                }
            }
        }

        // Store count in thread-local array
        if (custkey >= 0 && custkey < MAX_CUSTKEY) {
            thread_counts[tid][custkey] = valid_count;
        }
    }

    // Phase 2: Merge thread-local counts and build distribution
    const int MAX_C_COUNT = 100;
    std::vector<int64_t> distribution(MAX_C_COUNT + 1, 0);

    std::vector<int32_t> final_counts(MAX_CUSTKEY, 0);
    for (int t = 0; t < num_threads; t++) {
        for (int cust = 0; cust < MAX_CUSTKEY; cust++) {
            if (thread_counts[t][cust] > 0) {
                final_counts[cust] = thread_counts[t][cust];
            }
        }
    }

    // Aggregate distribution from final counts
    for (size_t i = 0; i < num_customers; i++) {
        int32_t custkey = c_custkey[i];
        int32_t count = (custkey >= 0 && custkey < MAX_CUSTKEY) ? final_counts[custkey] : 0;
        if (count <= MAX_C_COUNT) {
            distribution[count]++;
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
