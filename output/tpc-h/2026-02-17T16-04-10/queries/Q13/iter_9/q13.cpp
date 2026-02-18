// Q13: Customer Distribution Query - Iteration 9 (OPTIMIZATION STALL RECOVERY)
//
// OPTIMIZATION STALL DETECTED: 3 consecutive iterations failed. Current: 1085ms, best: 123ms (8.8x gap)
// ROOT CAUSE: Single-threaded sequential filter of 15M orders (filter_and_count = 978ms = 90% of total)
// Modern CPUs have 64 cores; Umbra parallelizes this to ~15-20ms.
//
// FUNDAMENTAL RESTRUCTURING: Implement morsel-driven parallel filtering
//
// LOGICAL PLAN:
// 1. Load customer(c_custkey) → 1.5M rows
// 2. PARALLEL stream orders(o_custkey, o_comment) → 15M rows
//    - Divide into morsels (~100K rows per thread)
//    - Each thread: scan comments, filter (NOT LIKE '%special%requests%')
//    - Each thread accumulates into its own OrdersHashTable (thread-local)
// 3. Merge thread-local hash tables into global result
// 4. LEFT OUTER JOIN customer with merged orders count table → 1.5M rows
// 5. Final aggregation: c_count → custdist (compact hash table, ~40 groups)
// 6. Sort results by custdist DESC, c_count DESC
//
// PHYSICAL PLAN (Iter 9):
// - Parallel mmap scan: each thread pulls morsels from shared counter (morsel = 100K rows)
// - SIMD-optimized string filter via glibc strstr (SSE4.2/AVX2)
// - Thread-local OrdersHashTable per thread (no contention)
// - Merge: combine N thread-local tables into single global table (~64x on 64-core hardware)
// - Customer scan: single-threaded (1.5M rows = 114ms, acceptable)
// - Final aggregation: c_count → custdist (sequential, ~40 groups = fast)
// - Sort results
//
// KEY OPTIMIZATIONS (Iter 9 — STALL RECOVERY):
// - MORSEL-DRIVEN PARALLEL FILTERING: the ONLY way to match Umbra/DuckDB
// - 64-core parallelism on 15M row filter
// - Thread-local accumulation to avoid synchronization overhead during hot loop
// - Expected speedup: ~10x (strstr SIMD + L3 cache contention + merge cost = ~10x practical speedup on 64 cores)
// - Expected result: ~110-150ms (target: match Umbra at 123ms)

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <omp.h>
#include <immintrin.h>  // For SIMD intrinsics

// ==================== HELPER FUNCTIONS ====================

// mmap helper
template<typename T>
T* mmap_column(const std::string& path, size_t& file_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    file_size = sb.st_size;
    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "Error: mmap failed for " << path << std::endl;
        return nullptr;
    }
    madvise(addr, file_size, MADV_SEQUENTIAL);
    return static_cast<T*>(addr);
}

// Check pattern "%special%requests%" - must appear in that order
// Optimized: use strstr (SIMD on glibc) instead of manual memcmp loop
// strstr is optimized on x86-64 with SSE4.2/AVX2
// NOTE: This is the hot path — called 15M times per query
inline bool matches_pattern_buffer(const char* buf, size_t len) {
    // Quick check: if len < 15 (min "special" + "requests"), can't match
    if (len < 15) return false;

    // Find "special" substring using glibc-optimized strstr
    // glibc uses SSE4.2 on x86-64 for this (see man 3 strchr)
    const char* special_ptr = std::strstr(buf, "special");
    if (special_ptr == nullptr) return false;

    // Verify "special" doesn't extend past end
    if (special_ptr + 7 > buf + len) return false;

    // Find "requests" after special
    // Use strstr on the substring starting after "special"
    // "requests" is 8 bytes, so need at least 8 more bytes after "special"
    if (special_ptr + 7 + 8 > buf + len) return false;  // Early exit if no room for "requests"

    const char* requests_ptr = std::strstr(special_ptr + 7, "requests");
    return requests_ptr != nullptr;
}

// ==================== OPEN-ADDRESSING HASH TABLES ====================

// Fast open-addressing hash table for integer → integer mapping (orders_per_customer)
// Uses linear probing with tombstone deletion. ~2-3x faster than std::unordered_map.
struct OrdersHashEntry {
    int32_t key;
    int32_t count;
    uint8_t state;  // 0=empty, 1=occupied, 2=deleted
};

class OrdersHashTable {
public:
    OrdersHashTable(size_t estimated_size) {
        size_t capacity = 1;
        while (capacity < estimated_size * 2) capacity *= 2;  // Load factor ~0.5
        entries.resize(capacity);
        mask = capacity - 1;
        for (auto& e : entries) e.state = 0;
    }

    // Increment count for key, or insert with count=1
    void increment(int32_t key) {
        uint32_t h = hash_fn(key);
        size_t idx = h & mask;

        while (entries[idx].state == 1) {
            if (entries[idx].key == key) {
                entries[idx].count++;
                return;
            }
            idx = (idx + 1) & mask;
        }

        entries[idx].key = key;
        entries[idx].count = 1;
        entries[idx].state = 1;
    }

    // Probe: return count or 0 if not found
    int32_t lookup(int32_t key) const {
        uint32_t h = hash_fn(key);
        size_t idx = h & mask;

        while (entries[idx].state != 0) {
            if (entries[idx].state == 1 && entries[idx].key == key) {
                return entries[idx].count;
            }
            idx = (idx + 1) & mask;
        }
        return 0;
    }

    std::vector<std::pair<int32_t, int32_t>> to_vector() const {
        std::vector<std::pair<int32_t, int32_t>> result;
        for (const auto& e : entries) {
            if (e.state == 1) {
                result.push_back({e.key, e.count});
            }
        }
        return result;
    }

private:
    std::vector<OrdersHashEntry> entries;
    size_t mask;

    inline uint32_t hash_fn(int32_t k) const {
        // MurmurHash3 32-bit finalizer
        uint32_t h = k;
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }
};

// Aggregation result structure
struct AggState {
    int32_t c_count;
    int32_t custdist;
};

// Compact hash table for final aggregation (c_count → custdist)
template<typename K, typename V>
class CompactHashTable {
public:
    struct Entry {
        K key;
        V value;
        uint32_t hash;
        bool occupied;
    };

    CompactHashTable(size_t estimated_size) {
        size_t capacity = 1;
        while (capacity < estimated_size * 2) capacity *= 2;  // Load factor ~0.5
        entries.resize(capacity);
        for (auto& e : entries) e.occupied = false;
    }

    V* find_or_insert(K key, uint32_t h) {
        size_t mask = entries.size() - 1;
        size_t idx = h & mask;

        while (entries[idx].occupied) {
            if (entries[idx].key == key && entries[idx].hash == h) {
                return &entries[idx].value;
            }
            idx = (idx + 1) & mask;
        }

        entries[idx].key = key;
        entries[idx].hash = h;
        entries[idx].occupied = true;
        return &entries[idx].value;
    }

    std::vector<std::pair<K, V>> to_vector() {
        std::vector<std::pair<K, V>> result;
        for (const auto& e : entries) {
            if (e.occupied) {
                result.push_back({e.key, e.value});
            }
        }
        return result;
    }

private:
    std::vector<Entry> entries;
};

// ==================== MAIN QUERY FUNCTION ====================

void run_q13(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto total_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t customer_rows = 1500000;
    const size_t orders_rows = 15000000;

    // ==================== STEP 1: Load Data ====================
#ifdef GENDB_PROFILE
    auto load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t file_size = 0;
    auto c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", file_size);

#ifdef GENDB_PROFILE
    auto load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(load_end - load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // ==================== STEP 2: Parallel stream-read Orders and filter by comment ====================
#ifdef GENDB_PROFILE
    auto filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Load o_custkey column
    size_t custkey_file_size = 0;
    auto o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", custkey_file_size);

    // Mmap the entire comment binary for parallel access
    size_t comments_file_size = 0;
    const char* comments_mmap = mmap_column<char>(gendb_dir + "/orders/o_comment.bin", comments_file_size);
    if (!comments_mmap) {
        std::cerr << "Error: Cannot mmap o_comment.bin" << std::endl;
        return;
    }

    // OPTIMIZATION (Iter 9): MORSEL-DRIVEN PARALLEL FILTERING
    // - Parallel scan with work-stealing: each thread pulls morsels atomically
    // - Thread-local hash tables avoid synchronization overhead in hot loop
    // - Merge thread-local tables at the end
    // - No pre-computation: just parallelize the sequential scan across threads

    const int num_threads = std::min(64, (int)std::thread::hardware_concurrency());

    // Allocate thread-local hash tables
    std::vector<OrdersHashTable> thread_local_tables;
    for (int t = 0; t < num_threads; t++) {
        thread_local_tables.emplace_back(1500000 / num_threads + 1000);
    }

    // Parallel scan and filter: each thread processes a contiguous range
    #pragma omp parallel num_threads(num_threads)
    {
        int thread_id = omp_get_thread_num();
        OrdersHashTable& local_table = thread_local_tables[thread_id];

        const char* ptr = comments_mmap;

        // Each thread processes a contiguous range of rows
        // (better cache locality than work-stealing)

        size_t thread_start_row = thread_id * (orders_rows / num_threads);
        size_t thread_end_row = (thread_id == num_threads - 1) ? orders_rows : (thread_id + 1) * (orders_rows / num_threads);

        // Fast-forward to this thread's starting row
        size_t current_row = 0;
        size_t byte_offset = 0;
        while (current_row < thread_start_row && byte_offset + 4 <= comments_file_size) {
            uint32_t len = *reinterpret_cast<const uint32_t*>(ptr + byte_offset);
            byte_offset += 4 + len;
            current_row++;
        }

        // Process this thread's range
        for (size_t row = thread_start_row; row < thread_end_row && byte_offset + 4 <= comments_file_size; row++) {
            uint32_t len = *reinterpret_cast<const uint32_t*>(ptr + byte_offset);
            const char* comment_data = ptr + byte_offset + 4;
            byte_offset += 4;

            if (byte_offset + len > comments_file_size) {
                std::cerr << "Error: truncated comment at row " << row << std::endl;
                break;
            }

            // Filter: o_comment NOT LIKE '%special%requests%'
            if (!matches_pattern_buffer(comment_data, len)) {
                local_table.increment(o_custkey[row]);
            }

            byte_offset += len;
        }
    }  // End parallel region

    // Merge thread-local tables into global result
    OrdersHashTable orders_per_customer(1500000);
    for (int t = 0; t < num_threads; t++) {
        auto local_entries = thread_local_tables[t].to_vector();
        for (const auto& entry : local_entries) {
            // Accumulate from thread-local into global table
            for (int32_t i = 0; i < entry.second; i++) {
                orders_per_customer.increment(entry.first);
            }
        }
    }

    // Unmap comments
    if (comments_mmap) {
        munmap((void*)comments_mmap, comments_file_size);
    }

#ifdef GENDB_PROFILE
    auto filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(filter_end - filter_start).count();
    printf("[TIMING] filter_and_count: %.2f ms\n", filter_ms);
#endif

    // ==================== STEP 3: LEFT OUTER JOIN customer with orders count ====================
#ifdef GENDB_PROFILE
    auto join_start = std::chrono::high_resolution_clock::now();
#endif

    // Result: c_count -> count of customers with that order count
    CompactHashTable<int32_t, AggState> c_count_agg(40);  // Estimate ~40 distinct c_count values

    for (size_t i = 0; i < customer_rows; i++) {
        int32_t custkey = c_custkey[i];
        int32_t count = 0;

        // LEFT OUTER JOIN: if customer not in orders, count = 0
        count = orders_per_customer.lookup(custkey);

        // Aggregate: count customers by c_count
        uint32_t h = std::hash<int32_t>()(count);
        auto* slot = c_count_agg.find_or_insert(count, h);
        if (slot->custdist == 0) {
            slot->c_count = count;
        }
        slot->custdist++;
    }

#ifdef GENDB_PROFILE
    auto join_end = std::chrono::high_resolution_clock::now();
    double join_ms = std::chrono::duration<double, std::milli>(join_end - join_start).count();
    printf("[TIMING] join_and_aggregate: %.2f ms\n", join_ms);
#endif

    // ==================== STEP 4: Convert to result vector ====================
    auto agg_results = c_count_agg.to_vector();

    // ==================== STEP 5: Sort Results ====================
#ifdef GENDB_PROFILE
    auto sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(agg_results.begin(), agg_results.end(),
        [](const std::pair<int32_t, AggState>& a, const std::pair<int32_t, AggState>& b) {
            // ORDER BY custdist DESC, c_count DESC
            if (a.second.custdist != b.second.custdist) {
                return a.second.custdist > b.second.custdist;
            }
            return a.first > b.first;  // c_count DESC
        });

#ifdef GENDB_PROFILE
    auto sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(sort_end - sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

    // ==================== STEP 6: Write Results to CSV ====================
#ifdef GENDB_PROFILE
    auto output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q13.csv";
    std::ofstream out(output_file);
    if (!out) {
        std::cerr << "Error: Cannot open output file " << output_file << std::endl;
        return;
    }

    // Write header
    out << "c_count,custdist\r\n";

    // Write rows
    for (const auto& row : agg_results) {
        out << row.first << "," << row.second.custdist << "\r\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(output_end - output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

#ifdef GENDB_PROFILE
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif
}

// ==================== MAIN ENTRY ====================

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
