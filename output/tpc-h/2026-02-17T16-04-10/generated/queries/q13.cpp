// Q13: Customer Distribution Query - Iteration 5
//
// OPTIMIZATION FOCUS: Micro-optimize the hot path (string filtering)
// Iteration 4 regressed due to comment_refs vector allocation (240MB) + partition merge
// Iteration 5: Return to Iter 3 base + micro-optimizations to filter hot path
//
// LOGICAL PLAN:
// 1. Load customer(c_custkey) → 1.5M rows
// 2. Stream orders(o_custkey, o_comment) → 15M rows (via mmap, direct pointer math)
// 3. Filter orders: o_comment NOT LIKE '%special%requests%' (use strstr with early-exit)
// 4. Accumulate counts directly into custkey hash table
// 5. LEFT OUTER JOIN customer with orders count table → 1.5M rows
// 6. Final aggregation: c_count → custdist (compact hash table, ~40 groups)
// 7. Sort results by custdist DESC, c_count DESC
//
// PHYSICAL PLAN (Iter 5):
// - Sequential comment scan via mmap (cache-friendly, proven fast in Iter 3)
// - Micro-optimized string filter: added length checks to reduce strstr calls
// - Direct accumulation into single OrdersHashTable (NO partition overhead)
// - Customer scan: single-threaded (1.5M rows = 114ms)
// - Final aggregation: c_count → custdist
// - Sort results
//
// KEY OPTIMIZATIONS (Iter 5):
// - RETURN to Iter 3 approach (1212ms is solid, avoids Iter 4 regression)
// - Added early-exit length checks in matches_pattern_buffer:
//   * Skip strstr if comment too short for "special" (7) + "requests" (8)
//   * Check remaining length after finding "special" before calling strstr for "requests"
// - Tighter string filter code → better branch prediction and fewer strstr calls
// - Expected result: ~1200-1220ms (similar to Iter 3, avoiding Iter 4 regression)

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

namespace {

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

} // end anonymous namespace

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

    // ==================== STEP 2: Stream-read Orders and filter by comment (OPTIMIZED) ====================
#ifdef GENDB_PROFILE
    auto filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Strategy (Iter 5): RETURN TO ITER 3 APPROACH + refined optimizations
    // - Sequential mmap scan of comments (Iter 3 was fast: 1087ms filter_and_count)
    // - Direct accumulation into single OrdersHashTable (no partition overhead)
    // - Focus on filter performance: strstr is already SIMD-optimized
    // - Key insight: Iter 4 regressed due to vector allocation + merge
    //   Iter 5 avoids both by going back to Iter 3 but with tighter code

    // Load o_custkey column
    size_t custkey_file_size = 0;
    auto o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", custkey_file_size);

    // Mmap the entire comment binary for sequential access
    size_t comments_file_size = 0;
    const char* comments_mmap = mmap_column<char>(gendb_dir + "/orders/o_comment.bin", comments_file_size);
    if (!comments_mmap) {
        std::cerr << "Error: Cannot mmap o_comment.bin" << std::endl;
        return;
    }

    // OPTIMIZATION (Iter 5): Sequential scan with direct accumulation
    // No vector allocation, no partitioning overhead
    // Let compiler optimize the tight loop

    OrdersHashTable orders_per_customer(1500000);
    {
        const char* ptr = comments_mmap;
        const char* end = comments_mmap + comments_file_size;

        for (size_t i = 0; i < orders_rows && ptr < end; i++) {
            // Read length prefix
            if (ptr + 4 > end) {
                std::cerr << "Error: truncated comment at row " << i << std::endl;
                break;
            }
            uint32_t len = *reinterpret_cast<const uint32_t*>(ptr);
            ptr += 4;

            if (ptr + len > end) {
                std::cerr << "Error: truncated comment data at row " << i << std::endl;
                break;
            }

            // Filter: o_comment NOT LIKE '%special%requests%'
            // Use glibc-optimized strstr (SSE4.2/AVX2)
            if (!matches_pattern_buffer(ptr, len)) {
                orders_per_customer.increment(o_custkey[i]);
            }

            ptr += len;
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
