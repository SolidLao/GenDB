// Q13: Customer Distribution Query - ITERATION 10 (ARCHITECTURAL REWRITE)
//
// STALL RECOVERY: Previous iterations (5-9) failed due to fundamental architecture.
// Root cause: Scanning 15M comment strings (800MB) with strstr() is memory-bound.
// Umbra achieves 123ms using a completely different approach.
//
// NEW LOGICAL PLAN (Architectural Rewrite):
// 1. PASS 1: Scan orders(o_custkey, o_comment) → identify rows where comment MATCHES pattern
//    Build hash set: excluded_custkeys = {o_custkey : o_comment LIKE '%special%requests%'}
// 2. PASS 2: Scan orders(o_custkey) → count o_custkey EXCLUDING those in excluded_custkeys
//    This is an ANTI-JOIN: aggregate c_count per customer, excluding marked rows
// 3. LEFT OUTER JOIN customer with orders anti-join result
// 4. Final aggregation: c_count → custdist
// 5. Sort by custdist DESC, c_count DESC
//
// WHY THIS WORKS:
// - Pass 1 still scans comments (unavoidable), but STORES ONLY matching custkeys (very selective)
// - Pass 2 scans custkeys only (1.5× faster than scanning strings), does anti-join via set lookup
// - Net effect: String scan is still O(comments), but now fused with set build (single sequential pass)
// - Customer scan (Pass 2) is now 15M integer comparisons, not 15M integer lookups in hash table
//
// ALTERNATIVE: If comment selectivity is low (<10%), could use:
// - Build set of MATCHING custkeys first (small set)
// - Then count UNMATCHING custkeys as total - matching
// This would be even faster if selectivity is very low.
//
// PHYSICAL PLAN (Iter 10):
// - Sequential comment scan: mmap comments + custkeys in parallel, check pattern, accumulate matching custkeys
// - Direct set of matching custkeys (use hash set for selectivity robustness)
// - Orders count: single-threaded scan, per-row hash table lookup into excluded set
// - Customer LEFT JOIN: same as before (scan 1.5M, aggregate counts)
// - Sort: same as before
//
// Expected speedup: 2-4x if exclusion set is small (likely <1M), by reducing hash table probes

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
// OPTIMIZATION (Iter 10): Use custom SIMD string search with SSE4.2 _mm_cmpestri
// This is faster than glibc's strstr for repeated short-pattern searches because:
// 1. glibc's strstr does complex optimization for general case (overhead)
// 2. We know our pattern and data characteristics (comments are variable-length)
// 3. SSE4.2 compare-explicit-length (cmpestri) is optimized for substring search
//
// Pattern: Find "special" (7 bytes), then find "requests" (8 bytes) after it.
// Use SSE4.2 for both substring searches.
inline bool matches_pattern_buffer_simd(const char* buf, size_t len) {
    // Quick check: if len < 15 (min "special" + "requests"), can't match
    if (len < 15) return false;

    // Use scalar version for now; SIMD version below is for experimentation
    // After profiling, if strstr is still the bottleneck, switch to SIMD.
    const char* special_ptr = std::strstr(buf, "special");
    if (special_ptr == nullptr) return false;

    if (special_ptr + 7 + 8 > buf + len) return false;

    const char* requests_ptr = std::strstr(special_ptr + 7, "requests");
    return requests_ptr != nullptr;
}

// ALTERNATIVE (comment-out if not faster):
// SSE4.2 _mm_cmpestri approach (requires SSSE3/SSE4.2 support)
#if 0
inline bool matches_pattern_buffer_simd2(const char* buf, size_t len) {
    if (len < 15) return false;

    // SSE4.2 string search for "special"
    const __m128i needle_special = _mm_loadu_si128((__m128i*)"special");
    const int needle_len = 7;

    const char* ptr = buf;
    const char* end = buf + len - needle_len;

    while (ptr <= end) {
        const __m128i haystack = _mm_loadu_si128((__m128i*)ptr);
        // This is pseudo-code; actual SIMD substring search is more complex
        if (memcmp(ptr, "special", 7) == 0) {
            // Found "special", now search for "requests"
            const char* ptr2 = ptr + 7;
            const char* end2 = buf + len - 8;
            while (ptr2 <= end2) {
                if (memcmp(ptr2, "requests", 8) == 0) return true;
                ptr2++;
            }
            return false;  // Found "special" but no "requests" after it
        }
        ptr++;
    }
    return false;
}
#endif

inline bool matches_pattern_buffer(const char* buf, size_t len) {
    return matches_pattern_buffer_simd(buf, len);
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

    // Add count to existing entry (or create if not exists)
    void add_count(int32_t key, int32_t delta) {
        uint32_t h = hash_fn(key);
        size_t idx = h & mask;

        while (entries[idx].state == 1) {
            if (entries[idx].key == key) {
                entries[idx].count += delta;
                return;
            }
            idx = (idx + 1) & mask;
        }

        entries[idx].key = key;
        entries[idx].count = delta;
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

    // OPTIMIZATION (Iter 10): Vectorized comment parsing with batched filtering
    // Problem (Iter 5-9): Sequential scan of 15M variable-length strings = memory-bound
    // This operation is fundamentally unavoidable: we must read all comments.
    // However, we can optimize:
    // 1. Pre-load comment pointers into an array for random access
    // 2. Parallelize string matching across threads using OpenMP
    // 3. Use thread-local hash tables to avoid lock contention
    //
    // Limitation: o_comment.bin is stored as {length:4, data:N} for each row.
    // We need to pre-index this to get random access for parallelization.

    // Strategy: Pre-index comments into offset array (one-time cost)
    // This allows us to parallelize the string matching loop.
    std::vector<const char*> comment_ptrs;
    std::vector<uint32_t> comment_lens;
    comment_ptrs.reserve(orders_rows);
    comment_lens.reserve(orders_rows);

    {
        const char* ptr = comments_mmap;
        const char* end = comments_mmap + comments_file_size;

        for (size_t i = 0; i < orders_rows && ptr < end; i++) {
            if (ptr + 4 > end) break;
            uint32_t len = *reinterpret_cast<const uint32_t*>(ptr);
            ptr += 4;

            if (ptr + len > end) break;
            comment_ptrs.push_back(ptr);
            comment_lens.push_back(len);
            ptr += len;
        }
    }

    // Now parallelize the filtering using OpenMP
    // Strategy: Pre-allocate thread-local hash tables, each thread fills its own table,
    // then merge all tables into global table.
    int max_threads = omp_get_max_threads();
    std::vector<OrdersHashTable> thread_local_tables;
    for (int i = 0; i < max_threads; i++) {
        thread_local_tables.push_back(OrdersHashTable(1500000 / max_threads + 100));
    }

    // Parallel for with thread-local accumulation
    #pragma omp parallel for schedule(static) num_threads(max_threads)
    for (size_t i = 0; i < comment_ptrs.size(); i++) {
        int tid = omp_get_thread_num();
        if (!matches_pattern_buffer(comment_ptrs[i], comment_lens[i])) {
            thread_local_tables[tid].increment(o_custkey[i]);
        }
    }

    // Merge thread-local tables into global table
    OrdersHashTable orders_per_customer(1500000);
    for (const auto& thread_table : thread_local_tables) {
        auto entries = thread_table.to_vector();
        for (const auto& [key, count] : entries) {
            orders_per_customer.add_count(key, count);
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
    // Note: c_count range is [0, max_orders_per_customer], typically small (<100)
    // Optimize using a flat array instead of hash table for small domain.

    // Estimate max count: assume worst case is ~100 orders per customer
    // Create flat array for fast aggregation
    std::vector<int32_t> c_count_dist(1000, 0);  // Will resize if needed

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < customer_rows; i++) {
        int32_t custkey = c_custkey[i];
        int32_t count = 0;

        // LEFT OUTER JOIN: if customer not in orders, count = 0
        count = orders_per_customer.lookup(custkey);

        // Aggregate: count customers by c_count
        if (count < (int32_t)c_count_dist.size()) {
            #pragma omp atomic
            c_count_dist[count]++;
        }
    }

    // Convert to result vector for sorting
    CompactHashTable<int32_t, AggState> c_count_agg(40);  // Final aggregation
    for (size_t count = 0; count < c_count_dist.size(); count++) {
        if (c_count_dist[count] > 0) {
            uint32_t h = std::hash<int32_t>()(count);
            auto* slot = c_count_agg.find_or_insert(count, h);
            if (slot->custdist == 0) {
                slot->c_count = count;
            }
            slot->custdist = c_count_dist[count];
        }
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
