// Q3: Shipping Priority Query - Self-contained implementation
// Parallel 3-way join: customer -> orders -> lineitem with filtering and aggregation
//
// OPTIMIZATIONS APPLIED (iter1):
// 1. CRITICAL FIX: Revenue precision corrected to preserve 4 decimal places (x10000 instead of x100)
// 2. Improved hash table pre-sizing based on observed selectivity
// 3. Zone map pruning on sorted l_shipdate and o_orderdate (I/O optimization)
// 4. HDD-optimized madvise hints (MADV_SEQUENTIAL for scans, MADV_WILLNEED for prefetch)
// 5. Lazy column loading - only mmap columns actually used by query
//
// OPTIMIZATIONS APPLIED (iter2 - CPU bound):
// 6. Bitmap-based hash tables for O(1) lookups (replaces std::unordered_map)
// 7. SIMD (AVX2) for date filtering in orders and lineitem scans
// 8. Lock-free thread-local aggregation with final merge

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <cstdint>
#include <climits>
#include <queue>
#include <immintrin.h>  // AVX2/SSE intrinsics

// ============================================================================
// Date Utilities (inline)
// ============================================================================

inline int32_t date_to_days(int year, int month, int day) {
    // Days since 1970-01-01
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 719469;
}

// ============================================================================
// SIMD Utilities (inline) - for vectorized date filtering
// ============================================================================

// Check if CPU supports AVX2 at compile time
#ifdef __AVX2__
#define USE_AVX2 1
#else
#define USE_AVX2 0
#endif

#if USE_AVX2
// Optimized SIMD: Generate bitmask for dates < threshold
// Returns bitmask where bit i is set if dates[i] < threshold
inline uint64_t simd_compare_dates_less_than_batch8(
    const int32_t* dates,
    int32_t threshold)
{
    __m256i threshold_vec = _mm256_set1_epi32(threshold);
    __m256i dates_vec = _mm256_loadu_si256((__m256i*)dates);
    __m256i cmp = _mm256_cmpgt_epi32(threshold_vec, dates_vec);  // threshold > dates

    // Convert comparison mask to bitmask
    // Each comparison result is 0xFFFFFFFF (match) or 0x00000000 (no match)
    int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));
    return mask;
}

// Optimized SIMD: Generate bitmask for dates > threshold
inline uint64_t simd_compare_dates_greater_than_batch8(
    const int32_t* dates,
    int32_t threshold)
{
    __m256i threshold_vec = _mm256_set1_epi32(threshold);
    __m256i dates_vec = _mm256_loadu_si256((__m256i*)dates);
    __m256i cmp = _mm256_cmpgt_epi32(dates_vec, threshold_vec);  // dates > threshold

    // Convert comparison mask to bitmask
    int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));
    return mask;
}
#endif

// ============================================================================
// Bloom Filter (inline) - for join reduction
// ============================================================================

class BloomFilter {
private:
    std::vector<uint64_t> bits;
    size_t num_bits;

    // Two independent hash functions
    inline size_t hash1(int32_t key) const {
        uint64_t h = static_cast<uint64_t>(key);
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        return h % num_bits;
    }

    inline size_t hash2(int32_t key) const {
        uint64_t h = static_cast<uint64_t>(key);
        h ^= h >> 33;
        h *= 0xc4ceb9fe1a85ec53ULL;
        h ^= h >> 33;
        return h % num_bits;
    }

public:
    BloomFilter(size_t expected_elements, double false_positive_rate = 0.1) {
        // Calculate optimal bit array size: m = -n*ln(p) / (ln(2)^2)
        // For p=0.1, bits_per_element = -ln(0.1) / (ln(2)^2) ≈ 4.8 bits
        // Use 4 bits per element for memory efficiency
        num_bits = expected_elements * 4;
        bits.resize((num_bits + 63) / 64, 0);
    }

    inline void insert(int32_t key) {
        size_t pos1 = hash1(key);
        size_t pos2 = hash2(key);
        bits[pos1 / 64] |= (1ULL << (pos1 % 64));
        bits[pos2 / 64] |= (1ULL << (pos2 % 64));
    }

    inline bool might_contain(int32_t key) const {
        size_t pos1 = hash1(key);
        size_t pos2 = hash2(key);
        return (bits[pos1 / 64] & (1ULL << (pos1 % 64))) &&
               (bits[pos2 / 64] & (1ULL << (pos2 % 64)));
    }
};

// ============================================================================
// Zone Map Structure (for block pruning)
// ============================================================================

struct ZoneMapBlock {
    int32_t min_value;
    int32_t max_value;
    size_t start_offset;  // Row offset where block starts
    size_t end_offset;    // Row offset where block ends
};

struct ZoneMap {
    std::vector<ZoneMapBlock> blocks;

    bool load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;

        // Read number of blocks (first 8 bytes)
        uint64_t num_blocks;
        if (read(fd, &num_blocks, sizeof(uint64_t)) != sizeof(uint64_t)) {
            close(fd);
            return false;
        }

        blocks.reserve(num_blocks);

        // Read each block: min (4), max (4), start_offset (8), end_offset (8) = 24 bytes
        for (uint64_t i = 0; i < num_blocks; i++) {
            int32_t min_val, max_val;
            uint64_t start_offset, end_offset;

            if (read(fd, &min_val, sizeof(int32_t)) != sizeof(int32_t) ||
                read(fd, &max_val, sizeof(int32_t)) != sizeof(int32_t) ||
                read(fd, &start_offset, sizeof(uint64_t)) != sizeof(uint64_t) ||
                read(fd, &end_offset, sizeof(uint64_t)) != sizeof(uint64_t)) {
                close(fd);
                return false;
            }

            blocks.push_back({min_val, max_val, start_offset, end_offset});
        }

        close(fd);
        return true;
    }

    // Returns list of (start_row, end_row) ranges that could match predicate
    std::vector<std::pair<size_t, size_t>> get_matching_ranges(
        int32_t min_threshold, int32_t max_threshold) const {

        std::vector<std::pair<size_t, size_t>> ranges;

        for (const auto& block : blocks) {
            // Check if block overlaps with predicate range [min_threshold, max_threshold]
            bool overlaps = !(block.max_value < min_threshold || block.min_value > max_threshold);

            if (overlaps) {
                ranges.push_back({block.start_offset, block.end_offset});
            }
        }

        return ranges;
    }
};

// ============================================================================
// Memory-mapped Column Loader (inline)
// ============================================================================

template<typename T>
class MMapColumn {
public:
    T* data;
    size_t size;
    int fd;
    void* mapped_addr;
    size_t mapped_size;

    MMapColumn() : data(nullptr), size(0), fd(-1), mapped_addr(nullptr), mapped_size(0) {}

    bool load(const std::string& path, size_t row_count, bool sequential = true, bool prefetch = false) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        mapped_size = row_count * sizeof(T);
        mapped_addr = mmap(nullptr, mapped_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped_addr == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            close(fd);
            fd = -1;
            return false;
        }

        // HDD-optimized madvise hints
        if (sequential) {
            // Sequential access pattern - kernel should read ahead
            madvise(mapped_addr, mapped_size, MADV_SEQUENTIAL);
        } else {
            // Random access pattern
            madvise(mapped_addr, mapped_size, MADV_RANDOM);
        }

        if (prefetch) {
            // Prefetch data into page cache (non-blocking)
            madvise(mapped_addr, mapped_size, MADV_WILLNEED);
        }

        data = static_cast<T*>(mapped_addr);
        size = row_count;
        return true;
    }

    ~MMapColumn() {
        if (mapped_addr && mapped_addr != MAP_FAILED) {
            munmap(mapped_addr, mapped_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
};

// ============================================================================
// Dictionary Loader (inline)
// ============================================================================

std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream file(path);
    if (!file.is_open()) {
        return dict;
    }
    std::string line;
    while (std::getline(file, line)) {
        dict.push_back(line);
    }
    return dict;
}

// ============================================================================
// Metadata Loader (inline)
// ============================================================================

struct TableMetadata {
    size_t row_count;
    std::string sorted_by;
};

TableMetadata load_metadata(const std::string& path) {
    TableMetadata meta = {0, ""};
    std::ifstream file(path);
    if (!file.is_open()) {
        return meta;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.find("\"row_count\"") != std::string::npos) {
            size_t pos = line.find(":");
            if (pos != std::string::npos) {
                std::string val = line.substr(pos + 1);
                val.erase(std::remove(val.begin(), val.end(), ','), val.end());
                val.erase(std::remove(val.begin(), val.end(), ' '), val.end());
                meta.row_count = std::stoull(val);
            }
        }
        if (line.find("\"sorted_by\"") != std::string::npos) {
            size_t pos1 = line.find(": \"");
            size_t pos2 = line.find("\"", pos1 + 3);
            if (pos1 != std::string::npos && pos2 != std::string::npos) {
                meta.sorted_by = line.substr(pos1 + 3, pos2 - pos1 - 3);
            }
        }
    }
    return meta;
}

// ============================================================================
// Aggregation Key and Result (inline, specialized for Q3)
// ============================================================================

struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey
            && o_orderdate == other.o_orderdate
            && o_shippriority == other.o_shippriority;
    }
};

namespace std {
    template<>
    struct hash<AggKey> {
        size_t operator()(const AggKey& k) const {
            size_t h1 = std::hash<int32_t>()(k.l_orderkey);
            size_t h2 = std::hash<int32_t>()(k.o_orderdate);
            size_t h3 = std::hash<int32_t>()(k.o_shippriority);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
}

struct AggValue {
    int64_t revenue_sum; // in 0.0001 units (x10000 for 4 decimal precision)
};

// ============================================================================
// Result Row for Top-K
// ============================================================================

struct ResultRow {
    int32_t l_orderkey;
    int64_t revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const ResultRow& other) const {
        // Standard less-than for sorting
        if (revenue != other.revenue) return revenue < other.revenue;
        return o_orderdate > other.o_orderdate;
    }

    bool operator>(const ResultRow& other) const {
        // For std::greater in priority_queue (min-heap)
        if (revenue != other.revenue) return revenue > other.revenue;
        return o_orderdate < other.o_orderdate;
    }
};

// ============================================================================
// Main Query Execution
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc >= 3 ? argv[2] : "";

    auto start_time = std::chrono::high_resolution_clock::now();

    // ------------------------------------------------------------------------
    // Step 1: Load metadata
    // ------------------------------------------------------------------------

    std::string cust_dir = gendb_dir + "/customer";
    std::string orders_dir = gendb_dir + "/orders";
    std::string lineitem_dir = gendb_dir + "/lineitem";

    auto cust_meta = load_metadata(cust_dir + "/metadata.json");
    auto orders_meta = load_metadata(orders_dir + "/metadata.json");
    auto lineitem_meta = load_metadata(lineitem_dir + "/metadata.json");

    if (cust_meta.row_count == 0 || orders_meta.row_count == 0 || lineitem_meta.row_count == 0) {
        std::cerr << "Failed to load metadata" << std::endl;
        return 1;
    }

    // ------------------------------------------------------------------------
    // Step 2: Load customer columns (c_custkey, c_mktsegment)
    // OPTIMIZATION: Only load columns needed for query (lazy loading)
    // Sequential scan hint for HDD
    // ------------------------------------------------------------------------

    MMapColumn<int32_t> c_custkey;
    MMapColumn<uint8_t> c_mktsegment;

    if (!c_custkey.load(cust_dir + "/c_custkey.bin", cust_meta.row_count, true, false)) return 1;
    if (!c_mktsegment.load(cust_dir + "/c_mktsegment.bin", cust_meta.row_count, true, false)) return 1;

    auto mktsegment_dict = load_dictionary(cust_dir + "/c_mktsegment.dict");

    // Find code for 'BUILDING'
    uint8_t building_code = 255;
    for (size_t i = 0; i < mktsegment_dict.size(); i++) {
        if (mktsegment_dict[i] == "BUILDING") {
            building_code = static_cast<uint8_t>(i);
            break;
        }
    }

    if (building_code == 255) {
        std::cerr << "BUILDING not found in dictionary" << std::endl;
        return 1;
    }

    // ------------------------------------------------------------------------
    // Step 3: Filter customer (c_mktsegment = 'BUILDING') and build bitmap
    // OPTIMIZATION: Use bitmap instead of hash map for O(1) lookups
    // ------------------------------------------------------------------------

    // Find max customer key to size the bitmap
    int32_t max_custkey = 0;
    for (size_t i = 0; i < cust_meta.row_count; i++) {
        if (c_custkey.data[i] > max_custkey) max_custkey = c_custkey.data[i];
    }

    // Bitmap: O(1) lookup, cache-friendly, no hash collisions
    std::vector<uint8_t> customer_bitmap(max_custkey + 1, 0);
    size_t filtered_customer_count = 0;

    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;

    const size_t cust_morsel_size = 50000;
    std::atomic<size_t> cust_morsel_idx{0};
    std::vector<std::thread> threads;

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            while (true) {
                size_t start = cust_morsel_idx.fetch_add(cust_morsel_size, std::memory_order_relaxed);
                if (start >= cust_meta.row_count) break;
                size_t end = std::min(start + cust_morsel_size, cust_meta.row_count);

                for (size_t i = start; i < end; i++) {
                    if (c_mktsegment.data[i] == building_code) {
                        int32_t ck = c_custkey.data[i];
                        customer_bitmap[ck] = 1;
                    }
                }
            }
        });
    }

    for (auto& th : threads) th.join();
    threads.clear();

    // Count filtered customers
    for (size_t i = 0; i <= max_custkey; i++) {
        if (customer_bitmap[i]) filtered_customer_count++;
    }

    std::cout << "Filtered customers: " << filtered_customer_count << std::endl;

    // ------------------------------------------------------------------------
    // Step 4: Load orders columns and filter by date + customer
    // OPTIMIZATION: Load zone map to skip blocks where o_orderdate >= 1995-03-15
    // ------------------------------------------------------------------------

    int32_t date_threshold = date_to_days(1995, 3, 15); // o_orderdate < '1995-03-15'

    // Load zone map for o_orderdate
    ZoneMap orders_zonemap;
    bool has_orders_zonemap = orders_zonemap.load(orders_dir + "/o_orderdate_zonemap.idx");

    std::vector<std::pair<size_t, size_t>> orders_ranges;
    if (has_orders_zonemap) {
        // Get ranges where o_orderdate could be < date_threshold
        // Predicate: o_orderdate < date_threshold means we want [INT32_MIN, date_threshold-1]
        orders_ranges = orders_zonemap.get_matching_ranges(INT32_MIN, date_threshold - 1);

        size_t total_rows = 0;
        for (const auto& range : orders_ranges) {
            total_rows += range.second - range.first;
        }
        std::cout << "Zone map pruning (orders): scanning " << total_rows << " / "
                  << orders_meta.row_count << " rows ("
                  << (100.0 * total_rows / orders_meta.row_count) << "%)" << std::endl;
    } else {
        // No zone map, scan all rows
        orders_ranges.push_back({0, orders_meta.row_count});
    }

    MMapColumn<int32_t> o_orderkey;
    MMapColumn<int32_t> o_custkey;
    MMapColumn<int32_t> o_orderdate;
    MMapColumn<int32_t> o_shippriority;

    // Sequential scan patterns for HDD
    if (!o_orderkey.load(orders_dir + "/o_orderkey.bin", orders_meta.row_count, true, false)) return 1;
    if (!o_custkey.load(orders_dir + "/o_custkey.bin", orders_meta.row_count, true, false)) return 1;
    if (!o_orderdate.load(orders_dir + "/o_orderdate.bin", orders_meta.row_count, true, false)) return 1;
    if (!o_shippriority.load(orders_dir + "/o_shippriority.bin", orders_meta.row_count, true, false)) return 1;

    // Find max order key to size the structures
    int32_t max_orderkey = 0;
    for (size_t i = 0; i < orders_meta.row_count; i++) {
        if (o_orderkey.data[i] > max_orderkey) max_orderkey = o_orderkey.data[i];
    }

    // Use parallel arrays for O(1) lookup: orderkey -> (orderdate, shippriority)
    // Bitmap to check if order is filtered
    std::vector<uint8_t> orders_bitmap(max_orderkey + 1, 0);
    std::vector<int32_t> orders_orderdate(max_orderkey + 1, 0);
    std::vector<int32_t> orders_shippriority(max_orderkey + 1, 0);

    // OPTIMIZATION: Use zone map to determine scan range
    size_t orders_scan_start = orders_ranges.empty() ? 0 : orders_ranges.front().first;
    size_t orders_scan_end = orders_ranges.empty() ? orders_meta.row_count : orders_ranges.back().second;

    const size_t orders_morsel_size = 50000;
    std::atomic<size_t> orders_morsel_idx{orders_scan_start};  // Start at zone map offset
    std::atomic<size_t> filtered_order_count{0};

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            size_t local_count = 0;

            while (true) {
                size_t start = orders_morsel_idx.fetch_add(orders_morsel_size, std::memory_order_relaxed);
                if (start >= orders_scan_end) break;
                size_t end = std::min(start + orders_morsel_size, orders_scan_end);

#if USE_AVX2
                // Use SIMD to process dates in batches of 8
                size_t i = start;
                for (; i + 7 < end; i += 8) {
                    uint64_t mask = simd_compare_dates_less_than_batch8(&o_orderdate.data[i], date_threshold);

                    // Process matching dates (mask has bits set for matches)
                    for (int j = 0; j < 8; j++) {
                        if (mask & (1 << j)) {
                            size_t idx = i + j;
                            int32_t ck = o_custkey.data[idx];
                            // Bitmap lookup: O(1), cache-friendly
                            if (customer_bitmap[ck]) {
                                int32_t ok = o_orderkey.data[idx];
                                orders_bitmap[ok] = 1;
                                orders_orderdate[ok] = o_orderdate.data[idx];
                                orders_shippriority[ok] = o_shippriority.data[idx];
                                local_count++;
                            }
                        }
                    }
                }

                // Handle remainder with scalar code
                for (; i < end; i++) {
                    if (o_orderdate.data[i] < date_threshold) {
                        int32_t ck = o_custkey.data[i];
                        if (customer_bitmap[ck]) {
                            int32_t ok = o_orderkey.data[i];
                            orders_bitmap[ok] = 1;
                            orders_orderdate[ok] = o_orderdate.data[i];
                            orders_shippriority[ok] = o_shippriority.data[i];
                            local_count++;
                        }
                    }
                }
#else
                // Fallback to scalar processing
                for (size_t i = start; i < end; i++) {
                    // Still check date (zone map is approximate, gives min/max per block)
                    if (o_orderdate.data[i] < date_threshold) {
                        int32_t ck = o_custkey.data[i];
                        // Bitmap lookup: O(1), cache-friendly
                        if (customer_bitmap[ck]) {
                            int32_t ok = o_orderkey.data[i];
                            orders_bitmap[ok] = 1;
                            orders_orderdate[ok] = o_orderdate.data[i];
                            orders_shippriority[ok] = o_shippriority.data[i];
                            local_count++;
                        }
                    }
                }
#endif
            }

            filtered_order_count.fetch_add(local_count, std::memory_order_relaxed);
        });
    }

    for (auto& th : threads) th.join();
    threads.clear();

    std::cout << "Filtered orders: " << filtered_order_count.load() << std::endl;

    // ------------------------------------------------------------------------
    // Step 5: Load lineitem columns and join with orders, aggregate
    // OPTIMIZATION: Load zone map to skip blocks where l_shipdate <= 1995-03-15
    // ------------------------------------------------------------------------

    int32_t shipdate_threshold = date_to_days(1995, 3, 15); // l_shipdate > '1995-03-15'

    // Load zone map for l_shipdate
    ZoneMap lineitem_zonemap;
    bool has_lineitem_zonemap = lineitem_zonemap.load(lineitem_dir + "/l_shipdate_zonemap.idx");

    std::vector<std::pair<size_t, size_t>> lineitem_ranges;
    if (has_lineitem_zonemap) {
        // Get ranges where l_shipdate could be > shipdate_threshold
        // Predicate: l_shipdate > shipdate_threshold means we want [shipdate_threshold+1, INT32_MAX]
        lineitem_ranges = lineitem_zonemap.get_matching_ranges(shipdate_threshold + 1, INT32_MAX);

        size_t total_rows = 0;
        for (const auto& range : lineitem_ranges) {
            total_rows += range.second - range.first;
        }
        std::cout << "Zone map pruning (lineitem): scanning " << total_rows << " / "
                  << lineitem_meta.row_count << " rows ("
                  << (100.0 * total_rows / lineitem_meta.row_count) << "%)" << std::endl;
    } else {
        // No zone map, scan all rows
        lineitem_ranges.push_back({0, lineitem_meta.row_count});
    }

    MMapColumn<int32_t> l_orderkey;
    MMapColumn<int64_t> l_extendedprice;
    MMapColumn<int64_t> l_discount;
    MMapColumn<int32_t> l_shipdate;

    // Sequential scan patterns for HDD
    if (!l_orderkey.load(lineitem_dir + "/l_orderkey.bin", lineitem_meta.row_count, true, false)) return 1;
    if (!l_extendedprice.load(lineitem_dir + "/l_extendedprice.bin", lineitem_meta.row_count, true, false)) return 1;
    if (!l_discount.load(lineitem_dir + "/l_discount.bin", lineitem_meta.row_count, true, false)) return 1;
    if (!l_shipdate.load(lineitem_dir + "/l_shipdate.bin", lineitem_meta.row_count, true, false)) return 1;

    // Thread-local aggregation
    std::vector<std::unordered_map<AggKey, AggValue>> local_agg_tables(num_threads);
    for (auto& table : local_agg_tables) {
        table.reserve(10000);
    }

    // OPTIMIZATION: Use zone map to determine scan range
    size_t lineitem_scan_start = lineitem_ranges.empty() ? 0 : lineitem_ranges.front().first;
    size_t lineitem_scan_end = lineitem_ranges.empty() ? lineitem_meta.row_count : lineitem_ranges.back().second;

    const size_t lineitem_morsel_size = 100000;
    std::atomic<size_t> lineitem_morsel_idx{lineitem_scan_start};  // Start at zone map offset

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            auto& local_agg = local_agg_tables[t];

            while (true) {
                size_t start = lineitem_morsel_idx.fetch_add(lineitem_morsel_size, std::memory_order_relaxed);
                if (start >= lineitem_scan_end) break;
                size_t end = std::min(start + lineitem_morsel_size, lineitem_scan_end);

#if USE_AVX2
                // Use SIMD to process dates in batches of 8
                size_t i = start;
                for (; i + 7 < end; i += 8) {
                    uint64_t mask = simd_compare_dates_greater_than_batch8(&l_shipdate.data[i], shipdate_threshold);

                    // Process matching dates (mask has bits set for matches)
                    for (int j = 0; j < 8; j++) {
                        if (mask & (1 << j)) {
                            size_t idx = i + j;
                            int32_t ok = l_orderkey.data[idx];
                            // Bitmap lookup: O(1), cache-friendly
                            if (orders_bitmap[ok]) {
                                // CRITICAL FIX: Preserve 4 decimal precision in revenue calculation
                                int64_t price_cents = l_extendedprice.data[idx];
                                int64_t discount = l_discount.data[idx];
                                int64_t revenue_x10000 = price_cents * (100 - discount);

                                AggKey key{ok, orders_orderdate[ok], orders_shippriority[ok]};
                                local_agg[key].revenue_sum += revenue_x10000;
                            }
                        }
                    }
                }

                // Handle remainder with scalar code
                for (; i < end; i++) {
                    if (l_shipdate.data[i] > shipdate_threshold) {
                        int32_t ok = l_orderkey.data[i];
                        if (orders_bitmap[ok]) {
                            int64_t price_cents = l_extendedprice.data[i];
                            int64_t discount = l_discount.data[i];
                            int64_t revenue_x10000 = price_cents * (100 - discount);

                            AggKey key{ok, orders_orderdate[ok], orders_shippriority[ok]};
                            local_agg[key].revenue_sum += revenue_x10000;
                        }
                    }
                }
#else
                // Fallback to scalar processing
                for (size_t i = start; i < end; i++) {
                    // Still check date (zone map is approximate, gives min/max per block)
                    if (l_shipdate.data[i] > shipdate_threshold) {
                        int32_t ok = l_orderkey.data[i];
                        // Bitmap lookup: O(1), cache-friendly
                        if (orders_bitmap[ok]) {
                            // CRITICAL FIX: Preserve 4 decimal precision in revenue calculation
                            // OLD (WRONG): revenue = price_cents * (100 - discount) / 100  -> loses 2 decimals
                            // NEW (CORRECT): revenue_x10000 = price_cents * (100 - discount) -> keeps 4 decimals
                            // Example: price=9050100 cents ($90,501.00), discount=4 (0.04)
                            // OLD: 9050100 * 96 / 100 = 8688096 cents = $86,880.96 (WRONG, should be $86,880.9600)
                            // NEW: 9050100 * 96 = 868809600 (x10000 units) = $86,880.9600 (CORRECT)
                            int64_t price_cents = l_extendedprice.data[i];
                            int64_t discount = l_discount.data[i];
                            int64_t revenue_x10000 = price_cents * (100 - discount);

                            AggKey key{ok, orders_orderdate[ok], orders_shippriority[ok]};
                            local_agg[key].revenue_sum += revenue_x10000;
                        }
                    }
                }
#endif
            }
        });
    }

    for (auto& th : threads) th.join();
    threads.clear();

    // ------------------------------------------------------------------------
    // Step 6: Merge thread-local aggregation results
    // ------------------------------------------------------------------------

    std::unordered_map<AggKey, AggValue> global_agg;
    // OPT: Pre-size for observed ~114K aggregation groups
    global_agg.reserve(150000);

    for (auto& local_agg : local_agg_tables) {
        for (auto& kv : local_agg) {
            global_agg[kv.first].revenue_sum += kv.second.revenue_sum;
        }
    }

    std::cout << "Aggregation groups: " << global_agg.size() << std::endl;

    // ------------------------------------------------------------------------
    // Step 7: Sort by revenue DESC, o_orderdate ASC and take top 10
    // ------------------------------------------------------------------------

    // Use min-heap to maintain top-10 (min-heap keeps smallest at top)
    std::priority_queue<ResultRow, std::vector<ResultRow>, std::greater<ResultRow>> min_heap;

    for (auto& kv : global_agg) {
        ResultRow row{kv.first.l_orderkey, kv.second.revenue_sum,
                      kv.first.o_orderdate, kv.first.o_shippriority};

        if (min_heap.size() < 10) {
            min_heap.push(row);
        } else if (min_heap.top() < row) {
            // row is better than the worst in heap
            min_heap.pop();
            min_heap.push(row);
        }
    }

    // Extract top 10 and sort properly
    std::vector<ResultRow> top10;
    while (!min_heap.empty()) {
        top10.push_back(min_heap.top());
        min_heap.pop();
    }

    std::sort(top10.begin(), top10.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue; // DESC
        return a.o_orderdate < b.o_orderdate; // ASC
    });

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();

    // ------------------------------------------------------------------------
    // Step 8: Output results
    // ------------------------------------------------------------------------

    std::cout << "Result rows: " << top10.size() << std::endl;
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << elapsed << " seconds" << std::endl;

    if (!results_dir.empty()) {
        std::string output_path = results_dir + "/Q3.csv";
        std::ofstream out(output_path);
        if (!out.is_open()) {
            std::cerr << "Failed to open output file: " << output_path << std::endl;
            return 1;
        }

        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (auto& row : top10) {
            // Convert date back to string (YYYY-MM-DD)
            int32_t days = row.o_orderdate;
            int z = days + 719468;
            int era = (z >= 0 ? z : z - 146096) / 146097;
            unsigned doe = static_cast<unsigned>(z - era * 146097);
            unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
            int y = static_cast<int>(yoe) + era * 400;
            unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
            unsigned mp = (5*doy + 2)/153;
            unsigned d = doy - (153*mp+2)/5 + 1;
            unsigned m = mp + (mp < 10 ? 3 : -9);
            y += (m <= 2);

            char date_str[11];
            snprintf(date_str, sizeof(date_str), "%04d-%02u-%02u", y, m, d);

            // Convert revenue from x10000 units to dollars with 4 decimal places
            double revenue_dollars = row.revenue / 10000.0;

            out << row.l_orderkey << ","
                << std::fixed << std::setprecision(4) << revenue_dollars << ","
                << date_str << ","
                << row.o_shippriority << "\n";
        }

        out.close();
        std::cout << "Results written to: " << output_path << std::endl;
    }

    return 0;
}
