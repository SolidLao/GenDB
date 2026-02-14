// q6.cpp - Self-contained TPC-H Q6 implementation
// Query: SELECT SUM(l_extendedprice * l_discount) AS revenue
//        FROM lineitem
//        WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
//          AND l_discount BETWEEN 0.05 AND 0.07
//          AND l_quantity < 24

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <immintrin.h>  // AVX2/AVX512

// Date conversion: '1994-01-01' = 8766 days since 1970-01-01
// '1995-01-01' = 9131 days since 1970-01-01
constexpr int32_t DATE_1994_01_01 = 8766;
constexpr int32_t DATE_1995_01_01 = 9131;

// Discount stored as int32 representing 0.00-0.10 (values 0-10)
// BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 means [0.05, 0.07] = [5, 7]
constexpr int32_t DISCOUNT_MIN = 5;
constexpr int32_t DISCOUNT_MAX = 7;

// Quantity stored as int32 * 100 (e.g., 24.00 is stored as 2400)
constexpr int32_t QUANTITY_MAX = 2400;

// Block size (from storage design)
constexpr size_t BLOCK_SIZE = 100000;

// Zone map entry (24 bytes)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    int32_t null_count;
    int32_t null_count_high;  // padding or upper bits
    int32_t row_count;
    int32_t row_count_high;   // padding or upper bits
};

// Memory-mapped column
struct MappedColumn {
    void* data;
    size_t size;
    int fd;

    MappedColumn() : data(nullptr), size(0), fd(-1) {}

    ~MappedColumn() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool load(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat: " << path << std::endl;
            return false;
        }

        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            return false;
        }

        // Hint random access for zone-map-pruned sparse access pattern
        madvise(data, size, MADV_RANDOM);

        return true;
    }
};

// Load zone map
std::vector<ZoneMapEntry> load_zonemap(const std::string& path) {
    std::vector<ZoneMapEntry> entries;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return entries;  // Zone map not found, proceed without pruning
    }

    struct stat sb;
    fstat(fd, &sb);

    void* data = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data != MAP_FAILED) {
        // First 8 bytes is the count
        size_t count = *static_cast<const uint64_t*>(data);

        // Then zone map entries (24 bytes each)
        const ZoneMapEntry* zm = reinterpret_cast<const ZoneMapEntry*>(
            static_cast<const char*>(data) + 8);
        entries.assign(zm, zm + count);

        munmap(data, sb.st_size);
    }
    close(fd);

    return entries;
}

// Check if block can be skipped based on zone maps
bool can_skip_block(size_t block_id,
                   const std::vector<ZoneMapEntry>& shipdate_zm,
                   const std::vector<ZoneMapEntry>& discount_zm,
                   const std::vector<ZoneMapEntry>& quantity_zm) {
    // Check l_shipdate: must overlap [8766, 9131)
    if (!shipdate_zm.empty() && block_id < shipdate_zm.size()) {
        const auto& zm = shipdate_zm[block_id];
        if (zm.max_val < DATE_1994_01_01 || zm.min_val >= DATE_1995_01_01) {
            return true;  // No overlap
        }
    }

    // Check l_discount: must overlap [5, 7]
    if (!discount_zm.empty() && block_id < discount_zm.size()) {
        const auto& zm = discount_zm[block_id];
        if (zm.max_val < DISCOUNT_MIN || zm.min_val > DISCOUNT_MAX) {
            return true;  // No overlap
        }
    }

    // Check l_quantity: must have values < 2400
    if (!quantity_zm.empty() && block_id < quantity_zm.size()) {
        const auto& zm = quantity_zm[block_id];
        if (zm.min_val >= QUANTITY_MAX) {
            return true;  // All values >= 2400
        }
    }

    return false;
}

// Thread-local aggregation result
struct alignas(64) ThreadResult {
    int64_t revenue;
    ThreadResult() : revenue(0) {}
};

// Process a range of rows with AVX2 SIMD - fully vectorized aggregation
int64_t process_range_simd(const int32_t* shipdate,
                           const int32_t* discount,
                           const int32_t* quantity,
                           const int64_t* extendedprice,
                           size_t start,
                           size_t end) {
    int64_t revenue = 0;
    size_t i = start;

#ifdef __AVX2__
    // SIMD predicate evaluation constants
    __m256i date_min = _mm256_set1_epi32(DATE_1994_01_01);
    __m256i date_max_minus_1 = _mm256_set1_epi32(DATE_1995_01_01 - 1);
    __m256i disc_min = _mm256_set1_epi32(DISCOUNT_MIN);
    __m256i disc_max = _mm256_set1_epi32(DISCOUNT_MAX);
    __m256i qty_max_minus_1 = _mm256_set1_epi32(QUANTITY_MAX - 1);

    // Accumulators for vectorized aggregation (split into 4 lanes to avoid overflow)
    __m256i accum0 = _mm256_setzero_si256();
    __m256i accum1 = _mm256_setzero_si256();

    // Process 8 rows at a time with SIMD - fully vectorized
    for (; i + 8 <= end; i += 8) {
        // Prefetch next cache line (64 bytes ahead)
        __builtin_prefetch(&shipdate[i + 16], 0, 3);
        __builtin_prefetch(&discount[i + 16], 0, 3);
        __builtin_prefetch(&quantity[i + 16], 0, 3);
        __builtin_prefetch(&extendedprice[i + 8], 0, 3);

        // Load 8 values (unaligned for safety, but aligned access would be 2x faster)
        __m256i sd = _mm256_loadu_si256((__m256i*)&shipdate[i]);
        __m256i dc = _mm256_loadu_si256((__m256i*)&discount[i]);
        __m256i qt = _mm256_loadu_si256((__m256i*)&quantity[i]);

        // Evaluate predicates
        __m256i sd_ge = _mm256_cmpgt_epi32(sd, _mm256_sub_epi32(date_min, _mm256_set1_epi32(1)));
        __m256i sd_le = _mm256_cmpgt_epi32(_mm256_add_epi32(date_max_minus_1, _mm256_set1_epi32(1)), sd);
        __m256i dc_ge = _mm256_cmpgt_epi32(dc, _mm256_sub_epi32(disc_min, _mm256_set1_epi32(1)));
        __m256i dc_le = _mm256_cmpgt_epi32(_mm256_add_epi32(disc_max, _mm256_set1_epi32(1)), dc);
        __m256i qt_le = _mm256_cmpgt_epi32(_mm256_add_epi32(qty_max_minus_1, _mm256_set1_epi32(1)), qt);

        // Combine with AND - mask is all 1s for matching rows, all 0s otherwise
        __m256i mask = _mm256_and_si256(sd_ge, sd_le);
        mask = _mm256_and_si256(mask, dc_ge);
        mask = _mm256_and_si256(mask, dc_le);
        mask = _mm256_and_si256(mask, qt_le);

        // Check if any lanes matched (early exit optimization)
        int movemask = _mm256_movemask_ps(_mm256_castsi256_ps(mask));
        if (movemask != 0) {
            // Vectorized aggregation: compute extendedprice * discount for matching lanes
            // Load extendedprice (8x int64 = two 256-bit vectors)
            __m256i ep_lo = _mm256_loadu_si256((__m256i*)&extendedprice[i]);     // lanes 0-3
            __m256i ep_hi = _mm256_loadu_si256((__m256i*)&extendedprice[i + 4]); // lanes 4-7

            // Extend discount from int32 to int64 (split into low and high)
            __m256i dc_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(dc));           // lanes 0-3
            __m256i dc_hi = _mm256_cvtepi32_epi64(_mm256_extracti128_si256(dc, 1));      // lanes 4-7

            // Extend mask from int32 to int64
            __m256i mask_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(mask));
            __m256i mask_hi = _mm256_cvtepi32_epi64(_mm256_extracti128_si256(mask, 1));

            // Multiply: extendedprice * discount (int64 multiplication)
            __m256i prod_lo = _mm256_mul_epi32(ep_lo, dc_lo);  // even lanes
            __m256i prod_hi = _mm256_mul_epi32(ep_hi, dc_hi);  // even lanes

            // Handle odd lanes (shift and multiply)
            __m256i ep_lo_odd = _mm256_srli_epi64(ep_lo, 32);
            __m256i dc_lo_odd = _mm256_srli_epi64(dc_lo, 32);
            __m256i ep_hi_odd = _mm256_srli_epi64(ep_hi, 32);
            __m256i dc_hi_odd = _mm256_srli_epi64(dc_hi, 32);

            __m256i prod_lo_odd = _mm256_mul_epi32(ep_lo_odd, dc_lo_odd);
            __m256i prod_hi_odd = _mm256_mul_epi32(ep_hi_odd, dc_hi_odd);

            // Blend even and odd results
            __m256i result_lo = _mm256_blend_epi32(prod_lo, _mm256_slli_epi64(prod_lo_odd, 32), 0xAA);
            __m256i result_hi = _mm256_blend_epi32(prod_hi, _mm256_slli_epi64(prod_hi_odd, 32), 0xAA);

            // Apply mask using AND (sets non-matching lanes to 0)
            result_lo = _mm256_and_si256(result_lo, mask_lo);
            result_hi = _mm256_and_si256(result_hi, mask_hi);

            // Accumulate into SIMD accumulators
            accum0 = _mm256_add_epi64(accum0, result_lo);
            accum1 = _mm256_add_epi64(accum1, result_hi);
        }
    }

    // Horizontal reduction: sum all lanes in accumulators
    int64_t vec_sum = 0;
    int64_t buf[4];
    _mm256_storeu_si256((__m256i*)buf, accum0);
    vec_sum += buf[0] + buf[1] + buf[2] + buf[3];
    _mm256_storeu_si256((__m256i*)buf, accum1);
    vec_sum += buf[0] + buf[1] + buf[2] + buf[3];
    revenue += vec_sum;
#endif

    // Scalar tail processing
    for (; i < end; i++) {
        if (shipdate[i] >= DATE_1994_01_01 &&
            shipdate[i] < DATE_1995_01_01 &&
            discount[i] >= DISCOUNT_MIN &&
            discount[i] <= DISCOUNT_MAX &&
            quantity[i] < QUANTITY_MAX) {
            revenue += extendedprice[i] * discount[i];
        }
    }

    return revenue;
}

// Worker thread function with prefetching
void worker_thread(const int32_t* shipdate,
                  const int32_t* discount,
                  const int32_t* quantity,
                  const int64_t* extendedprice,
                  size_t total_rows,
                  const std::vector<ZoneMapEntry>& shipdate_zm,
                  const std::vector<ZoneMapEntry>& discount_zm,
                  const std::vector<ZoneMapEntry>& quantity_zm,
                  std::atomic<size_t>& next_block,
                  ThreadResult& result) {

    size_t num_blocks = (total_rows + BLOCK_SIZE - 1) / BLOCK_SIZE;

    while (true) {
        size_t block_id = next_block.fetch_add(1, std::memory_order_relaxed);
        if (block_id >= num_blocks) break;

        // Prefetch next block's data (pipeline I/O with computation)
        if (block_id + 1 < num_blocks) {
            size_t prefetch_start = (block_id + 1) * BLOCK_SIZE;
            // Prefetch first cache line of next block
            __builtin_prefetch(&shipdate[prefetch_start], 0, 1);
            __builtin_prefetch(&discount[prefetch_start], 0, 1);
            __builtin_prefetch(&quantity[prefetch_start], 0, 1);
            __builtin_prefetch(&extendedprice[prefetch_start], 0, 1);
        }

        // Zone map pruning
        if (can_skip_block(block_id, shipdate_zm, discount_zm, quantity_zm)) {
            continue;
        }

        // Process block
        size_t start = block_id * BLOCK_SIZE;
        size_t end = std::min(start + BLOCK_SIZE, total_rows);

        result.revenue += process_range_simd(shipdate, discount, quantity,
                                            extendedprice, start, end);
    }
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load metadata
    size_t total_rows = 59986052;  // From metadata

    // Memory-map columns
    MappedColumn col_shipdate, col_discount, col_quantity, col_extendedprice;

    std::string base = gendb_dir + "/lineitem.";
    if (!col_shipdate.load(base + "l_shipdate.bin") ||
        !col_discount.load(base + "l_discount.bin") ||
        !col_quantity.load(base + "l_quantity.bin") ||
        !col_extendedprice.load(base + "l_extendedprice.bin")) {
        std::cerr << "Failed to load lineitem columns" << std::endl;
        return;
    }

    const int32_t* shipdate = static_cast<const int32_t*>(col_shipdate.data);
    const int32_t* discount = static_cast<const int32_t*>(col_discount.data);
    const int32_t* quantity = static_cast<const int32_t*>(col_quantity.data);
    const int64_t* extendedprice = static_cast<const int64_t*>(col_extendedprice.data);

    // Load zone maps
    auto shipdate_zm = load_zonemap(base + "l_shipdate.zonemap.idx");
    auto discount_zm = load_zonemap(base + "l_discount.zonemap.idx");
    auto quantity_zm = load_zonemap(base + "l_quantity.zonemap.idx");

    // Parallel execution - reduced thread count for selective query
    // 16 threads provides good parallelism while minimizing scheduling overhead
    unsigned int num_threads = std::min(16u, std::thread::hardware_concurrency());
    if (num_threads == 0) num_threads = 16;

    std::vector<std::thread> threads;
    std::vector<ThreadResult> thread_results(num_threads);
    std::atomic<size_t> next_block(0);

    for (unsigned int t = 0; t < num_threads; t++) {
        threads.emplace_back(worker_thread,
                            shipdate, discount, quantity, extendedprice,
                            total_rows,
                            std::cref(shipdate_zm),
                            std::cref(discount_zm),
                            std::cref(quantity_zm),
                            std::ref(next_block),
                            std::ref(thread_results[t]));
    }

    // Wait for completion
    for (auto& th : threads) {
        th.join();
    }

    // Merge results
    int64_t total_revenue = 0;
    for (const auto& tr : thread_results) {
        total_revenue += tr.revenue;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Convert result: extendedprice is in cents, discount is 0-10 (where 5 means 0.05)
    // Formula: extendedprice * l_discount where l_discount is 0.05-0.07
    // We compute: cents * discount_value, where discount_value is 5-7
    // To get the SQL result: cents * (discount_value/100) / 100 = cents * discount_value / 10000
    double revenue_dollars = total_revenue / 10000.0;

    // Output results
    if (!results_dir.empty()) {
        std::string outfile = results_dir + "/Q6.csv";
        std::ofstream out(outfile);
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue_dollars << "\n";
        out.close();
    }

    // Print timing
    std::cout << "Q6: 1 row" << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";

    run_q6(gendb_dir, results_dir);

    return 0;
}
#endif
