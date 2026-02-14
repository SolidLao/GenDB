#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <thread>
#include <mutex>
#include <atomic>
#include <immintrin.h>
#include <cmath>

// Helper to mmap a file with madvise hints
void* mmapFile(const std::string& filepath, size_t& out_size, int madvise_flag = MADV_SEQUENTIAL) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        perror("fstat");
        close(fd);
        return nullptr;
    }

    out_size = sb.st_size;
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        perror("mmap");
        return nullptr;
    }

    // Apply madvise hint for sequential access optimization
    madvise(ptr, sb.st_size, madvise_flag);

    return ptr;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load columns from lineitem with MADV_SEQUENTIAL for HDD optimization
    size_t shipdate_size = 0, discount_size = 0, quantity_size = 0;
    size_t extendedprice_size = 0;

    const int32_t* shipdate_raw = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size, MADV_SEQUENTIAL);
    const double* discount_raw = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_discount.bin", discount_size, MADV_SEQUENTIAL);
    const double* quantity_raw = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_quantity.bin", quantity_size, MADV_SEQUENTIAL);
    const double* extendedprice_raw = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_extendedprice.bin", extendedprice_size, MADV_SEQUENTIAL);

    if (!shipdate_raw || !discount_raw || !quantity_raw || !extendedprice_raw) {
        std::cerr << "Failed to mmap columns" << std::endl;
        return;
    }

    // Calculate original row count
    size_t row_count = shipdate_size / sizeof(int32_t);

    // Pad arrays to 4-element SIMD boundary with sentinel values that fail all predicates
    // This eliminates the scalar tail loop entirely
    const size_t simd_batch = 4;
    size_t padded_row_count = ((row_count + simd_batch - 1) / simd_batch) * simd_batch;

    // Allocate padded arrays
    std::vector<int32_t> shipdate_padded(padded_row_count, 10000);  // Sentinel: future date, fails date range
    std::vector<double> discount_padded(padded_row_count, 0.0);     // Sentinel: out of range [0.05, 0.07]
    std::vector<double> quantity_padded(padded_row_count, 100.0);   // Sentinel: > 24, fails quantity check
    std::vector<double> extendedprice_padded(padded_row_count, 0.0);

    // Copy original data into padded arrays
    std::memcpy(shipdate_padded.data(), shipdate_raw, shipdate_size);
    std::memcpy(discount_padded.data(), discount_raw, discount_size);
    std::memcpy(quantity_padded.data(), quantity_raw, quantity_size);
    std::memcpy(extendedprice_padded.data(), extendedprice_raw, extendedprice_size);

    const int32_t* shipdate = shipdate_padded.data();
    const double* discount = discount_padded.data();
    const double* quantity = quantity_padded.data();
    const double* extendedprice = extendedprice_padded.data();

    // Date range: 1994-01-01 to 1994-12-31 (inclusive)
    // Epoch days: 1994-01-01 = 8766, 1995-01-01 = 9131
    const int32_t date_min = 8766;   // 1994-01-01
    const int32_t date_max = 9130;   // 1994-12-31 (< 9131)

    // Discount range: 0.05 to 0.07 (0.06 +/- 0.01)
    // SIMD-friendly representation
    const double discount_min = 0.05;
    const double discount_max = 0.07;

    // Quantity < 24
    const double quantity_max = 24.0;

    // Parallel scan with thread-local aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = (padded_row_count + num_threads - 1) / num_threads;

    std::vector<double> thread_sums(num_threads, 0.0);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // Optimization: Horizontal SIMD aggregation
            // Maintain 4 partial sums in SIMD registers instead of scalar
            __m256d sum_vec = _mm256_setzero_pd();

            size_t start_row = t * morsel_size;
            size_t end_row = std::min(start_row + morsel_size, padded_row_count);

            // Optimization 1: Predicate reordering by selectivity
            // Most selective predicates first (discount 9%) before less selective (shipdate 14.5%, quantity 48%)
            //
            // Optimization 2 & 3: SIMD vectorization with mask blending + horizontal aggregation
            // Use AVX2 to evaluate 4 rows at a time with blended accumulation

            size_t i = start_row;

            // Fully vectorized loop with NO scalar tail (arrays are padded to SIMD boundary)
            while (i < end_row) {
                // Load 4 values from each column for SIMD processing
                __m256d disc_vals = _mm256_loadu_pd(&discount[i]);
                __m256d qty_vals = _mm256_loadu_pd(&quantity[i]);
                // Convert 4 int32 shipdate values to doubles for comparison
                int32_t ship_vals[4] = {shipdate[i], shipdate[i+1], shipdate[i+2], shipdate[i+3]};
                __m256d shipd_vals = _mm256_set_pd(ship_vals[3], ship_vals[2], ship_vals[1], ship_vals[0]);
                __m256d price_vals = _mm256_loadu_pd(&extendedprice[i]);

                // PREDICATE 1 (MOST SELECTIVE): Discount BETWEEN 0.05 and 0.07 (9% selectivity)
                __m256d disc_gte = _mm256_cmp_pd(disc_vals, _mm256_set1_pd(discount_min), _CMP_GE_OQ);
                __m256d disc_lte = _mm256_cmp_pd(disc_vals, _mm256_set1_pd(discount_max), _CMP_LE_OQ);
                __m256d disc_mask = _mm256_and_pd(disc_gte, disc_lte);

                // PREDICATE 2: Shipdate range (14.5% selectivity)
                __m256d ship_gte = _mm256_cmp_pd(shipd_vals, _mm256_set1_pd(date_min), _CMP_GE_OQ);
                __m256d ship_lte = _mm256_cmp_pd(shipd_vals, _mm256_set1_pd(date_max), _CMP_LE_OQ);
                __m256d ship_mask = _mm256_and_pd(ship_gte, ship_lte);

                // PREDICATE 3: Quantity < 24 (48% selectivity)
                __m256d qty_mask = _mm256_cmp_pd(qty_vals, _mm256_set1_pd(quantity_max), _CMP_LT_OQ);

                // Combine all predicates with AND
                __m256d combined = _mm256_and_pd(disc_mask, _mm256_and_pd(ship_mask, qty_mask));

                // Optimization: Direct SIMD blending without scalar extraction
                // Compute price * discount first
                __m256d products = _mm256_mul_pd(price_vals, disc_vals);

                // Use blending to zero out non-matching products, then accumulate
                // combined mask has all 1s (as bit pattern) for matching rows, all 0s for non-matching
                __m256d masked_products = _mm256_and_pd(combined, products);

                // Accumulate directly into SIMD register (horizontal aggregation)
                sum_vec = _mm256_add_pd(sum_vec, masked_products);

                i += 4;
            }

            // Horizontal reduction: sum all 4 lanes of sum_vec into scalar
            // Use horizontal add to combine lanes
            __m256d sum_12 = _mm256_hadd_pd(sum_vec, sum_vec);
            // sum_12 now has [sum[0]+sum[1], sum[2]+sum[3], sum[0]+sum[1], sum[2]+sum[3]]
            __m128d sum_128 = _mm256_extractf128_pd(sum_12, 1);  // Upper 128 bits
            __m128d sum_lower = _mm256_castpd256_pd128(sum_12);   // Lower 128 bits
            __m128d final = _mm_add_pd(sum_128, sum_lower);

            // Extract scalar from lower 128-bit (first element)
            double local_sum = _mm_cvtsd_f64(final);

            thread_sums[t] = local_sum;
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Aggregate thread-local sums
    double revenue = 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        revenue += thread_sums[t];
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Write results if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/q6.csv");
        outfile << std::fixed << std::setprecision(4);
        outfile << "revenue\n";
        outfile << revenue << "\n";
        outfile.close();
    }

    // Print stats
    std::cout << "Query returned 1 rows\n";
    std::cout << "Execution time: " << duration_ms << " ms\n";

    // Cleanup: unmap original files (padded arrays are on stack/heap, no unmap needed)
    munmap((void*)shipdate_raw, shipdate_size);
    munmap((void*)discount_raw, discount_size);
    munmap((void*)quantity_raw, quantity_size);
    munmap((void*)extendedprice_raw, extendedprice_size);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q6(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
