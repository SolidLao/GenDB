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

    const int32_t* shipdate = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size, MADV_SEQUENTIAL);
    const double* discount = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_discount.bin", discount_size, MADV_SEQUENTIAL);
    const double* quantity = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_quantity.bin", quantity_size, MADV_SEQUENTIAL);
    const double* extendedprice = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_extendedprice.bin", extendedprice_size, MADV_SEQUENTIAL);

    if (!shipdate || !discount || !quantity || !extendedprice) {
        std::cerr << "Failed to mmap columns" << std::endl;
        return;
    }

    // Calculate row count
    size_t row_count = shipdate_size / sizeof(int32_t);

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
    const size_t morsel_size = (row_count + num_threads - 1) / num_threads;

    std::vector<double> thread_sums(num_threads, 0.0);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_sum = 0.0;
            size_t start_row = t * morsel_size;
            size_t end_row = std::min(start_row + morsel_size, row_count);

            // Optimization 1: Predicate reordering by selectivity
            // Most selective predicates first (discount 9%) before less selective (shipdate 14.5%, quantity 48%)
            // This uses short-circuit AND: filter early, reducing work on later predicates
            //
            // Optimization 2: SIMD vectorization for predicate evaluation
            // Use AVX2 to evaluate 4 rows at a time, processing discount/shipdate/quantity comparisons in parallel

            size_t i = start_row;
            const size_t simd_batch = 4;  // Process 4 rows per SIMD iteration

            // Vectorized batch loop (AVX2 can process 4 doubles at a time)
            size_t batch_end = start_row + ((end_row - start_row) / simd_batch) * simd_batch;
            while (i < batch_end) {
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

                // For each row matching all predicates, add price*discount to sum
                // Extract boolean masks and process each row individually
                double d_combined[4];
                double d_prices[4];
                double d_discounts[4];
                _mm256_storeu_pd(d_combined, combined);
                _mm256_storeu_pd(d_prices, price_vals);
                _mm256_storeu_pd(d_discounts, disc_vals);

                for (int j = 0; j < 4; ++j) {
                    // A negative double (mask = all 1s) or positive double (mask = all 0s)
                    // Check if combined[j] is non-zero (true) using != 0
                    if (d_combined[j] != 0.0) {
                        local_sum += d_prices[j] * d_discounts[j];
                    }
                }

                i += simd_batch;
            }

            // Scalar tail: handle remaining rows
            while (i < end_row) {
                // Predicate reordering: discount first (most selective), then shipdate, then quantity
                if (discount[i] >= discount_min && discount[i] <= discount_max &&
                    shipdate[i] >= date_min && shipdate[i] <= date_max &&
                    quantity[i] < quantity_max) {

                    local_sum += extendedprice[i] * discount[i];
                }
                i++;
            }

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

    // Cleanup
    munmap((void*)shipdate, shipdate_size);
    munmap((void*)discount, discount_size);
    munmap((void*)quantity, quantity_size);
    munmap((void*)extendedprice, extendedprice_size);
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
