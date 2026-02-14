#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <iomanip>
#include <cmath>
#include <atomic>
#include <algorithm>

// Helper function to memory-map a file with optimized madvise
void* mmapFile(const std::string& path, size_t& size, bool prefetch = true) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat file: " << path << std::endl;
        close(fd);
        return nullptr;
    }

    size = sb.st_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << path << std::endl;
        return nullptr;
    }

    // Optimized madvise: SEQUENTIAL + WILLNEED for parallel prefetch
    int advice = MADV_SEQUENTIAL;
    if (prefetch) {
        // WILLNEED triggers async prefetch; combined with SEQUENTIAL provides good HDD performance
        madvise(ptr, size, advice);
        madvise(ptr, size, MADV_WILLNEED);
    } else {
        madvise(ptr, size, advice);
    }
    return ptr;
}

// Thread-local accumulator structure to avoid false sharing
struct ThreadLocalAccumulator {
    alignas(64) double revenue;  // Align to cache line to prevent false sharing
    size_t count;

    ThreadLocalAccumulator() : revenue(0.0), count(0) {}
};

// Q6: Forecasting Revenue Change
// Morsel-driven parallel scan with thread-local aggregation
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// WHERE l_shipdate >= 1994-01-01 AND l_shipdate < 1995-01-01
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24
void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load column data
    size_t shipdate_size = 0, discount_size = 0, quantity_size = 0, extendedprice_size = 0;

    const int32_t* l_shipdate = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size, true);
    const double* l_discount = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_discount.bin", discount_size, true);
    const double* l_quantity = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_quantity.bin", quantity_size, true);
    const double* l_extendedprice = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_extendedprice.bin", extendedprice_size, true);

    if (!l_shipdate || !l_discount || !l_quantity || !l_extendedprice) {
        std::cerr << "Failed to load column data\n";
        return;
    }

    // Calculate row count from largest file
    size_t row_count = shipdate_size / sizeof(int32_t);

    // Q6 Filter constants
    // l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
    const int32_t shipdate_lower = 8766;  // 1994-01-01 in epoch days
    const int32_t shipdate_upper = 9131;  // 1995-01-01 in epoch days
    // l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    const double discount_lower = 0.05;
    const double discount_upper = 0.07;
    // l_quantity < 24
    const double quantity_upper = 24.0;

    // Morsel-driven parallelism setup
    unsigned num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 1;

    // Adjust to avoid over-subscription; cap at 32 threads for 64-core system to balance parallelism with memory bandwidth
    if (num_threads > 32) num_threads = 32;

    // Morsel size: aim for ~50K-100K rows per thread
    size_t morsel_size = std::max(size_t(50000), row_count / (num_threads * 4));

    // Thread-local accumulators (cache-line aligned to prevent false sharing)
    std::vector<ThreadLocalAccumulator> thread_accumulators(num_threads);

    // Atomic counter for work-stealing
    std::atomic<size_t> next_morsel_idx(0);

    // Worker thread lambda
    auto worker = [&](unsigned thread_id) {
        ThreadLocalAccumulator& local_acc = thread_accumulators[thread_id];

        size_t morsel_start;
        while ((morsel_start = next_morsel_idx.fetch_add(morsel_size)) < row_count) {
            size_t morsel_end = std::min(morsel_start + morsel_size, row_count);

            // Process this morsel with predicates
            for (size_t i = morsel_start; i < morsel_end; ++i) {
                // Apply all predicates (most selective first for better branch prediction)
                if (l_shipdate[i] >= shipdate_lower &&
                    l_shipdate[i] < shipdate_upper &&
                    l_discount[i] >= discount_lower &&
                    l_discount[i] <= discount_upper &&
                    l_quantity[i] < quantity_upper) {

                    local_acc.revenue += l_extendedprice[i] * l_discount[i];
                    local_acc.count++;
                }
            }
        }
    };

    // Spawn threads
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local results into final result
    double total_revenue = 0.0;
    size_t filtered_count = 0;
    for (unsigned i = 0; i < num_threads; ++i) {
        total_revenue += thread_accumulators[i].revenue;
        filtered_count += thread_accumulators[i].count;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Write results if results_dir provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(4) << total_revenue << "\n";
        out.close();
    }

    // Print summary
    std::cout << "Query returned 1 rows\n";
    std::cout << "Execution time: " << duration.count() << " ms\n";

    // Cleanup
    munmap((void*)l_shipdate, shipdate_size);
    munmap((void*)l_discount, discount_size);
    munmap((void*)l_quantity, quantity_size);
    munmap((void*)l_extendedprice, extendedprice_size);
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
