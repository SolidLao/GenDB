#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <cstring>

namespace {

// Helper function to mmap a file
void* mmap_file(const std::string& filename, size_t& file_size) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filename << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat " << filename << std::endl;
        close(fd);
        return nullptr;
    }

    file_size = sb.st_size;
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filename << std::endl;
        return nullptr;
    }

    // Hint sequential access
    madvise(ptr, file_size, MADV_SEQUENTIAL);
    return ptr;
}

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Construct file paths
    std::string lineitem_dir = gendb_dir + "/lineitem.l_shipdate.col";
    std::string lineitem_discount = gendb_dir + "/lineitem.l_discount.col";
    std::string lineitem_quantity = gendb_dir + "/lineitem.l_quantity.col";
    std::string lineitem_extendedprice = gendb_dir + "/lineitem.l_extendedprice.col";

    size_t shipdate_size = 0, discount_size = 0, quantity_size = 0, extendedprice_size = 0;

    // mmap columns
    const int32_t* shipdate_data = (const int32_t*)mmap_file(lineitem_dir, shipdate_size);
    const double* discount_data = (const double*)mmap_file(lineitem_discount, discount_size);
    const double* quantity_data = (const double*)mmap_file(lineitem_quantity, quantity_size);
    const double* extendedprice_data = (const double*)mmap_file(lineitem_extendedprice, extendedprice_size);

    if (!shipdate_data || !discount_data || !quantity_data || !extendedprice_data) {
        std::cerr << "Failed to mmap columns" << std::endl;
        return;
    }

    // Determine row count
    size_t row_count = shipdate_size / sizeof(int32_t);

    // Date range: 1994-01-01 to 1994-12-31
    // Epoch days: 8766 to 9130
    const int32_t date_lower = 8766;  // 1994-01-01
    const int32_t date_upper = 9131;  // 1995-01-01 (exclusive upper bound)

    // Discount range: 0.05 to 0.07
    const double discount_lower = 0.05;
    const double discount_upper = 0.07;

    // Quantity threshold
    const double quantity_threshold = 24.0;

    // Parallel scan with morsel-driven parallelism
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::vector<double> partial_sums(num_threads, 0.0);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_sum = 0.0;

            // Process morsels: each thread handles different morsels
            for (size_t morsel_start = t * morsel_size; morsel_start < row_count; morsel_start += num_threads * morsel_size) {
                size_t morsel_end = std::min(morsel_start + morsel_size, row_count);

                for (size_t i = morsel_start; i < morsel_end; ++i) {
                    // Apply all predicates
                    if (shipdate_data[i] >= date_lower &&
                        shipdate_data[i] < date_upper &&
                        discount_data[i] >= discount_lower &&
                        discount_data[i] <= discount_upper &&
                        quantity_data[i] < quantity_threshold) {

                        // Accumulate: SUM(l_extendedprice * l_discount)
                        local_sum += extendedprice_data[i] * discount_data[i];
                    }
                }
            }

            partial_sums[t] = local_sum;
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Final reduction
    double total_revenue = 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        total_revenue += partial_sums[t];
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Write results if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q6.csv");
        outfile << "revenue\n";
        outfile << std::fixed << std::setprecision(2) << total_revenue << "\n";
        outfile.close();
    }

    // Print summary
    std::cout << "Query returned 1 rows\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << duration.count() << " ms\n";

    // Cleanup
    munmap((void*)shipdate_data, shipdate_size);
    munmap((void*)discount_data, discount_size);
    munmap((void*)quantity_data, quantity_size);
    munmap((void*)extendedprice_data, extendedprice_size);
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
