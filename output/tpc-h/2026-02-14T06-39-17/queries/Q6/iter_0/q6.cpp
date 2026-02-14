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

// Helper to mmap a file
void* mmapFile(const std::string& filepath, size_t& out_size) {
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

    return ptr;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load columns from lineitem
    size_t shipdate_size = 0, discount_size = 0, quantity_size = 0;
    size_t extendedprice_size = 0;

    const int32_t* shipdate = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size);
    const double* discount = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_discount.bin", discount_size);
    const double* quantity = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_quantity.bin", quantity_size);
    const double* extendedprice = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_extendedprice.bin", extendedprice_size);

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

            for (size_t i = start_row; i < end_row; ++i) {
                // Apply predicates
                if (shipdate[i] >= date_min && shipdate[i] <= date_max &&
                    discount[i] >= discount_min && discount[i] <= discount_max &&
                    quantity[i] < quantity_max) {

                    // Aggregate: SUM(l_extendedprice * l_discount)
                    local_sum += extendedprice[i] * discount[i];
                }
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
