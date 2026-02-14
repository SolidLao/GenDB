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

// Helper function to memory-map a file
void* mmapFile(const std::string& path, size_t& size) {
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

    // Hint for sequential access
    madvise(ptr, size, MADV_SEQUENTIAL);
    return ptr;
}

// Q6: Forecasting Revenue Change
// Single-table scan with range predicates on lineitem
// SELECT SUM(l_extendedprice * l_discount) AS revenue
// WHERE l_shipdate >= 1994-01-01 AND l_shipdate < 1995-01-01
//   AND l_discount BETWEEN 0.05 AND 0.07
//   AND l_quantity < 24
void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load column data
    size_t shipdate_size = 0, discount_size = 0, quantity_size = 0, extendedprice_size = 0;

    const int32_t* l_shipdate = (const int32_t*)mmapFile(
        gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size);
    const double* l_discount = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_discount.bin", discount_size);
    const double* l_quantity = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_quantity.bin", quantity_size);
    const double* l_extendedprice = (const double*)mmapFile(
        gendb_dir + "/lineitem/l_extendedprice.bin", extendedprice_size);

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

    // Single-threaded scan (parallel morsels would add complexity with shared accumulator)
    // For a single aggregation, a simple sequential scan is often fastest
    double total_revenue = 0.0;
    size_t filtered_count = 0;

    for (size_t i = 0; i < row_count; ++i) {
        // Apply all predicates
        if (l_shipdate[i] >= shipdate_lower &&
            l_shipdate[i] < shipdate_upper &&
            l_discount[i] >= discount_lower &&
            l_discount[i] <= discount_upper &&
            l_quantity[i] < quantity_upper) {

            total_revenue += l_extendedprice[i] * l_discount[i];
            filtered_count++;
        }
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
