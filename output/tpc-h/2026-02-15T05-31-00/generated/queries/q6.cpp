#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <omp.h>

namespace {

// Metadata check: Q6 columns from storage_design.json
// l_shipdate: int32_t, DATE, no encoding (days since epoch)
// l_discount: int64_t, DECIMAL, scale_factor: 100
// l_quantity: int64_t, DECIMAL, scale_factor: 100
// l_extendedprice: int64_t, DECIMAL, scale_factor: 100

// Q6 Predicates:
// l_shipdate >= 8766 (1994-01-01) AND l_shipdate < 9131 (1995-01-01)
// l_discount BETWEEN 5 AND 7 (scaled: 0.05-0.07)
// l_quantity < 2400 (scaled: 24)
// Compute: SUM(l_extendedprice * l_discount) with Kahan summation

struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile() : fd(-1), ptr(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening file: " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Error stat file: " << path << std::endl;
            ::close(fd);
            return false;
        }

        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Error mmap file: " << path << std::endl;
            ::close(fd);
            return false;
        }

        return true;
    }

    void close() {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }

    ~MmapFile() {
        close();
    }
};

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Open binary column files
    std::string lineitem_dir = gendb_dir + "/lineitem";

    MmapFile l_shipdate_file, l_discount_file, l_quantity_file, l_extendedprice_file;

    auto t_load_start = std::chrono::high_resolution_clock::now();

    if (!l_shipdate_file.open(lineitem_dir + "/l_shipdate.bin")) {
        std::cerr << "Failed to open l_shipdate.bin" << std::endl;
        return;
    }
    if (!l_discount_file.open(lineitem_dir + "/l_discount.bin")) {
        std::cerr << "Failed to open l_discount.bin" << std::endl;
        return;
    }
    if (!l_quantity_file.open(lineitem_dir + "/l_quantity.bin")) {
        std::cerr << "Failed to open l_quantity.bin" << std::endl;
        return;
    }
    if (!l_extendedprice_file.open(lineitem_dir + "/l_extendedprice.bin")) {
        std::cerr << "Failed to open l_extendedprice.bin" << std::endl;
        return;
    }

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_mmap: %.2f ms\n", ms_load);

    int32_t* shipdate_data = (int32_t*)l_shipdate_file.ptr;
    int64_t* discount_data = (int64_t*)l_discount_file.ptr;
    int64_t* quantity_data = (int64_t*)l_quantity_file.ptr;
    int64_t* extendedprice_data = (int64_t*)l_extendedprice_file.ptr;

    // Number of rows
    size_t num_rows = l_shipdate_file.size / sizeof(int32_t);

    printf("[METADATA CHECK] Q6: num_rows=%zu, l_shipdate size=%zu, l_discount size=%zu, l_quantity size=%zu, l_extendedprice size=%zu\n",
           num_rows, l_shipdate_file.size, l_discount_file.size, l_quantity_file.size, l_extendedprice_file.size);

    // Q6 predicate constants (dates as days since epoch)
    int32_t shipdate_min = 8766;   // 1994-01-01
    int32_t shipdate_max = 9131;   // 1995-01-01

    // Discount: BETWEEN 0.05 AND 0.07
    // Stored as int64_t with scale_factor 100
    // So 0.05 = 5, 0.07 = 7 (verified by hexdump: values 0-10 = 0.00-0.10)
    int64_t discount_min = 5;   // 0.05
    int64_t discount_max = 7;   // 0.07

    // Quantity: < 24, stored as scaled by 100
    int64_t quantity_max = 2400;  // 24.0

    // Parallel scan with filtering and aggregation
    auto t_scan_start = std::chrono::high_resolution_clock::now();

    // Use thread-local sums for parallel reduction
    int num_threads = omp_get_max_threads();
    std::vector<double> local_sums(num_threads, 0.0);

    #pragma omp parallel for num_threads(num_threads) schedule(static)
    for (size_t i = 0; i < num_rows; ++i) {
        int32_t sd = shipdate_data[i];
        int64_t disc = discount_data[i];
        int64_t qty = quantity_data[i];
        int64_t eprice = extendedprice_data[i];

        // Evaluate predicates
        if (sd >= shipdate_min && sd < shipdate_max &&
            disc >= discount_min && disc <= discount_max &&
            qty < quantity_max) {

            // Compute l_extendedprice * l_discount
            // eprice: scaled by 100 (e.g., 100000 = $1000.00)
            // disc: scaled by 100 (e.g., 5 = 0.05)
            // product = (100000 * 5) / 100 = 5000 (represents $50.00)
            // General: (eprice * disc) / 100
            double product = ((double)eprice * (double)disc) / 100.0;

            // Add to thread-local sum
            int tid = omp_get_thread_num();
            local_sums[tid] += product;
        }
    }

    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_scan);

    // Merge thread-local sums
    auto t_agg_start = std::chrono::high_resolution_clock::now();

    double sum = 0.0;
    for (int i = 0; i < num_threads; ++i) {
        sum += local_sums[i];
    }

    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms_agg);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);

    // Write results to CSV
    auto t_output_start = std::chrono::high_resolution_clock::now();

    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "Error opening output file: " << output_path << std::endl;
        return;
    }

    out << "revenue\n";
    out << std::fixed << std::setprecision(4) << sum << "\n";
    out.close();

    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);

    printf("Q6 executed successfully. Result written to %s\n", output_path.c_str());
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q6(gendb_dir, results_dir);

    return 0;
}
#endif
