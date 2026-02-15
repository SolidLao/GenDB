#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

// Helper function to mmap a file as read-only
template <typename T>
const T* mmap_file(const std::string& path, size_t& num_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Failed to stat file: " << path << std::endl;
        close(fd);
        return nullptr;
    }
    num_elements = st.st_size / sizeof(T);
    const T* data = (const T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << path << std::endl;
        close(fd);
        return nullptr;
    }
    close(fd);
    return data;
}

void run_Q6(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load binary columns
#ifdef GENDB_PROFILE
    auto t_mmap_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_rows = 0;
    std::string shipdate_path = gendb_dir + "/lineitem/l_shipdate.bin";
    std::string discount_path = gendb_dir + "/lineitem/l_discount.bin";
    std::string quantity_path = gendb_dir + "/lineitem/l_quantity.bin";
    std::string extendedprice_path = gendb_dir + "/lineitem/l_extendedprice.bin";

    const int32_t* l_shipdate = mmap_file<int32_t>(shipdate_path, num_rows);
    const int64_t* l_discount = mmap_file<int64_t>(discount_path, num_rows);
    const int64_t* l_quantity = mmap_file<int64_t>(quantity_path, num_rows);
    const int64_t* l_extendedprice = mmap_file<int64_t>(extendedprice_path, num_rows);

    if (!l_shipdate || !l_discount || !l_quantity || !l_extendedprice) {
        std::cerr << "Failed to load required columns" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_mmap_end = std::chrono::high_resolution_clock::now();
    double mmap_ms = std::chrono::duration<double, std::milli>(t_mmap_end - t_mmap_start).count();
    printf("[TIMING] mmap_columns: %.2f ms\n", mmap_ms);
#endif

    // Filter and aggregate
    // Predicates:
    // - l_shipdate >= 8766 AND l_shipdate < 9131
    // - l_discount BETWEEN 5 AND 7 (scaled by 100)
    // - l_quantity < 2400 (scaled by 100)
    // Aggregate: SUM(l_extendedprice * l_discount)

    const int32_t SHIPDATE_MIN = 8766;  // 1994-01-01
    const int32_t SHIPDATE_MAX = 9131;  // 1995-01-01
    const int64_t DISCOUNT_MIN = 5;     // 0.05 * 100
    const int64_t DISCOUNT_MAX = 7;     // 0.07 * 100
    const int64_t QUANTITY_MAX = 2400;  // 24 * 100

#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Parallel aggregation: thread-local sums
    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_sums(num_threads, 0);

#pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        int64_t local_sum = 0;

#pragma omp for nowait schedule(dynamic, 100000)
        for (size_t i = 0; i < num_rows; i++) {
            // Evaluate predicates: all must be true for a match
            int32_t ship = l_shipdate[i];
            int64_t disc = l_discount[i];
            int64_t qty = l_quantity[i];

            // Check all predicates
            bool pred_ship = (ship >= SHIPDATE_MIN) && (ship < SHIPDATE_MAX);
            bool pred_disc = (disc >= DISCOUNT_MIN) && (disc <= DISCOUNT_MAX);
            bool pred_qty = (qty < QUANTITY_MAX);

            if (pred_ship && pred_disc && pred_qty) {
                // Multiply extendedprice * discount (both scaled by 100)
                // Result will be scaled by 10000 (100 * 100)
                // Keep as int64_t to accumulate, scale down later
                int64_t product = l_extendedprice[i] * disc;
                local_sum += product;
            }
        }

        thread_sums[thread_id] = local_sum;
    }

#ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_total_ms =
        std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", filter_total_ms);
#endif

    // Merge thread-local results
    int64_t total_sum = 0;
    for (int i = 0; i < num_threads; i++) {
        total_sum += thread_sums[i];
    }

    // Scale down: product is scaled by 10000 (100 * 100), convert to 2 decimal places (divide by 10000)
    // Result needs to be in monetary format: revenue = total_sum / 10000
    double revenue = (double)total_sum / 10000.0;

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    // Write results to CSV
    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    out << "revenue\n";
    out.precision(4);
    out << std::fixed << revenue << "\n";
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Cleanup
    munmap((void*)l_shipdate, num_rows * sizeof(int32_t));
    munmap((void*)l_discount, num_rows * sizeof(int64_t));
    munmap((void*)l_quantity, num_rows * sizeof(int64_t));
    munmap((void*)l_extendedprice, num_rows * sizeof(int64_t));
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q6(gendb_dir, results_dir);
    return 0;
}
#endif
