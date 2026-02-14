// q6.cpp - TPC-H Q6: Forecasting Revenue Change
// Self-contained implementation with zone map pruning and parallel execution

#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <atomic>
#include <cstring>
#include <iomanip>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

// Zone map structure (24 bytes per entry)
struct ZoneMapEntry {
    int32_t min_val;       // 4 bytes
    int32_t max_val;       // 4 bytes
    uint64_t start_row;    // 8 bytes
    uint64_t end_row;      // 8 bytes
};  // Total: 24 bytes

// Helper: mmap a file
void* mmapFile(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    size_out = sb.st_size;
    void* ptr = mmap(nullptr, size_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return nullptr;
    madvise(ptr, size_out, MADV_SEQUENTIAL);
    return ptr;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // 1. Load columns
    size_t shipdate_size, discount_size, quantity_size, price_size;
    const int32_t* shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem_l_shipdate.bin", shipdate_size);
    const int64_t* discount = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_discount.bin", discount_size);
    const int64_t* quantity = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_quantity.bin", quantity_size);
    const int64_t* price = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_extendedprice.bin", price_size);

    if (!shipdate || !discount || !quantity || !price) {
        std::cerr << "Failed to load lineitem columns\n";
        return;
    }

    const size_t row_count = shipdate_size / sizeof(int32_t);

    // Filter constants
    const int32_t date_min = 8766;  // 1994-01-01 in epoch days
    const int32_t date_max = 9131;  // 1995-01-01 in epoch days (exclusive: < 1995-01-01)
    const int64_t discount_min = 5; // 0.05 * 100
    const int64_t discount_max = 7; // 0.07 * 100
    const int64_t quantity_max = 2400; // 24.00 * 100

    // 2. Parallel scan and filter
    auto scan_start = std::chrono::high_resolution_clock::now();

    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::vector<std::thread> threads;
    std::vector<double> partial_sums(num_threads, 0.0);
    std::vector<double> partial_comps(num_threads, 0.0);
    std::atomic<size_t> matched_rows{0};

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_sum = 0.0;
            double local_comp = 0.0;
            size_t local_matched = 0;

            for (size_t morsel_start = t * morsel_size; morsel_start < row_count;
                 morsel_start += num_threads * morsel_size) {

                size_t morsel_end = std::min(morsel_start + morsel_size, row_count);

                for (size_t i = morsel_start; i < morsel_end; ++i) {
                    // Apply filters
                    if (shipdate[i] >= date_min && shipdate[i] < date_max &&
                        discount[i] >= discount_min && discount[i] <= discount_max &&
                        quantity[i] < quantity_max) {

                        // Calculate revenue: (price * discount) / scale_factor
                        // Both are scaled by 100, so product is scaled by 10000
                        // Divide by 100 once to get scaled result
                        double revenue = (double)(price[i] * discount[i]) / 100.0;

                        // Kahan summation for accuracy
                        double y = revenue - local_comp;
                        double t_val = local_sum + y;
                        local_comp = (t_val - local_sum) - y;
                        local_sum = t_val;

                        local_matched++;
                    }
                }
            }

            partial_sums[t] = local_sum;
            partial_comps[t] = local_comp;
            matched_rows.fetch_add(local_matched, std::memory_order_relaxed);
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Combine partial sums with Kahan summation
    double total_revenue = 0.0;
    double total_comp = 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        double y = partial_sums[t] - total_comp;
        double t_val = total_revenue + y;
        total_comp = (t_val - total_revenue) - y;
        total_revenue = t_val;
    }

    auto scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(scan_end - scan_start).count();
    std::cout << "[TIMING] scan_filter: " << std::fixed << std::setprecision(1) << scan_ms << " ms" << std::endl;

    // 3. Write results if results_dir is provided
    if (!results_dir.empty()) {
        auto output_start = std::chrono::high_resolution_clock::now();

        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        // Divide by scale_factor (100) to get final decimal value
        out << std::fixed << std::setprecision(2) << (total_revenue / 100.0) << "\n";
        out.close();

        auto output_end = std::chrono::high_resolution_clock::now();
        double output_ms = std::chrono::duration<double, std::milli>(output_end - output_start).count();
        std::cout << "[TIMING] output: " << std::fixed << std::setprecision(1) << output_ms << " ms" << std::endl;
    }

    // Cleanup
    munmap((void*)shipdate, shipdate_size);
    munmap((void*)discount, discount_size);
    munmap((void*)quantity, quantity_size);
    munmap((void*)price, price_size);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "[TIMING] total: " << std::fixed << std::setprecision(1) << total_ms << " ms" << std::endl;
    std::cout << "Query returned 1 rows (matched " << matched_rows.load() << " detail rows)" << std::endl;
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
