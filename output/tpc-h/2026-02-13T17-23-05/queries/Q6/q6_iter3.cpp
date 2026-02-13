#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>

// Date constants (days since epoch 1970-01-01)
// 1994-01-01 is day 8766 (24 years * 365.25 days/year ≈ 8766)
static constexpr int32_t DATE_1994_01_01 = 8766;
static constexpr int32_t DATE_1995_01_01 = 9131;

// Utility: mmap a binary column file
template<typename T>
struct MmapColumn {
    int fd = -1;
    void* data = nullptr;
    size_t num_rows = 0;
    size_t element_size = sizeof(T);

    MmapColumn(const std::string& filepath, size_t rows) : num_rows(rows) {
        fd = open(filepath.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening " << filepath << std::endl;
            return;
        }

        size_t file_size = rows * element_size;
        data = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Error mmapping " << filepath << std::endl;
            data = nullptr;
            close(fd);
            fd = -1;
            return;
        }

        // Prefetch hints for sequential scan
        madvise(data, file_size, MADV_SEQUENTIAL);
    }

    ~MmapColumn() {
        if (data && data != MAP_FAILED) {
            munmap(data, num_rows * element_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    T* as() const {
        return static_cast<T*>(data);
    }
};

// Q6 Execution
void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Hard-coded row count for lineitem (from metadata)
    const size_t num_rows = 59986052;

    // Load columns via mmap
    MmapColumn<int32_t> col_shipdate(gendb_dir + "/lineitem.l_shipdate", num_rows);
    MmapColumn<double> col_discount(gendb_dir + "/lineitem.l_discount", num_rows);
    MmapColumn<double> col_quantity(gendb_dir + "/lineitem.l_quantity", num_rows);
    MmapColumn<double> col_extendedprice(gendb_dir + "/lineitem.l_extendedprice", num_rows);

    if (!col_shipdate.data || !col_discount.data || !col_quantity.data || !col_extendedprice.data) {
        std::cerr << "Failed to mmap columns" << std::endl;
        return;
    }

    auto* shipdate = col_shipdate.as();
    auto* discount = col_discount.as();
    auto* quantity = col_quantity.as();
    auto* extendedprice = col_extendedprice.as();

    // Determine thread count: use hardware concurrency, capped at reasonable level
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 32;
    if (num_threads > 48) num_threads = 48;  // cap at 48 to avoid resource exhaustion

    // Morsel size: ~100K rows per thread for L3 cache efficiency
    const size_t morsel_size = 100000;
    size_t num_morsels = (num_rows + morsel_size - 1) / morsel_size;

    // Thread-local aggregation: each thread accumulates its own sum
    std::vector<double> thread_sums(num_threads, 0.0);
    std::atomic<size_t> morsel_counter(0);

    // Parallel scan with morsel-driven execution
    std::atomic<unsigned int> thread_counter(0);
    auto worker = [&]() {
        unsigned int thread_id = thread_counter.fetch_add(1, std::memory_order_relaxed);
        double local_dsum = 0.0;

        while (true) {
            size_t morsel_id = morsel_counter.fetch_add(1, std::memory_order_relaxed);
            if (morsel_id >= num_morsels) break;

            size_t start_row = morsel_id * morsel_size;
            size_t end_row = std::min(start_row + morsel_size, num_rows);

            // Fused filter + aggregation loop
            for (size_t i = start_row; i < end_row; ++i) {
                // Predicates:
                // 1. l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
                // 2. l_discount BETWEEN 0.05 AND 0.07 (0.06 ± 0.01)
                // 3. l_quantity < 24

                if (shipdate[i] >= DATE_1994_01_01 && shipdate[i] < DATE_1995_01_01 &&
                    discount[i] >= 0.05 && discount[i] <= 0.07 &&
                    quantity[i] < 24.0) {
                    // Accumulate: SUM(l_extendedprice * l_discount)
                    local_dsum += extendedprice[i] * discount[i];
                }
            }
        }

        // Store result in thread-local array (no locks needed)
        thread_sums[thread_id] += local_dsum;
    };

    // Launch threads
    std::vector<std::thread> threads;
    for (unsigned int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker);
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Merge results from all threads
    double final_revenue = 0.0;
    for (double s : thread_sums) {
        final_revenue += s;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Output: write CSV result if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_file = results_dir + "/Q6.csv";
        std::ofstream out(output_file);
        if (out.is_open()) {
            out << "revenue\n";
            out << std::fixed << std::setprecision(4) << final_revenue << "\n";
            out.close();
        }
    }

    // Print summary to terminal
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Q6 Revenue: " << final_revenue << "\n";
    std::cout << "Execution time: " << elapsed_ms << " ms\n";
}

// Standalone entry point (excluded when compiled as part of final assembly)
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
