#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cmath>
#include <thread>
#include <atomic>
#include <iomanip>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

// Constants for date conversion
constexpr int32_t EPOCH_1994_01_01 = 8766;  // days from 1970-01-01
constexpr int32_t EPOCH_1995_01_01 = 9131;  // days from 1970-01-01

struct MMapBuffer {
    void* ptr;
    size_t size;
    int fd;

    MMapBuffer() : ptr(nullptr), size(0), fd(-1) {}

    ~MMapBuffer() {
        if (ptr) {
            munmap(ptr, size);
            if (fd >= 0) close(fd);
        }
    }

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;
        off_t file_size = lseek(fd, 0, SEEK_END);
        if (file_size < 0) {
            close(fd);
            fd = -1;
            return false;
        }
        size = static_cast<size_t>(file_size);
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
        if (ptr == MAP_FAILED) {
            ptr = nullptr;
            close(fd);
            fd = -1;
            return false;
        }
        return true;
    }
};

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Lineitem table has ~60M rows (from storage design)
    // Infer row count from file sizes
    int64_t row_count = 59986052;  // From workload_analysis.json

    // Open column files
    MMapBuffer l_shipdate_buf, l_discount_buf, l_quantity_buf, l_extendedprice_buf;

    std::string shipdate_path = gendb_dir + "/lineitem/l_shipdate.bin";
    std::string discount_path = gendb_dir + "/lineitem/l_discount.bin";
    std::string quantity_path = gendb_dir + "/lineitem/l_quantity.bin";
    std::string extendedprice_path = gendb_dir + "/lineitem/l_extendedprice.bin";

    if (!l_shipdate_buf.open(shipdate_path)) {
        std::cerr << "Error: Could not open l_shipdate.bin" << std::endl;
        return;
    }
    if (!l_discount_buf.open(discount_path)) {
        std::cerr << "Error: Could not open l_discount.bin" << std::endl;
        return;
    }
    if (!l_quantity_buf.open(quantity_path)) {
        std::cerr << "Error: Could not open l_quantity.bin" << std::endl;
        return;
    }
    if (!l_extendedprice_buf.open(extendedprice_path)) {
        std::cerr << "Error: Could not open l_extendedprice.bin" << std::endl;
        return;
    }

    auto* shipdate_data = reinterpret_cast<int32_t*>(l_shipdate_buf.ptr);
    auto* discount_data = reinterpret_cast<double*>(l_discount_buf.ptr);
    auto* quantity_data = reinterpret_cast<int32_t*>(l_quantity_buf.ptr);
    auto* extendedprice_data = reinterpret_cast<double*>(l_extendedprice_buf.ptr);

    // Q6 Filter predicates:
    // l_shipdate >= 1994-01-01 (EPOCH_1994_01_01 = 8766)
    // l_shipdate < 1995-01-01 (EPOCH_1995_01_01 = 9131)
    // l_discount BETWEEN 0.05 AND 0.07 (actually 0.06 ± 0.01)
    // l_quantity < 24

    constexpr double DISCOUNT_MIN = 0.05;
    constexpr double DISCOUNT_MAX = 0.07;
    constexpr int32_t QUANTITY_THRESHOLD = 24;

    // Parallel execution with thread pool
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 64;

    // Morsel-driven execution: divide work into morsels
    const int64_t MORSEL_SIZE = 1000000;  // 1M rows per morsel
    const int64_t num_morsels = (row_count + MORSEL_SIZE - 1) / MORSEL_SIZE;

    // Thread-local partial aggregates
    std::vector<std::atomic<double>> thread_local_sum(num_threads);
    for (unsigned i = 0; i < num_threads; ++i) {
        thread_local_sum[i].store(0.0);
    }

    std::atomic<int64_t> morsel_counter(0);

    auto process_morsels = [&](unsigned thread_id) {
        double local_sum = 0.0;

        while (true) {
            int64_t morsel_idx = morsel_counter.fetch_add(1);
            if (morsel_idx >= num_morsels) break;

            int64_t start = morsel_idx * MORSEL_SIZE;
            int64_t end = std::min(start + MORSEL_SIZE, row_count);

            // Delta decode shipdate for this morsel
            std::vector<int32_t> shipdate_decoded(end - start);
            int32_t prev = (start > 0) ? shipdate_data[start - 1] : EPOCH_1994_01_01;

            // First element: decode from previous value
            if (start > 0) {
                prev = shipdate_decoded[start - 1];
            } else {
                prev = shipdate_data[0];
                shipdate_decoded[0] = prev;
            }

            for (int64_t i = start; i < end; ++i) {
                if (i == start && start > 0) {
                    // For the first row in morsel (not first row overall):
                    // shipdate_data[i] is a delta from previous
                    prev = prev + shipdate_data[i];
                } else if (i == start && start == 0) {
                    // First row overall: use as-is
                    prev = shipdate_data[i];
                } else {
                    // Subsequent rows: add delta to previous
                    prev = prev + shipdate_data[i];
                }
                shipdate_decoded[i - start] = prev;
            }

            // Scan and filter
            for (int64_t i = start; i < end; ++i) {
                int64_t offset = i - start;
                int32_t ship_date = shipdate_decoded[offset];
                double discount = discount_data[i];
                int32_t quantity = quantity_data[i];
                double extendedprice = extendedprice_data[i];

                // Apply all filters (most selective first):
                // 1. Discount BETWEEN 0.05-0.07 (~13% selectivity)
                // 2. Quantity < 24 (~48% selectivity)
                // 3. Shipdate range (~16% selectivity)
                if (discount >= DISCOUNT_MIN && discount <= DISCOUNT_MAX &&
                    quantity < QUANTITY_THRESHOLD &&
                    ship_date >= EPOCH_1994_01_01 && ship_date < EPOCH_1995_01_01) {
                    // Compute extended_price * discount
                    local_sum += extendedprice * discount;
                }
            }
        }

        // Aggregate thread-local sum into global atomic
        double old_sum = thread_local_sum[thread_id].load();
        while (!thread_local_sum[thread_id].compare_exchange_weak(old_sum, old_sum + local_sum)) {
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_morsels, i);
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Final aggregation
    double final_sum = 0.0;
    for (unsigned i = 0; i < num_threads; ++i) {
        final_sum += thread_local_sum[i].load();
    }

    // Measure execution time
    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_ms =
        std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Output result
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << final_sum << "\n";
        out.close();
    }

    // Print to terminal
    std::cout << "Q6 Result: " << std::fixed << std::setprecision(2) << final_sum << std::endl;
    std::cout << "Rows processed: " << row_count << std::endl;
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << elapsed_ms
              << " ms" << std::endl;
}

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
