#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <atomic>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <cmath>
#include <algorithm>

// ============================================================================
// Note: Date Encoding
// ============================================================================
// Dates are stored as year-only values (int32_t): 1992-1998
// This simplifies range predicates to simple integer comparisons

// ============================================================================
// Column Loader (mmap-based zero-copy access)
// ============================================================================

template<typename T>
class ColumnLoader {
private:
    int fd;
    void* mapped;
    size_t size;

public:
    ColumnLoader(const std::string& path, size_t expected_rows) : fd(-1), mapped(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Failed to open column file: " + path);
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            throw std::runtime_error("Failed to stat column file: " + path);
        }

        size = st.st_size;
        mapped = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("Failed to mmap column file: " + path);
        }

        // Advise sequential access
        madvise(mapped, size, MADV_SEQUENTIAL);
    }

    ~ColumnLoader() {
        if (mapped && mapped != MAP_FAILED) {
            munmap(mapped, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    inline T get(size_t idx) const {
        return ((T*)mapped)[idx];
    }

    inline const T* ptr() const {
        return (T*)mapped;
    }

    size_t row_count() const {
        return size / sizeof(T);
    }
};

// ============================================================================
// String Column Loader (for variable-length data)
// ============================================================================

class StringColumnLoader {
private:
    int fd;
    void* mapped;
    size_t size;

public:
    StringColumnLoader(const std::string& path) : fd(-1), mapped(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            // Not all columns exist; this is OK for Q6
            return;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            return;
        }

        size = st.st_size;
        if (size == 0) {
            close(fd);
            return;
        }

        mapped = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped == MAP_FAILED) {
            close(fd);
            mapped = nullptr;
        } else {
            madvise(mapped, size, MADV_SEQUENTIAL);
        }
    }

    ~StringColumnLoader() {
        if (mapped && mapped != MAP_FAILED) {
            munmap(mapped, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool is_valid() const {
        return mapped != nullptr && mapped != MAP_FAILED;
    }
};

// ============================================================================
// Q6 Query Implementation
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : "";

    auto start_time = std::chrono::high_resolution_clock::now();

    try {
        // Build column file paths
        std::string lineitem_dir = gendb_dir + "/lineitem/";

        // Load required columns via mmap (Q6 needs: l_shipdate, l_discount, l_quantity, l_extendedprice)
        ColumnLoader<int32_t> l_shipdate(lineitem_dir + "l_shipdate.col", 60000000);
        ColumnLoader<double> l_discount(lineitem_dir + "l_discount.col", 60000000);
        ColumnLoader<double> l_quantity(lineitem_dir + "l_quantity.col", 60000000);
        ColumnLoader<double> l_extendedprice(lineitem_dir + "l_extendedprice.col", 60000000);

        // Parse date constants
        // Q6: l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
        // Note: Dates are stored as year-only (1992-1998), so:
        // - l_shipdate >= '1994-01-01' means shipdate >= 1994
        // - l_shipdate < '1995-01-01' means shipdate < 1995
        // This simplifies to: shipdate == 1994
        int32_t date_min = 1994;
        int32_t date_max = 1995;

        // Discount bounds: 0.06 +/- 0.01 => [0.05, 0.07]
        double discount_min = 0.05;
        double discount_max = 0.07;
        double quantity_max = 24.0;

        size_t total_rows = l_shipdate.row_count();

        // Morsel-driven parallel scan
        // Use hardware concurrency for thread count
        int num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) num_threads = 64; // fallback for 64-core system
        if (num_threads > 64) num_threads = 64; // cap at 64

        size_t morsel_size = (total_rows + num_threads - 1) / num_threads;
        morsel_size = std::max(size_t(10000), morsel_size); // min morsel size for cache efficiency

        std::vector<double> local_results(num_threads, 0.0);
        std::atomic<size_t> completed_rows(0);

        // Thread function for parallel scan
        auto scan_and_aggregate = [&](int thread_id) {
            double local_sum = 0.0;

            // Work-stealing: each thread processes morsels in sequence
            size_t start = thread_id * morsel_size;
            size_t end = std::min(start + morsel_size, total_rows);

            // Main scan loop with compound filter
            for (size_t i = start; i < end; i++) {
                int32_t shipdate = l_shipdate.get(i);
                double discount = l_discount.get(i);
                double quantity = l_quantity.get(i);

                // Compound predicate: range check on date, discount, and quantity
                if (shipdate >= date_min && shipdate < date_max &&
                    discount >= discount_min && discount <= discount_max &&
                    quantity < quantity_max) {

                    double extendedprice = l_extendedprice.get(i);
                    local_sum += extendedprice * discount;
                }
            }

            // Store thread-local result
            local_results[thread_id] = local_sum;
        };

        // Launch worker threads
        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(scan_and_aggregate, t);
        }

        // Wait for all threads to complete
        for (auto& t : threads) {
            t.join();
        }

        // Global merge: sum all local results
        double revenue = 0.0;
        for (int t = 0; t < num_threads; t++) {
            revenue += local_results[t];
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        // Output results
        if (!results_dir.empty()) {
            std::ofstream out(results_dir + "/Q6.csv");
            out << std::fixed << std::setprecision(2);
            out << "revenue\n";
            out << revenue << "\n";
            out.close();
        }

        // Print to terminal
        std::cout << "Q6 Result: " << std::fixed << std::setprecision(2) << revenue << std::endl;
        std::cout << "Execution time: " << elapsed_ms << " ms" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
