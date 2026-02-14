#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <iomanip>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// Helper to mmap a binary column file
template<typename T>
class MmapColumn {
public:
    T* data = nullptr;
    size_t count = 0;
    int fd = -1;

    ~MmapColumn() {
        if (data != nullptr) {
            munmap(data, count * sizeof(T));
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool load(const std::string& path, size_t expected_count) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            close(fd);
            fd = -1;
            return false;
        }

        size_t file_size = st.st_size;
        size_t expected_size = expected_count * sizeof(T);

        if (file_size != expected_size) {
            std::cerr << "Size mismatch for " << path << ": expected " << expected_size
                      << " bytes, got " << file_size << std::endl;
            close(fd);
            fd = -1;
            return false;
        }

        data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            fd = -1;
            data = nullptr;
            return false;
        }

        count = expected_count;

        // Hint sequential access
        madvise(data, file_size, MADV_SEQUENTIAL);

        return true;
    }

    T& operator[](size_t i) const { return data[i]; }
};

// Parse the metadata file to get row count
size_t getRowCount(const std::string& metadata_path) {
    std::ifstream file(metadata_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open metadata: " << metadata_path << std::endl;
        return 0;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.find("rows:") == 0) {
            return std::stoul(line.substr(5));
        }
    }

    std::cerr << "Could not find row count in metadata" << std::endl;
    return 0;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load metadata
    std::string metadata_path = gendb_dir + "/lineitem_metadata.txt";
    size_t num_rows = getRowCount(metadata_path);
    if (num_rows == 0) {
        std::cerr << "Error: Could not determine row count\n";
        return;
    }

    // Load binary columns for Q6
    MmapColumn<int32_t> l_quantity;
    MmapColumn<int64_t> l_extendedprice;
    MmapColumn<int32_t> l_discount;
    MmapColumn<int32_t> l_shipdate;

    if (!l_quantity.load(gendb_dir + "/lineitem_l_quantity.col", num_rows)) {
        std::cerr << "Failed to load l_quantity column\n";
        return;
    }
    if (!l_extendedprice.load(gendb_dir + "/lineitem_l_extendedprice.col", num_rows)) {
        std::cerr << "Failed to load l_extendedprice column\n";
        return;
    }
    if (!l_discount.load(gendb_dir + "/lineitem_l_discount.col", num_rows)) {
        std::cerr << "Failed to load l_discount column\n";
        return;
    }
    if (!l_shipdate.load(gendb_dir + "/lineitem_l_shipdate.col", num_rows)) {
        std::cerr << "Failed to load l_shipdate column\n";
        return;
    }

    // Date constants (days since epoch 1970-01-01)
    // 1994-01-01 = 8766 days
    // 1995-01-01 = 9131 days
    const int32_t date_1994_01_01 = 8766;
    const int32_t date_1995_01_01 = 9131;

    // Discount bounds (stored as int32_t, raw values)
    // 0.05 = 5, 0.07 = 7 (assuming encoding is value * 100)
    const int32_t discount_min = 5;
    const int32_t discount_max = 7;

    // Quantity threshold
    const int32_t qty_threshold = 24;

    // Thread-local accumulators (aligned to cache line to avoid false sharing)
    size_t num_threads = std::thread::hardware_concurrency();
    struct alignas(64) LocalSum {
        double sum = 0.0;
    };
    std::vector<LocalSum> local_sums(num_threads);
    std::vector<std::thread> threads;

    // Parallel scan with static assignment
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_sum = 0.0;

            // Each thread processes every num_threads-th row
            for (size_t i = t; i < num_rows; i += num_threads) {
                // Apply Q6 filters in order of selectivity
                // 1. l_discount BETWEEN 0.05 AND 0.07 (most selective)
                if (l_discount[i] >= discount_min && l_discount[i] <= discount_max) {
                    // 2. l_quantity < 24
                    if (l_quantity[i] < qty_threshold) {
                        // 3. l_shipdate in [1994-01-01, 1995-01-01)
                        if (l_shipdate[i] >= date_1994_01_01 && l_shipdate[i] < date_1995_01_01) {
                            // Compute revenue: l_extendedprice * l_discount / 100.0
                            // l_extendedprice is int64_t (stored as cents * 100)
                            // l_discount is int32_t (stored as cents, e.g., 5 = 0.05)
                            // Result is the actual revenue value
                            double revenue = (static_cast<double>(l_extendedprice[i]) *
                                            static_cast<double>(l_discount[i])) / 10000.0;
                            local_sum += revenue;
                        }
                    }
                }
            }

            local_sums[t].sum = local_sum;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Merge results from all threads
    double total_revenue = 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        total_revenue += local_sums[t].sum;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(4) << total_revenue << "\n";
        out.close();
    }

    // Also print detailed result for debugging
    std::cerr << "DEBUG: Result = " << std::fixed << std::setprecision(4) << total_revenue << std::endl;

    // Print summary
    std::cout << "Query returned 1 row\n";
    std::cout << "Execution time: " << duration_ms << " ms\n";
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
