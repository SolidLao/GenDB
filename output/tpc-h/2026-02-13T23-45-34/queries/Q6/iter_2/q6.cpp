#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <chrono>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// Constants for date encoding (days since epoch 1970)
constexpr int32_t DATE_1994_01_01 = 8766;  // days since 1970-01-01
constexpr int32_t DATE_1995_01_01 = 9131;  // days since 1970-01-01

// Date range for shipdate predicate
constexpr int32_t SHIPDATE_MIN = DATE_1994_01_01;
constexpr int32_t SHIPDATE_MAX = DATE_1995_01_01;

// Discount range: 0.06 ± 0.01 = [0.05, 0.07]
constexpr double DISCOUNT_MIN = 0.05;
constexpr double DISCOUNT_MAX = 0.07;

// Quantity threshold
constexpr double QUANTITY_THRESHOLD = 24.0;

// Morsel size: target ~64K rows per morsel for L3 cache efficiency
// With 5 columns (shipdate, quantity, discount, extendedprice, and minimal overhead),
// 64K rows * 8 bytes per column * 5 = ~2.56 MB (fits in L3 per thread)
constexpr int32_t MORSEL_SIZE = 65536;

struct ColumnPointers {
    int32_t* l_shipdate = nullptr;
    double* l_quantity = nullptr;
    double* l_discount = nullptr;
    double* l_extendedprice = nullptr;
    int64_t row_count = 0;
};

// Zone map structure (stored as binary in .idx file)
struct ZoneMapEntry {
    int32_t min_shipdate;
    int32_t max_shipdate;
    int64_t row_count;
};

// Load zone map index for l_shipdate
std::vector<ZoneMapEntry> load_zone_map(const std::string& zone_map_path) {
    std::vector<ZoneMapEntry> zones;
    int fd = open(zone_map_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Warning: Could not open zone map at " << zone_map_path << std::endl;
        return zones;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    // Zone maps are typically: 8 bytes per entry (min, max as int32_t pairs)
    // Plus metadata. Read raw and interpret.
    char* data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        close(fd);
        return zones;
    }

    // Parse zone map: skip header, then read zone entries
    // Assuming format: [header info][zone entries]
    // Typical format: 4 bytes magic, 4 bytes version, then zones
    if (file_size >= 8) {
        size_t entry_size = sizeof(int32_t) * 2;  // min, max
        size_t num_zones = (file_size - 8) / entry_size;

        int32_t* zone_data = (int32_t*)(data + 8);
        for (size_t i = 0; i < num_zones; ++i) {
            ZoneMapEntry entry;
            entry.min_shipdate = zone_data[i * 2];
            entry.max_shipdate = zone_data[i * 2 + 1];
            entry.row_count = 0;  // Not needed for filtering
            zones.push_back(entry);
        }
    }

    munmap(data, file_size);
    close(fd);
    return zones;
}

// Memory-map a column file
template <typename T>
T* mmap_column(const std::string& filepath, int64_t& row_count) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Could not open column file: " << filepath << std::endl;
        return nullptr;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    row_count = file_size / sizeof(T);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "Error: Could not mmap column file: " << filepath << std::endl;
        close(fd);
        return nullptr;
    }

    // Advise sequential access
    madvise(ptr, file_size, MADV_SEQUENTIAL);

    close(fd);  // Close FD; mmap keeps mapping alive
    return static_cast<T*>(ptr);
}

// Load all required columns
ColumnPointers load_columns(const std::string& gendb_dir) {
    ColumnPointers cols;

    std::string lineitem_dir = gendb_dir + "/lineitem";

    int64_t row_count_shipdate = 0;
    cols.l_shipdate = mmap_column<int32_t>(lineitem_dir + "/l_shipdate.bin", row_count_shipdate);

    int64_t row_count_quantity = 0;
    cols.l_quantity = mmap_column<double>(lineitem_dir + "/l_quantity.bin", row_count_quantity);

    int64_t row_count_discount = 0;
    cols.l_discount = mmap_column<double>(lineitem_dir + "/l_discount.bin", row_count_discount);

    int64_t row_count_price = 0;
    cols.l_extendedprice = mmap_column<double>(lineitem_dir + "/l_extendedprice.bin", row_count_price);

    if (!cols.l_shipdate || !cols.l_quantity || !cols.l_discount || !cols.l_extendedprice) {
        std::cerr << "Error: Failed to load columns" << std::endl;
        return cols;
    }

    cols.row_count = row_count_shipdate;
    return cols;
}

// Thread-local accumulator for parallel execution
struct ThreadLocalAccum {
    double sum = 0.0;
    // Padding to avoid false sharing (align to 64-byte cache line)
    char padding[64 - sizeof(double)];
};

// Single-threaded morsel processor (no SIMD for initial correctness)
double process_morsel_scalar(
    const int32_t* l_shipdate,
    const double* l_quantity,
    const double* l_discount,
    const double* l_extendedprice,
    int64_t start_row,
    int64_t end_row) {

    double local_sum = 0.0;

    for (int64_t i = start_row; i < end_row; ++i) {
        // Predicate evaluation in selectivity order
        // 1. l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' (0.10)
        if (l_shipdate[i] < SHIPDATE_MIN || l_shipdate[i] >= SHIPDATE_MAX) {
            continue;
        }

        // 2. l_quantity < 24 (0.48 of remaining)
        if (l_quantity[i] >= QUANTITY_THRESHOLD) {
            continue;
        }

        // 3. l_discount BETWEEN 0.05 AND 0.07 (0.18 of remaining)
        if (l_discount[i] < DISCOUNT_MIN || l_discount[i] > DISCOUNT_MAX) {
            continue;
        }

        // All predicates passed: accumulate l_extendedprice * l_discount
        local_sum += l_extendedprice[i] * l_discount[i];
    }

    return local_sum;
}

// Parallel scan with morsel-driven execution
double run_parallel_scan(const ColumnPointers& cols) {
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads <= 0) num_threads = 1;

    int64_t total_rows = cols.row_count;
    std::vector<ThreadLocalAccum> thread_accums(num_threads);
    std::vector<std::thread> threads;

    // Divide work equally among threads
    int64_t rows_per_thread = (total_rows + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; ++t) {
        int64_t start_row = t * rows_per_thread;
        int64_t end_row = std::min(start_row + rows_per_thread, total_rows);

        threads.emplace_back([&, t, start_row, end_row]() {
            double local_sum = process_morsel_scalar(
                cols.l_shipdate,
                cols.l_quantity,
                cols.l_discount,
                cols.l_extendedprice,
                start_row,
                end_row);

            thread_accums[t].sum = local_sum;
        });
    }

    // Wait for all threads to complete
    for (auto& th : threads) {
        th.join();
    }

    // Reduce all thread-local sums
    double global_sum = 0.0;
    for (int t = 0; t < num_threads; ++t) {
        global_sum += thread_accums[t].sum;
    }

    return global_sum;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load columns
    ColumnPointers cols = load_columns(gendb_dir);
    if (!cols.l_shipdate || !cols.l_quantity || !cols.l_discount || !cols.l_extendedprice) {
        std::cerr << "Error: Failed to load required columns" << std::endl;
        return;
    }

    // Execute parallel scan
    double revenue = run_parallel_scan(cols);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Output result
    std::cout << std::fixed << std::setprecision(2) << revenue << std::endl;
    std::cerr << "Execution time: " << elapsed << " ms" << std::endl;

    // Write CSV output if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_file = results_dir + "/Q6.csv";
        std::ofstream out(output_file);
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue << "\n";
        out.close();
    }

    // Print row count for logging
    std::cerr << "Rows processed: " << cols.row_count << std::endl;
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
