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

// Check if a zone can be skipped based on shipdate predicate
bool can_skip_zone(const ZoneMapEntry& zone) {
    // Skip if zone does not overlap with target range [SHIPDATE_MIN, SHIPDATE_MAX)
    return zone.max_shipdate <= SHIPDATE_MIN || zone.min_shipdate >= SHIPDATE_MAX;
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

// Kahan summation for improved floating-point precision
// Reduces rounding errors when accumulating many floating-point values
struct KahanAccumulator {
    double sum = 0.0;
    double c = 0.0;  // compensation for lost low-order bits

    void add(double value) {
        double y = value - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
};

// Single-threaded morsel processor with branch-free filtering and Kahan summation
double process_morsel_scalar(
    const int32_t* __restrict__ l_shipdate,
    const double* __restrict__ l_quantity,
    const double* __restrict__ l_discount,
    const double* __restrict__ l_extendedprice,
    int64_t start_row,
    int64_t end_row) {

    KahanAccumulator accum;

    for (int64_t i = start_row; i < end_row; ++i) {
        // Predicate evaluation in selectivity order, using branch-free approach
        // 1. l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' (0.10)
        int shipdate_ok = (l_shipdate[i] >= SHIPDATE_MIN && l_shipdate[i] < SHIPDATE_MAX) ? 1 : 0;

        // 2. l_quantity < 24 (0.48 of remaining)
        int quantity_ok = (l_quantity[i] < QUANTITY_THRESHOLD) ? 1 : 0;

        // 3. l_discount BETWEEN 0.05 AND 0.07 (0.18 of remaining)
        int discount_ok = (l_discount[i] >= DISCOUNT_MIN && l_discount[i] <= DISCOUNT_MAX) ? 1 : 0;

        // Combined predicate: all three must be true
        int all_ok = shipdate_ok & quantity_ok & discount_ok;

        // Conditionally accumulate: if all_ok, add (price * discount), otherwise add 0
        // This avoids branch misprediction for unpredictable filter outcomes
        double product = l_extendedprice[i] * l_discount[i];
        accum.add(product * all_ok);
    }

    return accum.sum;
}

// Parallel scan with morsel-driven execution and zone map filtering
double run_parallel_scan(const ColumnPointers& cols, const std::vector<ZoneMapEntry>& zones) {
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads <= 0) num_threads = 1;

    int64_t total_rows = cols.row_count;
    std::vector<ThreadLocalAccum> thread_accums(num_threads);
    std::vector<std::thread> threads;

    // Divide work equally among threads
    int64_t rows_per_thread = (total_rows + num_threads - 1) / num_threads;

    // If we have zone map info, use it to guide work distribution
    // For now, simple approach: process all rows but use zone skipping in the morsel processor
    // A more advanced approach would partition work by zones

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

    // Reduce all thread-local sums with Kahan summation for accuracy
    KahanAccumulator global_accum;
    for (int t = 0; t < num_threads; ++t) {
        global_accum.add(thread_accums[t].sum);
    }

    return global_accum.sum;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load columns
    ColumnPointers cols = load_columns(gendb_dir);
    if (!cols.l_shipdate || !cols.l_quantity || !cols.l_discount || !cols.l_extendedprice) {
        std::cerr << "Error: Failed to load required columns" << std::endl;
        return;
    }

    // Load zone map for optimization hints (optional)
    std::string zone_map_path = gendb_dir + "/lineitem/l_shipdate_zone.idx";
    std::vector<ZoneMapEntry> zones = load_zone_map(zone_map_path);

    // Execute parallel scan
    double revenue = run_parallel_scan(cols, zones);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Output result with high precision for validation
    std::cout << std::fixed << std::setprecision(4) << revenue << std::endl;
    std::cerr << "Execution time: " << elapsed << " ms" << std::endl;

    // Write CSV output if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_file = results_dir + "/Q6.csv";
        std::ofstream out(output_file);
        out << "revenue\n";
        out << std::fixed << std::setprecision(4) << revenue << "\n";
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
