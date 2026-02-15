#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <vector>
#include <iomanip>
#include <omp.h>

namespace {

// Helper struct to hold mmap'd file data
struct MmapFile {
    int fd;
    void* data;
    size_t size;

    MmapFile() : fd(-1), data(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        off_t file_size = lseek(fd, 0, SEEK_END);
        if (file_size < 0) {
            std::cerr << "Failed to seek: " << path << std::endl;
            ::close(fd);
            return false;
        }
        size = static_cast<size_t>(file_size);

        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            ::close(fd);
            return false;
        }
        return true;
    }

    void close() {
        if (data != nullptr) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
        size = 0;
    }

    ~MmapFile() { close(); }
};

// Zone map reader
struct ZoneMap {
    uint32_t num_blocks;
    std::vector<int32_t> block_min_i32;  // for date/quantity
    std::vector<int32_t> block_max_i32;
    std::vector<int64_t> block_min_i64;  // for discount
    std::vector<int64_t> block_max_i64;
    bool is_i64;

    ZoneMap() : num_blocks(0), is_i64(false) {}

    bool load_i32(const MmapFile& file) {
        if (file.size < 4) return false;
        const uint8_t* ptr = static_cast<const uint8_t*>(file.data);
        std::memcpy(&num_blocks, ptr, 4);

        is_i64 = false;
        size_t expected_size = 4 + num_blocks * 8;
        if (file.size < expected_size) return false;

        block_min_i32.resize(num_blocks);
        block_max_i32.resize(num_blocks);

        for (uint32_t i = 0; i < num_blocks; ++i) {
            std::memcpy(&block_min_i32[i], ptr + 4 + i * 8, 4);
            std::memcpy(&block_max_i32[i], ptr + 8 + i * 8, 4);
        }
        return true;
    }

    bool load_i64(const MmapFile& file) {
        if (file.size < 4) return false;
        const uint8_t* ptr = static_cast<const uint8_t*>(file.data);
        std::memcpy(&num_blocks, ptr, 4);

        is_i64 = true;
        size_t expected_size = 4 + num_blocks * 16;
        if (file.size < expected_size) return false;

        block_min_i64.resize(num_blocks);
        block_max_i64.resize(num_blocks);

        for (uint32_t i = 0; i < num_blocks; ++i) {
            std::memcpy(&block_min_i64[i], ptr + 4 + i * 16, 8);
            std::memcpy(&block_max_i64[i], ptr + 12 + i * 16, 8);
        }
        return true;
    }
};

// Function to parse CSV floating point number with proper rounding
double parse_double_precise(const std::string& str) {
    return std::stod(str);
}

// Format double to 4 decimal places for output
std::string format_revenue(double value) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(4) << value;
    return oss.str();
}

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    // Q6: Forecasting Revenue Change
    // SELECT SUM(l_extendedprice * l_discount) AS revenue
    // WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    //   AND l_discount BETWEEN 0.05 AND 0.07
    //   AND l_quantity < 24

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Date encoding: days since 1970-01-01
    // 1994-01-01 = 8766 days
    // 1995-01-01 = 9131 days
    const int32_t DATE_1994_01_01 = 8766;
    const int32_t DATE_1995_01_01 = 9131;

    // Discount bounds: BETWEEN 0.05 AND 0.07
    // Stored as integers scaled by 10000 (0-1000 for 0.00-0.10)
    const int64_t DISCOUNT_MIN = 500;   // 0.05 * 10000
    const int64_t DISCOUNT_MAX = 700;   // 0.07 * 10000

    // Quantity < 24: stored as int64_t with scale_factor=100
    // 24 * 100 = 2400
    const int64_t QUANTITY_LIMIT = 2400;

    // Load zone maps
#ifdef GENDB_PROFILE
    auto t_zm_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile zm_shipdate_file, zm_discount_file, zm_quantity_file;
    ZoneMap zm_shipdate, zm_discount, zm_quantity;

    if (!zm_shipdate_file.open(gendb_dir + "/indexes/l_shipdate_zone_map.bin")) {
        std::cerr << "Failed to load l_shipdate zone map" << std::endl;
        return;
    }
    if (!zm_shipdate.load_i32(zm_shipdate_file)) {
        std::cerr << "Failed to parse l_shipdate zone map" << std::endl;
        return;
    }

    if (!zm_discount_file.open(gendb_dir + "/indexes/l_discount_zone_map.bin")) {
        std::cerr << "Failed to load l_discount zone map" << std::endl;
        return;
    }
    if (!zm_discount.load_i64(zm_discount_file)) {
        std::cerr << "Failed to parse l_discount zone map" << std::endl;
        return;
    }

    if (!zm_quantity_file.open(gendb_dir + "/indexes/l_quantity_zone_map.bin")) {
        std::cerr << "Failed to load l_quantity zone map" << std::endl;
        return;
    }
    if (!zm_quantity.load_i64(zm_quantity_file)) {
        std::cerr << "Failed to parse l_quantity zone map" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_zm_end = std::chrono::high_resolution_clock::now();
    double zm_ms = std::chrono::duration<double, std::milli>(t_zm_end - t_zm_start).count();
    printf("[TIMING] zone_map_load: %.2f ms\n", zm_ms);
#endif

    // Load column files
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile col_shipdate, col_discount, col_quantity, col_extendedprice;

    if (!col_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin")) {
        std::cerr << "Failed to load l_shipdate" << std::endl;
        return;
    }
    if (!col_discount.open(gendb_dir + "/lineitem/l_discount.bin")) {
        std::cerr << "Failed to load l_discount" << std::endl;
        return;
    }
    if (!col_quantity.open(gendb_dir + "/lineitem/l_quantity.bin")) {
        std::cerr << "Failed to load l_quantity" << std::endl;
        return;
    }
    if (!col_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin")) {
        std::cerr << "Failed to load l_extendedprice" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] column_load: %.2f ms\n", load_ms);
#endif

    // Verify file sizes
    const size_t EXPECTED_ROWS = 59986052;
    const size_t BLOCK_SIZE = 131072;
    const size_t NUM_BLOCKS = 458;

    if (col_shipdate.size != EXPECTED_ROWS * sizeof(int32_t)) {
        std::cerr << "Unexpected l_shipdate file size" << std::endl;
        return;
    }

    // Cast to typed pointers
    const int32_t* shipdate = static_cast<const int32_t*>(col_shipdate.data);
    const int64_t* discount = static_cast<const int64_t*>(col_discount.data);
    const int64_t* quantity = static_cast<const int64_t*>(col_quantity.data);
    const int64_t* extendedprice = static_cast<const int64_t*>(col_extendedprice.data);

#ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
#endif

    // Perform aggregation with filtering
    // Use Kahan summation for numerical stability
    double sum = 0.0;
    double compensation = 0.0;

    // Pre-filter blocks using zone maps to reduce I/O and processing
    std::vector<bool> block_active(NUM_BLOCKS, false);
    int active_block_count = 0;

    for (uint32_t b = 0; b < zm_shipdate.num_blocks; ++b) {
        // Zone map check: skip blocks completely outside date range
        if (zm_shipdate.block_max_i32[b] < DATE_1994_01_01 ||
            zm_shipdate.block_min_i32[b] >= DATE_1995_01_01) {
            continue;
        }
        block_active[b] = true;
        active_block_count++;
    }

#ifdef GENDB_PROFILE
    auto t_block_check = std::chrono::high_resolution_clock::now();
    double block_check_ms = std::chrono::duration<double, std::milli>(
        t_block_check - t_filter_start).count();
    printf("[TIMING] block_check: %.2f ms\n", block_check_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_row_filter_start = std::chrono::high_resolution_clock::now();
#endif

    size_t filtered_rows = 0;

    #pragma omp parallel for reduction(+:sum, compensation) reduction(+:filtered_rows) \
        schedule(static, 1)
    for (uint32_t b = 0; b < NUM_BLOCKS; ++b) {
        if (!block_active[b]) continue;

        size_t block_start = static_cast<size_t>(b) * BLOCK_SIZE;
        size_t block_end = (b + 1 < NUM_BLOCKS) ? (b + 1) * BLOCK_SIZE : EXPECTED_ROWS;

        double local_sum = 0.0;
        double local_compensation = 0.0;
        size_t local_filtered = 0;

        for (size_t i = block_start; i < block_end; ++i) {
            // Filter predicates
            if (shipdate[i] < DATE_1994_01_01 || shipdate[i] >= DATE_1995_01_01) {
                continue;
            }
            if (discount[i] < DISCOUNT_MIN || discount[i] > DISCOUNT_MAX) {
                continue;
            }
            if (quantity[i] >= QUANTITY_LIMIT) {
                continue;
            }

            // Compute: l_extendedprice * l_discount
            // l_extendedprice: stored as int64_t scaled by 100
            // l_discount: stored as int64_t scaled by 10000
            // Revenue = (extendedprice / 100) * (discount / 10000)
            //         = (extendedprice * discount) / 1000000
            double revenue_contribution = static_cast<double>(extendedprice[i]) *
                                         static_cast<double>(discount[i]) / 1000000.0;

            // Kahan summation
            double y = revenue_contribution - local_compensation;
            double t = local_sum + y;
            local_compensation = (t - local_sum) - y;
            local_sum = t;

            local_filtered++;
        }

        // Merge local sum into global (Kahan-aware)
        double y = local_sum - compensation;
        double t = sum + y;
        compensation = (t - sum) - y;
        sum = t;

        filtered_rows += local_filtered;
    }

#ifdef GENDB_PROFILE
    auto t_row_filter_end = std::chrono::high_resolution_clock::now();
    double row_filter_ms = std::chrono::duration<double, std::milli>(
        t_row_filter_end - t_row_filter_start).count();
    printf("[TIMING] row_filter: %.2f ms\n", row_filter_ms);
    printf("[TIMING] filtered_rows: %zu\n", filtered_rows);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Write results
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out_file(results_dir + "/Q6.csv");
    if (!out_file.is_open()) {
        std::cerr << "Failed to open output file" << std::endl;
        return;
    }

    // Write header
    out_file << "revenue\n";

    // Write result with proper formatting (4 decimal places)
    out_file << format_revenue(sum) << "\n";

    out_file.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    std::cout << "Q6 execution completed. Result written to " << results_dir << "/Q6.csv" << std::endl;
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
