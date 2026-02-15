#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <omp.h>
#include <cmath>

// Helper function to calculate epoch days for 1994-01-01 and 1995-01-01
inline int32_t days_since_epoch(int year, int month, int day) {
    // Count days from 1970-01-01 to the given date
    int32_t days = 0;

    // Add days for complete years
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days in each month (non-leap year)
    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Adjust for leap year
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        month_days[2] = 29;
    }

    // Add days for complete months in the year
    for (int m = 1; m < month; ++m) {
        days += month_days[m];
    }

    // Add remaining days
    days += day;

    return days;
}

// Memory-mapped file wrapper
struct MMapFile {
    int fd;
    void* ptr;
    size_t size;

    MMapFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return;
        }

        off_t file_size = lseek(fd, 0, SEEK_END);
        size = static_cast<size_t>(file_size);

        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            fd = -1;
            ptr = nullptr;
        }
    }

    ~MMapFile() {
        if (ptr != nullptr) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool is_valid() const {
        return ptr != nullptr && fd >= 0;
    }
};

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // [METADATA CHECK]
    printf("[METADATA CHECK] Q6 Query\n");
    printf("[METADATA CHECK] Table: lineitem, Columns: l_shipdate(DATE), l_discount(DECIMAL,scale=100), l_quantity(DECIMAL,scale=100), l_extendedprice(DECIMAL,scale=100)\n");
    printf("[METADATA CHECK] Predicates: l_shipdate >= 1994-01-01 AND l_shipdate < 1995-01-01 AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24\n");
    printf("[METADATA CHECK] Aggregation: SUM(l_extendedprice * l_discount)\n");

    // Calculate date boundaries
    // 1994-01-01: day 8766 (verified from workload analysis)
    // 1995-01-01: day 9131 (365 days later)
    int32_t date_start = days_since_epoch(1994, 1, 1);
    int32_t date_end = days_since_epoch(1995, 1, 1);
    printf("[METADATA CHECK] Date range: %d to %d\n", date_start, date_end);

    // Discount range: 0.06 +/- 0.01 = [0.05, 0.07]
    // Scaled by 100: [5, 7]
    int64_t discount_min = 5;   // 0.05
    int64_t discount_max = 7;   // 0.07

    // Quantity < 24, scaled by 100: < 2400
    int64_t quantity_max = 2400;

    printf("[METADATA CHECK] Discount range (scaled): %ld to %ld\n", discount_min, discount_max);
    printf("[METADATA CHECK] Quantity max (scaled): %ld\n", quantity_max);

    // Load binary columns
    std::string lineitem_dir = gendb_dir + "/lineitem/";

    printf("[TIMING] Loading columns...\n");
    auto t_load_start = std::chrono::high_resolution_clock::now();

    MMapFile l_shipdate_file(lineitem_dir + "l_shipdate.bin");
    MMapFile l_discount_file(lineitem_dir + "l_discount.bin");
    MMapFile l_quantity_file(lineitem_dir + "l_quantity.bin");
    MMapFile l_extendedprice_file(lineitem_dir + "l_extendedprice.bin");

    if (!l_shipdate_file.is_valid() || !l_discount_file.is_valid() ||
        !l_quantity_file.is_valid() || !l_extendedprice_file.is_valid()) {
        std::cerr << "Failed to load required columns" << std::endl;
        return;
    }

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);

    // Cast to typed pointers
    const int32_t* shipdate_data = static_cast<const int32_t*>(l_shipdate_file.ptr);
    const int64_t* discount_data = static_cast<const int64_t*>(l_discount_file.ptr);
    const int64_t* quantity_data = static_cast<const int64_t*>(l_quantity_file.ptr);
    const int64_t* extendedprice_data = static_cast<const int64_t*>(l_extendedprice_file.ptr);

    size_t num_rows = l_shipdate_file.size / sizeof(int32_t);
    printf("[METADATA CHECK] Total rows: %zu\n", num_rows);

    // Scan, filter, and aggregate
    printf("[TIMING] Starting scan-filter-aggregate...\n");
    auto t_scan_start = std::chrono::high_resolution_clock::now();

    // Use Kahan summation for better precision
    double sum = 0.0;
    double c = 0.0;  // Compensation for lost low-order bits

    #pragma omp parallel for reduction(+:sum) reduction(+:c) schedule(static, 65536)
    for (size_t i = 0; i < num_rows; ++i) {
        // Apply predicates
        int32_t ship_date = shipdate_data[i];
        int64_t discount = discount_data[i];
        int64_t quantity = quantity_data[i];
        int64_t extendedprice = extendedprice_data[i];

        // All four predicates must be true
        if (ship_date >= date_start &&
            ship_date < date_end &&
            discount >= discount_min &&
            discount <= discount_max &&
            quantity < quantity_max) {

            // Compute l_extendedprice * l_discount (both scaled by 100)
            // Result is scaled by 100*100 = 10000
            int64_t product = extendedprice * discount;

            // Convert to double and unscale by dividing by 10000
            double value = static_cast<double>(product) / 10000.0;

            // Kahan summation
            double y = value - c;
            double t = sum + y;
            c = (t - sum) - y;
            sum = t;
        }
    }

    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);

    // Write results to CSV
    printf("[TIMING] Writing output...\n");
    auto t_output_start = std::chrono::high_resolution_clock::now();

    std::ofstream output_file(results_dir + "/Q6.csv");
    if (!output_file.is_open()) {
        std::cerr << "Failed to open output file" << std::endl;
        return;
    }

    // Write header
    output_file << "revenue\n";

    // Write result with 4 decimal places
    output_file << std::fixed << std::setprecision(4) << sum << "\n";

    output_file.close();

    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);

    printf("Result written to %s/Q6.csv\n", results_dir.c_str());
    printf("Revenue: %.4f\n", sum);
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
