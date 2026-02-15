#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstring>
#include <cmath>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <omp.h>

namespace {

// Helper function to get file size
size_t get_file_size(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        std::cerr << "Error: Cannot stat file " << path << std::endl;
        return 0;
    }
    return st.st_size;
}

// Helper function to mmap a file
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open file " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        std::cerr << "Error: Cannot fstat file " << path << std::endl;
        close(fd);
        return nullptr;
    }

    size = st.st_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Error: Cannot mmap file " << path << std::endl;
        return nullptr;
    }

    return ptr;
}

// Convert YYYY-MM-DD to epoch days (days since 1970-01-01)
int32_t date_to_epoch(int year, int month, int day) {
    // Days in each month (non-leap year)
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int32_t epoch_days = 0;

    // Count days for years 1970-1999
    for (int y = 1970; y < year; y++) {
        if ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)) {
            epoch_days += 366;
        } else {
            epoch_days += 365;
        }
    }

    // Add days for months
    for (int m = 1; m < month; m++) {
        epoch_days += days_in_month[m];
        if (m == 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
            epoch_days += 1;
        }
    }

    // Add days
    epoch_days += day;

    return epoch_days;
}

// Convert epoch days back to YYYY-MM-DD string
std::string epoch_to_date(int32_t epoch_days) {
    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int year = 1970;
    int remaining_days = epoch_days;

    while (true) {
        int days_in_year = ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) ? 366 : 365;
        if (remaining_days > days_in_year) {
            remaining_days -= days_in_year;
            year++;
        } else {
            break;
        }
    }

    int month = 1;
    int day = remaining_days;

    while (month <= 12) {
        int days_in_m = days_in_month[month];
        if (month == 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
            days_in_m = 29;
        }

        if (day <= days_in_m) {
            break;
        }
        day -= days_in_m;
        month++;
    }

    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

} // anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    printf("[TIMING] Q6 start\n");

    // File paths for lineitem columns
    std::string lineitem_dir = gendb_dir + "/lineitem";
    std::string l_shipdate_path = lineitem_dir + "/l_shipdate.bin";
    std::string l_discount_path = lineitem_dir + "/l_discount.bin";
    std::string l_extendedprice_path = lineitem_dir + "/l_extendedprice.bin";
    std::string l_quantity_path = lineitem_dir + "/l_quantity.bin";

    // Memory-map all columns
    size_t shipdate_size = 0, discount_size = 0, extendedprice_size = 0, quantity_size = 0;

    auto t_load_start = std::chrono::high_resolution_clock::now();

    void* shipdate_ptr = mmap_file(l_shipdate_path, shipdate_size);
    void* discount_ptr = mmap_file(l_discount_path, discount_size);
    void* extendedprice_ptr = mmap_file(l_extendedprice_path, extendedprice_size);
    void* quantity_ptr = mmap_file(l_quantity_path, quantity_size);

    if (!shipdate_ptr || !discount_ptr || !extendedprice_ptr || !quantity_ptr) {
        std::cerr << "Error: Failed to mmap required columns" << std::endl;
        return;
    }

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);

    // Cast to typed pointers
    int32_t* l_shipdate = (int32_t*)shipdate_ptr;
    int64_t* l_discount = (int64_t*)discount_ptr;
    int64_t* l_extendedprice = (int64_t*)extendedprice_ptr;
    int64_t* l_quantity = (int64_t*)quantity_ptr;

    size_t num_rows = shipdate_size / sizeof(int32_t);

    printf("Processing %zu lineitem rows\n", num_rows);

    // Convert date literals to epoch days
    // l_shipdate >= DATE '1994-01-01'
    int32_t shipdate_lower = date_to_epoch(1994, 1, 1);
    // l_shipdate < DATE '1995-01-01'
    int32_t shipdate_upper = date_to_epoch(1995, 1, 1);

    // l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    // 0.05 <= l_discount <= 0.07 (with scale=100)
    int64_t discount_lower = 5;  // 0.05 * 100
    int64_t discount_upper = 7;  // 0.07 * 100

    // l_quantity < 24 (scale=100, so 24.00 = 2400)
    int64_t quantity_upper = 2400;

    // Filter and aggregate in parallel
    double sum_revenue = 0.0;
    double comp = 0.0;  // Kahan summation compensation

    auto t_filter_start = std::chrono::high_resolution_clock::now();

    // Use a more aggressive parallelization strategy for HDD
    #pragma omp parallel for reduction(+:sum_revenue) schedule(static, 100000)
    for (size_t i = 0; i < num_rows; i++) {
        // Check all predicates
        if (l_shipdate[i] >= shipdate_lower &&
            l_shipdate[i] < shipdate_upper &&
            l_discount[i] >= discount_lower &&
            l_discount[i] <= discount_upper &&
            l_quantity[i] < quantity_upper) {

            // Compute l_extendedprice * l_discount
            // Both are scaled by 100, so product is scaled by 10000
            // Convert to double before dividing to preserve precision
            double actual_price = (double)l_extendedprice[i] / 100.0;
            double actual_discount = (double)l_discount[i] / 100.0;
            double value = actual_price * actual_discount;

            // Accumulate with Kahan summation
            double y = value - comp;
            double t = sum_revenue + y;
            comp = (t - sum_revenue) - y;
            sum_revenue = t;
        }
    }

    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] filter_aggregate: %.2f ms\n", filter_ms);

    // Munmap all columns
    munmap(shipdate_ptr, shipdate_size);
    munmap(discount_ptr, discount_size);
    munmap(extendedprice_ptr, extendedprice_size);
    munmap(quantity_ptr, quantity_size);

    // Output result to CSV
    auto t_output_start = std::chrono::high_resolution_clock::now();

    std::string results_file = results_dir + "/Q6.csv";
    std::ofstream out(results_file);

    if (!out.is_open()) {
        std::cerr << "Error: Cannot open results file " << results_file << std::endl;
        return;
    }

    out << "revenue" << std::endl;
    out << std::fixed;
    out.precision(4);
    out << sum_revenue << std::endl;

    out.close();

    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);

    printf("Result: revenue = %.4f\n", sum_revenue);
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
