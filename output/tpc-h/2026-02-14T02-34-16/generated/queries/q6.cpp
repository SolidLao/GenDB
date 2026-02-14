#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <ctime>

namespace {

// Zone map structure: 24 bytes per entry
struct ZoneMapEntry {
    int32_t min_val;       // 4 bytes
    int32_t max_val;       // 4 bytes
    uint64_t start_row;    // 8 bytes
    uint64_t end_row;      // 8 bytes
};

// Helper function to convert YYYY-MM-DD date to epoch days
inline int32_t dateToEpochDays(int year, int month, int day) {
    // Days since 1970-01-01
    // Simplified calculation using known epoch days for reference dates
    static const int daysInMonth[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

    int days = 0;
    // Add days for years (accounting for leap years)
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add days for months
    days += daysInMonth[month - 1];
    // Check for leap year when adding months
    if (month > 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days += 1;
    }
    // Add days (day - 1 because Jan 1 is day 0 of the year, not day 1)
    days += day - 1;
    return days;
}

// Load zone map from file
std::vector<ZoneMapEntry> loadZoneMap(const std::string& filename) {
    std::vector<ZoneMapEntry> result;
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open zone map: " << filename << std::endl;
        return result;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    size_t num_entries = file_size / sizeof(ZoneMapEntry);

    ZoneMapEntry* zonemap = (ZoneMapEntry*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (zonemap == MAP_FAILED) {
        std::cerr << "Failed to mmap zone map" << std::endl;
        close(fd);
        return result;
    }

    result.assign(zonemap, zonemap + num_entries);

    munmap(zonemap, file_size);
    close(fd);
    return result;
}

// Memory-mapped column loader
template<typename T>
class ColumnLoader {
public:
    const T* data;
    size_t row_count;
    int fd;
    void* mmap_ptr;
    size_t mmap_size;

    ColumnLoader(const std::string& filename, size_t expected_rows)
        : data(nullptr), row_count(expected_rows), fd(-1), mmap_ptr(nullptr), mmap_size(0) {

        fd = open(filename.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open column file: " << filename << std::endl;
            return;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat column file: " << filename << std::endl;
            close(fd);
            fd = -1;
            return;
        }

        mmap_size = sb.st_size;
        mmap_ptr = mmap(nullptr, mmap_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mmap_ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap column file: " << filename << std::endl;
            close(fd);
            fd = -1;
            mmap_ptr = nullptr;
            return;
        }

        data = (const T*)mmap_ptr;
        row_count = mmap_size / sizeof(T);

        // Prefetch for sequential access
        madvise(mmap_ptr, mmap_size, MADV_SEQUENTIAL);
    }

    ~ColumnLoader() {
        if (mmap_ptr != nullptr && mmap_ptr != MAP_FAILED) {
            munmap(mmap_ptr, mmap_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool isValid() const {
        return data != nullptr && fd >= 0;
    }
};

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    const std::string lineitem_dir = gendb_dir + "/lineitem";

    // Load columns
    ColumnLoader<int32_t> shipdate_col(lineitem_dir + "/l_shipdate.col", 0);
    ColumnLoader<double> discount_col(lineitem_dir + "/l_discount.col", 0);
    ColumnLoader<double> quantity_col(lineitem_dir + "/l_quantity.col", 0);
    ColumnLoader<double> extendedprice_col(lineitem_dir + "/l_extendedprice.col", 0);

    if (!shipdate_col.isValid() || !discount_col.isValid() ||
        !quantity_col.isValid() || !extendedprice_col.isValid()) {
        std::cerr << "Failed to load columns" << std::endl;
        return;
    }

    size_t row_count = shipdate_col.row_count;

    // Date range: 1994-01-01 to 1995-01-01 (exclusive)
    // 1994-01-01 = 8766 days since epoch (1970-01-01)
    // 1995-01-01 = 9131 days since epoch
    const int32_t date_start = dateToEpochDays(1994, 1, 1);
    const int32_t date_end = dateToEpochDays(1995, 1, 1);

    // Discount range: [0.05, 0.07]
    const double discount_min = 0.05;
    const double discount_max = 0.07;

    // Quantity threshold: < 24
    const double quantity_max = 24.0;

    // Parallel execution with thread-local aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<double> morsel_results;
    std::mutex results_mutex;

    // Function to process a range of rows
    auto process_range = [&](uint64_t start_row, uint64_t end_row) {
        double local_sum = 0.0;
        uint64_t local_count = 0;

        for (uint64_t i = start_row; i < end_row; ++i) {
            // Apply all predicates
            int32_t shipdate = shipdate_col.data[i];
            double discount = discount_col.data[i];
            double quantity = quantity_col.data[i];
            double extendedprice = extendedprice_col.data[i];

            // Check all conditions
            if (shipdate >= date_start && shipdate < date_end &&
                discount >= discount_min && discount <= discount_max &&
                quantity < quantity_max) {
                // Accumulate revenue
                local_sum += extendedprice * discount;
                local_count++;
            }
        }

        {
            std::lock_guard<std::mutex> lock(results_mutex);
            morsel_results.push_back(local_sum);
        }
    };

    // Distribute work across threads using morsel-driven parallelism
    const size_t morsel_size = 200000;  // 200K rows per morsel
    std::vector<std::thread> threads;

    for (uint64_t start = 0; start < row_count; start += morsel_size) {
        uint64_t end = std::min(start + morsel_size, row_count);

        // Wait for oldest thread if we've queued enough
        if (threads.size() >= num_threads) {
            threads[0].join();
            threads.erase(threads.begin());
        }

        threads.emplace_back(process_range, start, end);
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    // Aggregate results
    double final_sum = 0.0;
    for (double result : morsel_results) {
        final_sum += result;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Output result
    std::cout << "Query returned 1 row" << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;
    std::cout << "Revenue: " << std::fixed << std::setprecision(2) << final_sum << std::endl;

    // Write result to CSV if results_dir is specified
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/q6_results.csv");
        outfile << "revenue" << std::endl;
        outfile << std::fixed << std::setprecision(2) << final_sum << std::endl;
        outfile.close();
    }
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
