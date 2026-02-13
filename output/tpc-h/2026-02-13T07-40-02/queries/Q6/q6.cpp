// Q6: Forecasting Revenue Change - Self-contained implementation
// Single-table scan with range predicates on lineitem

#include <iostream>
#include <fstream>
#include <iomanip>
#include <string>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <vector>
#include <chrono>
#include <cmath>

// Date utilities - inline for this query
inline int32_t date_to_days(int year, int month, int day) {
    // Days since Unix epoch (1970-01-01)
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 719469;
}

// Memory-mapped column loader
template<typename T>
class MMapColumn {
private:
    int fd;
    void* mapped;
    size_t size;
    const T* data;

public:
    MMapColumn() : fd(-1), mapped(nullptr), size(0), data(nullptr) {}

    ~MMapColumn() {
        if (mapped && mapped != MAP_FAILED) {
            munmap(mapped, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool load(const std::string& path, size_t row_count) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        size = row_count * sizeof(T);
        mapped = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            close(fd);
            fd = -1;
            return false;
        }

        // Hint for sequential access
        madvise(mapped, size, MADV_SEQUENTIAL);
        madvise(mapped, size, MADV_WILLNEED);

        data = static_cast<const T*>(mapped);
        return true;
    }

    const T& operator[](size_t idx) const { return data[idx]; }
    const T* get() const { return data; }
};

// Parallel scan worker
struct ScanWorker {
    const int32_t* l_shipdate;
    const int64_t* l_discount;
    const int64_t* l_quantity;
    const int64_t* l_extendedprice;
    size_t row_count;
    std::atomic<size_t>& next_morsel;
    size_t morsel_size;
    int32_t date_start;
    int32_t date_end;
    int64_t discount_min;
    int64_t discount_max;
    int64_t quantity_max;

    int64_t local_revenue;

    ScanWorker(const int32_t* sd, const int64_t* disc, const int64_t* qty,
               const int64_t* price, size_t rc, std::atomic<size_t>& nm, size_t ms,
               int32_t ds, int32_t de, int64_t dmin, int64_t dmax, int64_t qmax)
        : l_shipdate(sd), l_discount(disc), l_quantity(qty), l_extendedprice(price),
          row_count(rc), next_morsel(nm), morsel_size(ms),
          date_start(ds), date_end(de), discount_min(dmin), discount_max(dmax),
          quantity_max(qmax), local_revenue(0) {}

    void operator()() {
        while (true) {
            size_t morsel_id = next_morsel.fetch_add(1, std::memory_order_relaxed);
            size_t start = morsel_id * morsel_size;
            if (start >= row_count) break;

            size_t end = std::min(start + morsel_size, row_count);

            // Tight scan loop with fused filters
            int64_t revenue = 0;
            for (size_t i = start; i < end; ++i) {
                int32_t shipdate = l_shipdate[i];
                // Early rejection on date range (most selective typically)
                if (shipdate < date_start || shipdate >= date_end) continue;

                int64_t discount = l_discount[i];
                if (discount < discount_min || discount > discount_max) continue;

                int64_t quantity = l_quantity[i];
                if (quantity >= quantity_max) continue;

                // All filters passed - accumulate revenue
                // Accumulate full product to minimize rounding errors
                // Will convert at the end
                revenue += l_extendedprice[i] * discount;
            }

            local_revenue += revenue;
        }
    }
};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    auto start_time = std::chrono::high_resolution_clock::now();

    // Read metadata to get row count
    std::string lineitem_dir = gendb_dir + "/lineitem";
    std::ifstream meta_file(lineitem_dir + "/metadata.json");
    if (!meta_file) {
        std::cerr << "Failed to open metadata.json" << std::endl;
        return 1;
    }

    size_t row_count = 0;
    std::string line;
    while (std::getline(meta_file, line)) {
        size_t pos = line.find("\"row_count\":");
        if (pos != std::string::npos) {
            size_t num_start = line.find_first_of("0123456789", pos);
            if (num_start != std::string::npos) {
                row_count = std::stoull(line.substr(num_start));
                break;
            }
        }
    }
    meta_file.close();

    if (row_count == 0) {
        std::cerr << "Failed to read row_count from metadata" << std::endl;
        return 1;
    }

    std::cout << "Loading lineitem table: " << row_count << " rows" << std::endl;

    // Load only the 4 columns needed for Q6
    MMapColumn<int32_t> l_shipdate_col;
    MMapColumn<int64_t> l_discount_col;
    MMapColumn<int64_t> l_quantity_col;
    MMapColumn<int64_t> l_extendedprice_col;

    if (!l_shipdate_col.load(lineitem_dir + "/l_shipdate.bin", row_count) ||
        !l_discount_col.load(lineitem_dir + "/l_discount.bin", row_count) ||
        !l_quantity_col.load(lineitem_dir + "/l_quantity.bin", row_count) ||
        !l_extendedprice_col.load(lineitem_dir + "/l_extendedprice.bin", row_count)) {
        std::cerr << "Failed to load columns" << std::endl;
        return 1;
    }

    auto load_time = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(load_time - start_time).count();
    std::cout << "Columns loaded in " << std::fixed << std::setprecision(2) << load_ms << " ms" << std::endl;

    // Query constants
    // l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    int32_t date_start = date_to_days(1994, 1, 1);
    int32_t date_end = date_to_days(1995, 1, 1);

    // l_discount BETWEEN 0.05 AND 0.07 (stored as cents * 100, so 5 and 7)
    int64_t discount_min = 5;  // 0.05 in storage format
    int64_t discount_max = 7;  // 0.07 in storage format

    // l_quantity < 24 (stored as quantity * 100, so 2400)
    int64_t quantity_max = 2400;  // 24.00 in storage format

    // Parallel execution setup
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 64;  // fallback

    // Morsel size: target ~100K rows per morsel for good cache behavior
    size_t morsel_size = 100000;

    std::cout << "Executing Q6 with " << num_threads << " threads, morsel size " << morsel_size << std::endl;

    std::atomic<size_t> next_morsel(0);
    std::vector<std::thread> threads;
    std::vector<ScanWorker> workers;
    workers.reserve(num_threads);

    auto exec_start = std::chrono::high_resolution_clock::now();

    // Launch worker threads
    for (unsigned int i = 0; i < num_threads; ++i) {
        workers.emplace_back(
            l_shipdate_col.get(),
            l_discount_col.get(),
            l_quantity_col.get(),
            l_extendedprice_col.get(),
            row_count,
            next_morsel,
            morsel_size,
            date_start,
            date_end,
            discount_min,
            discount_max,
            quantity_max
        );
    }

    for (unsigned int i = 0; i < num_threads; ++i) {
        threads.emplace_back(std::ref(workers[i]));
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Merge results
    int64_t total_revenue = 0;
    for (const auto& w : workers) {
        total_revenue += w.local_revenue;
    }

    auto exec_end = std::chrono::high_resolution_clock::now();
    double exec_ms = std::chrono::duration<double, std::milli>(exec_end - exec_start).count();

    // Convert revenue from storage format
    // total_revenue = sum((price*100) * (disc*100)) = sum(price * disc) * 10000
    // So we divide by 10000.0 to get actual revenue
    // Using double division to preserve fractional cents
    double revenue = static_cast<double>(total_revenue) / 10000.0;

    // Output results
    std::cout << "\n=== Q6 Results ===" << std::endl;
    std::cout << "Revenue: " << std::fixed << std::setprecision(2) << revenue << std::endl;

    auto end_time = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    std::cout << "\nExecution time: " << std::fixed << std::setprecision(2) << exec_ms << " ms" << std::endl;
    std::cout << "Total time: " << std::fixed << std::setprecision(2) << total_ms << " ms" << std::endl;

    // Write to CSV if results_dir provided
    if (!results_dir.empty()) {
        std::string output_file = results_dir + "/Q6.csv";
        std::ofstream out(output_file);
        if (out) {
            out << "revenue" << std::endl;
            out << std::fixed << std::setprecision(2) << revenue << std::endl;
            out.close();
            std::cout << "Results written to " << output_file << std::endl;
        } else {
            std::cerr << "Failed to write results to " << output_file << std::endl;
        }
    }

    return 0;
}
