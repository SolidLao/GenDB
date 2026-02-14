// q6.cpp - Self-contained TPC-H Q6 implementation
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <iomanip>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <immintrin.h>

// Zone map entry structure
struct ZoneMapEntry {
    int32_t min_date;
    int32_t max_date;
    uint64_t start_row;
    uint64_t end_row;
};

// Memory-mapped column wrapper
template<typename T>
class MMapColumn {
public:
    const T* data = nullptr;
    size_t size = 0;

    bool load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            return false;
        }

        size = st.st_size / sizeof(T);
        void* addr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);

        if (addr == MAP_FAILED) {
            std::cerr << "mmap failed for: " << path << std::endl;
            return false;
        }

        data = static_cast<const T*>(addr);

        // Hint sequential access
        madvise(addr, st.st_size, MADV_SEQUENTIAL);

        return true;
    }

    ~MMapColumn() {
        if (data) {
            munmap(const_cast<T*>(data), size * sizeof(T));
        }
    }
};

// Load zone map from binary file
std::vector<ZoneMapEntry> load_zone_map(const std::string& path) {
    std::vector<ZoneMapEntry> zones;

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return zones; // Return empty if no zone map
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return zones;
    }

    size_t entry_size = sizeof(int32_t) * 2 + sizeof(uint64_t) * 2;
    size_t num_entries = st.st_size / entry_size;

    void* addr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        return zones;
    }

    const char* ptr = static_cast<const char*>(addr);
    zones.reserve(num_entries);

    for (size_t i = 0; i < num_entries; ++i) {
        ZoneMapEntry entry;
        memcpy(&entry.min_date, ptr, sizeof(int32_t));
        ptr += sizeof(int32_t);
        memcpy(&entry.max_date, ptr, sizeof(int32_t));
        ptr += sizeof(int32_t);
        memcpy(&entry.start_row, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        memcpy(&entry.end_row, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);

        zones.push_back(entry);
    }

    munmap(addr, st.st_size);
    return zones;
}

// Worker function for parallel scan
void scan_worker(
    const int32_t* shipdate,
    const double* discount,
    const double* quantity,
    const double* extendedprice,
    const std::vector<std::pair<size_t, size_t>>& ranges,
    std::atomic<size_t>& range_idx,
    std::vector<double>& thread_sums,
    size_t thread_id,
    int32_t min_date,
    int32_t max_date,
    double min_discount,
    double max_discount,
    double max_quantity
) {
    double local_sum = 0.0;

    while (true) {
        size_t idx = range_idx.fetch_add(1, std::memory_order_relaxed);
        if (idx >= ranges.size()) break;

        size_t start = ranges[idx].first;
        size_t end = ranges[idx].second;

        // Branch-free scalar processing (compiler auto-vectorization friendly)
        // Simple loop structure helps g++ -O3 -march=native auto-vectorize
        #pragma GCC ivdep
        for (size_t i = start; i < end; ++i) {
            // Short-circuit evaluation: most selective predicates first
            bool pass_date = (shipdate[i] >= min_date) & (shipdate[i] < max_date);
            bool pass_discount = (discount[i] >= min_discount) & (discount[i] <= max_discount);
            bool pass_quantity = (quantity[i] < max_quantity);

            // Branch-free accumulation
            double passes = static_cast<double>(pass_date & pass_discount & pass_quantity);
            local_sum += extendedprice[i] * discount[i] * passes;
        }
    }

    thread_sums[thread_id] = local_sum;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load columns
    MMapColumn<int32_t> l_shipdate;
    MMapColumn<double> l_discount;
    MMapColumn<double> l_quantity;
    MMapColumn<double> l_extendedprice;

    if (!l_shipdate.load(gendb_dir + "/lineitem_l_shipdate.bin") ||
        !l_discount.load(gendb_dir + "/lineitem_l_discount.bin") ||
        !l_quantity.load(gendb_dir + "/lineitem_l_quantity.bin") ||
        !l_extendedprice.load(gendb_dir + "/lineitem_l_extendedprice.bin")) {
        std::cerr << "Failed to load columns" << std::endl;
        return;
    }

    size_t row_count = l_shipdate.size;

    // Query predicates
    // 1994-01-01 to 1995-01-01 (exclusive)
    // Days since 1970-01-01: 1994-01-01 = 8766, 1995-01-01 = 9131
    int32_t min_date = 8766;
    int32_t max_date = 9131;
    double min_discount = 0.05;
    double max_discount = 0.07;
    double max_quantity = 24.0;

    // Load zone map and determine qualifying blocks
    auto zone_map = load_zone_map(gendb_dir + "/lineitem_l_shipdate_zonemap.idx");

    std::vector<std::pair<size_t, size_t>> scan_ranges;

    if (!zone_map.empty()) {
        // Use zone map to skip blocks
        for (const auto& zone : zone_map) {
            // Skip block if zone doesn't overlap with query range
            if (zone.max_date < min_date || zone.min_date >= max_date) {
                continue;
            }
            scan_ranges.emplace_back(zone.start_row, zone.end_row);
        }
    } else {
        // No zone map - scan full table in 10K row morsels
        const size_t morsel_size = 10000;
        for (size_t start = 0; start < row_count; start += morsel_size) {
            size_t end = std::min(start + morsel_size, row_count);
            scan_ranges.emplace_back(start, end);
        }
    }

    // Parallel execution
    size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<double> thread_sums(num_threads, 0.0);
    std::atomic<size_t> range_idx{0};

    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(
            scan_worker,
            l_shipdate.data,
            l_discount.data,
            l_quantity.data,
            l_extendedprice.data,
            std::cref(scan_ranges),
            std::ref(range_idx),
            std::ref(thread_sums),
            i,
            min_date,
            max_date,
            min_discount,
            max_discount,
            max_quantity
        );
    }

    for (auto& t : threads) {
        t.join();
    }

    // Aggregate results
    double revenue = 0.0;
    for (double sum : thread_sums) {
        revenue += sum;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Output results
    std::cout << "Q6 Results:" << std::endl;
    std::cout << "Revenue: " << std::fixed << std::setprecision(2) << revenue << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;

    // Write to file if results_dir specified
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << revenue << "\n";
        out.close();
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
