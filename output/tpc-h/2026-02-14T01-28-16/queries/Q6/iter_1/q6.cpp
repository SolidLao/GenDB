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

    bool load(const std::string& path, bool prefetch = false) {
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
        file_size = st.st_size;
        void* addr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);

        if (addr == MAP_FAILED) {
            std::cerr << "mmap failed for: " << path << std::endl;
            return false;
        }

        data = static_cast<const T*>(addr);
        mmap_addr = addr;

        // Hint sequential access
        madvise(addr, st.st_size, MADV_SEQUENTIAL);

        // Prefetch data if requested (for HDD optimization)
        if (prefetch) {
            madvise(addr, st.st_size, MADV_WILLNEED);
        }

        return true;
    }

    // Prefetch a specific range
    void prefetch_range(size_t start_row, size_t end_row) {
        if (!data || start_row >= size) return;
        end_row = std::min(end_row, size);
        size_t offset = start_row * sizeof(T);
        size_t length = (end_row - start_row) * sizeof(T);
        madvise(static_cast<char*>(mmap_addr) + offset, length, MADV_WILLNEED);
    }

    void* mmap_addr = nullptr;
    size_t file_size = 0;

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

// Worker function for parallel scan with AVX-512 SIMD
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
    // AVX-512 accumulator (8 doubles)
    __m512d sum_vec = _mm512_setzero_pd();

    // Broadcast predicates for SIMD comparison
    __m256i min_date_vec = _mm256_set1_epi32(min_date);
    __m256i max_date_vec = _mm256_set1_epi32(max_date);
    __m512d min_discount_vec = _mm512_set1_pd(min_discount);
    __m512d max_discount_vec = _mm512_set1_pd(max_discount);
    __m512d max_quantity_vec = _mm512_set1_pd(max_quantity);

    while (true) {
        size_t idx = range_idx.fetch_add(1, std::memory_order_relaxed);
        if (idx >= ranges.size()) break;

        size_t start = ranges[idx].first;
        size_t end = ranges[idx].second;
        size_t i = start;

        // Process in blocks of 8 (AVX-512 processes 8 doubles at once)
        for (; i + 8 <= end; i += 8) {
            // Load 8 dates (int32) - use 256-bit load and extend to 512-bit for comparison
            __m256i dates_256 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&shipdate[i]));
            __m512i dates = _mm512_cvtepi32_epi64(dates_256);
            __m512i min_date_64 = _mm512_cvtepi32_epi64(min_date_vec);
            __m512i max_date_64 = _mm512_cvtepi32_epi64(max_date_vec);

            // Compare dates: shipdate >= min_date AND shipdate < max_date
            __mmask8 date_mask = _mm512_cmp_epi64_mask(dates, min_date_64, _MM_CMPINT_GE);
            date_mask &= _mm512_cmp_epi64_mask(dates, max_date_64, _MM_CMPINT_LT);

            // Load 8 discounts
            __m512d discount_vec = _mm512_loadu_pd(&discount[i]);

            // Compare: discount >= min_discount AND discount <= max_discount
            __mmask8 discount_mask = _mm512_cmp_pd_mask(discount_vec, min_discount_vec, _CMP_GE_OQ);
            discount_mask &= _mm512_cmp_pd_mask(discount_vec, max_discount_vec, _CMP_LE_OQ);

            // Load 8 quantities
            __m512d quantity_vec = _mm512_loadu_pd(&quantity[i]);

            // Compare: quantity < max_quantity
            __mmask8 quantity_mask = _mm512_cmp_pd_mask(quantity_vec, max_quantity_vec, _CMP_LT_OQ);

            // Combine all masks
            __mmask8 final_mask = date_mask & discount_mask & quantity_mask;

            // Load extended prices and compute revenue with mask
            __m512d price_vec = _mm512_loadu_pd(&extendedprice[i]);
            __m512d revenue_vec = _mm512_mul_pd(price_vec, discount_vec);

            // Masked accumulation: only add where final_mask is true
            sum_vec = _mm512_mask_add_pd(sum_vec, final_mask, sum_vec, revenue_vec);
        }

        // Scalar tail processing for remaining elements
        double scalar_sum = 0.0;
        for (; i < end; ++i) {
            bool pass_quantity = (quantity[i] < max_quantity);
            bool pass_discount = (discount[i] >= min_discount) & (discount[i] <= max_discount);
            bool pass_date = (shipdate[i] >= min_date) & (shipdate[i] < max_date);

            if (pass_quantity & pass_discount & pass_date) {
                scalar_sum += extendedprice[i] * discount[i];
            }
        }
        sum_vec = _mm512_add_pd(sum_vec, _mm512_set1_pd(scalar_sum));
    }

    // Horizontal reduction: sum all 8 doubles in the vector
    double local_sum = _mm512_reduce_add_pd(sum_vec);
    thread_sums[thread_id] = local_sum;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load columns with prefetching enabled (HDD optimization)
    MMapColumn<int32_t> l_shipdate;
    MMapColumn<double> l_discount;
    MMapColumn<double> l_quantity;
    MMapColumn<double> l_extendedprice;

    if (!l_shipdate.load(gendb_dir + "/lineitem_l_shipdate.bin", true) ||
        !l_discount.load(gendb_dir + "/lineitem_l_discount.bin", true) ||
        !l_quantity.load(gendb_dir + "/lineitem_l_quantity.bin", true) ||
        !l_extendedprice.load(gendb_dir + "/lineitem_l_extendedprice.bin", true)) {
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

        // Issue targeted prefetch hints for qualified zones
        for (const auto& range : scan_ranges) {
            l_shipdate.prefetch_range(range.first, range.second);
            l_discount.prefetch_range(range.first, range.second);
            l_quantity.prefetch_range(range.first, range.second);
            l_extendedprice.prefetch_range(range.first, range.second);
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
