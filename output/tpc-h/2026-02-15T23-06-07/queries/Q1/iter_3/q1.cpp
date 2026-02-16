#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <unordered_map>
#include <algorithm>
#include <iomanip>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <mutex>
#include <immintrin.h>

// [METADATA CHECK]
// Query: Q1 (Pricing Summary Report)
// Tables: lineitem (59,986,052 rows)
// Columns needed:
//   - l_shipdate: int32_t (DATE, days since epoch)
//   - l_returnflag: int32_t (dictionary-encoded STRING)
//   - l_linestatus: int32_t (dictionary-encoded STRING)
//   - l_quantity: int64_t (DECIMAL, scale_factor=100)
//   - l_extendedprice: int64_t (DECIMAL, scale_factor=100)
//   - l_discount: int64_t (DECIMAL, scale_factor=100)
//   - l_tax: int64_t (DECIMAL, scale_factor=100)
// Predicate: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02' (epoch day 10471)
// Aggregation: GROUP BY (l_returnflag, l_linestatus) - ~6 groups expected

// Helper: Calculate epoch days from YYYY-MM-DD
constexpr int32_t date_to_epoch(int year, int month, int day) {
    int32_t days = 0;
    // Days from complete years (1970 to year-1)
    for (int y = 1970; y < year; ++y) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }
    // Days from complete months (1 to month-1) in target year
    const int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for (int m = 1; m < month; ++m) {
        days += month_days[m - 1];
        if (m == 2 && leap) days += 1;
    }
    // Add day of month (1-indexed, so subtract 1)
    days += (day - 1);
    return days;
}

// Aggregation state for each group
struct AggState {
    int64_t sum_qty_scaled = 0;           // scale = 100
    int64_t sum_base_price_scaled = 0;    // scale = 100
    __int128 sum_disc_price_scaled2 = 0;  // scale = 100^2 = 10000
    __int128 sum_charge_scaled3 = 0;      // scale = 100^3 = 1000000
    int64_t sum_discount_scaled = 0;      // scale = 100
    int64_t count = 0;
};

// Result row for output
struct ResultRow {
    std::string returnflag;
    std::string linestatus;
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double avg_qty;
    double avg_price;
    double avg_disc;
    int64_t count_order;
};

// Memory-mapped file wrapper
template<typename T>
class MmapFile {
public:
    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            exit(1);
        }
        size = sb.st_size / sizeof(T);
        data = static_cast<const T*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }
    }

    ~MmapFile() {
        if (data != MAP_FAILED) munmap((void*)data, size * sizeof(T));
        if (fd >= 0) close(fd);
    }

    const T* data;
    size_t size;
    int fd;
};

// Load dictionary from _dict.txt
std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Zone map entry structure
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t start_row;
    uint32_t row_count;
};

// Load zone map index
std::vector<ZoneMapEntry> load_zone_map(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open zone map " << path << std::endl;
        exit(1);
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat zone map " << path << std::endl;
        exit(1);
    }

    const uint8_t* data = static_cast<const uint8_t*>(
        mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap zone map " << path << std::endl;
        exit(1);
    }

    // Read number of entries (first 4 bytes)
    uint32_t num_entries = *reinterpret_cast<const uint32_t*>(data);

    // Read entries (16 bytes each: min, max, start_row, row_count)
    std::vector<ZoneMapEntry> entries;
    entries.reserve(num_entries);

    const uint8_t* ptr = data + 4;
    for (uint32_t i = 0; i < num_entries; ++i) {
        ZoneMapEntry entry;
        entry.min_val = *reinterpret_cast<const int32_t*>(ptr);
        ptr += 4;
        entry.max_val = *reinterpret_cast<const int32_t*>(ptr);
        ptr += 4;
        entry.start_row = *reinterpret_cast<const uint32_t*>(ptr);
        ptr += 4;
        entry.row_count = *reinterpret_cast<const uint32_t*>(ptr);
        ptr += 4;
        entries.push_back(entry);
    }

    munmap((void*)data, sb.st_size);
    close(fd);

    return entries;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    std::cout << "[METADATA CHECK] Q1 - Pricing Summary Report" << std::endl;
    std::cout << "  Table: lineitem (59,986,052 rows)" << std::endl;
    std::cout << "  Encodings:" << std::endl;
    std::cout << "    l_shipdate: int32_t (DATE, days since epoch)" << std::endl;
    std::cout << "    l_returnflag: int32_t (dictionary-encoded STRING)" << std::endl;
    std::cout << "    l_linestatus: int32_t (dictionary-encoded STRING)" << std::endl;
    std::cout << "    l_quantity: int64_t (DECIMAL, scale=100)" << std::endl;
    std::cout << "    l_extendedprice: int64_t (DECIMAL, scale=100)" << std::endl;
    std::cout << "    l_discount: int64_t (DECIMAL, scale=100)" << std::endl;
    std::cout << "    l_tax: int64_t (DECIMAL, scale=100)" << std::endl;

    // Compute predicate threshold: '1998-12-01' - 90 days = '1998-09-02'
    const int32_t threshold_date = date_to_epoch(1998, 9, 2);
    std::cout << "  Predicate: l_shipdate <= " << threshold_date << " (1998-09-02)" << std::endl;

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load data
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    std::string lineitem_dir = gendb_dir + "/lineitem/";
    MmapFile<int32_t> l_shipdate(lineitem_dir + "l_shipdate.bin");
    MmapFile<int32_t> l_returnflag(lineitem_dir + "l_returnflag.bin");
    MmapFile<int32_t> l_linestatus(lineitem_dir + "l_linestatus.bin");
    MmapFile<int64_t> l_quantity(lineitem_dir + "l_quantity.bin");
    MmapFile<int64_t> l_extendedprice(lineitem_dir + "l_extendedprice.bin");
    MmapFile<int64_t> l_discount(lineitem_dir + "l_discount.bin");
    MmapFile<int64_t> l_tax(lineitem_dir + "l_tax.bin");

    auto returnflag_dict = load_dictionary(lineitem_dir + "l_returnflag_dict.txt");
    auto linestatus_dict = load_dictionary(lineitem_dir + "l_linestatus_dict.txt");

    size_t num_rows = l_shipdate.size;
    std::cout << "  Loaded " << num_rows << " rows" << std::endl;

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Load zone map for l_shipdate
    std::string zonemap_path = gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin";
    auto zone_map = load_zone_map(zonemap_path);

    // Scan, filter, and aggregate
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Low-cardinality optimization: use flat array instead of hash table
    // returnflag has ~3 values (0-2), linestatus has ~2 values (0-1)
    // Max groups = returnflag_dict.size() * linestatus_dict.size()
    const size_t rf_card = returnflag_dict.size();
    const size_t ls_card = linestatus_dict.size();
    const size_t max_groups = rf_card * ls_card;

    // Thread-local aggregation: each thread maintains its own flat array
    const unsigned num_threads = std::thread::hardware_concurrency();
    std::vector<std::vector<AggState>> thread_agg_arrays(num_threads,
                                                          std::vector<AggState>(max_groups));

    // Morsel-driven parallelism: process zone map blocks in parallel
    std::atomic<size_t> next_block_idx{0};

    auto worker = [&](unsigned thread_id) {
        auto& local_agg = thread_agg_arrays[thread_id];

        while (true) {
            size_t block_idx = next_block_idx.fetch_add(1, std::memory_order_relaxed);
            if (block_idx >= zone_map.size()) break;

            const auto& zone = zone_map[block_idx];

            // Zone map pruning: skip block if min > threshold
            // For l_shipdate <= threshold_date: skip if min_val > threshold_date
            if (zone.min_val > threshold_date) continue;

            size_t start = zone.start_row;
            size_t end = zone.start_row + zone.row_count;

            const int32_t* shipdate_ptr = l_shipdate.data + start;
            const int32_t* rf_ptr = l_returnflag.data + start;
            const int32_t* ls_ptr = l_linestatus.data + start;
            const int64_t* qty_ptr = l_quantity.data + start;
            const int64_t* price_ptr = l_extendedprice.data + start;
            const int64_t* disc_ptr = l_discount.data + start;
            const int64_t* tax_ptr = l_tax.data + start;

            size_t block_size = end - start;

            // SIMD vectorized filtering (AVX2: 8 x int32 per iteration)
            size_t i = 0;
            __m256i threshold_vec = _mm256_set1_epi32(threshold_date);

            // Process 8 rows at a time with SIMD
            for (; i + 8 <= block_size; i += 8) {
                __m256i shipdate_vec = _mm256_loadu_si256((__m256i*)(shipdate_ptr + i));

                // Compare: shipdate <= threshold (using GT and inverting)
                // AVX2 has cmpgt: shipdate > threshold, we want !(shipdate > threshold)
                __m256i mask = _mm256_cmpgt_epi32(shipdate_vec, threshold_vec);

                // Get bitmask (1 bit per int32)
                int bitmask = _mm256_movemask_ps(_mm256_castsi256_ps(mask));

                // Process matching rows (bit == 0 means match)
                for (int lane = 0; lane < 8; ++lane) {
                    if ((bitmask & (1 << lane)) == 0) {
                        // Row passes filter
                        int32_t rf = rf_ptr[i + lane];
                        int32_t ls = ls_ptr[i + lane];
                        int64_t qty = qty_ptr[i + lane];
                        int64_t price = price_ptr[i + lane];
                        int64_t disc = disc_ptr[i + lane];
                        int64_t tax = tax_ptr[i + lane];

                        // Direct array indexing (perfect hashing for low cardinality)
                        size_t idx = rf * ls_card + ls;

                        AggState& state = local_agg[idx];
                        state.sum_qty_scaled += qty;
                        state.sum_base_price_scaled += price;

                        __int128 disc_price = static_cast<__int128>(price) * (100 - disc);
                        state.sum_disc_price_scaled2 += disc_price;

                        __int128 charge = disc_price * (100 + tax);
                        state.sum_charge_scaled3 += charge;

                        state.sum_discount_scaled += disc;
                        state.count += 1;
                    }
                }
            }

            // Scalar tail handling for remaining rows
            for (; i < block_size; ++i) {
                int32_t shipdate = shipdate_ptr[i];
                if (shipdate > threshold_date) continue;

                int32_t rf = rf_ptr[i];
                int32_t ls = ls_ptr[i];
                int64_t qty = qty_ptr[i];
                int64_t price = price_ptr[i];
                int64_t disc = disc_ptr[i];
                int64_t tax = tax_ptr[i];

                // Direct array indexing (perfect hashing for low cardinality)
                size_t idx = rf * ls_card + ls;

                AggState& state = local_agg[idx];
                state.sum_qty_scaled += qty;
                state.sum_base_price_scaled += price;

                __int128 disc_price = static_cast<__int128>(price) * (100 - disc);
                state.sum_disc_price_scaled2 += disc_price;

                __int128 charge = disc_price * (100 + tax);
                state.sum_charge_scaled3 += charge;

                state.sum_discount_scaled += disc;
                state.count += 1;
            }
        }
    };

    // Launch worker threads
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local aggregation results (array-based merge)
    std::vector<AggState> final_agg(max_groups);
    for (const auto& local_agg : thread_agg_arrays) {
        for (size_t idx = 0; idx < max_groups; ++idx) {
            const auto& state = local_agg[idx];
            if (state.count > 0) {  // Only merge non-empty groups
                auto& merged = final_agg[idx];
                merged.sum_qty_scaled += state.sum_qty_scaled;
                merged.sum_base_price_scaled += state.sum_base_price_scaled;
                merged.sum_disc_price_scaled2 += state.sum_disc_price_scaled2;
                merged.sum_charge_scaled3 += state.sum_charge_scaled3;
                merged.sum_discount_scaled += state.sum_discount_scaled;
                merged.count += state.count;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);
#endif

    // Prepare results
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    for (size_t idx = 0; idx < max_groups; ++idx) {
        const auto& state = final_agg[idx];
        if (state.count == 0) continue;  // Skip empty groups

        // Decode array index back to group keys
        int32_t rf = idx / ls_card;
        int32_t ls = idx % ls_card;

        ResultRow row;
        row.returnflag = returnflag_dict[rf];
        row.linestatus = linestatus_dict[ls];
        row.sum_qty = state.sum_qty_scaled / 100.0;
        row.sum_base_price = state.sum_base_price_scaled / 100.0;

        // Scale down from scale^2 to scale^0
        row.sum_disc_price = static_cast<double>(state.sum_disc_price_scaled2) / 10000.0;

        // Scale down from scale^3 to scale^0
        row.sum_charge = static_cast<double>(state.sum_charge_scaled3) / 1000000.0;

        row.avg_qty = state.sum_qty_scaled / (100.0 * state.count);
        row.avg_price = state.sum_base_price_scaled / (100.0 * state.count);
        row.avg_disc = state.sum_discount_scaled / (100.0 * state.count);
        row.count_order = state.count;

        results.push_back(row);
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
#endif

    // Sort by returnflag, linestatus
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);

    // Header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows
    for (const auto& row : results) {
        out << row.returnflag << ","
            << row.linestatus << ","
            << std::fixed << std::setprecision(2) << row.sum_qty << ","
            << std::fixed << std::setprecision(2) << row.sum_base_price << ","
            << std::fixed << std::setprecision(4) << row.sum_disc_price << ","
            << std::fixed << std::setprecision(6) << row.sum_charge << ","
            << std::fixed << std::setprecision(2) << row.avg_qty << ","
            << std::fixed << std::setprecision(2) << row.avg_price << ","
            << std::fixed << std::setprecision(2) << row.avg_disc << ","
            << row.count_order << "\n";
    }

    out.close();
    std::cout << "Results written to " << output_path << std::endl;

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
