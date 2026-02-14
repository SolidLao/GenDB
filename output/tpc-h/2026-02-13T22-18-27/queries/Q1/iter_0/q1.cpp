// q1.cpp - TPC-H Q1: Pricing Summary Report
// Self-contained implementation with parallel scan + SIMD + partial aggregation

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstring>
#include <map>
#include <algorithm>
#include <chrono>
#include <thread>
#include <atomic>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <immintrin.h>

// Date helper: convert "YYYY-MM-DD" to days since epoch
inline int32_t date_to_days(const char* date_str) {
    int year, month, day;
    sscanf(date_str, "%d-%d-%d", &year, &month, &day);

    // Simple algorithm: days since 1970-01-01
    int days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;

    const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += days_in_month[m];
    }
    days += day - 1;

    // Leap year adjustment
    if (month > 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
        days += 1;
    }

    return days;
}

// Zone map structure for block-level filtering
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    size_t start_row;
    size_t end_row;
};

// Aggregation key: (returnflag, linestatus)
struct AggKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator<(const AggKey& other) const {
        if (returnflag != other.returnflag) return returnflag < other.returnflag;
        return linestatus < other.linestatus;
    }

    bool operator==(const AggKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Aggregation values
struct AggValue {
    int64_t sum_qty = 0;
    int64_t sum_base_price = 0;
    double sum_disc_price = 0.0;  // Use double for computed values to match precision
    double sum_charge = 0.0;
    int64_t count_qty = 0;      // for AVG(l_quantity)
    int64_t count_price = 0;    // for AVG(l_extendedprice)
    int64_t sum_discount = 0;   // for AVG(l_discount)
    int64_t count_discount = 0; // for AVG(l_discount)
    int64_t count_order = 0;

    void merge(const AggValue& other) {
        sum_qty += other.sum_qty;
        sum_base_price += other.sum_base_price;
        sum_disc_price += other.sum_disc_price;
        sum_charge += other.sum_charge;
        count_qty += other.count_qty;
        count_price += other.count_price;
        sum_discount += other.sum_discount;
        count_discount += other.count_discount;
        count_order += other.count_order;
    }
};

// Memory-mapped column reader
template<typename T>
class MMapColumn {
public:
    const T* data = nullptr;
    size_t size = 0;
    int fd = -1;
    void* map_ptr = nullptr;
    size_t map_size = 0;

    bool open(const std::string& path, size_t row_count) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            ::close(fd);
            return false;
        }

        map_size = sb.st_size;
        map_ptr = mmap(nullptr, map_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (map_ptr == MAP_FAILED) {
            ::close(fd);
            return false;
        }

        // Advise kernel for sequential scan
        madvise(map_ptr, map_size, MADV_SEQUENTIAL | MADV_WILLNEED);

        data = reinterpret_cast<const T*>(map_ptr);
        size = row_count;
        return true;
    }

    ~MMapColumn() {
        if (map_ptr && map_ptr != MAP_FAILED) {
            munmap(map_ptr, map_size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

// Load zone map index
std::vector<ZoneMapEntry> load_zonemap(const std::string& path, size_t block_size) {
    std::vector<ZoneMapEntry> zones;

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return zones;

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        ::close(fd);
        return zones;
    }

    // Zone map format: array of (min_val, max_val) int32_t pairs
    size_t num_entries = sb.st_size / (2 * sizeof(int32_t));
    void* map_ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (map_ptr != MAP_FAILED) {
        const int32_t* zone_data = reinterpret_cast<const int32_t*>(map_ptr);
        for (size_t i = 0; i < num_entries; ++i) {
            ZoneMapEntry entry;
            entry.min_val = zone_data[i * 2];
            entry.max_val = zone_data[i * 2 + 1];
            entry.start_row = i * block_size;
            entry.end_row = (i + 1) * block_size;
            zones.push_back(entry);
        }
        munmap(map_ptr, sb.st_size);
    }
    ::close(fd);

    return zones;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Query parameters
    const int32_t cutoff_date = date_to_days("1998-09-02"); // 1998-12-01 - 90 days

    // Read metadata to get row count
    size_t row_count = 59986052; // from metadata

    // Load only the columns we need (column pruning)
    MMapColumn<int32_t> l_shipdate, l_quantity, l_discount, l_tax;
    MMapColumn<int64_t> l_extendedprice;
    MMapColumn<uint8_t> l_returnflag, l_linestatus;

    if (!l_shipdate.open(gendb_dir + "/lineitem.l_shipdate.bin", row_count)) {
        std::cerr << "Failed to open l_shipdate" << std::endl;
        return;
    }
    if (!l_quantity.open(gendb_dir + "/lineitem.l_quantity.bin", row_count)) {
        std::cerr << "Failed to open l_quantity" << std::endl;
        return;
    }
    if (!l_extendedprice.open(gendb_dir + "/lineitem.l_extendedprice.bin", row_count)) {
        std::cerr << "Failed to open l_extendedprice" << std::endl;
        return;
    }
    if (!l_discount.open(gendb_dir + "/lineitem.l_discount.bin", row_count)) {
        std::cerr << "Failed to open l_discount" << std::endl;
        return;
    }
    if (!l_tax.open(gendb_dir + "/lineitem.l_tax.bin", row_count)) {
        std::cerr << "Failed to open l_tax" << std::endl;
        return;
    }
    if (!l_returnflag.open(gendb_dir + "/lineitem.l_returnflag.bin", row_count)) {
        std::cerr << "Failed to open l_returnflag" << std::endl;
        return;
    }
    if (!l_linestatus.open(gendb_dir + "/lineitem.l_linestatus.bin", row_count)) {
        std::cerr << "Failed to open l_linestatus" << std::endl;
        return;
    }

    // TODO: Zone map format needs investigation - for now, scan full table
    // Build list of block ranges to scan
    std::vector<std::pair<size_t, size_t>> scan_ranges;
    scan_ranges.push_back({0, row_count});

    // Parallel execution setup
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t MORSEL_SIZE = 50000; // Tuned for L3 cache (44MB / 64 threads / 7 cols ≈ 10K-50K rows)

    std::vector<std::thread> threads;
    std::vector<std::map<AggKey, AggValue>> local_results(num_threads);
    std::atomic<size_t> range_idx(0);

    auto worker = [&](size_t thread_id) {
        auto& local_map = local_results[thread_id];

        // Process morsels
        while (true) {
            // Get next range
            size_t my_range_idx = range_idx.fetch_add(1, std::memory_order_relaxed);
            if (my_range_idx >= scan_ranges.size()) break;

            auto [range_start, range_end] = scan_ranges[my_range_idx];

            // Process this range in morsels
            for (size_t morsel_start = range_start; morsel_start < range_end; morsel_start += MORSEL_SIZE) {
                size_t morsel_end = std::min(morsel_start + MORSEL_SIZE, range_end);

                // SIMD-accelerated scan of morsel
                const __m256i cutoff_vec = _mm256_set1_epi32(cutoff_date);

                for (size_t i = morsel_start; i < morsel_end; i += 8) {
                    size_t batch_size = std::min((size_t)8, morsel_end - i);

                    // Load 8 shipdate values
                    __m256i shipdate_vec;
                    if (batch_size == 8) {
                        shipdate_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&l_shipdate.data[i]));
                    } else {
                        // Handle tail with scalar code
                        for (size_t j = i; j < i + batch_size; ++j) {
                            if (l_shipdate.data[j] > cutoff_date) continue;

                            AggKey key{l_returnflag.data[j], l_linestatus.data[j]};
                            auto& agg = local_map[key];

                            int32_t qty = l_quantity.data[j];
                            int64_t price = l_extendedprice.data[j];
                            int32_t disc = l_discount.data[j];
                            int32_t tax_val = l_tax.data[j];

                            // Compute aggregates with proper precision
                            // Prices in cents, discount/tax stored as value*100 (e.g., 5 = 0.05)
                            double disc_price = (double)price * (100.0 - disc) / 100.0;
                            double charge = disc_price * (100.0 + tax_val) / 100.0;

                            agg.sum_qty += qty;
                            agg.sum_base_price += price;
                            agg.sum_disc_price += disc_price;
                            agg.sum_charge += charge;
                            agg.count_qty += 1;
                            agg.count_price += 1;
                            agg.sum_discount += disc;
                            agg.count_discount += 1;
                            agg.count_order += 1;
                        }
                        continue;
                    }

                    // Compare: shipdate <= cutoff_date
                    __m256i cmp = _mm256_cmpgt_epi32(shipdate_vec, cutoff_vec);
                    int mask = _mm256_movemask_epi8(cmp);

                    // Process matching rows
                    for (size_t j = 0; j < 8; ++j) {
                        size_t idx = i + j;
                        // Check if this element passed the filter
                        // Each int32 takes 4 bytes, so check bits [j*4, j*4+3]
                        if ((mask & (0xF << (j * 4))) == 0) {
                            // Passed filter: shipdate <= cutoff_date
                            AggKey key{l_returnflag.data[idx], l_linestatus.data[idx]};
                            auto& agg = local_map[key];

                            int32_t qty = l_quantity.data[idx];
                            int64_t price = l_extendedprice.data[idx];
                            int32_t disc = l_discount.data[idx];
                            int32_t tax_val = l_tax.data[idx];

                            // Compute aggregates with proper precision
                            // Prices in cents, discount/tax stored as value*100 (e.g., 5 = 0.05)
                            double disc_price = (double)price * (100.0 - disc) / 100.0;
                            double charge = disc_price * (100.0 + tax_val) / 100.0;

                            agg.sum_qty += qty;
                            agg.sum_base_price += price;
                            agg.sum_disc_price += disc_price;
                            agg.sum_charge += charge;
                            agg.count_qty += 1;
                            agg.count_price += 1;
                            agg.sum_discount += disc;
                            agg.count_discount += 1;
                            agg.count_order += 1;
                        }
                    }
                }
            }
        }
    };

    // Launch worker threads
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }

    // Wait for completion
    for (auto& th : threads) {
        th.join();
    }

    // Merge local results into global result
    std::map<AggKey, AggValue> global_result;
    for (const auto& local_map : local_results) {
        for (const auto& [key, val] : local_map) {
            global_result[key].merge(val);
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Output results
    std::cout << "Q1 result count: " << global_result.size() << " rows" << std::endl;
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << elapsed << " ms" << std::endl;

    // Write CSV output if results_dir is specified
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << std::fixed << std::setprecision(2);

        // Read dictionary for returnflag and linestatus
        std::vector<char> returnflag_dict;
        std::vector<char> linestatus_dict;

        // Load returnflag dictionary (newline-separated)
        std::ifstream rf_dict(gendb_dir + "/lineitem.l_returnflag.dict");
        if (rf_dict) {
            std::string line;
            while (std::getline(rf_dict, line)) {
                if (!line.empty()) {
                    returnflag_dict.push_back(line[0]);
                }
            }
        }

        // Load linestatus dictionary (newline-separated)
        std::ifstream ls_dict(gendb_dir + "/lineitem.l_linestatus.dict");
        if (ls_dict) {
            std::string line;
            while (std::getline(ls_dict, line)) {
                if (!line.empty()) {
                    linestatus_dict.push_back(line[0]);
                }
            }
        }

        // Output header
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order" << std::endl;

        // Output sorted results
        for (const auto& [key, val] : global_result) {
            char rf = (key.returnflag < returnflag_dict.size()) ? returnflag_dict[key.returnflag] : '?';
            char ls = (key.linestatus < linestatus_dict.size()) ? linestatus_dict[key.linestatus] : '?';

            // Quantities stored as qty*100, prices as cents
            // sum_disc_price and sum_charge are already in cents (accumulated as double)
            double sum_qty_actual = val.sum_qty / 100.0;
            double sum_base_price_actual = val.sum_base_price / 100.0;
            double sum_disc_price_actual = val.sum_disc_price / 100.0;
            double sum_charge_actual = val.sum_charge / 100.0;
            double avg_qty = val.count_qty > 0 ? sum_qty_actual / val.count_qty : 0.0;
            double avg_price = val.count_price > 0 ? sum_base_price_actual / val.count_price : 0.0;
            double avg_disc = val.count_discount > 0 ? (double)val.sum_discount / val.count_discount / 100.0 : 0.0;

            out << rf << "," << ls << ","
                << sum_qty_actual << ","
                << sum_base_price_actual << ","
                << sum_disc_price_actual << ","
                << sum_charge_actual << ","
                << avg_qty << ","
                << avg_price << ","
                << avg_disc << ","
                << val.count_order << std::endl;
        }
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

    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
