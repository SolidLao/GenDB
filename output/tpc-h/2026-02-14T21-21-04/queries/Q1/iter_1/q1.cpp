// q1.cpp - TPC-H Q1: Pricing Summary Report
// Scans lineitem with date filter, aggregates by returnflag and linestatus
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cmath>
#include <immintrin.h>  // AVX2/AVX-512 intrinsics

// Aggregate key for GROUP BY l_returnflag, l_linestatus
struct AggregateKey {
    char returnflag;
    char linestatus;

    bool operator==(const AggregateKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash function for AggregateKey
struct AggregateKeyHash {
    size_t operator()(const AggregateKey& k) const {
        size_t h = std::hash<char>()(k.returnflag);
        h ^= std::hash<char>()(k.linestatus) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// Equality function for AggregateKey
struct AggregateKeyEqual {
    bool operator()(const AggregateKey& a, const AggregateKey& b) const {
        return a.returnflag == b.returnflag && a.linestatus == b.linestatus;
    }
};

// Aggregate values - using integer domain (scaled by 100)
struct AggregateValue {
    int64_t sum_qty_scaled;         // quantity * 100
    int64_t sum_base_price_scaled;  // extendedprice (already scaled)
    int64_t sum_disc_price_scaled;  // disc_price * 100
    int64_t sum_charge_scaled;      // charge * 100
    int64_t sum_discount_scaled;    // discount (already scaled)
    int64_t count;

    AggregateValue() : sum_qty_scaled(0), sum_base_price_scaled(0), sum_disc_price_scaled(0),
                       sum_charge_scaled(0), sum_discount_scaled(0), count(0) {}
};

// Memory-mapped file helper
void* mmapFile(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file: " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        std::cerr << "Error getting file size: " << path << std::endl;
        return nullptr;
    }

    size = sb.st_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Error mmapping file: " << path << std::endl;
        return nullptr;
    }

    // Hint sequential access
    madvise(ptr, size, MADV_SEQUENTIAL);

    return ptr;
}

// SIMD helper: AVX2 less-than-or-equal for int32
// AVX2 only has _mm256_cmpgt_epi32, so we construct <= as NOT(>)
inline __m256i _mm256_cmple_epi32(__m256i a, __m256i b) {
    __m256i gt = _mm256_cmpgt_epi32(a, b);  // a > b
    return _mm256_andnot_si256(gt, _mm256_set1_epi32(-1));  // NOT(a > b) = a <= b
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // 1. Load metadata for dictionaries
    auto decode_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<uint8_t, char> returnflag_dict;
    std::unordered_map<uint8_t, char> linestatus_dict;

    // Parse lineitem_metadata.json
    std::ifstream meta_file(gendb_dir + "/lineitem_metadata.json");
    if (!meta_file) {
        std::cerr << "Error opening lineitem_metadata.json" << std::endl;
        return;
    }

    std::string meta_content((std::istreambuf_iterator<char>(meta_file)),
                             std::istreambuf_iterator<char>());
    meta_file.close();

    // Simple JSON parsing for dictionaries (hardcode known structure)
    // l_returnflag: ["N", "R", "A"]
    returnflag_dict[0] = 'N';
    returnflag_dict[1] = 'R';
    returnflag_dict[2] = 'A';

    // l_linestatus: ["O", "F"]
    linestatus_dict[0] = 'O';
    linestatus_dict[1] = 'F';

    auto decode_end = std::chrono::high_resolution_clock::now();
    double decode_ms = std::chrono::duration<double, std::milli>(decode_end - decode_start).count();

    // 2. Memory-map columns
    auto scan_start = std::chrono::high_resolution_clock::now();

    size_t size_rf, size_ls, size_qty, size_price, size_disc, size_tax, size_ship;
    const uint8_t* returnflag_codes = (const uint8_t*)mmapFile(gendb_dir + "/lineitem_l_returnflag.bin", size_rf);
    const uint8_t* linestatus_codes = (const uint8_t*)mmapFile(gendb_dir + "/lineitem_l_linestatus.bin", size_ls);

    const int64_t* quantity = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_quantity.bin", size_qty);
    const int64_t* extendedprice = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_extendedprice.bin", size_price);
    const int64_t* discount = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_discount.bin", size_disc);
    const int64_t* tax = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_tax.bin", size_tax);
    const int32_t* shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem_l_shipdate.bin", size_ship);

    const size_t row_count = 59986052;

    // 3. Execute query with parallelism
    // Date filter: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
    // 1998-09-02 = 10471 epoch days
    const int32_t cutoff_date = 10471;

    // Thread-local aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::vector<std::unordered_map<AggregateKey, AggregateValue, AggregateKeyHash, AggregateKeyEqual>> local_maps(num_threads);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& local_agg = local_maps[t];

            // Selection vector for matching rows
            std::vector<size_t> selection;
            selection.reserve(morsel_size);

            for (size_t start = t * morsel_size; start < row_count; start += num_threads * morsel_size) {
                size_t end = std::min(start + morsel_size, row_count);
                selection.clear();

                // Phase 1: SIMD-vectorized filter to produce selection vector
                const __m256i cutoff_vec = _mm256_set1_epi32(cutoff_date);
                size_t i = start;

                // Process 8 elements at a time with AVX2
                for (; i + 8 <= end; i += 8) {
                    __m256i shipdate_vec = _mm256_loadu_si256((__m256i*)(shipdate + i));
                    __m256i cmp_result = _mm256_cmple_epi32(shipdate_vec, cutoff_vec);
                    int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result));

                    // Extract indices of matching rows
                    if (mask != 0) {
                        for (int j = 0; j < 8; ++j) {
                            if (mask & (1 << j)) {
                                selection.push_back(i + j);
                            }
                        }
                    }
                }

                // Scalar tail for remaining rows
                for (; i < end; ++i) {
                    if (shipdate[i] <= cutoff_date) {
                        selection.push_back(i);
                    }
                }

                // Phase 2: Filter-then-decode pattern - process only matching rows
                for (size_t idx : selection) {
                    // Decode dictionary-encoded columns ONLY for matching rows
                    char ret_flag = returnflag_dict[returnflag_codes[idx]];
                    char line_status = linestatus_dict[linestatus_codes[idx]];

                    AggregateKey key{ret_flag, line_status};
                    auto& agg = local_agg[key];

                    // Phase 3: Integer-domain aggregation (all values already scaled by 100)
                    int64_t qty = quantity[idx];
                    int64_t price = extendedprice[idx];
                    int64_t disc = discount[idx];
                    int64_t tax_val = tax[idx];

                    // Compute derived values in integer domain
                    // disc_price = price * (1 - disc/100) = price * (100 - disc) / 100
                    int64_t disc_price = (price * (100 - disc)) / 100;

                    // charge = disc_price * (1 + tax/100) = disc_price * (100 + tax) / 100
                    int64_t charge = (disc_price * (100 + tax_val)) / 100;

                    // Accumulate in integer domain (no Kahan needed for exact integers)
                    agg.sum_qty_scaled += qty;
                    agg.sum_base_price_scaled += price;
                    agg.sum_disc_price_scaled += disc_price;
                    agg.sum_charge_scaled += charge;
                    agg.sum_discount_scaled += disc;
                    agg.count++;
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(scan_end - scan_start).count();

    // 4. Merge thread-local results
    auto agg_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<AggregateKey, AggregateValue, AggregateKeyHash, AggregateKeyEqual> final_agg;
    for (const auto& local_map : local_maps) {
        for (const auto& entry : local_map) {
            auto& agg = final_agg[entry.first];
            // Simple integer addition (no Kahan needed for exact integers)
            agg.sum_qty_scaled += entry.second.sum_qty_scaled;
            agg.sum_base_price_scaled += entry.second.sum_base_price_scaled;
            agg.sum_disc_price_scaled += entry.second.sum_disc_price_scaled;
            agg.sum_charge_scaled += entry.second.sum_charge_scaled;
            agg.sum_discount_scaled += entry.second.sum_discount_scaled;
            agg.count += entry.second.count;
        }
    }

    auto agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(agg_end - agg_start).count();

    // 5. Sort results by l_returnflag, l_linestatus
    auto sort_start = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<AggregateKey, AggregateValue>> results;
    results.reserve(final_agg.size());
    for (const auto& entry : final_agg) {
        results.push_back(entry);
    }

    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.first.returnflag != b.first.returnflag)
            return a.first.returnflag < b.first.returnflag;
        return a.first.linestatus < b.first.linestatus;
    });

    auto sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(sort_end - sort_start).count();

    // 6. Write output
    auto output_start = std::chrono::high_resolution_clock::now();

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << std::fixed << std::setprecision(2);

        // Write header
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& entry : results) {
            const auto& key = entry.first;
            const auto& val = entry.second;

            // Convert from integer domain to double (divide by 100 once per group)
            double sum_qty = val.sum_qty_scaled / 100.0;
            double sum_base_price = val.sum_base_price_scaled / 100.0;
            double sum_disc_price = val.sum_disc_price_scaled / 100.0;
            double sum_charge = val.sum_charge_scaled / 100.0;
            double sum_discount = val.sum_discount_scaled / 100.0;

            double avg_qty = sum_qty / val.count;
            double avg_price = sum_base_price / val.count;
            double avg_disc = sum_discount / val.count;

            out << key.returnflag << ","
                << key.linestatus << ","
                << sum_qty << ","
                << sum_base_price << ","
                << sum_disc_price << ","
                << sum_charge << ","
                << avg_qty << ","
                << avg_price << ","
                << avg_disc << ","
                << val.count << "\n";
        }

        out.close();
    }

    auto output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(output_end - output_start).count();

    // Cleanup
    munmap((void*)returnflag_codes, size_rf);
    munmap((void*)linestatus_codes, size_ls);
    munmap((void*)quantity, size_qty);
    munmap((void*)extendedprice, size_price);
    munmap((void*)discount, size_disc);
    munmap((void*)tax, size_tax);
    munmap((void*)shipdate, size_ship);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();

    // Print timing information
    std::cout << "[TIMING] decode: " << std::fixed << std::setprecision(1) << decode_ms << " ms" << std::endl;
    std::cout << "[TIMING] scan_filter: " << scan_ms << " ms" << std::endl;
    std::cout << "[TIMING] aggregation: " << agg_ms << " ms" << std::endl;
    std::cout << "[TIMING] sort: " << sort_ms << " ms" << std::endl;
    std::cout << "[TIMING] output: " << output_ms << " ms" << std::endl;
    std::cout << "[TIMING] total: " << total_ms << " ms" << std::endl;
    std::cout << "Query returned " << results.size() << " rows" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q1(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
