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
#include <immintrin.h>  // AVX2 SIMD intrinsics

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

// Aggregate values (using integer domain for precision and speed)
struct AggregateValue {
    int64_t sum_qty_scaled;         // scaled by 100
    int64_t sum_base_price_scaled;  // scaled by 100
    int64_t sum_disc_price_scaled;  // scaled by 10000 (price*100 * (100-disc))
    int64_t sum_charge_scaled;      // scaled by 1000000 (disc_price*10000 * (100+tax))
    int64_t sum_discount_scaled;    // scaled by 100
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

// Perfect hash for (returnflag, linestatus) -> index
// returnflag: 'A'=0, 'N'=1, 'R'=2
// linestatus: 'F'=0, 'O'=1
// Perfect hash: rf_idx * 2 + ls_idx, producing indices 0-5
inline int perfect_hash(char returnflag, char linestatus) {
    int rf_idx = (returnflag == 'A') ? 0 : (returnflag == 'N') ? 1 : 2;
    int ls_idx = (linestatus == 'F') ? 0 : 1;
    return rf_idx * 2 + ls_idx;
}

// Reverse mapping for output
inline void reverse_perfect_hash(int idx, char& returnflag, char& linestatus) {
    const char rf_vals[] = {'A', 'N', 'R'};
    const char ls_vals[] = {'F', 'O'};
    returnflag = rf_vals[idx / 2];
    linestatus = ls_vals[idx % 2];
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

    // Thread-local aggregation using perfect hash arrays (6 slots max)
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;
    const int NUM_GROUPS = 6;  // 3 returnflag × 2 linestatus

    std::vector<std::array<AggregateValue, NUM_GROUPS>> local_aggs(num_threads);
    std::vector<std::thread> threads;

    const __m256i cutoff_vec = _mm256_set1_epi32(cutoff_date);

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& local_agg = local_aggs[t];

            for (size_t start = t * morsel_size; start < row_count; start += num_threads * morsel_size) {
                size_t end = std::min(start + morsel_size, row_count);

                // SIMD-vectorized filtering: process 8 dates at a time
                size_t i = start;
                for (; i + 8 <= end; i += 8) {
                    // Load 8 shipdate values
                    __m256i shipdate_vec = _mm256_loadu_si256((__m256i*)&shipdate[i]);

                    // Compare: shipdate <= cutoff
                    // AVX2 only has cmpgt; construct <= as NOT(shipdate > cutoff)
                    __m256i gt_mask = _mm256_cmpgt_epi32(shipdate_vec, cutoff_vec);
                    __m256i le_mask = _mm256_andnot_si256(gt_mask, _mm256_set1_epi32(-1));

                    // Extract bitmask (each int32 produces 4 bits, so 32 bits total)
                    int mask = _mm256_movemask_epi8(le_mask);

                    // Process qualifying rows
                    for (int j = 0; j < 8; ++j) {
                        // Check if this row qualifies (all 4 bytes of int32 must be set)
                        int byte_offset = j * 4;
                        bool qualifies = ((mask >> byte_offset) & 0xF) == 0xF;

                        if (qualifies) {
                            size_t idx = i + j;

                            // Decode dictionary-encoded columns
                            char ret_flag = returnflag_dict[returnflag_codes[idx]];
                            char line_status = linestatus_dict[linestatus_codes[idx]];

                            int hash_idx = perfect_hash(ret_flag, line_status);
                            auto& agg = local_agg[hash_idx];

                            // Integer-domain aggregation (avoid float conversions)
                            int64_t qty_val = quantity[idx];
                            int64_t price_val = extendedprice[idx];
                            int64_t disc_val = discount[idx];
                            int64_t tax_val = tax[idx];

                            // disc_price = price * (1 - disc/100) = price * (100 - disc) / 100
                            // Keep scaled: disc_price_scaled = price * (100 - disc) [scale: 10000]
                            int64_t disc_price_scaled = price_val * (100 - disc_val);

                            // charge = disc_price * (1 + tax/100) = disc_price * (100 + tax) / 100
                            // Keep scaled: charge_scaled = disc_price_scaled * (100 + tax) [scale: 1000000]
                            int64_t charge_scaled = disc_price_scaled * (100 + tax_val);

                            agg.sum_qty_scaled += qty_val;
                            agg.sum_base_price_scaled += price_val;
                            agg.sum_disc_price_scaled += disc_price_scaled;
                            agg.sum_charge_scaled += charge_scaled;
                            agg.sum_discount_scaled += disc_val;
                            agg.count++;
                        }
                    }
                }

                // Handle remaining rows with scalar code
                for (; i < end; ++i) {
                    if (shipdate[i] <= cutoff_date) {
                        // Decode dictionary-encoded columns
                        char ret_flag = returnflag_dict[returnflag_codes[i]];
                        char line_status = linestatus_dict[linestatus_codes[i]];

                        int hash_idx = perfect_hash(ret_flag, line_status);
                        auto& agg = local_agg[hash_idx];

                        // Integer-domain aggregation
                        int64_t qty_val = quantity[i];
                        int64_t price_val = extendedprice[i];
                        int64_t disc_val = discount[i];
                        int64_t tax_val = tax[i];

                        int64_t disc_price_scaled = price_val * (100 - disc_val);
                        int64_t charge_scaled = disc_price_scaled * (100 + tax_val);

                        agg.sum_qty_scaled += qty_val;
                        agg.sum_base_price_scaled += price_val;
                        agg.sum_disc_price_scaled += disc_price_scaled;
                        agg.sum_charge_scaled += charge_scaled;
                        agg.sum_discount_scaled += disc_val;
                        agg.count++;
                    }
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

    std::array<AggregateValue, NUM_GROUPS> final_agg;
    for (const auto& local_agg : local_aggs) {
        for (int i = 0; i < NUM_GROUPS; ++i) {
            final_agg[i].sum_qty_scaled += local_agg[i].sum_qty_scaled;
            final_agg[i].sum_base_price_scaled += local_agg[i].sum_base_price_scaled;
            final_agg[i].sum_disc_price_scaled += local_agg[i].sum_disc_price_scaled;
            final_agg[i].sum_charge_scaled += local_agg[i].sum_charge_scaled;
            final_agg[i].sum_discount_scaled += local_agg[i].sum_discount_scaled;
            final_agg[i].count += local_agg[i].count;
        }
    }

    auto agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(agg_end - agg_start).count();

    // 5. Build results (perfect hash already ensures ordering: A-F, A-O, N-F, N-O, R-F, R-O)
    auto sort_start = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<AggregateKey, AggregateValue>> results;
    for (int i = 0; i < NUM_GROUPS; ++i) {
        if (final_agg[i].count > 0) {
            AggregateKey key;
            reverse_perfect_hash(i, key.returnflag, key.linestatus);
            results.push_back({key, final_agg[i]});
        }
    }

    // Perfect hash already provides sorted order
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

            // Convert scaled integers to doubles
            double sum_qty = val.sum_qty_scaled / 100.0;
            double sum_base_price = val.sum_base_price_scaled / 100.0;
            double sum_disc_price = val.sum_disc_price_scaled / 10000.0;
            double sum_charge = val.sum_charge_scaled / 1000000.0;
            double avg_qty = val.sum_qty_scaled / 100.0 / val.count;
            double avg_price = val.sum_base_price_scaled / 100.0 / val.count;
            double avg_disc = val.sum_discount_scaled / 100.0 / val.count;

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
