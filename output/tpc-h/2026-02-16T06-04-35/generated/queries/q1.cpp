#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include <cmath>
#include <iomanip>
#include <immintrin.h>

// ==============================================================================
// METADATA CHECK
// ==============================================================================
// Q1 Storage Guide Verification:
// - l_quantity: int64_t, DECIMAL, scale_factor=100
// - l_extendedprice: int64_t, DECIMAL, scale_factor=100
// - l_discount: int64_t, DECIMAL, scale_factor=100
// - l_tax: int64_t, DECIMAL, scale_factor=100
// - l_returnflag: int8_t, STRING (dictionary-encoded)
// - l_linestatus: int8_t, STRING (dictionary-encoded)
// - l_shipdate: int32_t, DATE (days since epoch 1970-01-01)
// Zone map: lineitem_l_shipdate_zone.bin
//   - Layout: [uint32_t num_zones] then [min:int32_t, max:int32_t, row_count:uint32_t] × num_zones
// ==============================================================================

// Constants
constexpr int32_t EPOCH_DAYS_1998_09_02 = 10471;  // 1998-09-02 from epoch (1998-12-01 - 90 days)
constexpr int64_t SCALE_FACTOR = 100;

// Load dictionary from file
std::unordered_map<int8_t, char> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int8_t, char> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int8_t code = static_cast<int8_t>(std::stoi(line.substr(0, eq_pos)));
            char value = line[eq_pos + 1];
            dict[code] = value;
        }
    }
    return dict;
}

// Aggregate structure for GROUP BY (returnflag, linestatus)
struct AggResult {
    int8_t returnflag;
    int8_t linestatus;
    int64_t sum_qty;
    int64_t sum_base_price;
    int64_t sum_disc_price_unscaled;  // Accumulated at full scale (before /100)
    int64_t sum_charge_unscaled;      // Accumulated at full scale (before /10000)
    int64_t sum_discount;  // For computing AVG(discount)
    int64_t count_order;
};

// For low-cardinality GROUP BY: use flat array indexed by (returnflag*2 + linestatus)
// This eliminates hash table lookups entirely in the hot path
struct FlatAggResult {
    int64_t sum_qty;
    int64_t sum_base_price;
    int64_t sum_disc_price_unscaled;
    int64_t sum_charge_unscaled;
    int64_t sum_discount;
    int64_t count_order;
};

// Mmap file helper
struct MmapFile {
    int fd;
    void* data;
    size_t size;

    MmapFile() : fd(-1), data(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;

        struct stat st;
        if (fstat(fd, &st) < 0) {
            close();
            return false;
        }
        size = st.st_size;

        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            close();
            return false;
        }
        return true;
    }

    void close() {
        if (data) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

// Zone map entry structure (per storage guide)
struct ZoneMapEntry {
    int32_t min_value;
    int32_t max_value;
    uint32_t row_count;
};

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // === LOAD DICTIONARIES ===
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    auto returnflag_dict = load_dictionary(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    auto linestatus_dict = load_dictionary(gendb_dir + "/lineitem/l_linestatus_dict.txt");

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] dict_load: %.2f ms\n", ms);
#endif

    // === LOAD BINARY COLUMNS ===
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile l_quantity_file, l_extendedprice_file, l_discount_file, l_tax_file;
    MmapFile l_returnflag_file, l_linestatus_file, l_shipdate_file;

    if (!l_quantity_file.open(gendb_dir + "/lineitem/l_quantity.bin") ||
        !l_extendedprice_file.open(gendb_dir + "/lineitem/l_extendedprice.bin") ||
        !l_discount_file.open(gendb_dir + "/lineitem/l_discount.bin") ||
        !l_tax_file.open(gendb_dir + "/lineitem/l_tax.bin") ||
        !l_returnflag_file.open(gendb_dir + "/lineitem/l_returnflag.bin") ||
        !l_linestatus_file.open(gendb_dir + "/lineitem/l_linestatus.bin") ||
        !l_shipdate_file.open(gendb_dir + "/lineitem/l_shipdate.bin")) {
        std::cerr << "Error: failed to open binary column files\n";
        return;
    }

    const int64_t* l_quantity = static_cast<const int64_t*>(l_quantity_file.data);
    const int64_t* l_extendedprice = static_cast<const int64_t*>(l_extendedprice_file.data);
    const int64_t* l_discount = static_cast<const int64_t*>(l_discount_file.data);
    const int64_t* l_tax = static_cast<const int64_t*>(l_tax_file.data);
    const int8_t* l_returnflag = static_cast<const int8_t*>(l_returnflag_file.data);
    const int8_t* l_linestatus = static_cast<const int8_t*>(l_linestatus_file.data);
    const int32_t* l_shipdate = static_cast<const int32_t*>(l_shipdate_file.data);

    uint64_t total_rows = l_shipdate_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", ms);
#endif

    // === LOAD ZONE MAP INDEX ===
#ifdef GENDB_PROFILE
    auto t_zone_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile zone_map_file;
    std::vector<ZoneMapEntry> zone_entries;
    std::vector<uint64_t> zone_row_ranges;  // (start_row, end_row) pairs

    if (zone_map_file.open(gendb_dir + "/indexes/lineitem_l_shipdate_zone.bin")) {
        const uint32_t* zone_header = static_cast<const uint32_t*>(zone_map_file.data);
        uint32_t num_zones = *zone_header;
        const ZoneMapEntry* zones = reinterpret_cast<const ZoneMapEntry*>(zone_header + 1);

        uint64_t row_offset = 0;
        for (uint32_t z = 0; z < num_zones; z++) {
            uint64_t zone_start = row_offset;
            uint64_t zone_end = row_offset + zones[z].row_count;
            zone_end = std::min(zone_end, total_rows);

            // Check if zone might contain rows matching predicate l_shipdate <= EPOCH_DAYS_1998_09_02
            if (zones[z].max_value <= EPOCH_DAYS_1998_09_02) {
                // Entire zone qualifies - no per-row filtering needed
                zone_entries.push_back(zones[z]);
                zone_row_ranges.push_back(zone_start);
                zone_row_ranges.push_back(zone_end);
            } else if (zones[z].min_value <= EPOCH_DAYS_1998_09_02) {
                // Partial zone - need per-row filtering
                zone_entries.push_back(zones[z]);
                zone_row_ranges.push_back(zone_start);
                zone_row_ranges.push_back(zone_end);
            }
            // else: zone min > predicate value, skip entirely

            row_offset = zone_end;
        }
    } else {
        // No zone map, scan all rows
        ZoneMapEntry all_zones;
        all_zones.min_value = 0;
        all_zones.max_value = INT32_MAX;
        all_zones.row_count = total_rows;
        zone_entries.push_back(all_zones);
        zone_row_ranges.push_back(0);
        zone_row_ranges.push_back(total_rows);
    }

#ifdef GENDB_PROFILE
    auto t_zone_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_zone_end - t_zone_start).count();
    printf("[TIMING] zone_map_load: %.2f ms\n", ms);
#endif

    // === PARALLEL SCAN & FILTER ===
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local aggregation using flat arrays (low-cardinality optimization)
    // 6 groups: (returnflag 0-2) × (linestatus 0-1)
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<FlatAggResult>> local_agg_flat(num_threads, std::vector<FlatAggResult>(6));

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_flat = local_agg_flat[tid];

        // Initialize all 6 groups for this thread
        for (int i = 0; i < 6; i++) {
            local_flat[i] = {
                .sum_qty = 0,
                .sum_base_price = 0,
                .sum_disc_price_unscaled = 0,
                .sum_charge_unscaled = 0,
                .sum_discount = 0,
                .count_order = 0
            };
        }

        // Process each zone in parallel
        #pragma omp for schedule(dynamic)
        for (size_t z = 0; z < zone_entries.size(); z++) {
            uint64_t start_row = zone_row_ranges[2 * z];
            uint64_t end_row = zone_row_ranges[2 * z + 1];

            // Use restrict pointers for SIMD vectorization
            const int32_t * __restrict shipdate_ptr = l_shipdate + start_row;
            const int64_t * __restrict qty_ptr = l_quantity + start_row;
            const int64_t * __restrict price_ptr = l_extendedprice + start_row;
            const int64_t * __restrict discount_ptr = l_discount + start_row;
            const int64_t * __restrict tax_ptr = l_tax + start_row;
            const int8_t * __restrict returnflag_ptr = l_returnflag + start_row;
            const int8_t * __restrict linestatus_ptr = l_linestatus + start_row;

            uint64_t zone_rows = end_row - start_row;

            // Optimized hot path: tight loop with aggressive prefetching and reduced branching
            const uint64_t prefetch_distance = 64;  // prefetch further ahead (64 rows)

            // Unroll loop in pairs to improve instruction-level parallelism
            uint64_t offset = 0;

            // Process pairs of rows with explicit unrolling
            for (; offset + 1 < zone_rows; offset += 2) {
                // Prefetch 2 rows far ahead (128 bytes * rows)
                if (offset + prefetch_distance < zone_rows) {
                    __builtin_prefetch(&shipdate_ptr[offset + prefetch_distance], 0, 0);
                    __builtin_prefetch(&qty_ptr[offset + prefetch_distance], 0, 1);
                    __builtin_prefetch(&returnflag_ptr[offset + prefetch_distance], 0, 0);
                }

                // Process first row
                if (shipdate_ptr[offset] <= EPOCH_DAYS_1998_09_02) {
                    int8_t rf = returnflag_ptr[offset];
                    int8_t ls = linestatus_ptr[offset];
                    int64_t qty = qty_ptr[offset];
                    int64_t price = price_ptr[offset];
                    int64_t discount = discount_ptr[offset];
                    int64_t tax = tax_ptr[offset];

                    int agg_idx = rf * 2 + ls;
                    auto& agg = local_flat[agg_idx];

                    int64_t disc_price = price * (SCALE_FACTOR - discount);
                    agg.sum_qty += qty;
                    agg.sum_base_price += price;
                    agg.sum_discount += discount;
                    agg.sum_disc_price_unscaled += disc_price;
                    agg.sum_charge_unscaled += disc_price * (SCALE_FACTOR + tax);
                    agg.count_order++;
                }

                // Process second row
                if (shipdate_ptr[offset + 1] <= EPOCH_DAYS_1998_09_02) {
                    int8_t rf = returnflag_ptr[offset + 1];
                    int8_t ls = linestatus_ptr[offset + 1];
                    int64_t qty = qty_ptr[offset + 1];
                    int64_t price = price_ptr[offset + 1];
                    int64_t discount = discount_ptr[offset + 1];
                    int64_t tax = tax_ptr[offset + 1];

                    int agg_idx = rf * 2 + ls;
                    auto& agg = local_flat[agg_idx];

                    int64_t disc_price = price * (SCALE_FACTOR - discount);
                    agg.sum_qty += qty;
                    agg.sum_base_price += price;
                    agg.sum_discount += discount;
                    agg.sum_disc_price_unscaled += disc_price;
                    agg.sum_charge_unscaled += disc_price * (SCALE_FACTOR + tax);
                    agg.count_order++;
                }
            }

            // Handle remaining row if odd
            if (offset < zone_rows) {
                if (shipdate_ptr[offset] <= EPOCH_DAYS_1998_09_02) {
                    int8_t rf = returnflag_ptr[offset];
                    int8_t ls = linestatus_ptr[offset];
                    int64_t qty = qty_ptr[offset];
                    int64_t price = price_ptr[offset];
                    int64_t discount = discount_ptr[offset];
                    int64_t tax = tax_ptr[offset];

                    int agg_idx = rf * 2 + ls;
                    auto& agg = local_flat[agg_idx];

                    int64_t disc_price = price * (SCALE_FACTOR - discount);
                    agg.sum_qty += qty;
                    agg.sum_base_price += price;
                    agg.sum_discount += discount;
                    agg.sum_disc_price_unscaled += disc_price;
                    agg.sum_charge_unscaled += disc_price * (SCALE_FACTOR + tax);
                    agg.count_order++;
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms);
#endif

    // === MERGE THREAD-LOCAL RESULTS ===
#ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<FlatAggResult> global_agg_flat(6, {0, 0, 0, 0, 0, 0});

    for (int tid = 0; tid < num_threads; tid++) {
        for (int i = 0; i < 6; i++) {
            const auto& local_result = local_agg_flat[tid][i];
            auto& global_result = global_agg_flat[i];
            global_result.sum_qty += local_result.sum_qty;
            global_result.sum_base_price += local_result.sum_base_price;
            global_result.sum_disc_price_unscaled += local_result.sum_disc_price_unscaled;
            global_result.sum_charge_unscaled += local_result.sum_charge_unscaled;
            global_result.sum_discount += local_result.sum_discount;
            global_result.count_order += local_result.count_order;
        }
    }

#ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge_aggregates: %.2f ms\n", ms);
#endif

    // === COMPUTE AVERAGES & SORT ===
#ifdef GENDB_PROFILE
    auto t_final_start = std::chrono::high_resolution_clock::now();
#endif

    // Construct results from flat array in sorted order (naturally sorted by index)
    // Index mapping: 0 = rf0,ls0  1 = rf0,ls1  2 = rf1,ls0  3 = rf1,ls1  4 = rf2,ls0  5 = rf2,ls1
    std::vector<AggResult> results;
    for (int i = 0; i < 6; i++) {
        const auto& agg = global_agg_flat[i];
        if (agg.count_order > 0) {
            int rf = i / 2;
            int ls = i % 2;
            results.push_back({
                .returnflag = static_cast<int8_t>(rf),
                .linestatus = static_cast<int8_t>(ls),
                .sum_qty = agg.sum_qty,
                .sum_base_price = agg.sum_base_price,
                .sum_disc_price_unscaled = agg.sum_disc_price_unscaled,
                .sum_charge_unscaled = agg.sum_charge_unscaled,
                .sum_discount = agg.sum_discount,
                .count_order = agg.count_order
            });
        }
    }

#ifdef GENDB_PROFILE
    auto t_final_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_final_end - t_final_start).count();
    printf("[TIMING] compute_final: %.2f ms\n", ms);
#endif

    // === OUTPUT CSV ===
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream csv(results_dir + "/Q1.csv");
    csv << std::fixed << std::setprecision(2);
    csv << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    for (const auto& agg : results) {
        char rf = returnflag_dict[agg.returnflag];
        char ls = linestatus_dict[agg.linestatus];

        double avg_qty = static_cast<double>(agg.sum_qty) / agg.count_order / SCALE_FACTOR;
        double avg_price = static_cast<double>(agg.sum_base_price) / agg.count_order / SCALE_FACTOR;
        double avg_disc = static_cast<double>(agg.sum_discount) / agg.count_order / SCALE_FACTOR;

        // Compute final values with proper scaling
        // sum_disc_price = sum_disc_price_unscaled / 100 (since it's price * (100 - discount))
        double sum_disc_price = static_cast<double>(agg.sum_disc_price_unscaled) / (SCALE_FACTOR * SCALE_FACTOR);

        // sum_charge = sum_charge_unscaled / 10000 (since it's price * (100 - discount) * (100 + tax))
        double sum_charge = static_cast<double>(agg.sum_charge_unscaled) / (SCALE_FACTOR * SCALE_FACTOR * SCALE_FACTOR);

        csv << rf << ","
            << ls << ","
            << static_cast<double>(agg.sum_qty) / SCALE_FACTOR << ","
            << static_cast<double>(agg.sum_base_price) / SCALE_FACTOR << ","
            << sum_disc_price << ","
            << sum_charge << ","
            << avg_qty << ","
            << avg_price << ","
            << avg_disc << ","
            << agg.count_order << "\n";
    }
    csv.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms);
#endif

    std::cout << "Q1 results written to " << results_dir << "/Q1.csv\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
