#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <map>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <omp.h>
#include <cstdint>
#include <immintrin.h>

// Kahan summation for accurate floating-point aggregation
struct KahanSum {
    double sum = 0.0;
    double c = 0.0;  // compensation for lost low-order bits

    void add(double x) {
        double y = x - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }

    double get() const { return sum; }
};

// Aggregation result for each (returnflag, linestatus) pair
struct AggResult {
    int64_t sum_qty = 0;           // sum of l_quantity (scaled)
    int64_t sum_base_price = 0;    // sum of l_extendedprice (scaled)
    double sum_disc_price = 0.0;   // sum of l_extendedprice * (1 - l_discount)
    double sum_charge = 0.0;       // sum of l_extendedprice * (1 - l_discount) * (1 + l_tax)
    int64_t count = 0;

    KahanSum sum_disc_price_kahan;
    KahanSum sum_charge_kahan;
    KahanSum sum_discount_kahan;   // for computing avg_discount
};

// Load dictionary file and return mapping
std::map<std::string, uint8_t> load_dictionary(const std::string& dict_file) {
    std::map<std::string, uint8_t> dict;
    std::ifstream f(dict_file);
    if (!f) {
        std::cerr << "Failed to open dictionary file: " << dict_file << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        size_t eq_pos = line.find('=');
        if (eq_pos == std::string::npos) continue;

        uint8_t code = std::stoi(line.substr(0, eq_pos));
        std::string value = line.substr(eq_pos + 1);
        dict[value] = code;
    }
    return dict;
}

// Zone map entry for block skipping
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t block_num;
    uint32_t row_count;
};

// Load binary column via mmap
template <typename T>
T* load_column(const std::string& filename, size_t& count) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << filename << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    count = file_size / sizeof(T);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for: " << filename << std::endl;
        return nullptr;
    }

    return static_cast<T*>(ptr);
}

// Load zone map index
std::vector<ZoneMapEntry> load_zone_map(const std::string& filename) {
    std::vector<ZoneMapEntry> zones;
    std::ifstream f(filename, std::ios::binary);
    if (!f) {
        std::cerr << "Failed to open zone map: " << filename << std::endl;
        return zones;
    }

    uint32_t num_zones = 0;
    f.read(reinterpret_cast<char*>(&num_zones), sizeof(uint32_t));

    for (uint32_t i = 0; i < num_zones; i++) {
        ZoneMapEntry entry;
        f.read(reinterpret_cast<char*>(&entry.min_val), sizeof(int32_t));
        f.read(reinterpret_cast<char*>(&entry.max_val), sizeof(int32_t));
        f.read(reinterpret_cast<char*>(&entry.block_num), sizeof(uint32_t));
        f.read(reinterpret_cast<char*>(&entry.row_count), sizeof(uint32_t));
        zones.push_back(entry);
    }

    return zones;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Q1 Query: Pricing Summary Report\n");
    printf("[METADATA CHECK] Columns: l_returnflag (dict), l_linestatus (dict), l_shipdate (date), l_quantity (dec), l_extendedprice (dec), l_discount (dec), l_tax (dec)\n");
    printf("[METADATA CHECK] Date cutoff: 1998-09-02 = 10471 days since epoch\n");
#endif

    // Build gendb paths
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    std::string indexes_dir = gendb_dir + "/indexes/";

    // Load dictionary files
    auto returnflag_dict = load_dictionary(lineitem_dir + "l_returnflag_dict.txt");
    auto linestatus_dict = load_dictionary(lineitem_dir + "l_linestatus_dict.txt");

#ifdef GENDB_PROFILE
    printf("[METADATA CHECK] Dictionary loaded: returnflag_dict size=%zu, linestatus_dict size=%zu\n",
           returnflag_dict.size(), linestatus_dict.size());
#endif

    // Load columns
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t num_rows = 0;
    int32_t* l_shipdate = load_column<int32_t>(lineitem_dir + "l_shipdate.bin", num_rows);
    size_t _;
    int64_t* l_quantity = load_column<int64_t>(lineitem_dir + "l_quantity.bin", _);
    int64_t* l_extendedprice = load_column<int64_t>(lineitem_dir + "l_extendedprice.bin", _);
    int64_t* l_discount = load_column<int64_t>(lineitem_dir + "l_discount.bin", _);
    int64_t* l_tax = load_column<int64_t>(lineitem_dir + "l_tax.bin", _);
    uint8_t* l_returnflag = load_column<uint8_t>(lineitem_dir + "l_returnflag.bin", _);
    uint8_t* l_linestatus = load_column<uint8_t>(lineitem_dir + "l_linestatus.bin", _);

    if (!l_shipdate || !l_quantity || !l_extendedprice || !l_discount || !l_tax || !l_returnflag || !l_linestatus) {
        std::cerr << "Failed to load columns" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", load_ms);
#endif

    // Constants: scale factors (stored as int64_t)
    const int64_t SCALE_DECIMAL = 100;
    const int32_t DATE_CUTOFF = 10471;  // 1998-09-02

    // Load zone map for shipdate filtering
    auto zones = load_zone_map(indexes_dir + "lineitem_shipdate_zone.bin");

    // Pre-compute zone boundaries for efficient zone-by-zone processing
    struct ZoneBoundary {
        size_t start_row;
        size_t end_row;
        int32_t min_val;
        int32_t max_val;
        bool fully_included;  // true if max_val <= DATE_CUTOFF (skip per-row checks)
    };

    std::vector<ZoneBoundary> zone_boundaries;
    size_t current_row = 0;
    for (const auto& zone : zones) {
        size_t zone_end = current_row + zone.row_count;
        // Skip zones that are entirely beyond the cutoff
        if (zone.min_val <= DATE_CUTOFF) {
            bool fully_included = (zone.max_val <= DATE_CUTOFF);
            zone_boundaries.push_back({current_row, zone_end, zone.min_val, zone.max_val, fully_included});
        }
        current_row = zone_end;
    }

    // Group-by aggregation: use flat array for low cardinality (3*2=6 possible groups)
    // Key: returnflag_code * 2 + linestatus_code
    std::vector<AggResult> groups(6);
    std::vector<bool> group_seen(6, false);

    // Scan and filter with partial aggregation
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Use thread-local aggregation to avoid critical sections
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<AggResult>> thread_groups(num_threads, std::vector<AggResult>(6));
    std::vector<std::vector<bool>> thread_group_seen(num_threads, std::vector<bool>(6, false));

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local_groups = thread_groups[tid];
        auto& local_seen = thread_group_seen[tid];

        #pragma omp for schedule(static)
        for (size_t zone_idx = 0; zone_idx < zone_boundaries.size(); zone_idx++) {
            const auto& zb = zone_boundaries[zone_idx];
            size_t start_row = zb.start_row;
            size_t end_row = zb.end_row;

            // Fast path: entire zone is before cutoff, no per-row check needed
            if (zb.fully_included) {
                size_t i = start_row;
                size_t end = end_row;

                // Prefetch first iteration
                if (i < end) {
                    _mm_prefetch((const char*)(l_extendedprice + i), _MM_HINT_T0);
                    _mm_prefetch((const char*)(l_returnflag + i), _MM_HINT_T0);
                }

                // Manual loop unrolling by 4 to hide latency
                for (; i + 4 <= end; i += 4) {
                    // Prefetch next 2 iterations
                    if (i + 8 <= end) {
                        _mm_prefetch((const char*)(l_extendedprice + i + 8), _MM_HINT_T0);
                        _mm_prefetch((const char*)(l_returnflag + i + 8), _MM_HINT_T0);
                    }

                    // Process 4 rows in unrolled loop
                    for (int unroll = 0; unroll < 4; unroll++) {
                        size_t idx = i + unroll;
                        int rf = l_returnflag[idx];
                        int ls = l_linestatus[idx];
                        int group_idx = rf * 2 + ls;

                        local_seen[group_idx] = true;

                        // Scaled integer aggregations
                        local_groups[group_idx].sum_qty += l_quantity[idx];
                        local_groups[group_idx].sum_base_price += l_extendedprice[idx];
                        local_groups[group_idx].count++;

                        // Floating point aggregations (unscaled for precision)
                        double base_price = l_extendedprice[idx] / (double)SCALE_DECIMAL;
                        double discount = l_discount[idx] / (double)SCALE_DECIMAL;
                        double tax = l_tax[idx] / (double)SCALE_DECIMAL;

                        double disc_price = base_price * (1.0 - discount);
                        double charge = base_price * (1.0 - discount) * (1.0 + tax);

                        local_groups[group_idx].sum_disc_price_kahan.add(disc_price);
                        local_groups[group_idx].sum_charge_kahan.add(charge);
                        local_groups[group_idx].sum_discount_kahan.add(discount);
                    }
                }

                // Handle remaining rows (< 4)
                for (; i < end; i++) {
                    int rf = l_returnflag[i];
                    int ls = l_linestatus[i];
                    int group_idx = rf * 2 + ls;

                    local_seen[group_idx] = true;

                    // Scaled integer aggregations
                    local_groups[group_idx].sum_qty += l_quantity[i];
                    local_groups[group_idx].sum_base_price += l_extendedprice[i];
                    local_groups[group_idx].count++;

                    // Floating point aggregations (unscaled for precision)
                    double base_price = l_extendedprice[i] / (double)SCALE_DECIMAL;
                    double discount = l_discount[i] / (double)SCALE_DECIMAL;
                    double tax = l_tax[i] / (double)SCALE_DECIMAL;

                    double disc_price = base_price * (1.0 - discount);
                    double charge = base_price * (1.0 - discount) * (1.0 + tax);

                    local_groups[group_idx].sum_disc_price_kahan.add(disc_price);
                    local_groups[group_idx].sum_charge_kahan.add(charge);
                    local_groups[group_idx].sum_discount_kahan.add(discount);
                }
            } else {
                // Partial zone: need per-row filtering
                // Use restrict pointers and SIMD vectorization
                int32_t* __restrict__ dates_r = l_shipdate + start_row;
                int64_t* __restrict__ qty_r = l_quantity + start_row;
                int64_t* __restrict__ price_r = l_extendedprice + start_row;
                int64_t* __restrict__ disc_r = l_discount + start_row;
                int64_t* __restrict__ tax_r = l_tax + start_row;
                uint8_t* __restrict__ rf_r = l_returnflag + start_row;
                uint8_t* __restrict__ ls_r = l_linestatus + start_row;
                size_t zone_size = end_row - start_row;

                // SIMD-friendly filtering with prefetching and loop unrolling
                size_t j = 0;

                // Process in chunks of 8 with prefetching and scalar checks
                for (; j + 8 <= zone_size; j += 8) {
                    // Prefetch next 2 iterations ahead
                    if (j + 16 < zone_size) {
                        _mm_prefetch((const char*)(dates_r + j + 16), _MM_HINT_T0);
                        _mm_prefetch((const char*)(qty_r + j + 16), _MM_HINT_T0);
                    }

                    // Process 8 rows with unrolling to hide latency
                    for (int k = 0; k < 8; k++) {
                        // Check if this lane matched (mask is set for matching lanes)
                        if (dates_r[j + k] <= DATE_CUTOFF) {
                            int rf = rf_r[j + k];
                            int ls = ls_r[j + k];
                            int group_idx = rf * 2 + ls;

                            local_seen[group_idx] = true;

                            local_groups[group_idx].sum_qty += qty_r[j + k];
                            local_groups[group_idx].sum_base_price += price_r[j + k];
                            local_groups[group_idx].count++;

                            double base_price = price_r[j + k] / (double)SCALE_DECIMAL;
                            double discount = disc_r[j + k] / (double)SCALE_DECIMAL;
                            double tax = tax_r[j + k] / (double)SCALE_DECIMAL;

                            double disc_price = base_price * (1.0 - discount);
                            double charge = base_price * (1.0 - discount) * (1.0 + tax);

                            local_groups[group_idx].sum_disc_price_kahan.add(disc_price);
                            local_groups[group_idx].sum_charge_kahan.add(charge);
                            local_groups[group_idx].sum_discount_kahan.add(discount);
                        }
                    }
                }

                // Handle remaining rows (< 8)
                for (; j < zone_size; j++) {
                    if (dates_r[j] <= DATE_CUTOFF) {
                        int rf = rf_r[j];
                        int ls = ls_r[j];
                        int group_idx = rf * 2 + ls;

                        local_seen[group_idx] = true;

                        local_groups[group_idx].sum_qty += qty_r[j];
                        local_groups[group_idx].sum_base_price += price_r[j];
                        local_groups[group_idx].count++;

                        double base_price = price_r[j] / (double)SCALE_DECIMAL;
                        double discount = disc_r[j] / (double)SCALE_DECIMAL;
                        double tax = tax_r[j] / (double)SCALE_DECIMAL;

                        double disc_price = base_price * (1.0 - discount);
                        double charge = base_price * (1.0 - discount) * (1.0 + tax);

                        local_groups[group_idx].sum_disc_price_kahan.add(disc_price);
                        local_groups[group_idx].sum_charge_kahan.add(charge);
                        local_groups[group_idx].sum_discount_kahan.add(discount);
                    }
                }
            }
        }
    }

    // Merge thread-local aggregates
    for (int tid = 0; tid < num_threads; tid++) {
        for (int g = 0; g < 6; g++) {
            if (thread_group_seen[tid][g]) {
                group_seen[g] = true;
                groups[g].sum_qty += thread_groups[tid][g].sum_qty;
                groups[g].sum_base_price += thread_groups[tid][g].sum_base_price;
                groups[g].count += thread_groups[tid][g].count;
                groups[g].sum_disc_price_kahan.add(thread_groups[tid][g].sum_disc_price_kahan.get());
                groups[g].sum_charge_kahan.add(thread_groups[tid][g].sum_charge_kahan.get());
                groups[g].sum_discount_kahan.add(thread_groups[tid][g].sum_discount_kahan.get());
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);
#endif

    // Finalize Kahan sums
#ifdef GENDB_PROFILE
    auto t_agg_finalize_start = std::chrono::high_resolution_clock::now();
#endif
    for (int i = 0; i < 6; i++) {
        groups[i].sum_disc_price = groups[i].sum_disc_price_kahan.get();
        groups[i].sum_charge = groups[i].sum_charge_kahan.get();
    }
#ifdef GENDB_PROFILE
    auto t_agg_finalize_end = std::chrono::high_resolution_clock::now();
    double agg_finalize_ms = std::chrono::duration<double, std::milli>(t_agg_finalize_end - t_agg_finalize_start).count();
    printf("[TIMING] aggregation_finalize: %.2f ms\n", agg_finalize_ms);
#endif

    // Build inverse dictionary for output
    std::map<uint8_t, std::string> returnflag_inv, linestatus_inv;
    for (const auto& [str, code] : returnflag_dict) {
        returnflag_inv[code] = str;
    }
    for (const auto& [str, code] : linestatus_dict) {
        linestatus_inv[code] = str;
    }

    // Sort results by returnflag, then linestatus
    std::vector<int> sorted_groups;
    for (int i = 0; i < 6; i++) {
        if (group_seen[i]) {
            sorted_groups.push_back(i);
        }
    }
    std::sort(sorted_groups.begin(), sorted_groups.end());

    // Write output CSV
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q1.csv";
    std::ofstream out(output_file);

    // Header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows
    for (int group_idx : sorted_groups) {
        int rf = group_idx / 2;
        int ls = group_idx % 2;

        const AggResult& agg = groups[group_idx];
        double avg_qty = agg.sum_qty / (double)(SCALE_DECIMAL * agg.count);
        double avg_price = agg.sum_base_price / (double)(SCALE_DECIMAL * agg.count);
        double avg_disc = agg.sum_discount_kahan.get() / (double)agg.count;

        out << returnflag_inv[rf] << ","
            << linestatus_inv[ls] << ","
            << std::fixed << std::setprecision(2)
            << (agg.sum_qty / (double)SCALE_DECIMAL) << ","
            << (agg.sum_base_price / (double)SCALE_DECIMAL) << ","
            << agg.sum_disc_price << ","
            << agg.sum_charge << ","
            << avg_qty << ","
            << avg_price << ","
            << avg_disc << ","
            << agg.count << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    auto t_total_end = std::chrono::high_resolution_clock::now();
#ifdef GENDB_PROFILE
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
