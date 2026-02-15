#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <map>
#include <vector>
#include <algorithm>
#include <cmath>
#include <omp.h>
#include <immintrin.h>

// Memory-mapped file wrapper
class MmapFile {
public:
    int fd;
    void* ptr;
    size_t size;

    MmapFile() : fd(-1), ptr(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening " << path << std::endl;
            return false;
        }

        off_t file_size = lseek(fd, 0, SEEK_END);
        if (file_size < 0) {
            ::close(fd);
            return false;
        }
        size = (size_t)file_size;

        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Error mmapping " << path << std::endl;
            ::close(fd);
            return false;
        }
        return true;
    }

    ~MmapFile() {
        if (ptr != nullptr) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

// Load dictionary mapping from file
std::map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Error opening dictionary " << dict_path << std::endl;
        return dict;
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// Zone map entry for l_shipdate (int32_t min/max per block)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
};

// Aggregation state for one (returnflag, linestatus) group
struct AggregateRow {
    int64_t sum_qty = 0;
    int64_t sum_extendedprice = 0;
    int64_t sum_disc_price = 0;  // Stored at scale_factor^2 before division
    int64_t sum_charge = 0;      // Stored at scale_factor^3 before division
    int64_t sum_discount = 0;
    int64_t sum_tax = 0;
    int64_t count = 0;
};

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    // === TIMING: Total ===
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // === LOAD DICTIONARIES ===
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    auto returnflag_dict = load_dictionary(lineitem_dir + "l_returnflag_dict.txt");
    auto linestatus_dict = load_dictionary(lineitem_dir + "l_linestatus_dict.txt");
#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", dict_ms);
#endif

    // === LOAD ZONE MAP ===
    MmapFile zone_map_file;
    zone_map_file.open(gendb_dir + "/indexes/zone_map_l_shipdate.bin");

    const ZoneMapEntry* zone_maps = nullptr;
    uint32_t num_blocks = 0;

    if (zone_map_file.ptr) {
        // Zone map format: [uint32_t num_blocks] then [ZoneMapEntry] array
        const uint32_t* zone_header = (const uint32_t*)zone_map_file.ptr;
        num_blocks = *zone_header;
        zone_maps = (const ZoneMapEntry*)((const char*)zone_map_file.ptr + sizeof(uint32_t));
    }

    // === LOAD BINARY COLUMNS ===
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile l_shipdate_file, l_returnflag_file, l_linestatus_file;
    MmapFile l_quantity_file, l_extendedprice_file, l_discount_file, l_tax_file;

    l_shipdate_file.open(lineitem_dir + "l_shipdate.bin");
    l_returnflag_file.open(lineitem_dir + "l_returnflag.bin");
    l_linestatus_file.open(lineitem_dir + "l_linestatus.bin");
    l_quantity_file.open(lineitem_dir + "l_quantity.bin");
    l_extendedprice_file.open(lineitem_dir + "l_extendedprice.bin");
    l_discount_file.open(lineitem_dir + "l_discount.bin");
    l_tax_file.open(lineitem_dir + "l_tax.bin");

    if (!l_shipdate_file.ptr || !l_returnflag_file.ptr || !l_linestatus_file.ptr ||
        !l_quantity_file.ptr || !l_extendedprice_file.ptr || !l_discount_file.ptr || !l_tax_file.ptr) {
        std::cerr << "Error loading binary files" << std::endl;
        return;
    }

    int32_t* shipdate_data = (int32_t*)l_shipdate_file.ptr;
    int32_t* returnflag_data = (int32_t*)l_returnflag_file.ptr;
    int32_t* linestatus_data = (int32_t*)l_linestatus_file.ptr;
    int64_t* quantity_data = (int64_t*)l_quantity_file.ptr;
    int64_t* extendedprice_data = (int64_t*)l_extendedprice_file.ptr;
    int64_t* discount_data = (int64_t*)l_discount_file.ptr;
    int64_t* tax_data = (int64_t*)l_tax_file.ptr;

    int64_t num_rows = l_shipdate_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", load_ms);
#endif

    // === COMPUTE DATE THRESHOLD ===
    // DATE '1998-12-01' - INTERVAL '90' DAY
    // DATE '1998-12-01' = epoch day 10561
    // minus 90 days = 10471
    int32_t date_threshold = 10471;

    // === SCAN, FILTER, AGGREGATE with Zone Map Pruning ===
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Low-cardinality GROUP BY (6 groups max), use flat array indexed by (returnflag * 2 + linestatus)
    // returnflag codes: 0=N, 1=R, 2=A (3 values)
    // linestatus codes: 0=O, 1=F (2 values)
    // Total: 3 * 2 = 6 groups
    std::vector<AggregateRow> agg_groups(6);

    // Parallel scan with thread-local aggregation
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<AggregateRow>> thread_local_aggs(num_threads, std::vector<AggregateRow>(6));

    // Block-based parallel scan with zone map pruning
    int block_size = 100000;  // From storage guide

    if (zone_maps && num_blocks > 0) {
        // Zone map-aware scan: process each block, skipping blocks that fail predicate
#pragma omp parallel for schedule(static)
        for (uint32_t block_id = 0; block_id < num_blocks; block_id++) {
            const ZoneMapEntry& zone = zone_maps[block_id];

            // Skip entire block if min_val > threshold (all rows in block violate predicate)
            if (zone.min_val > date_threshold) {
                continue;
            }

            int64_t block_start = (int64_t)block_id * block_size;
            int64_t block_end = std::min(block_start + block_size, num_rows);

            int tid = omp_get_thread_num();

            // If entire block satisfies predicate (max_val <= threshold), process all rows
            if (zone.max_val <= date_threshold) {
                for (int64_t i = block_start; i < block_end; i++) {
                    int32_t rf = returnflag_data[i];
                    int32_t ls = linestatus_data[i];
                    int group_idx = rf * 2 + ls;

                    int64_t qty = quantity_data[i];
                    int64_t ep = extendedprice_data[i];
                    int64_t disc = discount_data[i];
                    int64_t tax = tax_data[i];

                    int64_t disc_price_scaled = ep * (100 - disc);
                    int64_t charge_scaled = ep * (100 - disc) * (100 + tax);

                    thread_local_aggs[tid][group_idx].sum_qty += qty;
                    thread_local_aggs[tid][group_idx].sum_extendedprice += ep;
                    thread_local_aggs[tid][group_idx].sum_disc_price += disc_price_scaled;
                    thread_local_aggs[tid][group_idx].sum_charge += charge_scaled;
                    thread_local_aggs[tid][group_idx].sum_discount += disc;
                    thread_local_aggs[tid][group_idx].sum_tax += tax;
                    thread_local_aggs[tid][group_idx].count += 1;
                }
            } else {
                // Partial block: check predicate per row
                for (int64_t i = block_start; i < block_end; i++) {
                    if (shipdate_data[i] <= date_threshold) {
                        int32_t rf = returnflag_data[i];
                        int32_t ls = linestatus_data[i];
                        int group_idx = rf * 2 + ls;

                        int64_t qty = quantity_data[i];
                        int64_t ep = extendedprice_data[i];
                        int64_t disc = discount_data[i];
                        int64_t tax = tax_data[i];

                        int64_t disc_price_scaled = ep * (100 - disc);
                        int64_t charge_scaled = ep * (100 - disc) * (100 + tax);

                        thread_local_aggs[tid][group_idx].sum_qty += qty;
                        thread_local_aggs[tid][group_idx].sum_extendedprice += ep;
                        thread_local_aggs[tid][group_idx].sum_disc_price += disc_price_scaled;
                        thread_local_aggs[tid][group_idx].sum_charge += charge_scaled;
                        thread_local_aggs[tid][group_idx].sum_discount += disc;
                        thread_local_aggs[tid][group_idx].sum_tax += tax;
                        thread_local_aggs[tid][group_idx].count += 1;
                    }
                }
            }
        }
    } else {
        // Fallback: zone map not available, full scan
#pragma omp parallel for schedule(static)
        for (int64_t i = 0; i < num_rows; i++) {
            if (shipdate_data[i] <= date_threshold) {
                int tid = omp_get_thread_num();
                int32_t rf = returnflag_data[i];
                int32_t ls = linestatus_data[i];
                int group_idx = rf * 2 + ls;

                int64_t qty = quantity_data[i];
                int64_t ep = extendedprice_data[i];
                int64_t disc = discount_data[i];
                int64_t tax = tax_data[i];

                int64_t disc_price_scaled = ep * (100 - disc);
                int64_t charge_scaled = ep * (100 - disc) * (100 + tax);

                thread_local_aggs[tid][group_idx].sum_qty += qty;
                thread_local_aggs[tid][group_idx].sum_extendedprice += ep;
                thread_local_aggs[tid][group_idx].sum_disc_price += disc_price_scaled;
                thread_local_aggs[tid][group_idx].sum_charge += charge_scaled;
                thread_local_aggs[tid][group_idx].sum_discount += disc;
                thread_local_aggs[tid][group_idx].sum_tax += tax;
                thread_local_aggs[tid][group_idx].count += 1;
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);
#endif

    // === MERGE THREAD-LOCAL RESULTS ===
#ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
#endif
    for (int tid = 0; tid < num_threads; tid++) {
        for (int g = 0; g < 6; g++) {
            agg_groups[g].sum_qty += thread_local_aggs[tid][g].sum_qty;
            agg_groups[g].sum_extendedprice += thread_local_aggs[tid][g].sum_extendedprice;
            agg_groups[g].sum_disc_price += thread_local_aggs[tid][g].sum_disc_price;
            agg_groups[g].sum_charge += thread_local_aggs[tid][g].sum_charge;
            agg_groups[g].sum_discount += thread_local_aggs[tid][g].sum_discount;
            agg_groups[g].sum_tax += thread_local_aggs[tid][g].sum_tax;
            agg_groups[g].count += thread_local_aggs[tid][g].count;
        }
    }
#ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge: %.2f ms\n", merge_ms);
#endif

    // === BUILD RESULT ===
    std::vector<std::tuple<std::string, std::string, AggregateRow>> results;
    for (int rf = 0; rf < 3; rf++) {
        for (int ls = 0; ls < 2; ls++) {
            int group_idx = rf * 2 + ls;
            if (agg_groups[group_idx].count > 0) {
                auto rf_it = returnflag_dict.find(rf);
                auto ls_it = linestatus_dict.find(ls);
                std::string rf_str = (rf_it != returnflag_dict.end()) ? rf_it->second : "";
                std::string ls_str = (ls_it != linestatus_dict.end()) ? ls_it->second : "";
                results.push_back(std::make_tuple(rf_str, ls_str, agg_groups[group_idx]));
            }
        }
    }

    // === SORT BY returnflag, linestatus ===
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (std::get<0>(a) != std::get<0>(b))
                return std::get<0>(a) < std::get<0>(b);
            return std::get<1>(a) < std::get<1>(b);
        });
#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

    // === WRITE CSV ===
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif
    std::ofstream out(results_dir + "/Q1.csv");
    if (!out.is_open()) {
        std::cerr << "Error opening output file" << std::endl;
        return;
    }

    // Header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows
    for (const auto& row : results) {
        std::string rf = std::get<0>(row);
        std::string ls = std::get<1>(row);
        const AggregateRow& agg = std::get<2>(row);

        // Convert to actual values (divide by scale_factor=100)
        // Using long double for higher precision
        long double sum_qty = agg.sum_qty / 100.0L;
        long double sum_base_price = agg.sum_extendedprice / 100.0L;
        long double sum_disc_price = agg.sum_disc_price / (100.0L * 100.0L);  // Divide by scale^2
        long double sum_charge = agg.sum_charge / (100.0L * 100.0L * 100.0L);  // Divide by scale^3
        long double avg_qty = (agg.count > 0) ? (sum_qty / agg.count) : 0.0L;
        long double avg_price = (agg.count > 0) ? (sum_base_price / agg.count) : 0.0L;
        long double avg_disc = (agg.count > 0) ? (agg.sum_discount / 100.0L / agg.count) : 0.0L;

        // Format with appropriate decimal places
        // sum_qty: 2 decimals, sum_base_price: 2 decimals, sum_disc_price: 4 decimals, sum_charge: 6 decimals
        char buffer[512];
        snprintf(buffer, sizeof(buffer),
            "%s,%s,%.2Lf,%.2Lf,%.4Lf,%.6Lf,%.2Lf,%.2Lf,%.2Lf,%ld\n",
            rf.c_str(), ls.c_str(),
            sum_qty, sum_base_price, sum_disc_price, sum_charge,
            avg_qty, avg_price, avg_disc, agg.count);
        out << buffer;
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // === TIMING: Total computation (excluding I/O) ===
#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms - output_ms);
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
