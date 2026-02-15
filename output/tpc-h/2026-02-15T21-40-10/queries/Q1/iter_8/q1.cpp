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

// Zone Map structure for l_shipdate
struct ZoneMap {
    int32_t min_val;
    int32_t max_val;
};

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

// Invert dictionary: value -> code
std::map<std::string, int32_t> invert_dictionary(const std::map<int32_t, std::string>& dict) {
    std::map<std::string, int32_t> inverted;
    for (const auto& kv : dict) {
        inverted[kv.second] = kv.first;
    }
    return inverted;
}

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

    // Invert dictionaries for reverse lookup
    auto returnflag_inv = invert_dictionary(returnflag_dict);
    auto linestatus_inv = invert_dictionary(linestatus_dict);
#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", dict_ms);
#endif

    // === LOAD ZONE MAP (l_shipdate) ===
#ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
#endif
    MmapFile zone_map_file;
    bool zonemap_loaded = zone_map_file.open(gendb_dir + "/indexes/zone_map_l_shipdate.bin");

    uint32_t num_blocks = 0;
    const ZoneMap* zone_maps = nullptr;
    if (zonemap_loaded && zone_map_file.size >= sizeof(uint32_t)) {
        num_blocks = *(uint32_t*)zone_map_file.ptr;
        zone_maps = (const ZoneMap*)((char*)zone_map_file.ptr + sizeof(uint32_t));
    }
#ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] load_zonemap: %.2f ms\n", zonemap_ms);
#endif

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

    // === SCAN, FILTER, AGGREGATE ===
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Low-cardinality GROUP BY (6 groups max = 3 returnflags * 2 linestatuses)
    // Use flat array for thread-local aggregation:
    // Index = rf_code * 2 + ls_code (assumes rf_code in [0,2], ls_code in [0,1])
    int num_threads = omp_get_max_threads();
    const int MAX_GROUPS = 6;  // 3 returnflags * 2 linestatuses
    std::vector<std::vector<AggregateRow>> thread_local_aggs(num_threads, std::vector<AggregateRow>(MAX_GROUPS));

    // If zone map is not loaded, process all rows as one block
    if (!zone_maps || num_blocks == 0) {
        num_blocks = 1;
        zone_maps = nullptr;  // Disable zone map pruning
    }

#pragma omp parallel for schedule(static)
    for (uint32_t block_id = 0; block_id < num_blocks; block_id++) {
        int tid = omp_get_thread_num();
        uint32_t block_start, block_end;

        if (zone_maps) {
            // Zone map pruning: skip blocks where min_shipdate > threshold
            if (zone_maps[block_id].min_val > date_threshold) {
                continue;  // Entire block fails predicate
            }
            block_start = block_id * 100000;  // Block size = 100,000 rows
            block_end = std::min(block_start + 100000, (uint32_t)num_rows);
        } else {
            // Process all rows
            block_start = 0;
            block_end = (uint32_t)num_rows;
        }

        for (uint32_t i = block_start; i < block_end; i++) {
            // Prefetch ahead for better cache locality
            if (i + 64 < block_end) {
                __builtin_prefetch(&shipdate_data[i + 64], 0, 3);
                __builtin_prefetch(&returnflag_data[i + 64], 0, 3);
                __builtin_prefetch(&linestatus_data[i + 64], 0, 3);
            }

            if (shipdate_data[i] <= date_threshold) {
                int32_t rf = returnflag_data[i];
                int32_t ls = linestatus_data[i];
                int group_idx = rf * 2 + ls;

                if (group_idx >= 0 && group_idx < MAX_GROUPS) {
                    int64_t qty = quantity_data[i];
                    int64_t ep = extendedprice_data[i];
                    int64_t disc = discount_data[i];
                    int64_t tax = tax_data[i];

                    // Compute disc_price = ep * (1 - disc/100)
                    // At scale_factor=100, disc is in [0, 10]
                    // disc_price_scaled = ep * (100 - disc)
                    int64_t disc_price_scaled = ep * (100 - disc);  // Result at scale^2

                    // Compute charge = ep * (1 - disc/100) * (1 + tax/100)
                    // charge_scaled = ep * (100 - disc) * (100 + tax) / 100
                    // But to avoid intermediate overflow, compute at full scale:
                    // charge_scaled_tmp = ep * (100 - disc) * (100 + tax)
                    // Then divide by 100 later
                    int64_t charge_scaled = ep * (100 - disc) * (100 + tax);  // Result at scale^3

                    auto& agg = thread_local_aggs[tid][group_idx];
                    agg.sum_qty += qty;
                    agg.sum_extendedprice += ep;
                    agg.sum_disc_price += disc_price_scaled;
                    agg.sum_charge += charge_scaled;
                    agg.sum_discount += disc;
                    agg.sum_tax += tax;
                    agg.count += 1;
                }
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
    std::vector<AggregateRow> merged_aggs(MAX_GROUPS);
    for (int tid = 0; tid < num_threads; tid++) {
        for (int g = 0; g < MAX_GROUPS; g++) {
            const auto& agg = thread_local_aggs[tid][g];
            merged_aggs[g].sum_qty += agg.sum_qty;
            merged_aggs[g].sum_extendedprice += agg.sum_extendedprice;
            merged_aggs[g].sum_disc_price += agg.sum_disc_price;
            merged_aggs[g].sum_charge += agg.sum_charge;
            merged_aggs[g].sum_discount += agg.sum_discount;
            merged_aggs[g].sum_tax += agg.sum_tax;
            merged_aggs[g].count += agg.count;
        }
    }
#ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge: %.2f ms\n", merge_ms);
#endif

    // === BUILD RESULT ===
    std::vector<std::tuple<std::string, std::string, AggregateRow>> results;
    for (int g = 0; g < MAX_GROUPS; g++) {
        const auto& agg = merged_aggs[g];
        if (agg.count > 0) {
            int32_t rf_code = g / 2;
            int32_t ls_code = g % 2;

            auto rf_it = returnflag_dict.find(rf_code);
            auto ls_it = linestatus_dict.find(ls_code);
            std::string rf_str = (rf_it != returnflag_dict.end()) ? rf_it->second : "";
            std::string ls_str = (ls_it != linestatus_dict.end()) ? ls_it->second : "";
            results.push_back(std::make_tuple(rf_str, ls_str, agg));
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
