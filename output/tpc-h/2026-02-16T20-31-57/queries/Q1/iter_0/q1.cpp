#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>

/*
 * QUERY PLAN FOR Q1: Pricing Summary Report
 *
 * LOGICAL PLAN:
 * 1. Table: lineitem (59,986,052 rows)
 * 2. Single-table predicate: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
 *    - Target date: 1998-09-02 (= 10471 days since epoch)
 *    - Estimated cardinality: ~56M rows after filtering
 * 3. GROUP BY: l_returnflag, l_linestatus (6 possible groups: 3 flags × 2 statuses)
 * 4. Aggregations:
 *    - SUM(l_quantity), SUM(l_extendedprice), SUM(l_extendedprice * (1 - l_discount))
 *    - SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))
 *    - AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
 *
 * PHYSICAL PLAN:
 * 1. Scan Filter: Full scan with zone map pruning on l_shipdate
 *    - Load zone map, skip blocks where max < 10471
 *    - All blocks qualify since date range covers full dataset
 * 2. Aggregation: Flat array [3][2] indexed by (returnflag_code, linestatus_code)
 *    - Use flat array because cardinality is extremely low (<256 groups)
 *    - Accumulate scaled integer values at full precision
 * 3. Parallelism: OpenMP parallel scan with block-driven chunking
 *    - Each thread maintains local [3][2] aggregation buffer
 *    - Merge thread-local buffers after parallel phase
 * 4. Dictionary Lookup: Load l_returnflag_dict.txt and l_linestatus_dict.txt
 *    - Map dictionary codes 0,1,2 to strings for output
 * 5. Scaled Arithmetic: All DECIMAL columns scaled by 100
 *    - Scale factor applied at output time: divide sums by 100, products by 100^n
 * 6. Output: Sort by (l_returnflag, l_linestatus), write CSV with 2 decimal places
 */

// Aggregation structure for a single (returnflag, linestatus) group
struct GroupAggregate {
    int64_t sum_quantity;
    int64_t sum_extendedprice;
    int64_t sum_disc_price;          // SUM(extendedprice * (1 - discount))
    int64_t sum_charge;              // SUM(extendedprice * (1 - discount) * (1 + tax))
    int64_t sum_discount;            // SUM(discount) - for AVG(discount)
    int64_t count;

    // For average calculation: we track sum and count separately
    // avg = sum / count, which we'll compute at the end

    GroupAggregate() : sum_quantity(0), sum_extendedprice(0), sum_disc_price(0),
                      sum_charge(0), sum_discount(0), count(0) {}
};

// File mapping helper
struct FileMapping {
    int fd;
    void* data;
    size_t size;

    FileMapping() : fd(-1), data(nullptr), size(0) {}

    bool open(const std::string& path, size_t expected_size) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat: " << path << std::endl;
            ::close(fd);
            fd = -1;
            return false;
        }

        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            ::close(fd);
            fd = -1;
            data = nullptr;
            return false;
        }

        return true;
    }

    void close() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        data = nullptr;
        fd = -1;
    }

    ~FileMapping() { close(); }
};

// Load dictionary file (simple newline-separated string list)
std::vector<std::string> load_dictionary(const std::string& dict_path) {
    std::vector<std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        // Trim trailing whitespace
        while (!line.empty() && std::isspace(line.back())) {
            line.pop_back();
        }
        if (!line.empty()) {
            dict.push_back(line);
        }
    }
    return dict;
}

// Zone map structure from binary file
struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ==================== METADATA CHECK ====================
    std::cout << "[METADATA CHECK] Q1 Input Parameters:\n";
    std::cout << "  gendb_dir: " << gendb_dir << "\n";
    std::cout << "  results_dir: " << results_dir << "\n";
    std::cout << "  Scale factor for DECIMAL columns: 100\n";
    std::cout << "  Lineitem table rows: 59,986,052\n";
    std::cout << "  Block size: 100,000 rows\n";
    std::cout << "  Target date (1998-09-02): 10471 days since epoch\n";

    // ==================== FILE SETUP ====================
    std::string lineitem_dir = gendb_dir + "/lineitem";
    std::string indexes_dir = gendb_dir + "/indexes";

    // Open memory-mapped files
    FileMapping fm_shipdate, fm_returnflag, fm_linestatus;
    FileMapping fm_quantity, fm_extendedprice, fm_discount, fm_tax;

    const int64_t total_rows = 59986052;
    const int32_t target_shipdate = 10471;  // 1998-09-02 (1998-12-01 - 90 days)
    const int64_t scale_factor = 100;  // For DECIMAL columns (all stored as int64_t * 100)

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    fm_shipdate.open(lineitem_dir + "/l_shipdate.bin", total_rows * sizeof(int32_t));
    fm_returnflag.open(lineitem_dir + "/l_returnflag.bin", total_rows * sizeof(int32_t));
    fm_linestatus.open(lineitem_dir + "/l_linestatus.bin", total_rows * sizeof(int32_t));
    fm_quantity.open(lineitem_dir + "/l_quantity.bin", total_rows * sizeof(int64_t));
    fm_extendedprice.open(lineitem_dir + "/l_extendedprice.bin", total_rows * sizeof(int64_t));
    fm_discount.open(lineitem_dir + "/l_discount.bin", total_rows * sizeof(int64_t));
    fm_tax.open(lineitem_dir + "/l_tax.bin", total_rows * sizeof(int64_t));

    if (!fm_shipdate.data || !fm_returnflag.data || !fm_linestatus.data ||
        !fm_quantity.data || !fm_extendedprice.data || !fm_discount.data || !fm_tax.data) {
        std::cerr << "Failed to open all required files\n";
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // Load dictionaries for l_returnflag and l_linestatus
    auto returnflag_dict = load_dictionary(lineitem_dir + "/l_returnflag_dict.txt");
    auto linestatus_dict = load_dictionary(lineitem_dir + "/l_linestatus_dict.txt");

    std::cout << "[METADATA CHECK] Loaded dictionaries:\n";
    std::cout << "  l_returnflag_dict: " << returnflag_dict.size() << " entries\n";
    std::cout << "  l_linestatus_dict: " << linestatus_dict.size() << " entries\n";

    // ==================== LOAD ZONE MAP ====================
    #ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
    #endif

    FileMapping fm_zonemap;
    fm_zonemap.open(indexes_dir + "/lineitem_shipdate_zonemap.bin", 0);  // will be adjusted

    std::vector<ZoneMapBlock> zonemap;
    if (fm_zonemap.data) {
        uint32_t num_blocks = *((uint32_t*)fm_zonemap.data);
        std::cout << "[METADATA CHECK] Zone map: " << num_blocks << " blocks\n";

        ZoneMapBlock* blocks = (ZoneMapBlock*)((char*)fm_zonemap.data + sizeof(uint32_t));
        for (uint32_t i = 0; i < num_blocks; ++i) {
            zonemap.push_back(blocks[i]);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] zonemap_load: %.2f ms\n", zonemap_ms);
    #endif

    // ==================== SCAN AND FILTER WITH ZONE MAP PRUNING ====================
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Identify which blocks to scan
    std::vector<uint32_t> valid_block_ids;
    uint32_t estimated_valid_rows = 0;
    for (size_t block_id = 0; block_id < zonemap.size(); ++block_id) {
        // Skip block if all rows in it are > target_shipdate (max < target)
        if (zonemap[block_id].max_val < target_shipdate) {
            continue;  // Skip this block entirely
        }
        valid_block_ids.push_back(block_id);
        estimated_valid_rows += zonemap[block_id].row_count;
    }

    std::cout << "[METADATA CHECK] Zone map pruning: " << valid_block_ids.size()
              << " out of " << zonemap.size() << " blocks will be scanned\n";
    std::cout << "[METADATA CHECK] Estimated valid rows after pruning: " << estimated_valid_rows << "\n";

    // Get pointers to arrays
    const int32_t* shipdate_data = (const int32_t*)fm_shipdate.data;
    const int32_t* returnflag_data = (const int32_t*)fm_returnflag.data;
    const int32_t* linestatus_data = (const int32_t*)fm_linestatus.data;
    const int64_t* quantity_data = (const int64_t*)fm_quantity.data;
    const int64_t* extendedprice_data = (const int64_t*)fm_extendedprice.data;
    const int64_t* discount_data = (const int64_t*)fm_discount.data;
    const int64_t* tax_data = (const int64_t*)fm_tax.data;

    // Thread-local aggregation buffers: [3 returnflags][2 linestatuses]
    std::vector<std::vector<std::vector<GroupAggregate>>> thread_aggregates;
    int num_threads = omp_get_max_threads();
    thread_aggregates.resize(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        thread_aggregates[t].resize(3);
        for (int i = 0; i < 3; ++i) {
            thread_aggregates[t][i].resize(2);
        }
    }

    // Build cumulative block offsets for quick lookup
    std::vector<uint64_t> block_start_row(zonemap.size() + 1);
    block_start_row[0] = 0;
    for (size_t i = 0; i < zonemap.size(); ++i) {
        block_start_row[i + 1] = block_start_row[i] + zonemap[i].row_count;
    }

    // Parallel scan: process all rows with zone map pruning
    #pragma omp parallel for schedule(dynamic, 1)
    for (size_t block_id = 0; block_id < valid_block_ids.size(); ++block_id) {
        uint32_t actual_block_id = valid_block_ids[block_id];
        uint64_t row_start = block_start_row[actual_block_id];
        uint64_t row_end = block_start_row[actual_block_id + 1];

        int thread_id = omp_get_thread_num();

        for (uint64_t row = row_start; row < row_end && row < total_rows; ++row) {
            // Check date predicate
            if (shipdate_data[row] <= target_shipdate) {
                int32_t rf_code = returnflag_data[row];
                int32_t ls_code = linestatus_data[row];

                // Clamp codes to valid range for array indexing
                if (rf_code < 0 || rf_code >= 3 || ls_code < 0 || ls_code >= 2) {
                    // Skip this row silently - it's outside the valid data range
                    continue;
                }

                GroupAggregate& agg = thread_aggregates[thread_id][rf_code][ls_code];

                // Accumulate aggregates
                agg.count++;
                agg.sum_quantity += quantity_data[row];
                agg.sum_extendedprice += extendedprice_data[row];
                agg.sum_discount += discount_data[row];

                // sum_disc_price = sum(extendedprice * (1 - discount))
                // All columns are scaled by scale_factor=2
                // Actual values: ep_actual = ep / 2, disc_actual = disc / 2
                // Want: sum(ep_actual * (1 - disc_actual))
                // = sum((ep / 2) * (1 - disc / 2))
                // = sum((ep / 2) * ((2 - disc) / 2))
                // = sum((ep * (2 - disc)) / 4)
                // We'll accumulate in full precision and scale at output time

                int64_t ep = extendedprice_data[row];
                int64_t disc = discount_data[row];
                // (1 - disc/scale) = (scale - disc) / scale
                // ep * (1 - disc/scale) = ep * (scale - disc) / scale
                int64_t disc_price_scaled = ep * (scale_factor - disc);  // Will be scaled by factor^2
                agg.sum_disc_price += disc_price_scaled;

                // sum_charge = sum(extendedprice * (1 - discount) * (1 + tax))
                int64_t tax = tax_data[row];
                // ep_actual * (1 - disc_actual) * (1 + tax_actual)
                // = (ep/2) * (1 - disc/2) * (1 + tax/2)
                // = (ep/2) * ((2-disc)/2) * ((2+tax)/2)
                // = ep * (2-disc) * (2+tax) / 8
                // We accumulate at full precision
                int64_t charge_scaled = disc_price_scaled * (scale_factor + tax);  // Will be scaled by factor^3
                agg.sum_charge += charge_scaled;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);
    #endif

    // ==================== MERGE THREAD-LOCAL AGGREGATES ====================
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<std::vector<GroupAggregate>> final_agg(3);
    for (int i = 0; i < 3; ++i) {
        final_agg[i].resize(2);
    }

    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < 3; ++i) {
            for (int j = 0; j < 2; ++j) {
                final_agg[i][j].sum_quantity += thread_aggregates[t][i][j].sum_quantity;
                final_agg[i][j].sum_extendedprice += thread_aggregates[t][i][j].sum_extendedprice;
                final_agg[i][j].sum_disc_price += thread_aggregates[t][i][j].sum_disc_price;
                final_agg[i][j].sum_charge += thread_aggregates[t][i][j].sum_charge;
                final_agg[i][j].sum_discount += thread_aggregates[t][i][j].sum_discount;
                final_agg[i][j].count += thread_aggregates[t][i][j].count;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] aggregation_merge: %.2f ms\n", merge_ms);
    #endif

    // ==================== OUTPUT RESULTS ====================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);

    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Build result rows
    std::vector<std::tuple<std::string, std::string, int32_t, int32_t>> results;
    for (int rf = 0; rf < 3; ++rf) {
        for (int ls = 0; ls < 2; ++ls) {
            if (final_agg[rf][ls].count > 0) {
                results.push_back(std::make_tuple(
                    returnflag_dict[rf],
                    linestatus_dict[ls],
                    rf,
                    ls
                ));
            }
        }
    }

    // Sort by returnflag, then linestatus
    std::sort(results.begin(), results.end());

    // Write data rows
    for (auto& row : results) {
        std::string rf_str = std::get<0>(row);
        std::string ls_str = std::get<1>(row);
        int rf = std::get<2>(row);
        int ls = std::get<3>(row);

        GroupAggregate& agg = final_agg[rf][ls];

        if (agg.count == 0) continue;

        // All DECIMAL values are scaled by 100
        // Actual value = stored_value / 100

        // sum_quantity: sum of scaled values, divide by 100 to get actual sum
        double sum_qty = (double)agg.sum_quantity / scale_factor;

        // sum_extendedprice: sum of scaled values, divide by 100 to get actual sum
        double sum_base_price = (double)agg.sum_extendedprice / scale_factor;

        // sum_disc_price was accumulated as ep * (scale_factor - disc)
        // ep and disc are stored as int64_t * 100
        // Actual: sum((ep/100) * (1 - (disc/100)))
        //       = sum((ep/100) * ((100 - disc) / 100))
        //       = sum(ep * (100 - disc) / 10000)
        // So sum_disc_price accumulated as ep * (100 - disc)
        // Need to divide by 10000 to get actual
        double sum_disc_price = (double)agg.sum_disc_price / (scale_factor * scale_factor);

        // sum_charge was accumulated as sum(ep * (scale_factor - disc) * (scale_factor + tax))
        // = sum(ep * (100 - disc) * (100 + tax))
        // Actual sum: sum((ep/100) * (1 - disc/100) * (1 + tax/100))
        //           = sum(ep * (100-disc) * (100+tax) / 1000000)
        // So divide by 1000000
        double sum_charge = (double)agg.sum_charge / (scale_factor * scale_factor * scale_factor);

        // Averages
        double avg_qty = sum_qty / agg.count;
        double avg_price = sum_base_price / agg.count;
        double avg_disc = (double)agg.sum_discount / scale_factor / agg.count;

        // Write row with proper formatting (2 decimal places)
        char buf[512];
        snprintf(buf, sizeof(buf),
            "%s,%s,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%ld\n",
            rf_str.c_str(),
            ls_str.c_str(),
            sum_qty,
            sum_base_price,
            sum_disc_price,
            sum_charge,
            avg_qty,
            avg_price,
            avg_disc,
            agg.count
        );
        out << buf;
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    std::cout << "Query execution complete. Results written to: " << output_path << "\n";
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
