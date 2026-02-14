// q6.cpp - TPC-H Q6: Forecasting Revenue Change
// Single-table scan with range predicates on lineitem

#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <atomic>
#include <cstring>
#include <iomanip>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

// Zone map structure for int32_t (shipdate)
struct ZoneMapInt32 {
    uint64_t start_row;
    int32_t min_val;
    int32_t max_val;
    uint64_t end_row;
};

// Zone map structure for double (discount, quantity)
struct ZoneMapDouble {
    uint64_t start_row;
    double min_val;
    double max_val;
    uint64_t end_row;
};

// Memory-mapped file helper
void* mmapFile(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }

    size_out = sb.st_size;
    void* ptr = mmap(nullptr, size_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        return nullptr;
    }

    // Advise sequential access
    madvise(ptr, size_out, MADV_SEQUENTIAL);
    return ptr;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // Q6 predicates:
    // l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
    // l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    // l_quantity < 24

    // Convert dates to epoch days (days since 1970-01-01)
    // 1994-01-01 = 8766 days
    // 1995-01-01 = 9131 days
    const int32_t shipdate_min = 8766;
    const int32_t shipdate_max = 9131;
    // NOTE: TPC-H specifies l_discount as DECIMAL(15,2), so the SQL engine
    // evaluates "0.06 - 0.01" as DECIMAL arithmetic = exactly 0.05
    // We use literal 0.05 and 0.07 to match DECIMAL semantics
    const double discount_min = 0.05;
    const double discount_max = 0.07;
    const double quantity_max = 24.0;

    const std::string lineitem_dir = gendb_dir + "/lineitem/";

    // Load columns
    size_t shipdate_size, discount_size, quantity_size, extendedprice_size;
    const int32_t* l_shipdate = (const int32_t*)mmapFile(lineitem_dir + "l_shipdate.bin", shipdate_size);
    const double* l_discount = (const double*)mmapFile(lineitem_dir + "l_discount.bin", discount_size);
    const double* l_quantity = (const double*)mmapFile(lineitem_dir + "l_quantity.bin", quantity_size);
    const double* l_extendedprice = (const double*)mmapFile(lineitem_dir + "l_extendedprice.bin", extendedprice_size);

    if (!l_shipdate || !l_discount || !l_quantity || !l_extendedprice) {
        std::cerr << "Failed to mmap columns" << std::endl;
        return;
    }

    const size_t row_count = shipdate_size / sizeof(int32_t);
    const size_t block_size = 100000;  // Match storage block size
    const size_t num_blocks = (row_count + block_size - 1) / block_size;

    // Load zone maps
    auto zonemap_start = std::chrono::high_resolution_clock::now();

    size_t shipdate_zm_size, discount_zm_size, quantity_zm_size;
    const ZoneMapInt32* shipdate_zm = (const ZoneMapInt32*)mmapFile(
        lineitem_dir + "l_shipdate.zonemap.idx", shipdate_zm_size);
    const ZoneMapDouble* discount_zm = (const ZoneMapDouble*)mmapFile(
        lineitem_dir + "l_discount.zonemap.idx", discount_zm_size);
    const ZoneMapDouble* quantity_zm = (const ZoneMapDouble*)mmapFile(
        lineitem_dir + "l_quantity.zonemap.idx", quantity_zm_size);

    const size_t num_shipdate_zones = shipdate_zm ? (shipdate_zm_size / sizeof(ZoneMapInt32)) : 0;
    const size_t num_discount_zones = discount_zm ? (discount_zm_size / sizeof(ZoneMapDouble)) : 0;
    const size_t num_quantity_zones = quantity_zm ? (quantity_zm_size / sizeof(ZoneMapDouble)) : 0;

    // Build skip list (zones to process)
    // Note: Zone maps store start_row but end_row field is unreliable
    // Calculate blocks based on block_size instead
    std::vector<bool> skip_block(num_blocks, false);
    size_t blocks_skipped = 0;

    for (size_t b = 0; b < num_blocks && b < num_shipdate_zones; ++b) {
        // Check shipdate zone map
        if (shipdate_zm && (shipdate_zm[b].max_val < shipdate_min ||
                           shipdate_zm[b].min_val >= shipdate_max)) {
            skip_block[b] = true;
            blocks_skipped++;
            continue;
        }

        // Check discount zone map (if available)
        if (discount_zm && b < num_discount_zones) {
            if (discount_zm[b].max_val < discount_min ||
                discount_zm[b].min_val > discount_max) {
                skip_block[b] = true;
                blocks_skipped++;
                continue;
            }
        }

        // Check quantity zone map (if available)
        if (quantity_zm && b < num_quantity_zones) {
            if (quantity_zm[b].min_val >= quantity_max) {
                skip_block[b] = true;
                blocks_skipped++;
                continue;
            }
        }
    }

    auto zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(zonemap_end - zonemap_start).count();
    std::cout << "[TIMING] zonemap_load: " << std::fixed << std::setprecision(1)
              << zonemap_ms << " ms (skipped " << blocks_skipped << "/"
              << num_blocks << " blocks)" << std::endl;

    // Parallel scan and aggregation
    auto scan_start = std::chrono::high_resolution_clock::now();

    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<double> thread_revenues(num_threads, 0.0);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            double local_revenue = 0.0;

            for (size_t block = t; block < num_blocks; block += num_threads) {
                // Check if block is skipped
                if (block < skip_block.size() && skip_block[block]) {
                    continue;
                }

                const size_t start_row = block * block_size;
                const size_t end_row = std::min(start_row + block_size, row_count);

                for (size_t i = start_row; i < end_row; ++i) {
                    // Apply filters
                    if (l_shipdate[i] >= shipdate_min &&
                        l_shipdate[i] < shipdate_max &&
                        l_discount[i] >= discount_min &&
                        l_discount[i] <= discount_max &&
                        l_quantity[i] < quantity_max) {

                        local_revenue += l_extendedprice[i] * l_discount[i];
                    }
                }
            }

            thread_revenues[t] = local_revenue;
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(scan_end - scan_start).count();
    std::cout << "[TIMING] scan_filter: " << std::fixed << std::setprecision(1)
              << scan_ms << " ms" << std::endl;

    // Aggregate results
    auto agg_start = std::chrono::high_resolution_clock::now();

    double total_revenue = 0.0;
    for (double rev : thread_revenues) {
        total_revenue += rev;
    }

    auto agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(agg_end - agg_start).count();
    std::cout << "[TIMING] aggregation: " << std::fixed << std::setprecision(1)
              << agg_ms << " ms" << std::endl;

    // Write results
    auto output_start = std::chrono::high_resolution_clock::now();

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q6.csv");
        out << "revenue\n";
        out << std::fixed << std::setprecision(2) << total_revenue << "\n";
        out.close();
    }

    auto output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(output_end - output_start).count();
    std::cout << "[TIMING] output: " << std::fixed << std::setprecision(1)
              << output_ms << " ms" << std::endl;

    // Cleanup
    munmap((void*)l_shipdate, shipdate_size);
    munmap((void*)l_discount, discount_size);
    munmap((void*)l_quantity, quantity_size);
    munmap((void*)l_extendedprice, extendedprice_size);
    if (shipdate_zm) munmap((void*)shipdate_zm, shipdate_zm_size);
    if (discount_zm) munmap((void*)discount_zm, discount_zm_size);
    if (quantity_zm) munmap((void*)quantity_zm, quantity_zm_size);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "[TIMING] total: " << std::fixed << std::setprecision(1)
              << total_ms << " ms" << std::endl;
    std::cout << "Query returned 1 rows" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q6(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
