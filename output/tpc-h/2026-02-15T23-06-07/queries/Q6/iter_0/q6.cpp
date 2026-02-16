#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <omp.h>

// Zone map structures
struct ZoneMapEntry_int32 {
    int32_t min_val;
    int32_t max_val;
    uint32_t start_row;
    uint32_t row_count;
};

struct ZoneMapEntry_int64 {
    int64_t min_val;
    int64_t max_val;
    uint32_t start_row;
    uint32_t row_count;
};

// Date constants (computed from epoch formula)
const int32_t DATE_1994_01_01 = 8766;  // days since 1970-01-01
const int32_t DATE_1995_01_01 = 9131;  // days since 1970-01-01

// Predicate constants (scaled integers)
const int64_t DISCOUNT_MIN = 5;   // 0.05 * 100
const int64_t DISCOUNT_MAX = 7;   // 0.07 * 100
const int64_t QUANTITY_MAX = 2400; // 24.00 * 100

// Memory-mapped file helper
template<typename T>
const T* mmap_file(const std::string& path, size_t& count, int& fd_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        std::cerr << "Error: Cannot stat " << path << std::endl;
        close(fd);
        return nullptr;
    }
    void* addr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        std::cerr << "Error: Cannot mmap " << path << std::endl;
        close(fd);
        return nullptr;
    }
    count = st.st_size / sizeof(T);
    fd_out = fd;
    return static_cast<const T*>(addr);
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load column data
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    int fd_shipdate, fd_discount, fd_quantity, fd_extendedprice;
    size_t count_shipdate, count_discount, count_quantity, count_extendedprice;

    const int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", count_shipdate, fd_shipdate);
    const int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", count_discount, fd_discount);
    const int64_t* l_quantity = mmap_file<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", count_quantity, fd_quantity);
    const int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", count_extendedprice, fd_extendedprice);

    if (!l_shipdate || !l_discount || !l_quantity || !l_extendedprice) {
        std::cerr << "Error: Failed to load column data" << std::endl;
        return;
    }

    size_t num_rows = count_shipdate;
    if (count_discount != num_rows || count_quantity != num_rows || count_extendedprice != num_rows) {
        std::cerr << "Error: Column size mismatch" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
    #endif

    // Load zone maps
    #ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
    #endif

    int fd_zm_shipdate, fd_zm_discount, fd_zm_quantity;
    size_t count_zm_shipdate, count_zm_discount, count_zm_quantity;

    const ZoneMapEntry_int32* zm_shipdate = mmap_file<ZoneMapEntry_int32>(
        gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin", count_zm_shipdate, fd_zm_shipdate);
    const ZoneMapEntry_int64* zm_discount = mmap_file<ZoneMapEntry_int64>(
        gendb_dir + "/indexes/lineitem_l_discount_zonemap.bin", count_zm_discount, fd_zm_discount);
    const ZoneMapEntry_int64* zm_quantity = mmap_file<ZoneMapEntry_int64>(
        gendb_dir + "/indexes/lineitem_l_quantity_zonemap.bin", count_zm_quantity, fd_zm_quantity);

    if (!zm_shipdate || !zm_discount || !zm_quantity) {
        std::cerr << "Error: Failed to load zone maps" << std::endl;
        return;
    }

    // Skip the first uint32_t (num_entries header)
    size_t num_zones_shipdate = (count_zm_shipdate > 0) ? count_zm_shipdate - 1 : 0;
    size_t num_zones_discount = (count_zm_discount > 0) ? count_zm_discount - 1 : 0;
    size_t num_zones_quantity = (count_zm_quantity > 0) ? count_zm_quantity - 1 : 0;

    // Adjust pointers to skip header (stored as first uint32_t)
    const uint32_t* header_shipdate = reinterpret_cast<const uint32_t*>(zm_shipdate);
    const uint32_t* header_discount = reinterpret_cast<const uint32_t*>(zm_discount);
    const uint32_t* header_quantity = reinterpret_cast<const uint32_t*>(zm_quantity);

    num_zones_shipdate = header_shipdate[0];
    num_zones_discount = header_discount[0];
    num_zones_quantity = header_quantity[0];

    zm_shipdate = reinterpret_cast<const ZoneMapEntry_int32*>(header_shipdate + 1);
    zm_discount = reinterpret_cast<const ZoneMapEntry_int64*>(header_discount + 1);
    zm_quantity = reinterpret_cast<const ZoneMapEntry_int64*>(header_quantity + 1);

    #ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double ms_zonemap = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] zonemap_load: %.2f ms\n", ms_zonemap);
    printf("[INFO] Zone maps loaded: shipdate=%zu, discount=%zu, quantity=%zu\n",
           num_zones_shipdate, num_zones_discount, num_zones_quantity);
    #endif

    // Build block alive bitmap by intersecting all zone map predicates
    #ifdef GENDB_PROFILE
    auto t_prune_start = std::chrono::high_resolution_clock::now();
    #endif

    // Use the minimum zone count (all should be the same)
    size_t num_zones = std::min({num_zones_shipdate, num_zones_discount, num_zones_quantity});
    std::vector<bool> block_alive(num_zones, true);

    size_t blocks_pruned = 0;

    for (size_t z = 0; z < num_zones; z++) {
        // Shipdate predicate: [8766, 9131)
        if (zm_shipdate[z].max_val < DATE_1994_01_01 || zm_shipdate[z].min_val >= DATE_1995_01_01) {
            block_alive[z] = false;
        }
        // Discount predicate: [5, 7]
        if (zm_discount[z].max_val < DISCOUNT_MIN || zm_discount[z].min_val > DISCOUNT_MAX) {
            block_alive[z] = false;
        }
        // Quantity predicate: < 2400
        if (zm_quantity[z].min_val >= QUANTITY_MAX) {
            block_alive[z] = false;
        }

        if (!block_alive[z]) blocks_pruned++;
    }

    #ifdef GENDB_PROFILE
    auto t_prune_end = std::chrono::high_resolution_clock::now();
    double ms_prune = std::chrono::duration<double, std::milli>(t_prune_end - t_prune_start).count();
    printf("[TIMING] prune: %.2f ms\n", ms_prune);
    printf("[INFO] Blocks pruned: %zu/%zu (%.1f%%)\n", blocks_pruned, num_zones, 100.0 * blocks_pruned / num_zones);
    #endif

    // Parallel scan and aggregation
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t total_revenue = 0;
    size_t qualifying_rows = 0;

    #pragma omp parallel reduction(+:total_revenue,qualifying_rows)
    {
        #pragma omp for schedule(dynamic, 1)
        for (size_t z = 0; z < num_zones; z++) {
            if (!block_alive[z]) continue;

            uint32_t start = zm_shipdate[z].start_row;
            uint32_t end = start + zm_shipdate[z].row_count;

            for (uint32_t r = start; r < end; r++) {
                // Apply all predicates
                int32_t shipdate = l_shipdate[r];
                int64_t discount = l_discount[r];
                int64_t quantity = l_quantity[r];

                if (shipdate >= DATE_1994_01_01 && shipdate < DATE_1995_01_01 &&
                    discount >= DISCOUNT_MIN && discount <= DISCOUNT_MAX &&
                    quantity < QUANTITY_MAX) {

                    // Accumulate: extendedprice * discount
                    // Both scaled by 100 → result scaled by 10000
                    int64_t revenue_contribution = l_extendedprice[r] * discount;
                    total_revenue += revenue_contribution;
                    qualifying_rows++;
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_scan);
    printf("[INFO] Qualifying rows: %zu\n", qualifying_rows);
    #endif

    // Scale down result: both columns scaled by 100 → divide by 10000
    double revenue_decimal = static_cast<double>(total_revenue) / 10000.0;

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif

    // Write results
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    if (!out) {
        std::cerr << "Error: Cannot write to " << output_path << std::endl;
        return;
    }

    out << "revenue\n";
    out << std::fixed << std::setprecision(2) << revenue_decimal << "\n";
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    // Cleanup
    munmap((void*)l_shipdate, count_shipdate * sizeof(int32_t));
    munmap((void*)l_discount, count_discount * sizeof(int64_t));
    munmap((void*)l_quantity, count_quantity * sizeof(int64_t));
    munmap((void*)l_extendedprice, count_extendedprice * sizeof(int64_t));

    close(fd_shipdate);
    close(fd_discount);
    close(fd_quantity);
    close(fd_extendedprice);

    munmap((void*)zm_shipdate, (count_zm_shipdate - 1) * sizeof(ZoneMapEntry_int32));
    munmap((void*)zm_discount, (count_zm_discount - 1) * sizeof(ZoneMapEntry_int64));
    munmap((void*)zm_quantity, (count_zm_quantity - 1) * sizeof(ZoneMapEntry_int64));

    close(fd_zm_shipdate);
    close(fd_zm_discount);
    close(fd_zm_quantity);

    std::cout << "Query Q6 completed. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
