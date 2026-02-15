#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

namespace {

// Memory-mapped file access
class MmapFile {
public:
    MmapFile(const std::string& path) : fd(-1), data(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Cannot open file: " + path);
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            throw std::runtime_error("Cannot stat file: " + path);
        }
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("Cannot mmap file: " + path);
        }
    }

    ~MmapFile() {
        if (data != nullptr && size > 0) {
            munmap(data, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    template <typename T>
    const T* as() const {
        return reinterpret_cast<const T*>(data);
    }

    size_t get_size() const {
        return size;
    }

    size_t row_count() const {
        return size / sizeof(int32_t);  // Default for int32_t, will be overridden for int64_t
    }

private:
    int fd;
    void* data;
    size_t size;
};

// Zone map entry for int32_t columns (e.g., l_shipdate)
struct ZoneMapInt32 {
    int32_t min_val;
    int32_t max_val;
};

// Zone map entry for int64_t columns (e.g., l_discount, l_quantity)
struct ZoneMapDecimal {
    int64_t min_val;
    int64_t max_val;
};

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load binary columns
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string lineitem_dir = gendb_dir + "/lineitem/";

    MmapFile l_shipdate_file(lineitem_dir + "l_shipdate.bin");
    MmapFile l_discount_file(lineitem_dir + "l_discount.bin");
    MmapFile l_quantity_file(lineitem_dir + "l_quantity.bin");
    MmapFile l_extendedprice_file(lineitem_dir + "l_extendedprice.bin");

    const int32_t* l_shipdate = l_shipdate_file.as<int32_t>();
    const int64_t* l_discount = l_discount_file.as<int64_t>();
    const int64_t* l_quantity = l_quantity_file.as<int64_t>();
    const int64_t* l_extendedprice = l_extendedprice_file.as<int64_t>();

    // Total rows in lineitem table
    size_t num_rows = l_shipdate_file.get_size() / sizeof(int32_t);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // Load zone maps for efficient block skipping
    #ifdef GENDB_PROFILE
    auto t_zonemap_load_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string indexes_dir = gendb_dir + "/indexes/";
    MmapFile zone_map_shipdate_file(indexes_dir + "zone_map_l_shipdate.bin");
    MmapFile zone_map_discount_file(indexes_dir + "zone_map_l_discount.bin");
    MmapFile zone_map_quantity_file(indexes_dir + "zone_map_l_quantity.bin");

    const ZoneMapInt32* zone_map_shipdate = zone_map_shipdate_file.as<ZoneMapInt32>();
    const ZoneMapDecimal* zone_map_discount = zone_map_discount_file.as<ZoneMapDecimal>();
    const ZoneMapDecimal* zone_map_quantity = zone_map_quantity_file.as<ZoneMapDecimal>();

    size_t num_zones = zone_map_shipdate_file.get_size() / sizeof(ZoneMapInt32);

    #ifdef GENDB_PROFILE
    auto t_zonemap_load_end = std::chrono::high_resolution_clock::now();
    double zonemap_load_ms = std::chrono::duration<double, std::milli>(t_zonemap_load_end - t_zonemap_load_start).count();
    printf("[TIMING] zonemap_load: %.2f ms\n", zonemap_load_ms);
    #endif

    // Query predicates (from the storage guide):
    // l_shipdate >= 1994-01-01 (epoch day 8766)
    // l_shipdate < 1995-01-01 (epoch day 9131)
    // l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 to 7, but Q6 uses 0.06 ± 0.01)
    // l_quantity < 24 (scaled: < 2400)

    // Note: Q6 uses BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 = BETWEEN 0.05 AND 0.07
    // Scaled: 5 to 7 (inclusive on both ends)

    const int32_t date_lower = 8766;  // 1994-01-01
    const int32_t date_upper = 9131;  // 1995-01-01
    const int64_t discount_lower = 5;  // 0.05 scaled by 100
    const int64_t discount_upper = 7;  // 0.07 scaled by 100
    const int64_t quantity_upper = 2400; // 24 scaled by 100

    // Block-level pruning using zone maps
    #ifdef GENDB_PROFILE
    auto t_pruning_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<bool> block_alive(num_zones, true);

    // Prune blocks using zone maps
    for (size_t z = 0; z < num_zones; z++) {
        // Predicate: l_shipdate >= 8766 AND l_shipdate < 9131
        if (zone_map_shipdate[z].max_val < date_lower || zone_map_shipdate[z].min_val >= date_upper) {
            block_alive[z] = false;
            continue;
        }

        // Predicate: l_discount BETWEEN 5 AND 7
        if (zone_map_discount[z].max_val < discount_lower || zone_map_discount[z].min_val > discount_upper) {
            block_alive[z] = false;
            continue;
        }

        // Predicate: l_quantity < 2400
        if (zone_map_quantity[z].min_val >= quantity_upper) {
            block_alive[z] = false;
            continue;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_pruning_end = std::chrono::high_resolution_clock::now();
    double pruning_ms = std::chrono::duration<double, std::milli>(t_pruning_end - t_pruning_start).count();
    printf("[TIMING] pruning: %.2f ms\n", pruning_ms);
    #endif

    // Determine block boundaries (blocks of 100,000 rows)
    const uint32_t block_size = 100000;
    std::vector<std::pair<uint32_t, uint32_t>> blocks_to_process;

    for (size_t z = 0; z < num_zones; z++) {
        if (block_alive[z]) {
            uint32_t start = z * block_size;
            uint32_t end = std::min((size_t)(start + block_size), num_rows);
            blocks_to_process.push_back({start, end});
        }
    }

    // Scan, filter, and aggregate in parallel
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Use thread-local aggregation to avoid lock contention
    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_local_sum(num_threads, 0);

    #pragma omp parallel for schedule(dynamic) collapse(1)
    for (size_t block_idx = 0; block_idx < blocks_to_process.size(); block_idx++) {
        uint32_t start_row = blocks_to_process[block_idx].first;
        uint32_t end_row = blocks_to_process[block_idx].second;
        int tid = omp_get_thread_num();

        for (uint32_t i = start_row; i < end_row; i++) {
            // Apply all four predicates
            if (l_shipdate[i] >= date_lower && l_shipdate[i] < date_upper &&
                l_discount[i] >= discount_lower && l_discount[i] <= discount_upper &&
                l_quantity[i] < quantity_upper) {

                // Accumulate: l_extendedprice * l_discount
                // Both are scaled by 100, so product is scaled by 10,000
                // We accumulate at full precision and scale down at the end
                int64_t product = l_extendedprice[i] * l_discount[i];
                thread_local_sum[tid] += product;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);
    #endif

    // Merge thread-local results
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t total_sum = 0;
    for (int i = 0; i < num_threads; i++) {
        total_sum += thread_local_sum[i];
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge: %.2f ms\n", merge_ms);
    #endif

    // Scale down: product was scaled by 10,000 (100 * 100), so divide by 10,000
    // However, we want the final result in the original scale (0.01 cents for TPC-H)
    // l_extendedprice is in 0.01 units, l_discount is in 0.01 units (0.01 = 1 in scaled form)
    // So l_extendedprice * l_discount / 100 gives us the revenue in 0.01 units
    double revenue = total_sum / 10000.0;

    // Write results to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);

    out << "revenue\n";
    out.precision(4);
    out << std::fixed << revenue << "\n";
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

    std::cout << "Q6 execution complete. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    try {
        run_q6(gendb_dir, results_dir);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#endif
