#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <vector>
#include <chrono>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// Constants
constexpr int32_t DATE_1994_01_01 = 8766;   // 1994-01-01 in days since epoch
constexpr int32_t DATE_1995_01_01 = 9131;   // 1995-01-01 in days since epoch
constexpr int64_t DISCOUNT_MIN = 5;         // 0.05 * 100
constexpr int64_t DISCOUNT_MAX = 7;         // 0.07 * 100
constexpr int64_t QUANTITY_MAX = 2400;      // 24 * 100

// Utility: map a file into memory
struct MmapFile {
    int fd = -1;
    void* data = nullptr;
    size_t size = 0;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            std::cerr << "Failed to stat: " << path << std::endl;
            ::close(fd);
            return false;
        }

        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            ::close(fd);
            return false;
        }

        return true;
    }

    void close() {
        if (data != nullptr) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

// Zone map reader
struct ZoneMapIndex {
    uint32_t num_zones = 0;
    struct Zone {
        int32_t min_value;
        int32_t max_value;
        uint32_t row_count;
    };
    std::vector<Zone> zones;

    bool load(const std::string& path) {
        MmapFile file;
        if (!file.open(path)) return false;

        if (file.size < sizeof(uint32_t)) {
            std::cerr << "Zone map file too small" << std::endl;
            return false;
        }

        // Read number of zones
        num_zones = *reinterpret_cast<uint32_t*>(file.data);

        // Expected size: 4 (header) + num_zones * 12 (zone entries)
        size_t expected_size = sizeof(uint32_t) + num_zones * sizeof(Zone);
        if (file.size != expected_size) {
            std::cerr << "Zone map size mismatch: expected " << expected_size
                      << ", got " << file.size << std::endl;
            return false;
        }

        // Read zones
        Zone* zone_ptr = reinterpret_cast<Zone*>(reinterpret_cast<uint8_t*>(file.data) + sizeof(uint32_t));
        zones.assign(zone_ptr, zone_ptr + num_zones);

        return true;
    }
};

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    // === [TIMING] Initialization ===
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Construct file paths
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    std::string indexes_dir = gendb_dir + "/indexes/";

    // === [TIMING] Load Zone Map ===
#ifdef GENDB_PROFILE
    auto t_zone_start = std::chrono::high_resolution_clock::now();
#endif

    ZoneMapIndex zone_map;
    if (!zone_map.load(indexes_dir + "lineitem_l_shipdate_zone.bin")) {
        std::cerr << "Failed to load zone map" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    auto t_zone_end = std::chrono::high_resolution_clock::now();
    double zone_ms = std::chrono::duration<double, std::milli>(t_zone_end - t_zone_start).count();
    printf("[TIMING] load_zone_map: %.2f ms\n", zone_ms);
#endif

    // === [TIMING] Mmap Binary Columns ===
#ifdef GENDB_PROFILE
    auto t_mmap_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile col_shipdate, col_discount, col_quantity, col_extendedprice;

    if (!col_shipdate.open(lineitem_dir + "l_shipdate.bin") ||
        !col_discount.open(lineitem_dir + "l_discount.bin") ||
        !col_quantity.open(lineitem_dir + "l_quantity.bin") ||
        !col_extendedprice.open(lineitem_dir + "l_extendedprice.bin")) {
        std::cerr << "Failed to open column files" << std::endl;
        return;
    }

    int32_t* shipdate_data = reinterpret_cast<int32_t*>(col_shipdate.data);
    int64_t* discount_data = reinterpret_cast<int64_t*>(col_discount.data);
    int64_t* quantity_data = reinterpret_cast<int64_t*>(col_quantity.data);
    int64_t* extendedprice_data = reinterpret_cast<int64_t*>(col_extendedprice.data);

    uint64_t total_rows = col_shipdate.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_mmap_end = std::chrono::high_resolution_clock::now();
    double mmap_ms = std::chrono::duration<double, std::milli>(t_mmap_end - t_mmap_start).count();
    printf("[TIMING] mmap_columns: %.2f ms\n", mmap_ms);
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", load_ms);
#endif

    // === [TIMING] Scan & Filter ===
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Use zone map to identify valid block ranges
    std::vector<std::pair<uint64_t, uint64_t>> valid_blocks;
    uint64_t row_offset = 0;

    for (uint32_t z = 0; z < zone_map.num_zones; ++z) {
        const auto& zone = zone_map.zones[z];

        // Check if zone overlaps with [DATE_1994_01_01, DATE_1995_01_01)
        if (zone.max_value >= DATE_1994_01_01 && zone.min_value < DATE_1995_01_01) {
            valid_blocks.push_back({row_offset, row_offset + zone.row_count});
        }

        row_offset += zone.row_count;
    }

#ifdef GENDB_PROFILE
    printf("[TIMING] zone_map_pruning: identified %zu valid blocks\n", valid_blocks.size());
#endif

    // === [TIMING] Aggregation ===
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // Compute SUM(l_extendedprice * l_discount) with Kahan summation
    int64_t sum_product = 0;
    double sum_kahan = 0.0;
    double kahan_c = 0.0;  // Compensation for lost low-order bits

    // Parallel aggregation with thread-local buffers
    #pragma omp parallel reduction(+:sum_product) reduction(+:sum_kahan)
    {
        int64_t local_sum = 0;

        #pragma omp for schedule(dynamic, 10000)
        for (size_t block_idx = 0; block_idx < valid_blocks.size(); ++block_idx) {
            uint64_t block_start = valid_blocks[block_idx].first;
            uint64_t block_end = valid_blocks[block_idx].second;

            for (uint64_t i = block_start; i < block_end; ++i) {
                int32_t shipdate = shipdate_data[i];
                int64_t discount = discount_data[i];
                int64_t quantity = quantity_data[i];
                int64_t extendedprice = extendedprice_data[i];

                // Apply filters
                if (shipdate >= DATE_1994_01_01 && shipdate < DATE_1995_01_01 &&
                    discount >= DISCOUNT_MIN && discount <= DISCOUNT_MAX &&
                    quantity < QUANTITY_MAX) {

                    // Compute product: extendedprice * discount
                    // Both are scaled by 100, so product is scaled by 10000
                    int64_t product = extendedprice * discount;
                    local_sum += product;
                }
            }
        }

        // Accumulate local sum into global (reduction handles synchronization)
        #pragma omp critical
        {
            sum_product += local_sum;
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
#endif

    // === Scale down result ===
    // sum_product is scaled by 10000 (100 * 100)
    // We need to scale down by 10000 to get the final value
    double revenue = static_cast<double>(sum_product) / 10000.0;

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // === [TIMING] Write Results ===
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q6.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "revenue\n";

    // Write result with 4 decimal places (to match expected precision)
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%.4f", revenue);
    out << buffer << "\n";

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    std::cout << "Query Q6 completed. Result written to " << output_file << std::endl;
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
