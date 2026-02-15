#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <vector>
#include <omp.h>

// Constants for date conversion
const int SCALE_FACTOR = 100;
const int32_t DATE_1994_01_01 = 8766;  // 1994-01-01 in days since epoch
const int32_t DATE_1995_01_01 = 9131;  // 1995-01-01 in days since epoch
// BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
// = BETWEEN 0.05 AND 0.07
// Due to floating point, 0.05 may be stored as slightly less, so use > 4 instead of >= 5
// And 0.07 may be stored as slightly more, so use <= 7 (inclusive)
const int64_t DISCOUNT_MIN_EXCLUSIVE = 4;  // discount > 4 (equivalent to >= 5 for integers)
const int64_t DISCOUNT_MAX = 7;           // discount <= 7
const int64_t QUANTITY_THRESHOLD = 2400; // 24 * scale_factor

// Helper function to safely mmap a file
void* mmap_file(const std::string& filepath, size_t& file_size) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return nullptr;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    if (size < 0) {
        std::cerr << "Failed to get file size: " << filepath << std::endl;
        close(fd);
        return nullptr;
    }
    file_size = static_cast<size_t>(size);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);  // File descriptor can be closed after mmap

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << filepath << std::endl;
        return nullptr;
    }
    return ptr;
}

// Helper function to munmap
void munmap_file(void* ptr, size_t size) {
    if (ptr != nullptr) {
        munmap(ptr, size);
    }
}

// Structure to hold zone map metadata
struct ZoneMapBlock {
    int32_t min_val;
    uint32_t max_val;
    uint32_t count;
    uint32_t null_count;
};

// Load zone map and determine which blocks to scan
std::vector<uint32_t> load_zone_map(const std::string& zone_map_path,
                                     int32_t date_min, int32_t date_max) {
    std::vector<uint32_t> blocks_to_scan;

    size_t file_size = 0;
    void* ptr = mmap_file(zone_map_path, file_size);
    if (ptr == nullptr) {
        std::cerr << "Failed to load zone map" << std::endl;
        return blocks_to_scan;
    }

    const uint8_t* data = static_cast<const uint8_t*>(ptr);
    uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(data);

    const ZoneMapBlock* blocks = reinterpret_cast<const ZoneMapBlock*>(data + 4);

    for (uint32_t i = 0; i < num_blocks; ++i) {
        const ZoneMapBlock& block = blocks[i];
        // Check if block overlaps with query range [date_min, date_max)
        if (block.max_val >= date_min && static_cast<int32_t>(block.min_val) < date_max) {
            blocks_to_scan.push_back(i);
        }
    }

    munmap_file(ptr, file_size);
    return blocks_to_scan;
}

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // File paths
    std::string lineitem_dir = gendb_dir + "/tables/lineitem/";
    std::string shipdate_path = lineitem_dir + "l_shipdate.bin";
    std::string discount_path = lineitem_dir + "l_discount.bin";
    std::string quantity_path = lineitem_dir + "l_quantity.bin";
    std::string extendedprice_path = lineitem_dir + "l_extendedprice.bin";
    std::string zone_map_path = gendb_dir + "/indexes/zone_map_l_shipdate.bin";

    // Load zone map to determine blocks to scan
#ifdef GENDB_PROFILE
    auto t_zone_start = std::chrono::high_resolution_clock::now();
#endif
    std::vector<uint32_t> blocks_to_scan = load_zone_map(zone_map_path,
                                                           DATE_1994_01_01,
                                                           DATE_1995_01_01);
#ifdef GENDB_PROFILE
    auto t_zone_end = std::chrono::high_resolution_clock::now();
    double zone_ms = std::chrono::duration<double, std::milli>(t_zone_end - t_zone_start).count();
    printf("[TIMING] zone_map_pruning: %.2f ms\n", zone_ms);
#endif

    // Load column data
    size_t shipdate_size = 0, discount_size = 0, quantity_size = 0, extendedprice_size = 0;

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    const int32_t* shipdate_data = static_cast<const int32_t*>(mmap_file(shipdate_path, shipdate_size));
    const int64_t* discount_data = static_cast<const int64_t*>(mmap_file(discount_path, discount_size));
    const int64_t* quantity_data = static_cast<const int64_t*>(mmap_file(quantity_path, quantity_size));
    const int64_t* extendedprice_data = static_cast<const int64_t*>(mmap_file(extendedprice_path, extendedprice_size));
#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] data_load: %.2f ms\n", load_ms);
#endif

    if (shipdate_data == nullptr || discount_data == nullptr ||
        quantity_data == nullptr || extendedprice_data == nullptr) {
        std::cerr << "Failed to load column data" << std::endl;
        return;
    }

    uint64_t num_rows = shipdate_size / sizeof(int32_t);
    const uint32_t BLOCK_SIZE = 256000;

    // Scan and aggregate with parallelization
    double global_sum = 0.0;
    int64_t filtered_rows = 0;

#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    #pragma omp parallel for schedule(dynamic, 1) reduction(+: global_sum, filtered_rows)
    for (size_t block_idx = 0; block_idx < blocks_to_scan.size(); ++block_idx) {
        uint32_t block_id = blocks_to_scan[block_idx];
        uint32_t start_row = block_id * BLOCK_SIZE;
        uint32_t end_row = std::min((block_id + 1) * BLOCK_SIZE, static_cast<uint32_t>(num_rows));

        double local_sum = 0.0;
        int64_t local_count = 0;

        for (uint32_t i = start_row; i < end_row; ++i) {
            int32_t shipdate = shipdate_data[i];
            int64_t discount = discount_data[i];
            int64_t quantity = quantity_data[i];
            int64_t extendedprice = extendedprice_data[i];

            // Apply predicates
            if (shipdate >= DATE_1994_01_01 && shipdate < DATE_1995_01_01 &&
                discount > DISCOUNT_MIN_EXCLUSIVE && discount <= DISCOUNT_MAX &&
                quantity < QUANTITY_THRESHOLD) {

                // Compute l_extendedprice * l_discount
                // Both are int64_t scaled by SCALE_FACTOR
                // Convert to double first, then divide by SCALE_FACTOR for each
                double price_dbl = static_cast<double>(extendedprice) / SCALE_FACTOR;
                double disc_dbl = static_cast<double>(discount) / SCALE_FACTOR;
                double product = price_dbl * disc_dbl;

                local_sum += product;
                local_count++;
            }
        }

        // Atomic-like reduction through OpenMP
        #pragma omp critical
        {
            global_sum += local_sum;
            filtered_rows += local_count;
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);
#endif

    // Cleanup
    munmap_file((void*)shipdate_data, shipdate_size);
    munmap_file((void*)discount_data, discount_size);
    munmap_file((void*)quantity_data, quantity_size);
    munmap_file((void*)extendedprice_data, extendedprice_size);

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Write results
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q6.csv";
    std::ofstream out(output_path);
    out << "revenue\r\n";
    out.precision(4);
    out << std::fixed << global_sum << "\r\n";
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    printf("[INFO] Filtered rows: %ld\n", filtered_rows);
    printf("[INFO] Revenue sum: %.4f\n", global_sum);
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

    run_q6(gendb_dir, results_dir);

    return 0;
}
#endif
