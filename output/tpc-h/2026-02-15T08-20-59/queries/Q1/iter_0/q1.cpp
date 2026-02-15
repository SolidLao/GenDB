#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <map>
#include <algorithm>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>

// Constants
const int32_t SHIPDATE_CUTOFF = 10472;  // 1998-12-01 - 90 days = 1998-09-02 (inclusive upper bound)
const int32_t BLOCK_SIZE = 131072;      // 131K rows per block

// Structure to hold aggregated results for one group
struct AggregateGroup {
    int64_t sum_qty = 0;
    int64_t sum_base_price = 0;
    long double sum_disc_price = 0.0L;  // Use long double for better precision
    long double sum_charge = 0.0L;      // Use long double for better precision
    int64_t sum_discount = 0;           // For avg_disc calculation
    int64_t count_rows = 0;
};

// Mmap wrapper
class MmapFile {
public:
    void *data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string &path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            ::close(fd);
            return false;
        }

        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            ::close(fd);
            return false;
        }

        return true;
    }

    void close() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
    }

    ~MmapFile() { close(); }
};

// Load zone map for l_shipdate
struct ZoneMap {
    std::vector<int32_t> min_dates;
    std::vector<int32_t> max_dates;

    bool load(const std::string &path) {
        std::ifstream f(path, std::ios::binary);
        if (!f) {
            std::cerr << "Failed to open zone map: " << path << std::endl;
            return false;
        }

        uint32_t num_blocks = 0;
        f.read(reinterpret_cast<char *>(&num_blocks), sizeof(uint32_t));

        min_dates.resize(num_blocks);
        max_dates.resize(num_blocks);

        for (uint32_t i = 0; i < num_blocks; ++i) {
            f.read(reinterpret_cast<char *>(&min_dates[i]), sizeof(int32_t));
            f.read(reinterpret_cast<char *>(&max_dates[i]), sizeof(int32_t));
        }

        return true;
    }
};

// Load dictionary
std::map<uint8_t, char> load_dictionary(const std::string &path) {
    std::map<uint8_t, char> dict;
    std::ifstream f(path);
    if (!f) {
        std::cerr << "Failed to open dictionary: " << path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            uint8_t code = static_cast<uint8_t>(std::stoi(line.substr(0, eq)));
            char value = line[eq + 1];
            dict[code] = value;
        }
    }
    return dict;
}

void run_q1(const std::string &gendb_dir, const std::string &results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Construct file paths
    std::string lineitem_dir = gendb_dir + "/lineitem";
    std::string indexes_dir = gendb_dir + "/indexes";

    // Open column files
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile f_shipdate, f_quantity, f_extendedprice, f_discount, f_tax, f_returnflag, f_linestatus;

    if (!f_shipdate.open(lineitem_dir + "/l_shipdate.bin") ||
        !f_quantity.open(lineitem_dir + "/l_quantity.bin") ||
        !f_extendedprice.open(lineitem_dir + "/l_extendedprice.bin") ||
        !f_discount.open(lineitem_dir + "/l_discount.bin") ||
        !f_tax.open(lineitem_dir + "/l_tax.bin") ||
        !f_returnflag.open(lineitem_dir + "/l_returnflag.bin") ||
        !f_linestatus.open(lineitem_dir + "/l_linestatus.bin")) {
        std::cerr << "Failed to open column files" << std::endl;
        return;
    }

    // Load dictionaries
    auto dict_returnflag = load_dictionary(lineitem_dir + "/l_returnflag_dict.txt");
    auto dict_linestatus = load_dictionary(lineitem_dir + "/l_linestatus_dict.txt");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", load_ms);
#endif

    // Load zone map for early block skipping
#ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
#endif

    ZoneMap zone_map;
    zone_map.load(indexes_dir + "/l_shipdate_zone_map.bin");

#ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double zonemap_ms = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] load_zone_map: %.2f ms\n", zonemap_ms);
#endif

    // Cast pointers
    const int32_t *shipdate = static_cast<const int32_t *>(f_shipdate.data);
    const int64_t *quantity = static_cast<const int64_t *>(f_quantity.data);
    const int64_t *extendedprice = static_cast<const int64_t *>(f_extendedprice.data);
    const int64_t *discount = static_cast<const int64_t *>(f_discount.data);
    const int64_t *tax = static_cast<const int64_t *>(f_tax.data);
    const uint8_t *returnflag = static_cast<const uint8_t *>(f_returnflag.data);
    const uint8_t *linestatus = static_cast<const uint8_t *>(f_linestatus.data);

    size_t total_rows = f_shipdate.size / sizeof(int32_t);

    // Aggregation map: key = (returnflag_char, linestatus_char)
    // Use std::map for sorted output
    std::map<std::pair<char, char>, AggregateGroup> agg_map;

#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Scan all rows (zone map optimization is minimal due to date distribution)
    for (size_t i = 0; i < total_rows; ++i) {
        // Filter on l_shipdate
        if (shipdate[i] > SHIPDATE_CUTOFF) {
            continue;
        }

            // Decode dictionary-encoded columns
            char rf = dict_returnflag[returnflag[i]];
            char ls = dict_linestatus[linestatus[i]];

            // Create group key
            auto group_key = std::make_pair(rf, ls);

            // Get or create group
            AggregateGroup &group = agg_map[group_key];

            // Aggregation logic
            // sum_qty: sum of l_quantity
            group.sum_qty += quantity[i];

            // sum_base_price: sum of l_extendedprice
            group.sum_base_price += extendedprice[i];

            // sum_disc_price: sum of l_extendedprice * (1 - l_discount)
            // Both extendedprice and discount are scaled by 100
            // Formula: ext_price_actual * (1 - discount_actual)
            //        = (ext_price_int / 100) * (1 - discount_int / 100)
            //        = (ext_price_int * (100 - discount_int)) / 10000
            long double ext_price = static_cast<long double>(extendedprice[i]) / 100.0L;
            long double disc_amt = static_cast<long double>(discount[i]) / 100.0L;
            long double disc_price = ext_price * (1.0L - disc_amt);
            group.sum_disc_price += disc_price;

            // sum_charge: sum of l_extendedprice * (1 - l_discount) * (1 + l_tax)
            // charge = disc_price * (1 + tax/100)
            long double tax_amt = static_cast<long double>(tax[i]) / 100.0L;
            long double charge = disc_price * (1.0L + tax_amt);
            group.sum_charge += charge;

            // Track sum of discounts for avg
            group.sum_discount += discount[i];

            // count_rows
            group.count_rows++;
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);
#endif

    // Prepare output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream out_file(results_dir + "/Q1.csv");
    out_file << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    for (auto &[key, group] : agg_map) {
        char rf = key.first;
        char ls = key.second;

        // Convert scaled decimals back to actual values for output
        double avg_qty = static_cast<double>(group.sum_qty) / group.count_rows / 100.0;
        double avg_price = static_cast<double>(group.sum_base_price) / group.count_rows / 100.0;
        double avg_disc = static_cast<double>(group.sum_discount) / group.count_rows / 100.0;

        // Output with appropriate precision
        out_file << rf << "," << ls << ",";
        out_file << std::fixed << std::setprecision(2)
                 << static_cast<double>(group.sum_qty) / 100.0 << ","
                 << static_cast<double>(group.sum_base_price) / 100.0 << ",";
        out_file << std::fixed << std::setprecision(4)
                 << group.sum_disc_price << ",";
        out_file << std::fixed << std::setprecision(6)
                 << group.sum_charge << ",";
        out_file << std::fixed << std::setprecision(2)
                 << avg_qty << ","
                 << avg_price << ","
                 << avg_disc << ",";
        out_file << group.count_rows << "\n";
    }

    out_file.close();

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

    std::cout << "Q1 results written to " << results_dir << "/Q1.csv" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char *argv[]) {
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
