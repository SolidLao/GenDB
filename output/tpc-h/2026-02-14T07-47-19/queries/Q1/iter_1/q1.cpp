#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <string>
#include <cstring>
#include <cmath>
#include <thread>
#include <mutex>
#include <iomanip>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>

// Aggregate group key and result
struct GroupKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator<(const GroupKey& other) const {
        if (returnflag != other.returnflag) return returnflag < other.returnflag;
        return linestatus < other.linestatus;
    }

    bool operator==(const GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

struct AggregateResult {
    // Main sums using Kahan summation
    double sum_qty = 0.0;
    double sum_qty_compensation = 0.0;

    double sum_base_price = 0.0;
    double sum_base_price_compensation = 0.0;

    double sum_disc_price = 0.0;
    double sum_disc_price_compensation = 0.0;

    double sum_charge = 0.0;
    double sum_charge_compensation = 0.0;

    double sum_discount = 0.0;
    double sum_discount_compensation = 0.0;

    int64_t count = 0;
};

// Kahan summation: compensated sum for numerical stability
inline void kahan_add(double& sum, double& compensation, double value) {
    double y = value - compensation;
    double t = sum + y;
    compensation = (t - sum) - y;
    sum = t;
}

// Hash function for GroupKey
namespace std {
template <>
struct hash<GroupKey> {
    size_t operator()(const GroupKey& key) const {
        return (size_t)key.returnflag * 31 + key.linestatus;
    }
};
}

// RAII mmap wrapper
class MmapFile {
public:
    void* ptr = nullptr;
    size_t size = 0;

    MmapFile(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat: " << path << std::endl;
            close(fd);
            return;
        }

        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            ptr = nullptr;
        }

        close(fd);
    }

    ~MmapFile() {
        if (ptr && size > 0) {
            munmap(ptr, size);
        }
    }

    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;
};

// Parse dictionary from file
std::unordered_map<uint8_t, std::string> loadDictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, std::string> result;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return result;
    }

    std::string line;
    while (std::getline(file, line)) {
        size_t pos = line.find('=');
        if (pos != std::string::npos) {
            uint8_t code = (uint8_t)std::stoi(line.substr(0, pos));
            std::string value = line.substr(pos + 1);
            result[code] = value;
        }
    }
    return result;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Load dictionaries
    std::string lineitem_dir = gendb_dir + "/lineitem/";
    auto returnflag_dict = loadDictionary(lineitem_dir + "l_returnflag_dict.txt");
    auto linestatus_dict = loadDictionary(lineitem_dir + "l_linestatus_dict.txt");

    // mmap columns
    MmapFile shipdate_mmap(lineitem_dir + "l_shipdate.bin");
    MmapFile returnflag_mmap(lineitem_dir + "l_returnflag.bin");
    MmapFile linestatus_mmap(lineitem_dir + "l_linestatus.bin");
    MmapFile quantity_mmap(lineitem_dir + "l_quantity.bin");
    MmapFile extendedprice_mmap(lineitem_dir + "l_extendedprice.bin");
    MmapFile discount_mmap(lineitem_dir + "l_discount.bin");
    MmapFile tax_mmap(lineitem_dir + "l_tax.bin");

    if (!shipdate_mmap.ptr || !returnflag_mmap.ptr || !linestatus_mmap.ptr ||
        !quantity_mmap.ptr || !extendedprice_mmap.ptr || !discount_mmap.ptr || !tax_mmap.ptr) {
        std::cerr << "Failed to mmap columns\n";
        return;
    }

    // Cast to proper types
    const int32_t* shipdate = (const int32_t*)shipdate_mmap.ptr;
    const uint8_t* returnflag_codes = (const uint8_t*)returnflag_mmap.ptr;
    const uint8_t* linestatus_codes = (const uint8_t*)linestatus_mmap.ptr;
    const double* quantity = (const double*)quantity_mmap.ptr;
    const double* extendedprice = (const double*)extendedprice_mmap.ptr;
    const double* discount = (const double*)discount_mmap.ptr;
    const double* tax = (const double*)tax_mmap.ptr;

    // Calculate row count
    size_t row_count = shipdate_mmap.size / sizeof(int32_t);

    // Filter date: DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 (epoch days: 10471)
    const int32_t cutoff_date = 10471;

    // Use map for ordered output (already sorted by GROUP BY keys)
    std::map<GroupKey, AggregateResult> agg_map;
    std::mutex agg_mutex;

    // Parallel scan with morsel-driven approach
    size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::unordered_map<GroupKey, AggregateResult> local_agg;

            for (size_t i = t * morsel_size; i < row_count; i += num_threads * morsel_size) {
                size_t end = std::min(i + morsel_size, row_count);

                for (size_t idx = i; idx < end; ++idx) {
                    // CRITICAL: Apply filter on absolute values, not codes
                    if (shipdate[idx] <= cutoff_date) {
                        GroupKey key;
                        key.returnflag = returnflag_codes[idx];
                        key.linestatus = linestatus_codes[idx];

                        double qty = quantity[idx];
                        double price = extendedprice[idx];
                        double disc = discount[idx];
                        double tx = tax[idx];

                        // Calculate derived values
                        double disc_price = price * (1.0 - disc);
                        double charge = disc_price * (1.0 + tx);

                        AggregateResult& agg = local_agg[key];
                        // Use Kahan summation for all floating-point sums
                        kahan_add(agg.sum_qty, agg.sum_qty_compensation, qty);
                        kahan_add(agg.sum_base_price, agg.sum_base_price_compensation, price);
                        kahan_add(agg.sum_disc_price, agg.sum_disc_price_compensation, disc_price);
                        kahan_add(agg.sum_charge, agg.sum_charge_compensation, charge);
                        kahan_add(agg.sum_discount, agg.sum_discount_compensation, disc);
                        agg.count += 1;
                    }
                }
            }

            // Merge local results with Kahan summation
            std::lock_guard<std::mutex> lock(agg_mutex);
            for (auto& [key, local_result] : local_agg) {
                AggregateResult& global_result = agg_map[key];
                kahan_add(global_result.sum_qty, global_result.sum_qty_compensation, local_result.sum_qty);
                kahan_add(global_result.sum_base_price, global_result.sum_base_price_compensation, local_result.sum_base_price);
                kahan_add(global_result.sum_disc_price, global_result.sum_disc_price_compensation, local_result.sum_disc_price);
                kahan_add(global_result.sum_charge, global_result.sum_charge_compensation, local_result.sum_charge);
                kahan_add(global_result.sum_discount, global_result.sum_discount_compensation, local_result.sum_discount);
                global_result.count += local_result.count;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();

    // Write results if requested
    if (!results_dir.empty()) {
        std::string results_path = results_dir + "/q1.csv";
        std::ofstream outfile(results_path);

        // Write header
        outfile << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,"
                << "avg_qty,avg_price,avg_disc,count_order\n";

        outfile << std::fixed << std::setprecision(2);

        // Create vector for sorting by decoded values (not encoded codes)
        std::vector<std::tuple<std::string, std::string, AggregateResult>> sorted_results;
        for (const auto& [key, result] : agg_map) {
            std::string rf = returnflag_dict[key.returnflag];
            std::string ls = linestatus_dict[key.linestatus];
            sorted_results.push_back(std::make_tuple(rf, ls, result));
        }

        // Sort by decoded values
        std::sort(sorted_results.begin(), sorted_results.end(),
                  [](const auto& a, const auto& b) {
                      if (std::get<0>(a) != std::get<0>(b))
                          return std::get<0>(a) < std::get<0>(b);
                      return std::get<1>(a) < std::get<1>(b);
                  });

        // Write results in order
        for (const auto& [rf, ls, result] : sorted_results) {

            double avg_qty = (result.count > 0) ? result.sum_qty / result.count : 0.0;
            double avg_price = (result.count > 0) ? result.sum_base_price / result.count : 0.0;
            double avg_disc = (result.count > 0) ? result.sum_discount / result.count : 0.0;

            outfile << rf << "," << ls << ","
                    << result.sum_qty << ","
                    << result.sum_base_price << ","
                    << result.sum_disc_price << ","
                    << result.sum_charge << ","
                    << avg_qty << ","
                    << avg_price << ","
                    << avg_disc << ","
                    << result.count << "\n";
        }

        outfile.close();
    }

    // Print statistics
    int64_t total_rows = 0;
    for (const auto& [key, result] : agg_map) {
        total_rows += result.count;
    }

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Query returned " << agg_map.size() << " rows\n";
    std::cout << "Execution time: " << duration_ms << " ms\n";
    std::cout << "Scanned rows: " << total_rows << "\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q1(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
