#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
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

// Aggregate result for one group
struct AggregateResult {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;
    int64_t count = 0;
};

// Compact array-based group lookup: only 6 possible groups (3 returnflags × 2 linestatuses)
// Indexed by: returnflag_code * 10 + linestatus_code (assuming codes are small 0-3 range)
// More robust: use a simple struct with key lookup
struct CompactGroupKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator<(const CompactGroupKey& other) const {
        if (returnflag != other.returnflag) return returnflag < other.returnflag;
        return linestatus < other.linestatus;
    }

    bool operator==(const CompactGroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

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

    // Filter date: DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02 (epoch days: 10472)
    const int32_t cutoff_date = 10472;

    // Compact array-based aggregation map (Q1 has only 6 possible groups)
    // We'll use a vector to hold (key, result) pairs and linear search for insertions
    // This is faster than unordered_map for such small cardinality
    struct GroupEntry {
        CompactGroupKey key;
        AggregateResult result;
    };

    std::vector<GroupEntry> agg_entries;
    std::mutex agg_mutex;

    // Parallel scan with morsel-driven approach
    size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // Local compact aggregation: use vector for small cardinality
            std::vector<GroupEntry> local_entries;

            for (size_t i = t * morsel_size; i < row_count; i += num_threads * morsel_size) {
                size_t end = std::min(i + morsel_size, row_count);

                for (size_t idx = i; idx < end; ++idx) {
                    // CRITICAL: Apply filter on absolute values, not codes
                    if (shipdate[idx] <= cutoff_date) {
                        CompactGroupKey key;
                        key.returnflag = returnflag_codes[idx];
                        key.linestatus = linestatus_codes[idx];

                        double qty = quantity[idx];
                        double price = extendedprice[idx];
                        double disc = discount[idx];
                        double tx = tax[idx];

                        // Calculate derived values
                        double disc_price = price * (1.0 - disc);
                        double charge = disc_price * (1.0 + tx);

                        // Linear search in local_entries (fast for 6 groups)
                        AggregateResult* agg = nullptr;
                        for (auto& entry : local_entries) {
                            if (entry.key == key) {
                                agg = &entry.result;
                                break;
                            }
                        }
                        if (!agg) {
                            local_entries.push_back({key, AggregateResult()});
                            agg = &local_entries.back().result;
                        }

                        agg->sum_qty += qty;
                        agg->sum_base_price += price;
                        agg->sum_disc_price += disc_price;
                        agg->sum_charge += charge;
                        agg->sum_discount += disc;
                        agg->count += 1;
                    }
                }
            }

            // Merge local results with thread-safe operations
            {
                std::lock_guard<std::mutex> lock(agg_mutex);
                for (auto& local_entry : local_entries) {
                    AggregateResult* global_agg = nullptr;
                    for (auto& global_entry : agg_entries) {
                        if (global_entry.key == local_entry.key) {
                            global_agg = &global_entry.result;
                            break;
                        }
                    }
                    if (!global_agg) {
                        agg_entries.push_back({local_entry.key, AggregateResult()});
                        global_agg = &agg_entries.back().result;
                    }
                    global_agg->sum_qty += local_entry.result.sum_qty;
                    global_agg->sum_base_price += local_entry.result.sum_base_price;
                    global_agg->sum_disc_price += local_entry.result.sum_disc_price;
                    global_agg->sum_charge += local_entry.result.sum_charge;
                    global_agg->sum_discount += local_entry.result.sum_discount;
                    global_agg->count += local_entry.result.count;
                }
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
        for (const auto& entry : agg_entries) {
            std::string rf = returnflag_dict[entry.key.returnflag];
            std::string ls = linestatus_dict[entry.key.linestatus];
            sorted_results.push_back(std::make_tuple(rf, ls, entry.result));
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
    for (const auto& entry : agg_entries) {
        total_rows += entry.result.count;
    }

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Query returned " << agg_entries.size() << " rows\n";
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
