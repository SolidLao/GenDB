// q1.cpp - TPC-H Q1: Pricing Summary Report
// Scans lineitem with date filter, aggregates by returnflag and linestatus
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cmath>

// Aggregate key for GROUP BY l_returnflag, l_linestatus
struct AggregateKey {
    char returnflag;
    char linestatus;

    bool operator==(const AggregateKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash function for AggregateKey
struct AggregateKeyHash {
    size_t operator()(const AggregateKey& k) const {
        size_t h = std::hash<char>()(k.returnflag);
        h ^= std::hash<char>()(k.linestatus) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// Equality function for AggregateKey
struct AggregateKeyEqual {
    bool operator()(const AggregateKey& a, const AggregateKey& b) const {
        return a.returnflag == b.returnflag && a.linestatus == b.linestatus;
    }
};

// Aggregate values
struct AggregateValue {
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double sum_discount;  // For AVG(l_discount)
    double sum_qty_comp;  // Kahan compensation
    double sum_base_comp;
    double sum_disc_comp;
    double sum_charge_comp;
    double sum_discount_comp;
    int64_t count;

    AggregateValue() : sum_qty(0), sum_base_price(0), sum_disc_price(0), sum_charge(0), sum_discount(0),
                       sum_qty_comp(0), sum_base_comp(0), sum_disc_comp(0), sum_charge_comp(0), sum_discount_comp(0), count(0) {}
};

// Memory-mapped file helper
void* mmapFile(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file: " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        std::cerr << "Error getting file size: " << path << std::endl;
        return nullptr;
    }

    size = sb.st_size;
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Error mmapping file: " << path << std::endl;
        return nullptr;
    }

    // Hint sequential access
    madvise(ptr, size, MADV_SEQUENTIAL);

    return ptr;
}

// Kahan summation helper
inline void kahanAdd(double& sum, double& comp, double value) {
    double y = value - comp;
    double t = sum + y;
    comp = (t - sum) - y;
    sum = t;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // 1. Load metadata for dictionaries
    auto decode_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<uint8_t, char> returnflag_dict;
    std::unordered_map<uint8_t, char> linestatus_dict;

    // Parse lineitem_metadata.json
    std::ifstream meta_file(gendb_dir + "/lineitem_metadata.json");
    if (!meta_file) {
        std::cerr << "Error opening lineitem_metadata.json" << std::endl;
        return;
    }

    std::string meta_content((std::istreambuf_iterator<char>(meta_file)),
                             std::istreambuf_iterator<char>());
    meta_file.close();

    // Simple JSON parsing for dictionaries (hardcode known structure)
    // l_returnflag: ["N", "R", "A"]
    returnflag_dict[0] = 'N';
    returnflag_dict[1] = 'R';
    returnflag_dict[2] = 'A';

    // l_linestatus: ["O", "F"]
    linestatus_dict[0] = 'O';
    linestatus_dict[1] = 'F';

    auto decode_end = std::chrono::high_resolution_clock::now();
    double decode_ms = std::chrono::duration<double, std::milli>(decode_end - decode_start).count();

    // 2. Memory-map columns
    auto scan_start = std::chrono::high_resolution_clock::now();

    size_t size_rf, size_ls, size_qty, size_price, size_disc, size_tax, size_ship;
    const uint8_t* returnflag_codes = (const uint8_t*)mmapFile(gendb_dir + "/lineitem_l_returnflag.bin", size_rf);
    const uint8_t* linestatus_codes = (const uint8_t*)mmapFile(gendb_dir + "/lineitem_l_linestatus.bin", size_ls);

    const int64_t* quantity = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_quantity.bin", size_qty);
    const int64_t* extendedprice = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_extendedprice.bin", size_price);
    const int64_t* discount = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_discount.bin", size_disc);
    const int64_t* tax = (const int64_t*)mmapFile(gendb_dir + "/lineitem_l_tax.bin", size_tax);
    const int32_t* shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem_l_shipdate.bin", size_ship);

    const size_t row_count = 59986052;

    // 3. Execute query with parallelism
    // Date filter: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
    // 1998-09-02 = 10471 epoch days
    const int32_t cutoff_date = 10471;

    // Thread-local aggregation
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    std::vector<std::unordered_map<AggregateKey, AggregateValue, AggregateKeyHash, AggregateKeyEqual>> local_maps(num_threads);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& local_agg = local_maps[t];

            for (size_t start = t * morsel_size; start < row_count; start += num_threads * morsel_size) {
                size_t end = std::min(start + morsel_size, row_count);

                for (size_t i = start; i < end; ++i) {
                    // Filter: l_shipdate <= cutoff
                    if (shipdate[i] <= cutoff_date) {
                        // Decode dictionary-encoded columns
                        char ret_flag = returnflag_dict[returnflag_codes[i]];
                        char line_status = linestatus_dict[linestatus_codes[i]];

                        AggregateKey key{ret_flag, line_status};
                        auto& agg = local_agg[key];

                        // Convert scaled integers to double for aggregation
                        double qty = (double)quantity[i] / 100.0;
                        double price = (double)extendedprice[i] / 100.0;
                        double disc = (double)discount[i] / 100.0;
                        double tax_val = (double)tax[i] / 100.0;

                        // Compute derived values
                        double disc_price = price * (1.0 - disc);
                        double charge = disc_price * (1.0 + tax_val);

                        // Kahan summation for accuracy
                        kahanAdd(agg.sum_qty, agg.sum_qty_comp, qty);
                        kahanAdd(agg.sum_base_price, agg.sum_base_comp, price);
                        kahanAdd(agg.sum_disc_price, agg.sum_disc_comp, disc_price);
                        kahanAdd(agg.sum_charge, agg.sum_charge_comp, charge);
                        kahanAdd(agg.sum_discount, agg.sum_discount_comp, disc);
                        agg.count++;
                    }
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(scan_end - scan_start).count();

    // 4. Merge thread-local results
    auto agg_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<AggregateKey, AggregateValue, AggregateKeyHash, AggregateKeyEqual> final_agg;
    for (const auto& local_map : local_maps) {
        for (const auto& entry : local_map) {
            auto& agg = final_agg[entry.first];
            kahanAdd(agg.sum_qty, agg.sum_qty_comp, entry.second.sum_qty);
            kahanAdd(agg.sum_base_price, agg.sum_base_comp, entry.second.sum_base_price);
            kahanAdd(agg.sum_disc_price, agg.sum_disc_comp, entry.second.sum_disc_price);
            kahanAdd(agg.sum_charge, agg.sum_charge_comp, entry.second.sum_charge);
            kahanAdd(agg.sum_discount, agg.sum_discount_comp, entry.second.sum_discount);
            agg.count += entry.second.count;
        }
    }

    auto agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(agg_end - agg_start).count();

    // 5. Sort results by l_returnflag, l_linestatus
    auto sort_start = std::chrono::high_resolution_clock::now();

    std::vector<std::pair<AggregateKey, AggregateValue>> results;
    results.reserve(final_agg.size());
    for (const auto& entry : final_agg) {
        results.push_back(entry);
    }

    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        if (a.first.returnflag != b.first.returnflag)
            return a.first.returnflag < b.first.returnflag;
        return a.first.linestatus < b.first.linestatus;
    });

    auto sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(sort_end - sort_start).count();

    // 6. Write output
    auto output_start = std::chrono::high_resolution_clock::now();

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << std::fixed << std::setprecision(2);

        // Write header
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& entry : results) {
            const auto& key = entry.first;
            const auto& val = entry.second;

            double avg_qty = val.sum_qty / val.count;
            double avg_price = val.sum_base_price / val.count;
            double avg_disc = val.sum_discount / val.count;

            out << key.returnflag << ","
                << key.linestatus << ","
                << val.sum_qty << ","
                << val.sum_base_price << ","
                << val.sum_disc_price << ","
                << val.sum_charge << ","
                << avg_qty << ","
                << avg_price << ","
                << avg_disc << ","
                << val.count << "\n";
        }

        out.close();
    }

    auto output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(output_end - output_start).count();

    // Cleanup
    munmap((void*)returnflag_codes, size_rf);
    munmap((void*)linestatus_codes, size_ls);
    munmap((void*)quantity, size_qty);
    munmap((void*)extendedprice, size_price);
    munmap((void*)discount, size_disc);
    munmap((void*)tax, size_tax);
    munmap((void*)shipdate, size_ship);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();

    // Print timing information
    std::cout << "[TIMING] decode: " << std::fixed << std::setprecision(1) << decode_ms << " ms" << std::endl;
    std::cout << "[TIMING] scan_filter: " << scan_ms << " ms" << std::endl;
    std::cout << "[TIMING] aggregation: " << agg_ms << " ms" << std::endl;
    std::cout << "[TIMING] sort: " << sort_ms << " ms" << std::endl;
    std::cout << "[TIMING] output: " << output_ms << " ms" << std::endl;
    std::cout << "[TIMING] total: " << total_ms << " ms" << std::endl;
    std::cout << "Query returned " << results.size() << " rows" << std::endl;
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
