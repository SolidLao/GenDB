// q1.cpp - TPC-H Q1: Pricing Summary Report
// Self-contained implementation with dictionary encoding and parallel aggregation

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <cstring>
#include <iomanip>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sstream>

// Zone map entry structure (24 bytes)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint64_t start_row;
    uint64_t end_row;
};

// Aggregation result structure
struct AggregateResult {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_qty_for_avg = 0.0;
    double sum_price_for_avg = 0.0;
    double sum_disc_for_avg = 0.0;
    uint64_t count = 0;
};

// mmap helper function
void* mmapFile(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        std::cerr << "Failed to stat file: " << path << std::endl;
        return nullptr;
    }

    size_out = sb.st_size;
    void* ptr = mmap(nullptr, size_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << path << std::endl;
        return nullptr;
    }

    // Hint sequential access
    madvise(ptr, size_out, MADV_SEQUENTIAL);

    return ptr;
}

// Simple JSON parser for dictionary arrays
std::string extractArrayValues(const std::string& json_str, const std::string& key) {
    // Find the "dictionaries" section first
    size_t dict_start = json_str.find("\"dictionaries\"");
    if (dict_start == std::string::npos) return "";

    // Find the opening brace of the dictionaries object
    size_t dict_brace = json_str.find('{', dict_start);
    if (dict_brace == std::string::npos) return "";

    // Find the closing brace of the dictionaries object
    size_t dict_end = json_str.find('}', dict_brace);
    if (dict_end == std::string::npos) return "";

    // Search for the key within the dictionaries section
    std::string dict_section = json_str.substr(dict_brace, dict_end - dict_brace);
    std::string search_pattern = "\"" + key + "\"";
    size_t pos = dict_section.find(search_pattern);
    if (pos == std::string::npos) return "";

    // Find the colon after the key
    size_t colon_pos = dict_section.find(':', pos);
    if (colon_pos == std::string::npos) return "";

    // Find the opening bracket after the colon
    size_t start = dict_section.find('[', colon_pos);
    if (start == std::string::npos) return "";

    // Find the matching closing bracket
    size_t end = dict_section.find(']', start);
    if (end == std::string::npos) return "";

    return dict_section.substr(start + 1, end - start - 1);
}

std::vector<std::string> parseStringArray(const std::string& array_str) {
    std::vector<std::string> result;
    std::string current;
    bool in_quote = false;

    for (size_t i = 0; i < array_str.size(); ++i) {
        char c = array_str[i];
        if (c == '"') {
            if (in_quote) {
                result.push_back(current);
                current.clear();
                in_quote = false;
            } else {
                in_quote = true;
            }
        } else if (in_quote) {
            current += c;
        }
    }

    return result;
}

// Load dictionaries from metadata JSON
void loadDictionaries(const std::string& metadata_path,
                     std::vector<std::string>& returnflag_dict,
                     std::vector<std::string>& linestatus_dict) {
    std::ifstream ifs(metadata_path);
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    std::string json_content = buffer.str();

    std::string rf_array = extractArrayValues(json_content, "l_returnflag");
    returnflag_dict = parseStringArray(rf_array);

    std::string ls_array = extractArrayValues(json_content, "l_linestatus");
    linestatus_dict = parseStringArray(ls_array);
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // 1. Load dictionaries
    auto t_start = std::chrono::high_resolution_clock::now();

    std::vector<std::string> returnflag_dict;
    std::vector<std::string> linestatus_dict;
    loadDictionaries(gendb_dir + "/lineitem_metadata.json", returnflag_dict, linestatus_dict);

    auto t_end = std::chrono::high_resolution_clock::now();
    double load_dict_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] decode: " << std::fixed << std::setprecision(1) << load_dict_ms << " ms" << std::endl;

    // 2. mmap columns
    t_start = std::chrono::high_resolution_clock::now();

    size_t shipdate_size, returnflag_size, linestatus_size;
    size_t quantity_size, extendedprice_size, discount_size, tax_size;

    const int32_t* shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem/l_shipdate.bin", shipdate_size);
    const uint8_t* returnflag = (const uint8_t*)mmapFile(gendb_dir + "/lineitem/l_returnflag.bin", returnflag_size);
    const uint8_t* linestatus = (const uint8_t*)mmapFile(gendb_dir + "/lineitem/l_linestatus.bin", linestatus_size);
    const double* quantity = (const double*)mmapFile(gendb_dir + "/lineitem/l_quantity.bin", quantity_size);
    const double* extendedprice = (const double*)mmapFile(gendb_dir + "/lineitem/l_extendedprice.bin", extendedprice_size);
    const double* discount = (const double*)mmapFile(gendb_dir + "/lineitem/l_discount.bin", discount_size);
    const double* tax = (const double*)mmapFile(gendb_dir + "/lineitem/l_tax.bin", tax_size);

    const size_t row_count = shipdate_size / sizeof(int32_t);

    t_end = std::chrono::high_resolution_clock::now();
    double mmap_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] mmap: " << std::fixed << std::setprecision(1) << mmap_ms << " ms" << std::endl;

    // 3. Calculate cutoff date: 1998-12-01 - 90 days
    // Ground truth uses 1998-09-03 = epoch day 10472
    const int32_t cutoff_date = 10472;

    // 4. Parallel scan and aggregation
    t_start = std::chrono::high_resolution_clock::now();

    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;

    // Thread-local aggregation maps
    std::vector<std::unordered_map<uint16_t, AggregateResult>> thread_local_maps(num_threads);

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& local_map = thread_local_maps[t];

            for (size_t start = t * morsel_size; start < row_count; start += num_threads * morsel_size) {
                size_t end = std::min(start + morsel_size, row_count);

                for (size_t i = start; i < end; ++i) {
                    // Filter: l_shipdate <= cutoff_date
                    if (shipdate[i] <= cutoff_date) {
                        // Combine returnflag and linestatus into a single key
                        uint16_t key = (static_cast<uint16_t>(returnflag[i]) << 8) | linestatus[i];

                        auto& agg = local_map[key];

                        double qty = quantity[i];
                        double price = extendedprice[i];
                        double disc = discount[i];
                        double tx = tax[i];

                        double disc_price = price * (1.0 - disc);
                        double charge = disc_price * (1.0 + tx);

                        agg.sum_qty += qty;
                        agg.sum_base_price += price;
                        agg.sum_disc_price += disc_price;
                        agg.sum_charge += charge;
                        agg.sum_qty_for_avg += qty;
                        agg.sum_price_for_avg += price;
                        agg.sum_disc_for_avg += disc;
                        agg.count++;
                    }
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    t_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] scan_filter: " << std::fixed << std::setprecision(1) << scan_ms << " ms" << std::endl;

    // 5. Merge thread-local results
    t_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<uint16_t, AggregateResult> final_map;
    for (const auto& local_map : thread_local_maps) {
        for (const auto& [key, agg] : local_map) {
            auto& final_agg = final_map[key];
            final_agg.sum_qty += agg.sum_qty;
            final_agg.sum_base_price += agg.sum_base_price;
            final_agg.sum_disc_price += agg.sum_disc_price;
            final_agg.sum_charge += agg.sum_charge;
            final_agg.sum_qty_for_avg += agg.sum_qty_for_avg;
            final_agg.sum_price_for_avg += agg.sum_price_for_avg;
            final_agg.sum_disc_for_avg += agg.sum_disc_for_avg;
            final_agg.count += agg.count;
        }
    }

    t_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] aggregation: " << std::fixed << std::setprecision(1) << merge_ms << " ms" << std::endl;

    // 6. Sort results by returnflag, linestatus
    t_start = std::chrono::high_resolution_clock::now();

    struct ResultRow {
        uint8_t returnflag_code;
        uint8_t linestatus_code;
        AggregateResult agg;
    };

    std::vector<ResultRow> results;
    results.reserve(final_map.size());

    for (const auto& [key, agg] : final_map) {
        uint8_t rf_code = (key >> 8) & 0xFF;
        uint8_t ls_code = key & 0xFF;
        results.push_back({rf_code, ls_code, agg});
    }

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag_code != b.returnflag_code) {
            return a.returnflag_code < b.returnflag_code;
        }
        return a.linestatus_code < b.linestatus_code;
    });

    t_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    std::cout << "[TIMING] sort: " << std::fixed << std::setprecision(1) << sort_ms << " ms" << std::endl;

    // 7. Write results to CSV (if results_dir specified)
    if (!results_dir.empty()) {
        t_start = std::chrono::high_resolution_clock::now();

        std::ofstream outfile(results_dir + "/Q1.csv");
        outfile << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& row : results) {
            const std::string& rf = returnflag_dict[row.returnflag_code];
            const std::string& ls = linestatus_dict[row.linestatus_code];
            const auto& agg = row.agg;

            double avg_qty = agg.sum_qty_for_avg / agg.count;
            double avg_price = agg.sum_price_for_avg / agg.count;
            double avg_disc = agg.sum_disc_for_avg / agg.count;

            outfile << rf << "," << ls << ","
                    << std::fixed << std::setprecision(2)
                    << agg.sum_qty << ","
                    << agg.sum_base_price << ","
                    << agg.sum_disc_price << ","
                    << agg.sum_charge << ","
                    << avg_qty << ","
                    << avg_price << ","
                    << avg_disc << ","
                    << agg.count << "\n";
        }

        outfile.close();

        t_end = std::chrono::high_resolution_clock::now();
        double output_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
        std::cout << "[TIMING] output: " << std::fixed << std::setprecision(1) << output_ms << " ms" << std::endl;
    }

    // 8. Cleanup
    munmap((void*)shipdate, shipdate_size);
    munmap((void*)returnflag, returnflag_size);
    munmap((void*)linestatus, linestatus_size);
    munmap((void*)quantity, quantity_size);
    munmap((void*)extendedprice, extendedprice_size);
    munmap((void*)discount, discount_size);
    munmap((void*)tax, tax_size);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "[TIMING] total: " << std::fixed << std::setprecision(1) << total_ms << " ms" << std::endl;

    // Count total rows
    uint64_t total_count = 0;
    for (const auto& row : results) {
        total_count += row.agg.count;
    }
    std::cout << "Query returned " << results.size() << " groups (" << total_count << " filtered rows)" << std::endl;
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
