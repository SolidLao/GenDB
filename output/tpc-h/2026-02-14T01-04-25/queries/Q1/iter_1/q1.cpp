#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <chrono>
#include <thread>
#include <atomic>
#include <algorithm>
#include <iomanip>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

// ============================================================================
// Data Types & Constants
// ============================================================================

struct AggregateState {
    int64_t sum_qty = 0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    int64_t count_order = 0;
    // For AVG: sum_qty, sum_price, sum_discount are already above; we track count
    double sum_price = 0.0;
    double sum_discount = 0.0;
};

struct GroupKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Simple perfect hash for (returnflag, linestatus) pairs
// Expected: 6 groups (A/N/R × F/O)
struct PerfectHashTable {
    // Map (returnflag, linestatus) code pair to array index
    // We'll use a simple 8x8 table (8 possible values for each)
    AggregateState table[8][8];
    bool present[8][8] = {};

    AggregateState& get(uint8_t rf, uint8_t ls) {
        return table[rf][ls];
    }

    bool has(uint8_t rf, uint8_t ls) {
        return present[rf][ls];
    }

    void set_present(uint8_t rf, uint8_t ls) {
        present[rf][ls] = true;
    }
};

// ============================================================================
// Utility: File Memory Mapping
// ============================================================================

class MmapFile {
public:
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            ::close(fd);
            return false;
        }

        size = sb.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            ::close(fd);
            return false;
        }

        // Prefetch hint for sequential access
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }

    void close() {
        if (data) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

// ============================================================================
// Dictionary Loading
// ============================================================================

// Parse dictionary file and return code->value mappings for specific columns
std::pair<std::vector<char>, std::vector<char>> load_dictionary_mappings(
    const std::string& gendb_dir) {
    std::vector<char> returnflag_dict, linestatus_dict;
    std::string dict_path = gendb_dir + "/lineitem/lineitem_dicts.txt";
    std::ifstream f(dict_path);

    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return {returnflag_dict, linestatus_dict};
    }

    std::string line;
    while (std::getline(f, line)) {
        // Format: "returnflag::A" or "returnflag: :N" (with space before colon)
        // or "linestatus::F" or "linestatus: :O"

        // Try to find "returnflag" prefix
        if (line.find("returnflag") == 0) {
            // Find the value (after :: or : :)
            size_t pos = line.find("::");
            std::string value;
            if (pos != std::string::npos) {
                value = line.substr(pos + 2);
            } else {
                // Try finding ": :" (with space)
                pos = line.find(": :");
                if (pos != std::string::npos) {
                    value = line.substr(pos + 3);
                }
            }
            if (!value.empty()) {
                returnflag_dict.push_back(value[0]);
            }
        }
        // Try to find "linestatus" prefix
        else if (line.find("linestatus") == 0) {
            // Find the value (after :: or : :)
            size_t pos = line.find("::");
            std::string value;
            if (pos != std::string::npos) {
                value = line.substr(pos + 2);
            } else {
                // Try finding ": :" (with space)
                pos = line.find(": :");
                if (pos != std::string::npos) {
                    value = line.substr(pos + 3);
                }
            }
            if (!value.empty()) {
                linestatus_dict.push_back(value[0]);
            }
        }
    }

    return {returnflag_dict, linestatus_dict};
}

// ============================================================================
// Delta Decoding Helper (on-the-fly for morsels)
// ============================================================================

int32_t decode_delta(const int32_t* encoded_data, size_t idx, int32_t last_value) {
    if (idx == 0) {
        return encoded_data[0];
    } else {
        int32_t result = last_value;
        for (size_t i = 0; i < idx; ++i) {
            result += encoded_data[i];
        }
        return result;
    }
}

// ============================================================================
// Main Query Execution
// ============================================================================

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Load dictionaries with correct code->value mappings
    auto [rf_dict, ls_dict] = load_dictionary_mappings(gendb_dir);

    // Open binary column files via mmap
    MmapFile l_quantity_file, l_extendedprice_file, l_discount_file, l_tax_file;
    MmapFile l_shipdate_file, l_returnflag_file, l_linestatus_file;

    std::string base = gendb_dir + "/lineitem/";
    if (!l_quantity_file.open(base + "l_quantity.bin")) {
        std::cerr << "Failed to open l_quantity.bin\n";
        return;
    }
    if (!l_extendedprice_file.open(base + "l_extendedprice.bin")) {
        std::cerr << "Failed to open l_extendedprice.bin\n";
        return;
    }
    if (!l_discount_file.open(base + "l_discount.bin")) {
        std::cerr << "Failed to open l_discount.bin\n";
        return;
    }
    if (!l_tax_file.open(base + "l_tax.bin")) {
        std::cerr << "Failed to open l_tax.bin\n";
        return;
    }
    if (!l_shipdate_file.open(base + "l_shipdate.bin")) {
        std::cerr << "Failed to open l_shipdate.bin\n";
        return;
    }
    if (!l_returnflag_file.open(base + "l_returnflag.bin")) {
        std::cerr << "Failed to open l_returnflag.bin\n";
        return;
    }
    if (!l_linestatus_file.open(base + "l_linestatus.bin")) {
        std::cerr << "Failed to open l_linestatus.bin\n";
        return;
    }

    // Constants
    const int32_t threshold_date = 10413;  // 1998-09-02 in days since 1970-01-01
    const int64_t total_rows = 59986052;
    const int num_threads = std::thread::hardware_concurrency();
    const int64_t morsel_size = 1000000;  // 1M rows per morsel

    // Type casts for data access
    const int32_t* l_quantity = (const int32_t*)l_quantity_file.data;
    const double* l_extendedprice = (const double*)l_extendedprice_file.data;
    const double* l_discount = (const double*)l_discount_file.data;
    const double* l_tax = (const double*)l_tax_file.data;
    const int32_t* l_shipdate_encoded = (const int32_t*)l_shipdate_file.data;
    const uint8_t* l_returnflag = (const uint8_t*)l_returnflag_file.data;
    const uint8_t* l_linestatus = (const uint8_t*)l_linestatus_file.data;

    // Global aggregation (will merge thread-local results)
    std::vector<std::vector<PerfectHashTable>> thread_local_agg(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        thread_local_agg[i].resize(1);
    }

    // Atomic counter for morsel distribution
    std::atomic<int64_t> morsel_idx(0);

    // Pre-compute morsel boundary values for incremental delta decoding
    // For each morsel start, compute the decoded value at that boundary
    std::vector<int32_t> morsel_start_dates;
    {
        int32_t cumsum = l_shipdate_encoded[0];
        morsel_start_dates.push_back(cumsum);
        for (int64_t i = morsel_size; i < total_rows; i += morsel_size) {
            // Compute cumulative sum up to position i
            for (int64_t j = morsel_start_dates.size() * morsel_size; j < i; ++j) {
                cumsum += l_shipdate_encoded[j];
            }
            morsel_start_dates.push_back(cumsum);
        }
    }

    // Lambda: worker thread processes morsels
    auto worker = [&](int thread_id) {
        PerfectHashTable& local_agg = thread_local_agg[thread_id][0];

        while (true) {
            int64_t start_row = morsel_idx.fetch_add(morsel_size);
            if (start_row >= total_rows) break;

            int64_t end_row = std::min(start_row + morsel_size, total_rows);

            // Decode delta-encoded shipdate for this morsel using pre-computed boundary
            std::vector<int32_t> l_shipdate_decoded(end_row - start_row);

            // Get the starting date for this morsel from pre-computed boundaries
            int64_t morsel_number = start_row / morsel_size;
            int32_t last_date = (morsel_number > 0) ? morsel_start_dates[morsel_number - 1] : l_shipdate_encoded[0];
            if (morsel_number == 0) {
                last_date = l_shipdate_encoded[0] - l_shipdate_encoded[0];  // Initialize to 0, will add first value
            }

            // Incremental delta decode within this morsel
            for (int64_t i = 0; i < end_row - start_row; ++i) {
                if (i == 0) {
                    if (start_row == 0) {
                        l_shipdate_decoded[i] = l_shipdate_encoded[0];
                    } else {
                        // Use pre-computed previous value as base
                        l_shipdate_decoded[i] = last_date + l_shipdate_encoded[start_row];
                    }
                } else {
                    l_shipdate_decoded[i] = l_shipdate_decoded[i - 1] + l_shipdate_encoded[start_row + i];
                }
            }

            // Process rows in this morsel
            for (int64_t row = start_row; row < end_row; ++row) {
                int64_t morsel_offset = row - start_row;

                // Filter: l_shipdate <= 1998-09-02
                int32_t ship_date = l_shipdate_decoded[morsel_offset];
                if (ship_date > threshold_date) continue;

                // Extract group key (use codes directly for aggregation)
                uint8_t rf_code = l_returnflag[row];
                uint8_t ls_code = l_linestatus[row];

                // Update aggregate
                auto& agg = local_agg.get(rf_code, ls_code);
                local_agg.set_present(rf_code, ls_code);

                int32_t qty = l_quantity[row];
                double price = l_extendedprice[row];
                double disc = l_discount[row];
                double tax = l_tax[row];

                agg.sum_qty += qty;
                agg.sum_base_price += price;
                agg.sum_disc_price += price * (1.0 - disc);
                agg.sum_charge += price * (1.0 - disc) * (1.0 + tax);
                agg.sum_price += price;
                agg.sum_discount += disc;
                agg.count_order += 1;
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.push_back(std::thread(worker, i));
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local results
    PerfectHashTable global_agg;
    for (int i = 0; i < num_threads; ++i) {
        for (size_t rf = 0; rf < 8; ++rf) {
            for (size_t ls = 0; ls < 8; ++ls) {
                if (thread_local_agg[i][0].present[rf][ls]) {
                    global_agg.set_present((uint8_t)rf, (uint8_t)ls);
                    auto& g = global_agg.get((uint8_t)rf, (uint8_t)ls);
                    auto& l = thread_local_agg[i][0].get((uint8_t)rf, (uint8_t)ls);
                    g.sum_qty += l.sum_qty;
                    g.sum_base_price += l.sum_base_price;
                    g.sum_disc_price += l.sum_disc_price;
                    g.sum_charge += l.sum_charge;
                    g.sum_price += l.sum_price;
                    g.sum_discount += l.sum_discount;
                    g.count_order += l.count_order;
                }
            }
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(
        end_time - start_time).count();

    // Count non-empty groups
    int64_t output_rows = 0;
    for (size_t rf = 0; rf < 8; ++rf) {
        for (size_t ls = 0; ls < 8; ++ls) {
            if (global_agg.present[rf][ls]) {
                output_rows++;
            }
        }
    }

    // Output results
    if (!results_dir.empty()) {
        std::string out_path = results_dir + "/Q1.csv";
        std::ofstream out(out_path);
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
               "sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (size_t rf = 0; rf < 8; ++rf) {
            for (size_t ls = 0; ls < 8; ++ls) {
                if (global_agg.present[rf][ls]) {
                    auto& agg = global_agg.get((uint8_t)rf, (uint8_t)ls);
                    char rf_char = (rf < rf_dict.size()) ? rf_dict[rf] : '?';
                    char ls_char = (ls < ls_dict.size()) ? ls_dict[ls] : '?';

                    double avg_qty = agg.count_order > 0 ? (double)agg.sum_qty / agg.count_order : 0.0;
                    double avg_price = agg.count_order > 0 ? agg.sum_price / agg.count_order : 0.0;
                    double avg_disc = agg.count_order > 0 ? agg.sum_discount / agg.count_order : 0.0;

                    out << rf_char << "," << ls_char << ","
                        << std::fixed << std::setprecision(2)
                        << agg.sum_qty << ","
                        << agg.sum_base_price << ","
                        << agg.sum_disc_price << ","
                        << agg.sum_charge << ","
                        << avg_qty << ","
                        << avg_price << ","
                        << avg_disc << ","
                        << agg.count_order << "\n";
                }
            }
        }
        out.close();
    }

    // Print timing and row count to terminal
    std::cout << "Q1 execution time: " << std::fixed << std::setprecision(2)
              << elapsed_ms << " ms\n";
    std::cout << "Output rows: " << output_rows << "\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
