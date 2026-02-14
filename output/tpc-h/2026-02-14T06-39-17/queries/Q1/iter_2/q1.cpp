#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <string>
#include <thread>
#include <mutex>
#include <cmath>
#include <chrono>
#include <iomanip>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>

// Aggregate structure for a single group
struct AggregateRow {
    uint8_t returnflag;
    uint8_t linestatus;
    long double sum_qty = 0.0;
    long double sum_base_price = 0.0;
    long double sum_disc_price = 0.0;
    long double sum_charge = 0.0;
    long double sum_discount = 0.0;  // For computing avg_disc
    int64_t count = 0;
};

// Composite key for grouping
struct GroupKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash function for GroupKey
struct GroupKeyHash {
    size_t operator()(const GroupKey& key) const {
        return ((size_t)key.returnflag << 8) | key.linestatus;
    }
};

// Helper function to mmap a binary file
void* mmapFile(const std::string& filepath, size_t& file_size) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening " << filepath << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Error stat'ing " << filepath << std::endl;
        close(fd);
        return nullptr;
    }

    file_size = sb.st_size;
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Error mmap'ing " << filepath << std::endl;
        return nullptr;
    }

    madvise(ptr, file_size, MADV_SEQUENTIAL);
    return ptr;
}

// Load dictionary mapping from .dict file
std::unordered_map<uint8_t, char> loadDictionary(const std::string& dict_file) {
    std::unordered_map<uint8_t, char> dict;
    std::ifstream file(dict_file, std::ios::binary);

    if (!file.is_open()) {
        std::cerr << "Warning: Could not open dictionary file " << dict_file << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(file, line)) {
        // Format: "0=N", "1=R", "2=A", etc.
        if (line.find('=') != std::string::npos) {
            size_t eq_pos = line.find('=');
            uint8_t code = std::stoi(line.substr(0, eq_pos));
            char value = line[eq_pos + 1];
            dict[code] = value;
        }
    }

    return dict;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Constants
    const int32_t CUTOFF_DATE = 10471;  // 1998-09-02 in epoch days (1998-12-01 minus 90 days)

    std::string lineitem_dir = gendb_dir + "/lineitem";

    // Load dictionaries
    auto returnflag_dict = loadDictionary(lineitem_dir + "/l_returnflag.bin.dict");
    auto linestatus_dict = loadDictionary(lineitem_dir + "/l_linestatus.bin.dict");

    // mmap columns
    size_t size_returnflag = 0;
    size_t size_linestatus = 0;
    size_t size_quantity = 0;
    size_t size_extendedprice = 0;
    size_t size_discount = 0;
    size_t size_tax = 0;
    size_t size_shipdate = 0;

    const uint8_t* col_returnflag = (const uint8_t*)mmapFile(lineitem_dir + "/l_returnflag.bin", size_returnflag);
    const uint8_t* col_linestatus = (const uint8_t*)mmapFile(lineitem_dir + "/l_linestatus.bin", size_linestatus);
    const double* col_quantity = (const double*)mmapFile(lineitem_dir + "/l_quantity.bin", size_quantity);
    const double* col_extendedprice = (const double*)mmapFile(lineitem_dir + "/l_extendedprice.bin", size_extendedprice);
    const double* col_discount = (const double*)mmapFile(lineitem_dir + "/l_discount.bin", size_discount);
    const double* col_tax = (const double*)mmapFile(lineitem_dir + "/l_tax.bin", size_tax);
    const int32_t* col_shipdate = (const int32_t*)mmapFile(lineitem_dir + "/l_shipdate.bin", size_shipdate);

    if (!col_returnflag || !col_linestatus || !col_quantity || !col_extendedprice ||
        !col_discount || !col_tax || !col_shipdate) {
        std::cerr << "Error: Failed to mmap one or more columns\n";
        return;
    }

    // Compute row count (assuming all columns have same row count)
    int64_t row_count = size_returnflag / sizeof(uint8_t);

    // Parallel aggregation using thread-local hash tables
    size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::unordered_map<GroupKey, AggregateRow, GroupKeyHash>> thread_aggregates(num_threads);
    std::vector<std::thread> threads;

    const size_t morsel_size = 100000;  // Process 100k rows per morsel

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& local_agg = thread_aggregates[t];

            for (int64_t i = t * morsel_size; i < row_count; i += num_threads * morsel_size) {
                int64_t end = std::min((int64_t)(i + morsel_size), row_count);

                for (int64_t j = i; j < end; ++j) {
                    // Apply filter: l_shipdate <= CUTOFF_DATE
                    if (col_shipdate[j] > CUTOFF_DATE) {
                        continue;
                    }

                    // Decode dictionary values
                    uint8_t returnflag_code = col_returnflag[j];
                    uint8_t linestatus_code = col_linestatus[j];

                    GroupKey key{returnflag_code, linestatus_code};

                    if (local_agg.find(key) == local_agg.end()) {
                        local_agg[key] = {returnflag_code, linestatus_code, 0.0L, 0.0L, 0.0L, 0.0L, 0.0L, 0};
                    }

                    AggregateRow& row = local_agg[key];

                    long double qty = (long double)col_quantity[j];
                    long double price = (long double)col_extendedprice[j];
                    long double disc = (long double)col_discount[j];
                    long double tax = (long double)col_tax[j];

                    row.sum_qty += qty;
                    row.sum_base_price += price;
                    row.sum_disc_price += price * (1.0L - disc);
                    row.sum_charge += price * (1.0L - disc) * (1.0L + tax);
                    row.sum_discount += disc;
                    row.count += 1;
                }
            }
        });
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local aggregates into global map
    std::unordered_map<GroupKey, AggregateRow, GroupKeyHash> global_agg;
    for (const auto& local_agg : thread_aggregates) {
        for (const auto& pair : local_agg) {
            GroupKey key = pair.first;
            const AggregateRow& row = pair.second;

            if (global_agg.find(key) == global_agg.end()) {
                global_agg[key] = row;
            } else {
                global_agg[key].sum_qty += row.sum_qty;
                global_agg[key].sum_base_price += row.sum_base_price;
                global_agg[key].sum_disc_price += row.sum_disc_price;
                global_agg[key].sum_charge += row.sum_charge;
                global_agg[key].sum_discount += row.sum_discount;
                global_agg[key].count += row.count;
            }
        }
    }

    // Convert to vector and sort by (l_returnflag, l_linestatus)
    // Sort by character values, not by dictionary codes
    std::vector<AggregateRow> results;
    for (const auto& pair : global_agg) {
        results.push_back(pair.second);
    }

    std::sort(results.begin(), results.end(), [&](const AggregateRow& a, const AggregateRow& b) {
        char a_rf = returnflag_dict.count(a.returnflag) ? returnflag_dict[a.returnflag] : '?';
        char b_rf = returnflag_dict.count(b.returnflag) ? returnflag_dict[b.returnflag] : '?';
        if (a_rf != b_rf) return a_rf < b_rf;

        char a_ls = linestatus_dict.count(a.linestatus) ? linestatus_dict[a.linestatus] : '?';
        char b_ls = linestatus_dict.count(b.linestatus) ? linestatus_dict[b.linestatus] : '?';
        return a_ls < b_ls;
    });

    // Compute total row count for reporting
    int64_t total_matching_rows = 0;
    for (const auto& row : results) {
        total_matching_rows += row.count;
    }

    // Write results to CSV if results_dir is specified
    if (!results_dir.empty()) {
        std::string output_file = results_dir + "/Q1.csv";
        std::ofstream out(output_file);

        if (!out.is_open()) {
            std::cerr << "Error: Could not open output file " << output_file << std::endl;
        } else {
            // Header
            out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

            // Rows
            for (const auto& row : results) {
                char rf = returnflag_dict.count(row.returnflag) ? returnflag_dict[row.returnflag] : '?';
                char ls = linestatus_dict.count(row.linestatus) ? linestatus_dict[row.linestatus] : '?';

                long double avg_qty = row.sum_qty / row.count;
                long double avg_price = row.sum_base_price / row.count;
                long double avg_disc = row.sum_discount / row.count;

                out << rf << "," << ls << ",";
                out << std::fixed << std::setprecision(2) << (double)row.sum_qty << ",";
                out << std::fixed << std::setprecision(2) << (double)row.sum_base_price << ",";
                out << std::fixed << std::setprecision(4) << (double)row.sum_disc_price << ",";
                out << std::fixed << std::setprecision(6) << (double)row.sum_charge << ",";
                out << std::fixed << std::setprecision(2) << (double)avg_qty << ",";
                out << std::fixed << std::setprecision(2) << (double)avg_price << ",";
                out << std::fixed << std::setprecision(2) << (double)avg_disc << ",";
                out << std::fixed << std::setprecision(0) << row.count << "\n";
            }

            out.close();
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << "Query returned " << total_matching_rows << " rows\n";
    std::cout << "Execution time: " << duration_ms << " ms\n";

    // Clean up mmaps
    munmap((void*)col_returnflag, size_returnflag);
    munmap((void*)col_linestatus, size_linestatus);
    munmap((void*)col_quantity, size_quantity);
    munmap((void*)col_extendedprice, size_extendedprice);
    munmap((void*)col_discount, size_discount);
    munmap((void*)col_tax, size_tax);
    munmap((void*)col_shipdate, size_shipdate);
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
