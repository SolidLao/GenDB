#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <cmath>

// Q1: Pricing Summary Report
// Scans lineitem with date filter, aggregates by returnflag and linestatus

struct AggregateRow {
    char returnflag;
    char linestatus;
    long double sum_qty = 0.0;
    long double sum_base_price = 0.0;
    long double sum_disc_price = 0.0;
    long double sum_charge = 0.0;
    long double sum_quantity_for_avg = 0.0;
    long double sum_price_for_avg = 0.0;
    long double sum_discount_for_avg = 0.0;
    int64_t count = 0;
};

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint64_t start_row;
    uint64_t end_row;
};

// Dictionary mappings
std::unordered_map<uint8_t, char> returnflag_dict;
std::unordered_map<uint8_t, char> linestatus_dict;

// Local aggregate structure for each thread
struct LocalAggregate {
    std::unordered_map<uint32_t, AggregateRow> groups;  // key = returnflag(8 bits) | linestatus(8 bits)
};

void load_dictionaries(const std::string& gendb_dir) {
    std::string dict_file = gendb_dir + "/lineitem/dictionaries.txt";
    std::ifstream f(dict_file);
    if (!f) {
        std::cerr << "Error opening dictionaries file: " << dict_file << std::endl;
        exit(1);
    }

    std::string line;
    std::string current_column;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        // Check if this is a column name (starts with 'l_')
        if (line.find('\t') == std::string::npos && line[0] == 'l') {
            current_column = line;
            continue;
        }

        // Parse "code\tvalue"
        size_t tab_pos = line.find('\t');
        if (tab_pos != std::string::npos) {
            std::string code_str = line.substr(0, tab_pos);
            std::string value = line.substr(tab_pos + 1);

            uint8_t code = std::stoi(code_str);

            if (current_column == "l_returnflag") {
                returnflag_dict[code] = value[0];
            } else if (current_column == "l_linestatus") {
                linestatus_dict[code] = value[0];
            }
        }
    }
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto overall_start = std::chrono::high_resolution_clock::now();

    // Load dictionaries
    load_dictionaries(gendb_dir);

    // Verify we got the dictionaries
    if (returnflag_dict.empty() || linestatus_dict.empty()) {
        std::cerr << "Failed to load dictionaries" << std::endl;
        exit(1);
    }

    // Open and mmap columns
    std::string lineitem_dir = gendb_dir + "/lineitem/";

    // l_shipdate (int32_t, no encoding)
    int fd_shipdate = open((lineitem_dir + "l_shipdate.col").c_str(), O_RDONLY);
    if (fd_shipdate < 0) {
        std::cerr << "Error opening l_shipdate.col" << std::endl;
        exit(1);
    }
    size_t shipdate_size = lseek(fd_shipdate, 0, SEEK_END);
    const int32_t* shipdate = (const int32_t*)mmap(nullptr, shipdate_size, PROT_READ, MAP_PRIVATE, fd_shipdate, 0);

    // l_quantity (double)
    int fd_quantity = open((lineitem_dir + "l_quantity.col").c_str(), O_RDONLY);
    if (fd_quantity < 0) {
        std::cerr << "Error opening l_quantity.col" << std::endl;
        exit(1);
    }
    size_t quantity_size = lseek(fd_quantity, 0, SEEK_END);
    const double* quantity = (const double*)mmap(nullptr, quantity_size, PROT_READ, MAP_PRIVATE, fd_quantity, 0);

    // l_extendedprice (double)
    int fd_extprice = open((lineitem_dir + "l_extendedprice.col").c_str(), O_RDONLY);
    if (fd_extprice < 0) {
        std::cerr << "Error opening l_extendedprice.col" << std::endl;
        exit(1);
    }
    size_t extprice_size = lseek(fd_extprice, 0, SEEK_END);
    const double* extprice = (const double*)mmap(nullptr, extprice_size, PROT_READ, MAP_PRIVATE, fd_extprice, 0);

    // l_discount (double)
    int fd_discount = open((lineitem_dir + "l_discount.col").c_str(), O_RDONLY);
    if (fd_discount < 0) {
        std::cerr << "Error opening l_discount.col" << std::endl;
        exit(1);
    }
    size_t discount_size = lseek(fd_discount, 0, SEEK_END);
    const double* discount = (const double*)mmap(nullptr, discount_size, PROT_READ, MAP_PRIVATE, fd_discount, 0);

    // l_tax (double)
    int fd_tax = open((lineitem_dir + "l_tax.col").c_str(), O_RDONLY);
    if (fd_tax < 0) {
        std::cerr << "Error opening l_tax.col" << std::endl;
        exit(1);
    }
    size_t tax_size = lseek(fd_tax, 0, SEEK_END);
    const double* tax = (const double*)mmap(nullptr, tax_size, PROT_READ, MAP_PRIVATE, fd_tax, 0);

    // l_returnflag (uint8_t dictionary)
    int fd_returnflag = open((lineitem_dir + "l_returnflag.col").c_str(), O_RDONLY);
    if (fd_returnflag < 0) {
        std::cerr << "Error opening l_returnflag.col" << std::endl;
        exit(1);
    }
    size_t returnflag_size = lseek(fd_returnflag, 0, SEEK_END);
    const uint8_t* returnflag_codes = (const uint8_t*)mmap(nullptr, returnflag_size, PROT_READ, MAP_PRIVATE, fd_returnflag, 0);

    // l_linestatus (uint8_t dictionary)
    int fd_linestatus = open((lineitem_dir + "l_linestatus.col").c_str(), O_RDONLY);
    if (fd_linestatus < 0) {
        std::cerr << "Error opening l_linestatus.col" << std::endl;
        exit(1);
    }
    size_t linestatus_size = lseek(fd_linestatus, 0, SEEK_END);
    const uint8_t* linestatus_codes = (const uint8_t*)mmap(nullptr, linestatus_size, PROT_READ, MAP_PRIVATE, fd_linestatus, 0);

    // Load and parse zone map (for future optimization)
    int fd_zonemap = open((lineitem_dir + "l_shipdate.zone_map.idx").c_str(), O_RDONLY);
    size_t zonemap_size = lseek(fd_zonemap, 0, SEEK_END);
    const ZoneMapEntry* zonemap = (const ZoneMapEntry*)mmap(nullptr, zonemap_size, PROT_READ, MAP_PRIVATE, fd_zonemap, 0);
    (void)zonemap;  // Mark as intentionally unused for now

    // Date filter: 1998-12-01 - 90 days = 1998-09-02
    // 1970-01-01 to 1998-09-02 is 10471 days
    const int32_t cutoff_date = 10471;

    // Get total row count from metadata
    uint64_t total_rows = 59986052;  // From metadata: exact_row_count

    // Parallel scan with morsel-driven approach
    size_t num_threads = std::thread::hardware_concurrency();
    std::vector<LocalAggregate> thread_results(num_threads);
    std::vector<std::thread> threads;

    // Distribute work across threads
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            LocalAggregate& local = thread_results[t];

            // Process all rows, with this thread taking every num_threads'th row
            for (uint64_t i = t; i < total_rows; i += num_threads) {
                // Filter: l_shipdate <= cutoff_date
                if (shipdate[i] <= cutoff_date) {
                    // Decode dictionary columns
                    char rf = returnflag_dict[returnflag_codes[i]];
                    char ls = linestatus_dict[linestatus_codes[i]];

                    // Create group key
                    uint32_t key = ((uint32_t)rf << 8) | (uint32_t)ls;

                    // Get or create group
                    if (local.groups.find(key) == local.groups.end()) {
                        AggregateRow row;
                        row.returnflag = rf;
                        row.linestatus = ls;
                        local.groups[key] = row;
                    }

                    AggregateRow& agg = local.groups[key];

                    // Compute aggregates with long double precision
                    long double disc_price = (long double)extprice[i] * (1.0L - (long double)discount[i]);
                    long double charge = disc_price * (1.0L + (long double)tax[i]);

                    agg.sum_qty += quantity[i];
                    agg.sum_base_price += extprice[i];
                    agg.sum_disc_price += disc_price;
                    agg.sum_charge += charge;
                    agg.sum_quantity_for_avg += quantity[i];
                    agg.sum_price_for_avg += extprice[i];
                    agg.sum_discount_for_avg += discount[i];
                    agg.count++;
                }
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Merge results from all threads
    std::unordered_map<uint32_t, AggregateRow> global_agg;

    for (const auto& local : thread_results) {
        for (const auto& [key, row] : local.groups) {
            if (global_agg.find(key) == global_agg.end()) {
                global_agg[key] = row;
            } else {
                AggregateRow& g = global_agg[key];
                g.sum_qty += row.sum_qty;
                g.sum_base_price += row.sum_base_price;
                g.sum_disc_price += row.sum_disc_price;
                g.sum_charge += row.sum_charge;
                g.sum_quantity_for_avg += row.sum_quantity_for_avg;
                g.sum_price_for_avg += row.sum_price_for_avg;
                g.sum_discount_for_avg += row.sum_discount_for_avg;
                g.count += row.count;
            }
        }
    }

    // Sort by returnflag, linestatus for output
    std::vector<AggregateRow> sorted_results;
    for (const auto& [key, row] : global_agg) {
        sorted_results.push_back(row);
    }
    std::sort(sorted_results.begin(), sorted_results.end(), [](const AggregateRow& a, const AggregateRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    auto overall_end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(overall_end - overall_start);

    // Write results if results_dir provided
    if (!results_dir.empty()) {
        std::string output_file = results_dir + "/Q1.csv";
        std::ofstream out(output_file);
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& row : sorted_results) {
            out << std::fixed << std::setprecision(2);
            out << row.returnflag << ","
                << row.linestatus << ","
                << (double)row.sum_qty << ","
                << (double)row.sum_base_price << ","
                << (double)row.sum_disc_price << ","
                << (double)row.sum_charge << ","
                << (double)(row.sum_quantity_for_avg / row.count) << ","
                << (double)(row.sum_price_for_avg / row.count) << ","
                << (double)(row.sum_discount_for_avg / row.count) << ","
                << (double)row.count << "\n";
        }
        out.close();
    }

    // Output to console
    std::cout << "Query returned " << sorted_results.size() << " rows" << std::endl;
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;

    // Cleanup
    munmap((void*)shipdate, shipdate_size);
    munmap((void*)quantity, quantity_size);
    munmap((void*)extprice, extprice_size);
    munmap((void*)discount, discount_size);
    munmap((void*)tax, tax_size);
    munmap((void*)returnflag_codes, returnflag_size);
    munmap((void*)linestatus_codes, linestatus_size);
    munmap((void*)zonemap, zonemap_size);
    close(fd_shipdate);
    close(fd_quantity);
    close(fd_extprice);
    close(fd_discount);
    close(fd_tax);
    close(fd_returnflag);
    close(fd_linestatus);
    close(fd_zonemap);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
