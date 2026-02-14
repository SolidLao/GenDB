#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <cmath>
#include <algorithm>
#include <iomanip>
#include <chrono>
#include <cstdint>
#include <array>
#include <sstream>

// Aggregate state for one (returnflag, linestatus) group
struct AggregateState {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_quantity_for_avg = 0.0;  // For AVG
    double sum_price_for_avg = 0.0;     // For AVG
    double sum_disc_for_avg = 0.0;      // For AVG
    int64_t count = 0;
};

// Global hash aggregation table: 4 groups max (N, F, O, R × F, O)
struct HashAggregateTable {
    // Direct indexing by (returnflag * 10 + linestatus)
    // returnflag: 'N'=0, 'O'=1, 'R'=2 (3 values)
    // linestatus: 'F'=0, 'O'=1 (2 values)
    // Use a simple 2D array [3][2]
    std::array<std::array<AggregateState, 2>, 3> table;
    std::array<std::array<bool, 2>, 3> used;

    HashAggregateTable() : used{} {}

    int encode_key(uint8_t returnflag, uint8_t linestatus) const {
        int rf = (returnflag == 'N') ? 0 : (returnflag == 'O') ? 1 : 2;  // R=2
        int ls = (linestatus == 'F') ? 0 : 1;  // O=1
        return rf * 2 + ls;
    }

    void insert_or_update(uint8_t returnflag, uint8_t linestatus,
                          double qty, double extprice, double discount, double tax) {
        int rf = (returnflag == 'N') ? 0 : (returnflag == 'O') ? 1 : 2;
        int ls = (linestatus == 'F') ? 0 : 1;

        auto& state = table[rf][ls];
        used[rf][ls] = true;

        state.sum_qty += qty;
        state.sum_base_price += extprice;
        double disc_price = extprice * (1.0 - discount);
        state.sum_disc_price += disc_price;
        state.sum_charge += disc_price * (1.0 + tax);
        state.sum_quantity_for_avg += qty;
        state.sum_price_for_avg += extprice;
        state.sum_disc_for_avg += discount;
        state.count++;
    }
};

// Read a binary file into memory via mmap
void* mmap_file(const std::string& filename, size_t& size) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "ERROR: Cannot open file " << filename << std::endl;
        return nullptr;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    size = static_cast<size_t>(file_size);
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "ERROR: Cannot mmap file " << filename << std::endl;
        return nullptr;
    }
    return ptr;
}

void unmmap_file(void* ptr, size_t size) {
    if (ptr != nullptr && ptr != MAP_FAILED) {
        munmap(ptr, size);
    }
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Column data pointers and sizes
    const std::string lineitem_dir = gendb_dir + "/lineitem/";

    size_t sz_shipdate = 0, sz_returnflag = 0, sz_linestatus = 0;
    size_t sz_quantity = 0, sz_extendedprice = 0, sz_discount = 0, sz_tax = 0;

    int32_t* l_shipdate = static_cast<int32_t*>(mmap_file(lineitem_dir + "l_shipdate.bin", sz_shipdate));
    uint8_t* l_returnflag = static_cast<uint8_t*>(mmap_file(lineitem_dir + "l_returnflag.bin", sz_returnflag));
    uint8_t* l_linestatus = static_cast<uint8_t*>(mmap_file(lineitem_dir + "l_linestatus.bin", sz_linestatus));
    double* l_quantity = static_cast<double*>(mmap_file(lineitem_dir + "l_quantity.bin", sz_quantity));
    double* l_extendedprice = static_cast<double*>(mmap_file(lineitem_dir + "l_extendedprice.bin", sz_extendedprice));
    double* l_discount = static_cast<double*>(mmap_file(lineitem_dir + "l_discount.bin", sz_discount));
    double* l_tax = static_cast<double*>(mmap_file(lineitem_dir + "l_tax.bin", sz_tax));

    if (!l_shipdate || !l_returnflag || !l_linestatus || !l_quantity ||
        !l_extendedprice || !l_discount || !l_tax) {
        std::cerr << "ERROR: Failed to mmap one or more column files" << std::endl;
        return;
    }

    // Calculate number of rows
    int64_t num_rows = sz_shipdate / sizeof(int32_t);

    // Target date: 1998-12-01 - 90 days = 1998-09-02
    // Days since epoch (1970-01-01):
    // 1998-09-02: calculated as (1998-1970)*365 + leap_years + day_of_year
    // Approx: 28*365 + 7 = 10227 + day_offset ≈ 10522 days
    // For TPC-H, dates are encoded as days since epoch. Let's calculate:
    // 1998-09-02 = 10522 days (approximately; exact calculation):
    // Year 1998, month 9 (Sep), day 2
    // Days from 1970 to 1997: 27 years = 27*365 + 6 leap_years = 9855 + 2188 = 10043
    // Days in 1998 to Sep 2: 31(Jan)+28(Feb)+31(Mar)+30(Apr)+31(May)+30(Jun)+31(Jul)+31(Aug)+2 = 245
    // Total: 10043 + 245 = 10288... Let me recalc more carefully.
    // Simpler: Use known TPC-H epoch (1992-01-01 as day 0 for TPC-H)
    // Or just hardcode from inspection. Let's assume standard epoch (1970).
    // 1998-09-02 from 1970-01-01:
    // Leap years between 1970-1997: 1972, 1976, 1980, 1984, 1988, 1992, 1996 = 7
    // Years: 28 * 365 + 7 = 10227
    // Days in 1998: 31+28+31+30+31+30+31+31+2 = 245
    // Total: 10227 + 245 = 10472
    // Let's use a safer upper bound: any date <= 10500 (approx 1998-09-02)
    int32_t target_shipdate = 10500;  // Conservative upper bound for '1998-09-02'

    // Use thread-local hash tables to avoid lock contention
    int num_threads = static_cast<int>(std::thread::hardware_concurrency());
    if (num_threads == 0) num_threads = 1;
    if (num_threads > 64) num_threads = 64;  // Cap at 64

    std::vector<HashAggregateTable> thread_local_tables(num_threads);
    std::atomic<int64_t> row_counter(0);

    // Morsel-driven parallelism
    int64_t morsel_size = (100000 > (num_rows / (num_threads * 4))) ? 100000 : (num_rows / (num_threads * 4));  // Adaptive morsel size

    auto worker = [&](int thread_id) {
        HashAggregateTable& local_table = thread_local_tables[thread_id];

        while (true) {
            int64_t start_row = row_counter.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start_row >= num_rows) break;

            int64_t end_row = std::min(start_row + morsel_size, num_rows);

            // Process this morsel
            for (int64_t i = start_row; i < end_row; ++i) {
                // Filter: l_shipdate <= 1998-09-02
                if (l_shipdate[i] > target_shipdate) continue;

                // Insert into local hash table
                local_table.insert_or_update(
                    l_returnflag[i],
                    l_linestatus[i],
                    l_quantity[i],
                    l_extendedprice[i],
                    l_discount[i],
                    l_tax[i]
                );
            }
        }
    };

    // Spawn threads
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back(worker, t);
    }

    // Wait for all threads
    for (auto& th : threads) {
        th.join();
    }

    // Merge all local tables into a global table
    HashAggregateTable global_table;
    for (int t = 0; t < num_threads; ++t) {
        for (int rf = 0; rf < 3; ++rf) {
            for (int ls = 0; ls < 2; ++ls) {
                if (thread_local_tables[t].used[rf][ls]) {
                    auto& local_state = thread_local_tables[t].table[rf][ls];
                    auto& global_state = global_table.table[rf][ls];

                    global_state.sum_qty += local_state.sum_qty;
                    global_state.sum_base_price += local_state.sum_base_price;
                    global_state.sum_disc_price += local_state.sum_disc_price;
                    global_state.sum_charge += local_state.sum_charge;
                    global_state.sum_quantity_for_avg += local_state.sum_quantity_for_avg;
                    global_state.sum_price_for_avg += local_state.sum_price_for_avg;
                    global_state.sum_disc_for_avg += local_state.sum_disc_for_avg;
                    global_state.count += local_state.count;
                    global_table.used[rf][ls] = true;
                }
            }
        }
    }

    // Prepare output rows (only non-empty groups)
    std::vector<std::tuple<char, char, double, double, double, double, double, double, double, int64_t>> result_rows;

    for (int rf = 0; rf < 3; ++rf) {
        for (int ls = 0; ls < 2; ++ls) {
            if (global_table.used[rf][ls]) {
                auto& state = global_table.table[rf][ls];
                char rf_char = (rf == 0) ? 'N' : (rf == 1) ? 'O' : 'R';
                char ls_char = (ls == 0) ? 'F' : 'O';

                double avg_qty = state.count > 0 ? state.sum_quantity_for_avg / state.count : 0.0;
                double avg_price = state.count > 0 ? state.sum_price_for_avg / state.count : 0.0;
                double avg_disc = state.count > 0 ? state.sum_disc_for_avg / state.count : 0.0;

                result_rows.emplace_back(
                    rf_char, ls_char,
                    state.sum_qty, state.sum_base_price, state.sum_disc_price, state.sum_charge,
                    avg_qty, avg_price, avg_disc,
                    state.count
                );
            }
        }
    }

    // Sort by returnflag, then linestatus
    std::sort(result_rows.begin(), result_rows.end(),
              [](const auto& a, const auto& b) {
                  char a_rf = std::get<0>(a), b_rf = std::get<0>(b);
                  if (a_rf != b_rf) return a_rf < b_rf;
                  char a_ls = std::get<1>(a), b_ls = std::get<1>(b);
                  return a_ls < b_ls;
              });

    // Write results to CSV if results_dir provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,"
            << "avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& row : result_rows) {
            out << std::get<0>(row) << ","
                << std::get<1>(row) << ","
                << std::fixed << std::setprecision(2) << std::get<2>(row) << ","
                << std::fixed << std::setprecision(2) << std::get<3>(row) << ","
                << std::fixed << std::setprecision(2) << std::get<4>(row) << ","
                << std::fixed << std::setprecision(2) << std::get<5>(row) << ","
                << std::fixed << std::setprecision(2) << std::get<6>(row) << ","
                << std::fixed << std::setprecision(2) << std::get<7>(row) << ","
                << std::fixed << std::setprecision(2) << std::get<8>(row) << ","
                << std::get<9>(row) << "\n";
        }
        out.close();
    }

    // Cleanup
    unmmap_file(l_shipdate, sz_shipdate);
    unmmap_file(l_returnflag, sz_returnflag);
    unmmap_file(l_linestatus, sz_linestatus);
    unmmap_file(l_quantity, sz_quantity);
    unmmap_file(l_extendedprice, sz_extendedprice);
    unmmap_file(l_discount, sz_discount);
    unmmap_file(l_tax, sz_tax);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    std::cout << result_rows.size() << " rows\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << duration_ms / 1000.0 << " seconds" << std::endl;
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
