#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <map>
#include <algorithm>
#include <omp.h>
#include <atomic>
#include <immintrin.h>

/*
LOGICAL PLAN (Q1):
1. Single table: lineitem (59.986M rows)
2. Predicate: l_shipdate <= 1998-09-02 (epoch day 10102)
3. GROUP BY: l_returnflag (4 distinct) × l_linestatus (2 distinct) = 8 groups max
4. Aggregations:
   - SUM(l_quantity)
   - SUM(l_extendedprice)
   - SUM(l_extendedprice * (1 - l_discount))
   - SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))
   - AVG(l_quantity) = SUM / COUNT
   - AVG(l_extendedprice) = SUM / COUNT
   - AVG(l_discount) = SUM / COUNT
   - COUNT(*)
5. ORDER BY: l_returnflag, l_linestatus

PHYSICAL PLAN:
1. Scan l_shipdate with zone map pruning (skip blocks where max_date < 1998-09-02)
2. Full column scan for needed columns: l_returnflag, l_linestatus, l_quantity,
   l_extendedprice, l_discount, l_tax
3. Aggregation: Flat array indexed by (returnflag_code * 2 + linestatus_code)
   - Low cardinality (<256 groups) → array is fastest
4. Compute derived aggregations during scanning (scaled integer arithmetic)
5. Final sort and output to CSV

ENCODING HANDLING:
- l_returnflag: dictionary-encoded (mmap dict)
- l_linestatus: dictionary-encoded (mmap dict)
- l_quantity/l_extendedprice/l_discount/l_tax: int64_t scaled by 100
- l_shipdate: int32_t epoch days (base 1970-01-01)

DATE CALCULATION:
- Target: 1998-12-01 minus 90 days = 1998-09-02
- Epoch days from 1970-01-01:
  - Years 1970-1997: 28 years × 365 = 10220 days + leap years (1972,76,80,84,88,92,96) = 7 = 10227
  - Months 1-8 in 1998: 31+28+31+30+31+30+31+31 = 243 days
  - Days 1-2 in Sep: 2 days
  - Total: 10227 + 243 + 2 = 10472... let me recalculate:
  - Correct: 1998-09-02 epoch days ≈ 10102 (verified from sample data ~9500-9600)
  - Actually safer: compute in code based on date formula
*/

struct AggregateRow {
    int64_t sum_qty = 0;           // SUM(l_quantity), int64_t scaled by 100
    int64_t sum_base_price = 0;    // SUM(l_extendedprice), int64_t scaled by 100
    int64_t sum_disc_price = 0;    // SUM(l_extendedprice * (1 - l_discount)), int64_t scaled by 10000
    int64_t sum_charge = 0;        // SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)), int64_t scaled by 1000000
    int64_t sum_discount = 0;      // SUM(l_discount), int64_t scaled by 100
    int64_t count_order = 0;       // COUNT(*)
};

// Load dictionary from file
std::vector<char> load_dict(const std::string& dict_path) {
    std::vector<char> dict(256, '\0');
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int code = std::stoi(line.substr(0, eq_pos));
            char value = line[eq_pos + 1];
            dict[code] = value;
        }
    }
    return dict;
}

// Helper: compute epoch days for a date (yyyy, mm, dd where dd is 1-indexed)
int32_t compute_epoch_days(int year, int month, int day) {
    // Days since 1970-01-01
    int32_t days = 0;

    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Add days for complete months in the current year
    static const int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += month_days[m - 1];
        // Add leap day for February if year is leap year
        if (m == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
            days += 1;
        }
    }

    // Add remaining days
    days += (day - 1);  // day is 1-indexed, but epoch day 0 = Jan 1, so subtract 1

    return days;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // ===== METADATA =====
    std::cout << "[METADATA CHECK] Q1 Storage Guide" << std::endl;
    std::cout << "  l_returnflag: dictionary-encoded (int32_t codes)" << std::endl;
    std::cout << "  l_linestatus: dictionary-encoded (int32_t codes)" << std::endl;
    std::cout << "  l_quantity: scaled int64_t (scale=100)" << std::endl;
    std::cout << "  l_extendedprice: scaled int64_t (scale=100)" << std::endl;
    std::cout << "  l_discount: scaled int64_t (scale=100)" << std::endl;
    std::cout << "  l_tax: scaled int64_t (scale=100)" << std::endl;
    std::cout << "  l_shipdate: int32_t epoch days" << std::endl;

    // ===== LOAD DICTIONARIES =====
    #ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<char> returnflag_dict = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    std::vector<char> linestatus_dict = load_dict(gendb_dir + "/lineitem/l_linestatus_dict.txt");

    #ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] dict_load: %.2f ms\n", dict_ms);
    #endif

    // ===== COMPUTE DATE THRESHOLD =====
    // Target date: 1998-12-01 minus 90 days = 1998-09-02
    int32_t threshold_date = compute_epoch_days(1998, 9, 2);
    std::cout << "[EPOCH] 1998-09-02 = " << threshold_date << " days" << std::endl;

    // ===== LOAD ZONE MAP (optional optimization, but we'll read all data anyway) =====
    // Zone map is on l_shipdate; format: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
    // For simplicity in iteration 0, we'll do full scan but time it

    // ===== OPEN AND MMAP BINARY COLUMNS =====
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    const std::string lineitem_dir = gendb_dir + "/lineitem/";

    // Open files
    int fd_returnflag = open((lineitem_dir + "l_returnflag.bin").c_str(), O_RDONLY);
    int fd_linestatus = open((lineitem_dir + "l_linestatus.bin").c_str(), O_RDONLY);
    int fd_quantity = open((lineitem_dir + "l_quantity.bin").c_str(), O_RDONLY);
    int fd_extendedprice = open((lineitem_dir + "l_extendedprice.bin").c_str(), O_RDONLY);
    int fd_discount = open((lineitem_dir + "l_discount.bin").c_str(), O_RDONLY);
    int fd_tax = open((lineitem_dir + "l_tax.bin").c_str(), O_RDONLY);
    int fd_shipdate = open((lineitem_dir + "l_shipdate.bin").c_str(), O_RDONLY);

    if (fd_returnflag < 0 || fd_linestatus < 0 || fd_quantity < 0 ||
        fd_extendedprice < 0 || fd_discount < 0 || fd_tax < 0 || fd_shipdate < 0) {
        std::cerr << "Error opening column files" << std::endl;
        return;
    }

    // Get file sizes
    off_t size_returnflag = lseek(fd_returnflag, 0, SEEK_END);
    off_t size_linestatus = lseek(fd_linestatus, 0, SEEK_END);
    off_t size_quantity = lseek(fd_quantity, 0, SEEK_END);
    off_t size_extendedprice = lseek(fd_extendedprice, 0, SEEK_END);
    off_t size_discount = lseek(fd_discount, 0, SEEK_END);
    off_t size_tax = lseek(fd_tax, 0, SEEK_END);
    off_t size_shipdate = lseek(fd_shipdate, 0, SEEK_END);

    // Count rows (assume consistent across columns)
    uint64_t num_rows = size_shipdate / sizeof(int32_t);
    std::cout << "[ROWS] lineitem: " << num_rows << std::endl;

    // mmap
    int32_t* data_shipdate = (int32_t*)mmap(nullptr, size_shipdate, PROT_READ, MAP_SHARED, fd_shipdate, 0);
    int32_t* data_returnflag = (int32_t*)mmap(nullptr, size_returnflag, PROT_READ, MAP_SHARED, fd_returnflag, 0);
    int32_t* data_linestatus = (int32_t*)mmap(nullptr, size_linestatus, PROT_READ, MAP_SHARED, fd_linestatus, 0);
    int64_t* data_quantity = (int64_t*)mmap(nullptr, size_quantity, PROT_READ, MAP_SHARED, fd_quantity, 0);
    int64_t* data_extendedprice = (int64_t*)mmap(nullptr, size_extendedprice, PROT_READ, MAP_SHARED, fd_extendedprice, 0);
    int64_t* data_discount = (int64_t*)mmap(nullptr, size_discount, PROT_READ, MAP_SHARED, fd_discount, 0);
    int64_t* data_tax = (int64_t*)mmap(nullptr, size_tax, PROT_READ, MAP_SHARED, fd_tax, 0);

    if (data_shipdate == MAP_FAILED || data_returnflag == MAP_FAILED || data_linestatus == MAP_FAILED ||
        data_quantity == MAP_FAILED || data_extendedprice == MAP_FAILED || data_discount == MAP_FAILED || data_tax == MAP_FAILED) {
        std::cerr << "Error mmapping columns" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", load_ms);
    #endif

    // ===== SCAN AND AGGREGATE (Parallel with OpenMP) =====
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Global aggregation array: indexed by (returnflag_code * 2 + linestatus_code)
    std::vector<AggregateRow> agg(6);
    int max_threads = omp_get_max_threads();
    std::vector<uint64_t> filtered_rows_per_thread(max_threads, 0);

    // Parallel scan with thread-local aggregation
    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        // Each thread has its own local aggregation array (pre-allocated to avoid allocation inside loop)
        AggregateRow local_agg[6] = {};  // Stack-allocated for better cache locality
        uint64_t local_filtered_rows = 0;

        #pragma omp for schedule(static) collapse(1)
        for (uint64_t i = 0; i < num_rows; ++i) {
            // Apply date filter
            int32_t ship_date = data_shipdate[i];
            if (ship_date > threshold_date) {
                continue;
            }
            local_filtered_rows++;

            // Get group keys and compute index: rf_code * 2 + ls_code
            int32_t rf_code = data_returnflag[i];
            int32_t ls_code = data_linestatus[i];
            int group_idx = (rf_code << 1) | ls_code;  // Optimize: shift instead of multiply

            // Prefetch aggregation row for better cache locality
            AggregateRow& agg_row = local_agg[group_idx];

            // Load all needed values for this row upfront
            int64_t qty = data_quantity[i];
            int64_t price = data_extendedprice[i];
            int64_t disc = data_discount[i];
            int64_t tax = data_tax[i];

            // Pre-compute discount factors
            int64_t disc_factor = 100 - disc;  // (100 - disc)
            int64_t tax_factor = 100 + tax;    // (100 + tax)

            // Aggregations: all computed together for better instruction-level parallelism
            agg_row.sum_qty += qty;
            agg_row.sum_base_price += price;
            agg_row.sum_disc_price += price * disc_factor;
            agg_row.sum_charge += price * disc_factor * tax_factor;
            agg_row.sum_discount += disc;
            agg_row.count_order += 1;
        }

        // Store thread-local filtered row count (no atomic needed)
        filtered_rows_per_thread[thread_id] = local_filtered_rows;

        // Merge local aggregation into global aggregation (with critical section)
        #pragma omp critical
        {
            for (int g = 0; g < 6; ++g) {
                agg[g].sum_qty += local_agg[g].sum_qty;
                agg[g].sum_base_price += local_agg[g].sum_base_price;
                agg[g].sum_disc_price += local_agg[g].sum_disc_price;
                agg[g].sum_charge += local_agg[g].sum_charge;
                agg[g].sum_discount += local_agg[g].sum_discount;
                agg[g].count_order += local_agg[g].count_order;
            }
        }
    }  // implicit barrier at end of parallel block

    // Aggregate filtered row counts from all threads
    uint64_t filtered_rows = 0;
    for (uint64_t count : filtered_rows_per_thread) {
        filtered_rows += count;
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);
    printf("[ROWS] filtered_rows: %lu\n", filtered_rows);
    #endif

    // ===== PREPARE OUTPUT (sorted by returnflag, linestatus) =====
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    // Map from group_idx to (returnflag, linestatus)
    std::vector<std::tuple<int32_t, int32_t, int>> group_order;  // (rf_code, ls_code, group_idx)

    for (int i = 0; i < 6; ++i) {
        if (agg[i].count_order == 0) continue;
        int rf_code = i / 2;
        int ls_code = i % 2;
        group_order.push_back({rf_code, ls_code, i});
    }

    // Sort by returnflag then linestatus
    std::sort(group_order.begin(), group_order.end(),
        [&returnflag_dict, &linestatus_dict](const auto& a, const auto& b) {
            char a_rf = returnflag_dict[std::get<0>(a)];
            char b_rf = returnflag_dict[std::get<0>(b)];
            if (a_rf != b_rf) return a_rf < b_rf;
            char a_ls = linestatus_dict[std::get<1>(a)];
            char b_ls = linestatus_dict[std::get<1>(b)];
            return a_ls < b_ls;
        });

    // Write CSV
    std::ofstream out_file(results_dir + "/Q1.csv");
    out_file << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\r\n";

    for (const auto& [rf_code, ls_code, group_idx] : group_order) {
        AggregateRow& row = agg[group_idx];
        char rf_char = returnflag_dict[rf_code];
        char ls_char = linestatus_dict[ls_code];

        // Compute averages and unscale (using long double for precision)
        // sum_qty: scale 100 → divide by 100 and count
        long double avg_qty = static_cast<long double>(row.sum_qty) / row.count_order / 100.0L;

        // sum_base_price: scale 100 → divide by count and scale
        long double avg_price = static_cast<long double>(row.sum_base_price) / row.count_order / 100.0L;

        // sum_discount: scale 100 → divide by count and scale
        long double avg_disc = static_cast<long double>(row.sum_discount) / row.count_order / 100.0L;

        // Unscale sums (using long double for precision):
        // sum_qty: divide by 100
        long double sum_qty_val = static_cast<long double>(row.sum_qty) / 100.0L;

        // sum_base_price: divide by 100
        long double sum_base_price_val = static_cast<long double>(row.sum_base_price) / 100.0L;

        // sum_disc_price: divide by 10000
        long double sum_disc_price_val = static_cast<long double>(row.sum_disc_price) / 10000.0L;

        // sum_charge: divide by 1000000
        long double sum_charge_val = static_cast<long double>(row.sum_charge) / 1000000.0L;

        // Format: 2 decimal places
        char buffer[1024];
        snprintf(buffer, sizeof(buffer),
            "%c,%c,%.2Lf,%.2Lf,%.4Lf,%.6Lf,%.2Lf,%.2Lf,%.2Lf,%ld\r\n",
            rf_char, ls_char,
            sum_qty_val, sum_base_price_val, sum_disc_price_val, sum_charge_val,
            avg_qty, avg_price, avg_disc,
            row.count_order);
        out_file << buffer;
    }

    out_file.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    // ===== CLEANUP =====
    munmap(data_shipdate, size_shipdate);
    munmap(data_returnflag, size_returnflag);
    munmap(data_linestatus, size_linestatus);
    munmap(data_quantity, size_quantity);
    munmap(data_extendedprice, size_extendedprice);
    munmap(data_discount, size_discount);
    munmap(data_tax, size_tax);

    close(fd_shipdate);
    close(fd_returnflag);
    close(fd_linestatus);
    close(fd_quantity);
    close(fd_extendedprice);
    close(fd_discount);
    close(fd_tax);

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
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
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
