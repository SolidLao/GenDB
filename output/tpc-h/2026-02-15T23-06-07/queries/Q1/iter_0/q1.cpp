#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <unordered_map>
#include <algorithm>
#include <iomanip>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// [METADATA CHECK]
// Query: Q1 (Pricing Summary Report)
// Tables: lineitem (59,986,052 rows)
// Columns needed:
//   - l_shipdate: int32_t (DATE, days since epoch)
//   - l_returnflag: int32_t (dictionary-encoded STRING)
//   - l_linestatus: int32_t (dictionary-encoded STRING)
//   - l_quantity: int64_t (DECIMAL, scale_factor=100)
//   - l_extendedprice: int64_t (DECIMAL, scale_factor=100)
//   - l_discount: int64_t (DECIMAL, scale_factor=100)
//   - l_tax: int64_t (DECIMAL, scale_factor=100)
// Predicate: l_shipdate <= '1998-12-01' - 90 days = '1998-09-02' (epoch day 10471)
// Aggregation: GROUP BY (l_returnflag, l_linestatus) - ~6 groups expected

// Helper: Calculate epoch days from YYYY-MM-DD
constexpr int32_t date_to_epoch(int year, int month, int day) {
    int32_t days = 0;
    // Days from complete years (1970 to year-1)
    for (int y = 1970; y < year; ++y) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }
    // Days from complete months (1 to month-1) in target year
    const int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for (int m = 1; m < month; ++m) {
        days += month_days[m - 1];
        if (m == 2 && leap) days += 1;
    }
    // Add day of month (1-indexed, so subtract 1)
    days += (day - 1);
    return days;
}

// Aggregation state for each group
struct AggState {
    int64_t sum_qty_scaled = 0;           // scale = 100
    int64_t sum_base_price_scaled = 0;    // scale = 100
    __int128 sum_disc_price_scaled2 = 0;  // scale = 100^2 = 10000
    __int128 sum_charge_scaled3 = 0;      // scale = 100^3 = 1000000
    int64_t sum_discount_scaled = 0;      // scale = 100
    int64_t count = 0;
};

// Result row for output
struct ResultRow {
    std::string returnflag;
    std::string linestatus;
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double avg_qty;
    double avg_price;
    double avg_disc;
    int64_t count_order;
};

// Memory-mapped file wrapper
template<typename T>
class MmapFile {
public:
    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            exit(1);
        }
        size = sb.st_size / sizeof(T);
        data = static_cast<const T*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }
    }

    ~MmapFile() {
        if (data != MAP_FAILED) munmap((void*)data, size * sizeof(T));
        if (fd >= 0) close(fd);
    }

    const T* data;
    size_t size;
    int fd;
};

// Load dictionary from _dict.txt
std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        dict.push_back(line);
    }
    return dict;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    std::cout << "[METADATA CHECK] Q1 - Pricing Summary Report" << std::endl;
    std::cout << "  Table: lineitem (59,986,052 rows)" << std::endl;
    std::cout << "  Encodings:" << std::endl;
    std::cout << "    l_shipdate: int32_t (DATE, days since epoch)" << std::endl;
    std::cout << "    l_returnflag: int32_t (dictionary-encoded STRING)" << std::endl;
    std::cout << "    l_linestatus: int32_t (dictionary-encoded STRING)" << std::endl;
    std::cout << "    l_quantity: int64_t (DECIMAL, scale=100)" << std::endl;
    std::cout << "    l_extendedprice: int64_t (DECIMAL, scale=100)" << std::endl;
    std::cout << "    l_discount: int64_t (DECIMAL, scale=100)" << std::endl;
    std::cout << "    l_tax: int64_t (DECIMAL, scale=100)" << std::endl;

    // Compute predicate threshold: '1998-12-01' - 90 days = '1998-09-02'
    const int32_t threshold_date = date_to_epoch(1998, 9, 2);
    std::cout << "  Predicate: l_shipdate <= " << threshold_date << " (1998-09-02)" << std::endl;

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // Load data
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    std::string lineitem_dir = gendb_dir + "/lineitem/";
    MmapFile<int32_t> l_shipdate(lineitem_dir + "l_shipdate.bin");
    MmapFile<int32_t> l_returnflag(lineitem_dir + "l_returnflag.bin");
    MmapFile<int32_t> l_linestatus(lineitem_dir + "l_linestatus.bin");
    MmapFile<int64_t> l_quantity(lineitem_dir + "l_quantity.bin");
    MmapFile<int64_t> l_extendedprice(lineitem_dir + "l_extendedprice.bin");
    MmapFile<int64_t> l_discount(lineitem_dir + "l_discount.bin");
    MmapFile<int64_t> l_tax(lineitem_dir + "l_tax.bin");

    auto returnflag_dict = load_dictionary(lineitem_dir + "l_returnflag_dict.txt");
    auto linestatus_dict = load_dictionary(lineitem_dir + "l_linestatus_dict.txt");

    size_t num_rows = l_shipdate.size;
    std::cout << "  Loaded " << num_rows << " rows" << std::endl;

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Scan, filter, and aggregate
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Use array-based aggregation for low cardinality (returnflag: 3, linestatus: 2 => 6 groups max)
    // We'll use a hash map keyed by (returnflag_code, linestatus_code) but since cardinality is tiny,
    // a flat array indexed by returnflag_code*10 + linestatus_code works well
    std::unordered_map<uint64_t, AggState> agg_map;

    for (size_t i = 0; i < num_rows; ++i) {
        int32_t shipdate = l_shipdate.data[i];

        // Filter: l_shipdate <= threshold_date
        if (shipdate > threshold_date) continue;

        int32_t rf = l_returnflag.data[i];
        int32_t ls = l_linestatus.data[i];
        int64_t qty = l_quantity.data[i];
        int64_t price = l_extendedprice.data[i];
        int64_t disc = l_discount.data[i];
        int64_t tax = l_tax.data[i];

        // Group key: combine returnflag and linestatus codes
        uint64_t key = (static_cast<uint64_t>(rf) << 32) | static_cast<uint64_t>(ls);

        AggState& state = agg_map[key];
        state.sum_qty_scaled += qty;
        state.sum_base_price_scaled += price;

        // sum_disc_price = sum(extendedprice * (1 - discount))
        // With scale=100: price * (100 - disc) / 100
        // To avoid integer division per row, accumulate at scale^2 and divide once
        __int128 disc_price = static_cast<__int128>(price) * (100 - disc);
        state.sum_disc_price_scaled2 += disc_price;

        // sum_charge = sum(extendedprice * (1 - discount) * (1 + tax))
        // = disc_price * (100 + tax) / 100
        // Accumulate at scale^3 to avoid per-row division
        __int128 charge = disc_price * (100 + tax);
        state.sum_charge_scaled3 += charge;

        state.sum_discount_scaled += disc;
        state.count += 1;
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);
#endif

    // Prepare results
#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    for (const auto& [key, state] : agg_map) {
        int32_t rf = static_cast<int32_t>(key >> 32);
        int32_t ls = static_cast<int32_t>(key & 0xFFFFFFFF);

        ResultRow row;
        row.returnflag = returnflag_dict[rf];
        row.linestatus = linestatus_dict[ls];
        row.sum_qty = state.sum_qty_scaled / 100.0;
        row.sum_base_price = state.sum_base_price_scaled / 100.0;

        // Scale down from scale^2 to scale^0
        row.sum_disc_price = static_cast<double>(state.sum_disc_price_scaled2) / 10000.0;

        // Scale down from scale^3 to scale^0
        row.sum_charge = static_cast<double>(state.sum_charge_scaled3) / 1000000.0;

        row.avg_qty = state.sum_qty_scaled / (100.0 * state.count);
        row.avg_price = state.sum_base_price_scaled / (100.0 * state.count);
        row.avg_disc = state.sum_discount_scaled / (100.0 * state.count);
        row.count_order = state.count;

        results.push_back(row);
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
#endif

    // Sort by returnflag, linestatus
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    // Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);

    // Header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows
    for (const auto& row : results) {
        out << row.returnflag << ","
            << row.linestatus << ","
            << std::fixed << std::setprecision(2) << row.sum_qty << ","
            << std::fixed << std::setprecision(2) << row.sum_base_price << ","
            << std::fixed << std::setprecision(4) << row.sum_disc_price << ","
            << std::fixed << std::setprecision(6) << row.sum_charge << ","
            << std::fixed << std::setprecision(2) << row.avg_qty << ","
            << std::fixed << std::setprecision(2) << row.avg_price << ","
            << std::fixed << std::setprecision(2) << row.avg_disc << ","
            << row.count_order << "\n";
    }

    out.close();
    std::cout << "Results written to " << output_path << std::endl;

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
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
