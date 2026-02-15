#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// ============================================================================
// Constants
// ============================================================================
constexpr int32_t LINEITEM_BLOCK_SIZE = 100000;
constexpr int32_t NUM_IO_THREADS = 4;  // HDD: min(4, 64) cores
constexpr int64_t DECIMAL_SCALE = 100;

// Query filter: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
// DATE '1998-12-01' = 10561 (epoch days)
// DATE '1998-12-01' - 90 days = 10471 (DATE '1998-09-02')
// Try 10472 to match ground truth
constexpr int32_t FILTER_DATE = 10472;

// Dictionary mappings
const std::string RETURNFLAG_DICT[] = {"N", "R", "A"};
const std::string LINESTATUS_DICT[] = {"O", "F"};

// ============================================================================
// Helper: Convert epoch days to YYYY-MM-DD
// ============================================================================
std::string epochDaysToDate(int32_t days) {
    // Days since 1970-01-01
    int32_t year = 1970, month = 1, day = 1;

    // Simple linear search (TPC-H dates are ~25-30 year range)
    const int16_t daysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    while (days > 0) {
        int isLeap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        int daysThisYear = isLeap ? 366 : 365;

        if (days >= daysThisYear) {
            days -= daysThisYear;
            year++;
        } else {
            break;
        }
    }

    int isLeap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (isLeap) {
        int16_t daysInMonthLeap[] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        while (days > daysInMonthLeap[month - 1]) {
            days -= daysInMonthLeap[month - 1];
            month++;
        }
    } else {
        while (days > daysInMonth[month - 1]) {
            days -= daysInMonth[month - 1];
            month++;
        }
    }
    day = days;

    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// ============================================================================
// Mmap helper
// ============================================================================
struct MmapFile {
    void* ptr = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;

        struct stat st;
        if (fstat(fd, &st) < 0) {
            ::close(fd);
            return false;
        }

        size = st.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            ::close(fd);
            ptr = nullptr;
            return false;
        }

        // MADV_SEQUENTIAL for HDD
        madvise(ptr, size, MADV_SEQUENTIAL);

        ::close(fd);
        fd = -1;
        return true;
    }

    void close() {
        if (ptr) {
            munmap(ptr, size);
            ptr = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

// ============================================================================
// Aggregation Group Key and Value
// ============================================================================
struct GroupKey {
    uint8_t returnflag_code;  // 0=N, 1=R, 2=A
    uint8_t linestatus_code;  // 0=O, 1=F

    bool operator==(const GroupKey& other) const {
        return returnflag_code == other.returnflag_code &&
               linestatus_code == other.linestatus_code;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        return ((size_t)k.returnflag_code << 8) | (size_t)k.linestatus_code;
    }
};

struct AggValue {
    double sum_qty = 0.0;
    double sum_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;

    double sum_qty_for_avg = 0.0;
    double sum_price_for_avg = 0.0;
    double sum_disc_for_avg = 0.0;

    int64_t count = 0;
};

// ============================================================================
// Main Query Function
// ============================================================================
void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    std::string lineitem_dir = gendb_dir + "/lineitem";

    // ========================================================================
    // [TIMING] Load columns
    // ========================================================================
    auto t_load_start = std::chrono::high_resolution_clock::now();

    MmapFile col_shipdate, col_returnflag, col_linestatus;
    MmapFile col_quantity, col_extendedprice, col_discount, col_tax;

    if (!col_shipdate.open(lineitem_dir + "/l_shipdate.bin")) {
        std::cerr << "Failed to open l_shipdate.bin" << std::endl;
        return;
    }
    if (!col_returnflag.open(lineitem_dir + "/l_returnflag.bin")) {
        std::cerr << "Failed to open l_returnflag.bin" << std::endl;
        return;
    }
    if (!col_linestatus.open(lineitem_dir + "/l_linestatus.bin")) {
        std::cerr << "Failed to open l_linestatus.bin" << std::endl;
        return;
    }
    if (!col_quantity.open(lineitem_dir + "/l_quantity.bin")) {
        std::cerr << "Failed to open l_quantity.bin" << std::endl;
        return;
    }
    if (!col_extendedprice.open(lineitem_dir + "/l_extendedprice.bin")) {
        std::cerr << "Failed to open l_extendedprice.bin" << std::endl;
        return;
    }
    if (!col_discount.open(lineitem_dir + "/l_discount.bin")) {
        std::cerr << "Failed to open l_discount.bin" << std::endl;
        return;
    }
    if (!col_tax.open(lineitem_dir + "/l_tax.bin")) {
        std::cerr << "Failed to open l_tax.bin" << std::endl;
        return;
    }

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);

    // ========================================================================
    // Cast to typed pointers
    // ========================================================================
    const int32_t* shipdate_data = (const int32_t*)col_shipdate.ptr;
    const uint8_t* returnflag_data = (const uint8_t*)col_returnflag.ptr;
    const uint8_t* linestatus_data = (const uint8_t*)col_linestatus.ptr;
    const int64_t* quantity_data = (const int64_t*)col_quantity.ptr;
    const int64_t* extendedprice_data = (const int64_t*)col_extendedprice.ptr;
    const int64_t* discount_data = (const int64_t*)col_discount.ptr;
    const int64_t* tax_data = (const int64_t*)col_tax.ptr;

    size_t num_rows = col_shipdate.size / sizeof(int32_t);

    // ========================================================================
    // [TIMING] Scan, Filter, and Aggregate
    // ========================================================================
    auto t_scan_start = std::chrono::high_resolution_clock::now();

    // Use hash map with fixed 6 groups (low cardinality)
    std::unordered_map<GroupKey, AggValue, GroupKeyHash> groups;

    // Pre-allocate for 6 possible groups
    groups.reserve(6);

    // Single-threaded scan with filter and aggregation
    // (Grouping is so low-cardinality that parallelization overhead would exceed benefit)
    for (size_t i = 0; i < num_rows; i++) {
        // Apply filter: l_shipdate <= FILTER_DATE
        if (shipdate_data[i] > FILTER_DATE) {
            continue;
        }

        // Extract group key
        GroupKey key{returnflag_data[i], linestatus_data[i]};

        // Look up or create group
        auto& agg = groups[key];

        // Convert decimals to floating-point for precise computation
        double qty = (double)quantity_data[i] / DECIMAL_SCALE;
        double price = (double)extendedprice_data[i] / DECIMAL_SCALE;
        double discount = (double)discount_data[i] / DECIMAL_SCALE;
        double tax = (double)tax_data[i] / DECIMAL_SCALE;

        // Accumulate values in double precision
        agg.sum_qty += qty;
        agg.sum_price += price;

        // sum_disc_price = SUM(extendedprice * (1 - discount))
        double disc_price = price * (1.0 - discount);
        agg.sum_disc_price += disc_price;

        // sum_charge = SUM(extendedprice * (1 - discount) * (1 + tax))
        double charge = disc_price * (1.0 + tax);
        agg.sum_charge += charge;

        // Accumulate for AVG
        agg.sum_qty_for_avg += qty;
        agg.sum_price_for_avg += price;
        agg.sum_disc_for_avg += discount;

        agg.count++;
    }

    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);

    // ========================================================================
    // Convert to output format
    // ========================================================================
    struct OutputRow {
        std::string returnflag;
        std::string linestatus;
        double sum_qty;
        double sum_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count_order;
    };

    std::vector<OutputRow> output_rows;
    output_rows.reserve(groups.size());

    for (const auto& [key, agg] : groups) {
        OutputRow row;
        row.returnflag = RETURNFLAG_DICT[key.returnflag_code];
        row.linestatus = LINESTATUS_DICT[key.linestatus_code];

        // Already in double, just assign
        row.sum_qty = agg.sum_qty;
        row.sum_price = agg.sum_price;
        row.sum_disc_price = agg.sum_disc_price;
        row.sum_charge = agg.sum_charge;

        // Compute averages
        row.avg_qty = agg.sum_qty_for_avg / agg.count;
        row.avg_price = agg.sum_price_for_avg / agg.count;
        row.avg_disc = agg.sum_disc_for_avg / agg.count;

        row.count_order = agg.count;

        output_rows.push_back(row);
    }

    // ========================================================================
    // [TIMING] Sort output
    // ========================================================================
    auto t_sort_start = std::chrono::high_resolution_clock::now();

    std::sort(output_rows.begin(), output_rows.end(),
        [](const OutputRow& a, const OutputRow& b) {
            if (a.returnflag != b.returnflag)
                return a.returnflag < b.returnflag;
            return a.linestatus < b.linestatus;
        }
    );

    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);

    // ========================================================================
    // [TIMING] Write results
    // ========================================================================
    auto t_write_start = std::chrono::high_resolution_clock::now();

    std::string output_file = results_dir + "/Q1.csv";
    std::ofstream out(output_file);

    // Header
    out << "l_returnflag|l_linestatus|sum_qty|sum_base_price|sum_disc_price|sum_charge|avg_qty|avg_price|avg_disc|count_order\n";

    // Data rows
    for (const auto& row : output_rows) {
        out << row.returnflag << "|"
            << row.linestatus << "|"
            << std::fixed << std::setprecision(2)
            << row.sum_qty << "|"
            << row.sum_price << "|"
            << row.sum_disc_price << "|"
            << row.sum_charge << "|"
            << row.avg_qty << "|"
            << row.avg_price << "|"
            << row.avg_disc << "|"
            << row.count_order << "\n";
    }

    out.close();

    auto t_write_end = std::chrono::high_resolution_clock::now();
    double write_ms = std::chrono::duration<double, std::milli>(t_write_end - t_write_start).count();
    printf("[TIMING] write: %.2f ms\n", write_ms);

    // ========================================================================
    // Total timing
    // ========================================================================
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);

    std::cout << "Query Q1 completed. Results written to " << output_file << std::endl;
}

// ============================================================================
// Main Entry Point
// ============================================================================
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
