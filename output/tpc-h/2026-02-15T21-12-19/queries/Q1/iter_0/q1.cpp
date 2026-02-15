#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <omp.h>

// ============================================================================
// METADATA CHECK: Q1 Column Encodings
// ============================================================================
// l_quantity:       int64_t, DECIMAL, scale_factor=100
// l_extendedprice:  int64_t, DECIMAL, scale_factor=100
// l_discount:       int64_t, DECIMAL, scale_factor=100
// l_tax:            int64_t, DECIMAL, scale_factor=100
// l_returnflag:     int32_t, STRING (dictionary-encoded)
// l_linestatus:     int32_t, STRING (dictionary-encoded)
// l_shipdate:       int32_t, DATE (days since epoch)
//
// Dictionary format: "code=value\n" lines in _dict.txt
// Date formula: epoch days = (years_from_1970 * 365) + leap_days + month_days + (day - 1)
// ============================================================================

struct AggregateState {
    int64_t sum_quantity = 0;
    int64_t sum_extendedprice = 0;
    int64_t sum_disc_price = 0;       // SUM(l_extendedprice * (1 - l_discount)) - accumulated at full precision
    int64_t sum_charge = 0;            // SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) - accumulated at full precision
    int64_t count_rows = 0;

    // For averages (accumulated scaled values)
    int64_t sum_all_quantity = 0;
    int64_t sum_all_extendedprice = 0;
    int64_t sum_all_discount = 0;
};

// Result row for output
struct ResultRow {
    char returnflag;
    char linestatus;
    int64_t sum_qty;
    int64_t sum_base_price;
    int64_t sum_disc_price;
    int64_t sum_charge;
    int64_t avg_qty;
    int64_t avg_price;
    int64_t avg_disc;
    int64_t count_order;
};

// ============================================================================
// Helper: Load and parse dictionary file
// ============================================================================
std::unordered_map<int32_t, char> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, char> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        throw std::runtime_error("Cannot open dictionary: " + dict_path);
    }

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            char value = line[eq_pos + 1];
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// ============================================================================
// Helper: Load mmap'd column
// ============================================================================
template <typename T>
const T* load_column(const std::string& path, size_t& out_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("Cannot open column file: " + path);
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        throw std::runtime_error("Cannot stat column file: " + path);
    }

    size_t file_size = st.st_size;
    out_count = file_size / sizeof(T);

    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Cannot mmap column file: " + path);
    }

    close(fd);
    return (const T*)addr;
}

// ============================================================================
// Helper: Convert epoch days to YYYY-MM-DD string
// ============================================================================
std::string format_date(int32_t epoch_days) {
    // Calculate year, month, day from days since 1970-01-01
    int year = 1970;
    int days_left = epoch_days;

    // Subtract complete years
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days_left < days_in_year) break;
        days_left -= days_in_year;
        year++;
    }

    // Subtract complete months
    int month = 1;
    int days_in_months[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_months[2] = 29;
    }

    while (days_left >= days_in_months[month]) {
        days_left -= days_in_months[month];
        month++;
    }

    int day = days_left + 1;  // days are 1-indexed

    char buf[16];
    snprintf(buf, 16, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// ============================================================================
// Compute the date threshold: DATE '1998-12-01' - INTERVAL '90' DAY
// ============================================================================
int32_t compute_date_threshold() {
    // Date '1998-12-01' = epoch days
    // From 1970-01-01 to 1998-12-01:
    // Years 1970-1997 (28 years): 28*365 = 10220, plus leap years (7): 10220+7 = 10227
    // From Jan 1 to Dec 1 in 1998: Jan(31) + Feb(28) + Mar(31) + Apr(30) + May(31) +
    //                              Jun(30) + Jul(31) + Aug(31) + Sep(30) + Oct(31) + Nov(30) = 334
    // Total: 10227 + 334 = 10561

    int32_t threshold_date = 10561;
    int32_t interval_days = 90;
    return threshold_date - interval_days;  // 10471
}

// ============================================================================
// Load zone map and use it for filtering (optional optimization)
// ============================================================================
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t count;
};

// ============================================================================
// Main query execution
// ============================================================================
void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    std::string lineitem_dir = gendb_dir + "/lineitem";

    // ========================================================================
    // 1. Load dictionaries
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_dict_start = std::chrono::high_resolution_clock::now();
#endif

    auto returnflag_dict = load_dictionary(lineitem_dir + "/l_returnflag_dict.txt");
    auto linestatus_dict = load_dictionary(lineitem_dir + "/l_linestatus_dict.txt");

#ifdef GENDB_PROFILE
    auto t_dict_end = std::chrono::high_resolution_clock::now();
    double dict_ms = std::chrono::duration<double, std::milli>(t_dict_end - t_dict_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", dict_ms);
#endif

    // ========================================================================
    // 2. Load columns via mmap
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t row_count = 0;
    const int64_t* l_quantity = load_column<int64_t>(lineitem_dir + "/l_quantity.bin", row_count);
    const int64_t* l_extendedprice = load_column<int64_t>(lineitem_dir + "/l_extendedprice.bin", row_count);
    const int64_t* l_discount = load_column<int64_t>(lineitem_dir + "/l_discount.bin", row_count);
    const int64_t* l_tax = load_column<int64_t>(lineitem_dir + "/l_tax.bin", row_count);
    const int32_t* l_returnflag = load_column<int32_t>(lineitem_dir + "/l_returnflag.bin", row_count);
    const int32_t* l_linestatus = load_column<int32_t>(lineitem_dir + "/l_linestatus.bin", row_count);
    const int32_t* l_shipdate = load_column<int32_t>(lineitem_dir + "/l_shipdate.bin", row_count);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_columns: %.2f ms\n", load_ms);
#endif

    // ========================================================================
    // 3. Compute date threshold and prepare for filtering/aggregation
    // ========================================================================
    int32_t date_threshold = compute_date_threshold();

    // Group key is (returnflag, linestatus) -> stored as (int32_t code, int32_t code)
    // We'll use a map to aggregate by group key
    std::unordered_map<uint64_t, AggregateState> groups;

    // Helper to create a combined key from two int32_t codes
    auto make_group_key = [](int32_t rf, int32_t ls) -> uint64_t {
        return ((uint64_t)rf << 32) | (uint32_t)ls;
    };

    // ========================================================================
    // 4. Main scan-filter-aggregate loop with threading
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Thread-local aggregation: each thread has its own map
    std::vector<std::unordered_map<uint64_t, AggregateState>> thread_groups(omp_get_max_threads());

    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < row_count; ++i) {
        // Filter: l_shipdate <= date_threshold
        if (l_shipdate[i] > date_threshold) {
            continue;
        }

        int32_t rf_code = l_returnflag[i];
        int32_t ls_code = l_linestatus[i];

        uint64_t group_key = make_group_key(rf_code, ls_code);

        int thread_id = omp_get_thread_num();
        AggregateState& state = thread_groups[thread_id][group_key];

        // Accumulate aggregates
        state.sum_quantity += l_quantity[i];
        state.sum_extendedprice += l_extendedprice[i];

        // All columns are scaled by 100 (e.g., 0.05 is stored as 5)
        // To maintain precision, accumulate at full scale (scale^2 = 10000)
        // SUM(l_extendedprice * (1 - l_discount))
        // Math: extendedprice_unscaled * (1 - discount_unscaled)
        // = (extendedprice_scaled/100) * (1 - discount_scaled/100)
        // = extendedprice_scaled * (100 - discount_scaled) / 10000
        // Accumulate: (extendedprice_scaled * (100 - discount_scaled)) at scale 10000
        state.sum_disc_price += l_extendedprice[i] * (100 - l_discount[i]);

        // SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))
        // = (extendedprice_scaled/100) * (1 - discount_scaled/100) * (1 + tax_scaled/100)
        // = extendedprice_scaled * (100 - discount_scaled) * (100 + tax_scaled) / 1000000
        // Accumulate: (extendedprice_scaled * (100 - discount_scaled) * (100 + tax_scaled)) at scale 1000000
        state.sum_charge += l_extendedprice[i] * (100 - l_discount[i]) * (100 + l_tax[i]);

        state.count_rows++;
        state.sum_all_quantity += l_quantity[i];
        state.sum_all_extendedprice += l_extendedprice[i];
        state.sum_all_discount += l_discount[i];
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", scan_ms);
#endif

    // ========================================================================
    // 5. Merge thread-local results
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<uint64_t, AggregateState> global_groups;
    for (int t = 0; t < (int)thread_groups.size(); ++t) {
        for (auto& [key, state] : thread_groups[t]) {
            auto& global_state = global_groups[key];
            global_state.sum_quantity += state.sum_quantity;
            global_state.sum_extendedprice += state.sum_extendedprice;
            global_state.sum_disc_price += state.sum_disc_price;
            global_state.sum_charge += state.sum_charge;
            global_state.count_rows += state.count_rows;
            global_state.sum_all_quantity += state.sum_all_quantity;
            global_state.sum_all_extendedprice += state.sum_all_extendedprice;
            global_state.sum_all_discount += state.sum_all_discount;
        }
    }

#ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge_aggregates: %.2f ms\n", merge_ms);
#endif

    // ========================================================================
    // 6. Build result rows and sort
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_format_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    for (auto& [key, state] : global_groups) {
        int32_t rf_code = (int32_t)(key >> 32);
        int32_t ls_code = (int32_t)(key & 0xFFFFFFFF);

        char rf_char = returnflag_dict.at(rf_code);
        char ls_char = linestatus_dict.at(ls_code);

        // Compute averages: divide accumulated scaled values by count
        int64_t avg_qty = (state.sum_all_quantity + state.count_rows / 2) / state.count_rows;
        int64_t avg_price = (state.sum_all_extendedprice + state.count_rows / 2) / state.count_rows;
        int64_t avg_disc = (state.sum_all_discount + state.count_rows / 2) / state.count_rows;

        results.push_back({
            rf_char,
            ls_char,
            state.sum_quantity,
            state.sum_extendedprice,
            state.sum_disc_price,
            state.sum_charge,
            avg_qty,
            avg_price,
            avg_disc,
            state.count_rows
        });
    }

    // Sort by returnflag, then linestatus
    std::sort(results.begin(), results.end(),
        [](const ResultRow& a, const ResultRow& b) {
            if (a.returnflag != b.returnflag) {
                return a.returnflag < b.returnflag;
            }
            return a.linestatus < b.linestatus;
        });

#ifdef GENDB_PROFILE
    auto t_format_end = std::chrono::high_resolution_clock::now();
    double format_ms = std::chrono::duration<double, std::milli>(t_format_end - t_format_start).count();
    printf("[TIMING] format_results: %.2f ms\n", format_ms);
#endif

    // ========================================================================
    // 7. Write results to CSV
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        throw std::runtime_error("Cannot open output file: " + output_path);
    }

    // Header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,"
        << "avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows - convert scaled integers to decimal strings with appropriate precision
    for (const auto& row : results) {
        // sum_disc_price: accumulated at scale 10000, needs 4 decimal places
        // sum_charge: accumulated at scale 1000000, needs 6 decimal places
        out << row.returnflag << ","
            << row.linestatus << ","
            << std::fixed << std::setprecision(2)
            << (double)row.sum_qty / 100.0 << ","
            << (double)row.sum_base_price / 100.0 << ",";

        // sum_disc_price with 4 decimal places
        out << std::fixed << std::setprecision(4)
            << (double)row.sum_disc_price / 10000.0 << ",";

        // sum_charge with 6 decimal places
        out << std::fixed << std::setprecision(6)
            << (double)row.sum_charge / 1000000.0 << ",";

        // avg values with 2 decimal places
        out << std::fixed << std::setprecision(2)
            << (double)row.avg_qty / 100.0 << ","
            << (double)row.avg_price / 100.0 << ","
            << (double)row.avg_disc / 100.0 << ","
            << row.count_order << "\n";
    }

    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
#endif

    // ========================================================================
    // 8. Total timing
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif

    std::cout << "Q1 results written to " << output_path << std::endl;
    std::cout << "Processed " << row_count << " rows, " << global_groups.size() << " groups" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    try {
        run_Q1(gendb_dir, results_dir);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#endif
