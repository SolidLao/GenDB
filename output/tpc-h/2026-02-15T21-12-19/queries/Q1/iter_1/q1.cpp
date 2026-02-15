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

    AggregateState& operator+=(const AggregateState& other) {
        sum_quantity += other.sum_quantity;
        sum_extendedprice += other.sum_extendedprice;
        sum_disc_price += other.sum_disc_price;
        sum_charge += other.sum_charge;
        count_rows += other.count_rows;
        sum_all_quantity += other.sum_all_quantity;
        sum_all_extendedprice += other.sum_all_extendedprice;
        sum_all_discount += other.sum_all_discount;
        return *this;
    }
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

// Load zone map from file if available
const ZoneMapEntry* load_zonemap(const std::string& path, size_t& out_num_zones) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        out_num_zones = 0;
        return nullptr;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        out_num_zones = 0;
        return nullptr;
    }

    size_t file_size = st.st_size;

    // Read num_zones (first uint32_t)
    uint32_t num_zones;
    if (read(fd, &num_zones, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        out_num_zones = 0;
        return nullptr;
    }

    // Expected size: 4 (num_zones) + num_zones * (4 + 4 + 4)
    size_t expected_size = 4 + num_zones * 12;
    if (file_size < expected_size) {
        close(fd);
        out_num_zones = 0;
        return nullptr;
    }

    // Reset to beginning for mmap
    lseek(fd, 0, SEEK_SET);
    void* addr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        out_num_zones = 0;
        return nullptr;
    }

    close(fd);

    // Skip the header uint32_t, return pointer to first zone entry
    out_num_zones = num_zones;
    return (const ZoneMapEntry*)((char*)addr + 4);
}

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

    // Load zone map for block pruning (if available)
    size_t num_zones = 0;
    const ZoneMapEntry* zones = load_zonemap(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", num_zones);

    // ========================================================================
    // 4. Main scan-filter-aggregate loop with threading
    // ========================================================================
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // For low-cardinality GROUP BY (only 6 possible groups: 3 returnflags * 2 linestatuses),
    // use flat array aggregation instead of hash table. This is much faster than hash table lookups.
    // Thread-local aggregation: each thread has its own 6-group flat array
    // We'll map (rf_code, ls_code) to array indices 0-5
    struct FlatGroupAggregates {
        AggregateState groups[10];  // Index by rf_code*3 + ls_code (max 3*3=9, but we use 10 for safety)
    };

    std::vector<FlatGroupAggregates> thread_aggregates(omp_get_max_threads());

    // Clear all thread aggregates
    for (auto& ta : thread_aggregates) {
        for (int i = 0; i < 10; i++) {
            ta.groups[i] = AggregateState();
        }
    }

    // Process blocks (either via zone map or entire table)
    if (zones && num_zones > 0) {
        // Zone map-based processing
        #pragma omp parallel for schedule(dynamic) collapse(1)
        for (size_t z = 0; z < num_zones; z++) {
            const ZoneMapEntry& zone = zones[z];

            // Zone map pruning: skip if entire block is outside date range
            if (zone.min_val > date_threshold) {
                continue;  // All rows in this block are after threshold
            }

            int thread_id = omp_get_thread_num();
            FlatGroupAggregates& agg = thread_aggregates[thread_id];

            // Process rows in this zone
            // Determine start/end row indices based on zone number
            uint32_t zone_start = z * 100000;  // Block size is 100,000
            uint32_t zone_end = std::min(zone_start + 100000u, (uint32_t)row_count);

            for (uint32_t i = zone_start; i < zone_end; i++) {
                // Filter: l_shipdate <= date_threshold
                if (l_shipdate[i] > date_threshold) {
                    continue;
                }

                int32_t rf_code = l_returnflag[i];
                int32_t ls_code = l_linestatus[i];

                // Map to flat array index
                int idx = rf_code * 3 + ls_code;
                if (idx >= 0 && idx < 10) {
                    AggregateState& state = agg.groups[idx];

                    state.sum_quantity += l_quantity[i];
                    state.sum_extendedprice += l_extendedprice[i];
                    state.sum_disc_price += l_extendedprice[i] * (100 - l_discount[i]);
                    state.sum_charge += l_extendedprice[i] * (100 - l_discount[i]) * (100 + l_tax[i]);
                    state.count_rows++;
                    state.sum_all_quantity += l_quantity[i];
                    state.sum_all_extendedprice += l_extendedprice[i];
                    state.sum_all_discount += l_discount[i];
                }
            }
        }
    } else {
        // No zone map: scan entire table with parallelization
        #pragma omp parallel for schedule(dynamic, 100000)
        for (size_t i = 0; i < row_count; ++i) {
            // Filter: l_shipdate <= date_threshold
            if (l_shipdate[i] > date_threshold) {
                continue;
            }

            int32_t rf_code = l_returnflag[i];
            int32_t ls_code = l_linestatus[i];

            // Map to flat array index
            int idx = rf_code * 3 + ls_code;
            if (idx >= 0 && idx < 10) {
                int thread_id = omp_get_thread_num();
                AggregateState& state = thread_aggregates[thread_id].groups[idx];

                state.sum_quantity += l_quantity[i];
                state.sum_extendedprice += l_extendedprice[i];
                state.sum_disc_price += l_extendedprice[i] * (100 - l_discount[i]);
                state.sum_charge += l_extendedprice[i] * (100 - l_discount[i]) * (100 + l_tax[i]);
                state.count_rows++;
                state.sum_all_quantity += l_quantity[i];
                state.sum_all_extendedprice += l_extendedprice[i];
                state.sum_all_discount += l_discount[i];
            }
        }
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

    // Merge thread-local flat arrays into global
    AggregateState global_groups[10];
    for (int i = 0; i < 10; i++) {
        global_groups[i] = AggregateState();
    }

    for (const auto& thread_agg : thread_aggregates) {
        for (int i = 0; i < 10; i++) {
            global_groups[i] += thread_agg.groups[i];
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

    // Iterate through flat array of groups
    for (int idx = 0; idx < 10; idx++) {
        const AggregateState& state = global_groups[idx];

        // Skip empty groups
        if (state.count_rows == 0) {
            continue;
        }

        // Decode (rf_code, ls_code) from index
        int32_t rf_code = idx / 3;
        int32_t ls_code = idx % 3;

        // Look up dictionary values
        auto rf_it = returnflag_dict.find(rf_code);
        auto ls_it = linestatus_dict.find(ls_code);

        if (rf_it == returnflag_dict.end() || ls_it == linestatus_dict.end()) {
            continue;  // Skip if codes not in dictionary
        }

        char rf_char = rf_it->second;
        char ls_char = ls_it->second;

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

    // Sort by returnflag, then linestatus (should already be sorted, but keep for correctness)
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
    std::cout << "Processed " << row_count << " rows, " << results.size() << " groups" << std::endl;
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
