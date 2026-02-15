#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <cmath>
#include <omp.h>

// ============================================================================
// CONSTANTS AND HELPERS
// ============================================================================

const int32_t DATE_CUTOFF = 10472;  // 1998-09-03 (l_shipdate <= this)
                                   // Note: l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
const int SCALE_DECIMAL = 100;      // Decimal scale factor

// Struct to hold mmap'd data
template<typename T>
struct MmappedColumn {
    T* data = nullptr;
    size_t size = 0;
    int fd = -1;

    ~MmappedColumn() {
        if (data && size > 0) {
            munmap(data, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
};

// Load a binary column via mmap
template<typename T>
bool load_column(const std::string& path, MmappedColumn<T>& col) {
    col.fd = open(path.c_str(), O_RDONLY);
    if (col.fd < 0) {
        std::cerr << "[ERROR] Cannot open " << path << std::endl;
        return false;
    }

    struct stat st;
    if (fstat(col.fd, &st) < 0) {
        std::cerr << "[ERROR] Cannot stat " << path << std::endl;
        close(col.fd);
        col.fd = -1;
        return false;
    }

    col.size = st.st_size / sizeof(T);
    col.data = (T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, col.fd, 0);
    if (col.data == MAP_FAILED) {
        std::cerr << "[ERROR] Cannot mmap " << path << std::endl;
        close(col.fd);
        col.fd = -1;
        return false;
    }

    return true;
}

// Load dictionary for a dictionary-encoded column
// Format: binary byte (code) + '=' + character + newline
std::unordered_map<uint8_t, char> load_dict(const std::string& path) {
    std::unordered_map<uint8_t, char> dict;
    std::ifstream f(path, std::ios::binary);

    // Read entire file as binary
    f.seekg(0, std::ios::end);
    size_t size = f.tellg();
    f.seekg(0, std::ios::beg);

    std::vector<uint8_t> data(size);
    f.read((char*)data.data(), size);
    f.close();

    // Parse: code (1 byte) + '=' (1 byte) + value (1 byte) + '\n' (1 byte)
    for (size_t i = 0; i < data.size(); ) {
        if (i + 3 >= data.size()) break;
        uint8_t code = data[i];
        if (data[i + 1] != '=') {
            i += 4;  // Skip malformed entry
            continue;
        }
        char value = (char)data[i + 2];
        dict[code] = value;
        i += 4;  // Skip to next entry (code + '=' + value + '\n')
    }

    return dict;
}

// Aggregate state: packed 2-byte key (1 byte returnflag, 1 byte linestatus)
// and all sums/counts. Use double to maintain precision during aggregation.
struct AggState {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;  // For AVG computation
    int64_t count = 0;
};

// Create compound key from (returnflag_code, linestatus_code)
inline uint16_t make_key(uint8_t rf, uint8_t ls) {
    return ((uint16_t)rf << 8) | (uint16_t)ls;
}

// Extract codes from compound key
inline void extract_key(uint16_t key, uint8_t& rf, uint8_t& ls) {
    rf = (uint8_t)(key >> 8);
    ls = (uint8_t)(key & 0xFF);
}

// Convert epoch days to YYYY-MM-DD (unused in Q1, kept for reference)
std::string format_date(int32_t /* days */) {
    // Not used in this query; dates aren't in output for Q1
    return "DATE";
}

// ============================================================================
// QUERY IMPLEMENTATION
// ============================================================================

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    std::cout << "[Q1] Starting execution..." << std::endl;

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ========================================================================
    // LOAD COLUMNS
    // ========================================================================

    auto t_load_start = std::chrono::high_resolution_clock::now();

    std::string lineitem_dir = gendb_dir + "/lineitem";

    MmappedColumn<int32_t> l_shipdate;
    MmappedColumn<int64_t> l_quantity;
    MmappedColumn<int64_t> l_extendedprice;
    MmappedColumn<int64_t> l_discount;
    MmappedColumn<int64_t> l_tax;
    MmappedColumn<uint8_t> l_returnflag;
    MmappedColumn<uint8_t> l_linestatus;

    if (!load_column<int32_t>(lineitem_dir + "/l_shipdate.bin", l_shipdate)) return;
    if (!load_column<int64_t>(lineitem_dir + "/l_quantity.bin", l_quantity)) return;
    if (!load_column<int64_t>(lineitem_dir + "/l_extendedprice.bin", l_extendedprice)) return;
    if (!load_column<int64_t>(lineitem_dir + "/l_discount.bin", l_discount)) return;
    if (!load_column<int64_t>(lineitem_dir + "/l_tax.bin", l_tax)) return;
    if (!load_column<uint8_t>(lineitem_dir + "/l_returnflag.bin", l_returnflag)) return;
    if (!load_column<uint8_t>(lineitem_dir + "/l_linestatus.bin", l_linestatus)) return;

    auto dict_rf = load_dict(lineitem_dir + "/l_returnflag_dict.txt");
    auto dict_ls = load_dict(lineitem_dir + "/l_linestatus_dict.txt");

    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);

    size_t n_rows = l_shipdate.size;
    printf("[METADATA CHECK] Loaded %zu rows\n", n_rows);
    printf("[METADATA CHECK] Dictionary sizes: returnflag=%zu, linestatus=%zu\n", dict_rf.size(), dict_ls.size());

    // ========================================================================
    // FILTER + AGGREGATE (parallel with thread-local aggregation)
    // ========================================================================

    auto t_scan_filter_start = std::chrono::high_resolution_clock::now();

    // Thread-local aggregation: each thread maintains its own hash table
    // Since cardinality is very low (max 6 groups), use hash table per thread
    // Keys are compound (returnflag_code, linestatus_code)

    // Parallel aggregation with thread-local tables
    std::vector<std::unordered_map<uint16_t, AggState>> thread_agg(omp_get_max_threads());

    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < n_rows; i++) {
        // Filter: l_shipdate <= DATE_CUTOFF
        if (l_shipdate.data[i] > DATE_CUTOFF) {
            continue;
        }

        int thread_id = omp_get_thread_num();
        auto& local_agg = thread_agg[thread_id];

        // Create compound key
        uint8_t rf_code = l_returnflag.data[i];
        uint8_t ls_code = l_linestatus.data[i];
        uint16_t key = make_key(rf_code, ls_code);

        // Get or create aggregate state
        auto it = local_agg.find(key);
        if (it == local_agg.end()) {
            it = local_agg.emplace(key, AggState()).first;
        }

        AggState& agg = it->second;

        // Compute values for aggregation
        int64_t qty = l_quantity.data[i];
        int64_t price = l_extendedprice.data[i];
        int64_t disc = l_discount.data[i];
        int64_t tax = l_tax.data[i];

        // Convert to double to maintain precision during aggregation
        double qty_d = (double)qty;
        double price_d = (double)price;
        double disc_d = (double)disc;
        double tax_d = (double)tax;

        // disc_price = price * (1 - discount/SCALE)
        // charge = price * (1 - discount/SCALE) * (1 + tax/SCALE)
        // All values are scaled by SCALE_DECIMAL (100)
        double disc_frac = disc_d / SCALE_DECIMAL;
        double tax_frac = tax_d / SCALE_DECIMAL;
        double disc_price_d = price_d * (1.0 - disc_frac);
        double charge_d = disc_price_d * (1.0 + tax_frac);

        // Update aggregates (keep sums in double for precision)
        agg.sum_qty += qty_d;
        agg.sum_base_price += price_d;
        agg.sum_disc_price += disc_price_d;
        agg.sum_charge += charge_d;
        agg.sum_discount += disc_d;
        agg.count++;
    }

    auto t_scan_filter_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_filter_end - t_scan_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", scan_ms);

    // ========================================================================
    // MERGE THREAD-LOCAL AGGREGATES
    // ========================================================================

    auto t_merge_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<uint16_t, AggState> global_agg;
    for (int tid = 0; tid < (int)thread_agg.size(); tid++) {
        for (auto& [key, state] : thread_agg[tid]) {
            auto it = global_agg.find(key);
            if (it == global_agg.end()) {
                it = global_agg.emplace(key, AggState()).first;
            }
            it->second.sum_qty += state.sum_qty;
            it->second.sum_base_price += state.sum_base_price;
            it->second.sum_disc_price += state.sum_disc_price;
            it->second.sum_charge += state.sum_charge;
            it->second.sum_discount += state.sum_discount;
            it->second.count += state.count;
        }
    }

    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge: %.2f ms\n", merge_ms);

    // ========================================================================
    // SORT RESULTS
    // ========================================================================

    auto t_sort_start = std::chrono::high_resolution_clock::now();

    // Collect results with decoded keys
    struct ResultRow {
        char returnflag;
        char linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        int64_t count;
        double sum_discount;
    };

    std::vector<ResultRow> results;
    for (auto& [key, agg] : global_agg) {
        uint8_t rf_code, ls_code;
        extract_key(key, rf_code, ls_code);

        char rf_char = dict_rf[rf_code];
        char ls_char = dict_ls[ls_code];

        results.push_back({
            rf_char, ls_char,
            agg.sum_qty,
            agg.sum_base_price,
            agg.sum_disc_price,
            agg.sum_charge,
            agg.count,
            agg.sum_discount
        });
    }

    // Sort by returnflag, then linestatus
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) {
            return a.returnflag < b.returnflag;
        }
        return a.linestatus < b.linestatus;
    });

    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);

    // ========================================================================
    // OUTPUT TO CSV
    // ========================================================================

    auto t_output_start = std::chrono::high_resolution_clock::now();

    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        std::cerr << "[ERROR] Cannot open output file: " << output_path << std::endl;
        return;
    }

    // Write header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Write rows with proper formatting
    for (const auto& row : results) {
        // AVG values (sums are already in double)
        double avg_qty = row.sum_qty / row.count / SCALE_DECIMAL;
        double avg_price = row.sum_base_price / row.count / SCALE_DECIMAL;
        double avg_disc = row.sum_discount / row.count / SCALE_DECIMAL;

        // SUM values (already in double, just scale)
        double sum_qty_val = row.sum_qty / SCALE_DECIMAL;
        double sum_base_val = row.sum_base_price / SCALE_DECIMAL;
        double sum_disc_val = row.sum_disc_price / SCALE_DECIMAL;
        double sum_charge_val = row.sum_charge / SCALE_DECIMAL;

        // Format: returnflag,linestatus,sum_qty,sum_base,sum_disc,sum_charge,avg_qty,avg_price,avg_disc,count
        // Note: sum_disc_price and sum_charge need higher precision (4 and 6 decimals respectively)
        out << row.returnflag << ","
            << row.linestatus << ","
            << std::fixed << std::setprecision(2) << sum_qty_val << ","
            << sum_base_val << ","
            << std::setprecision(4) << sum_disc_val << ","
            << std::setprecision(6) << sum_charge_val << ","
            << std::setprecision(2) << avg_qty << ","
            << avg_price << ","
            << avg_disc << ","
            << std::fixed << std::setprecision(0) << (double)row.count << "\n";
    }

    out.close();

    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);

    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);

    std::cout << "[Q1] Complete. Results written to " << output_path << std::endl;
    std::cout << "[Q1] Rows output: " << results.size() << std::endl;
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
