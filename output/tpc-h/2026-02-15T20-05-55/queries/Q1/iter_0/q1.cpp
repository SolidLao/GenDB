#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <omp.h>

// === Helper Structs & Constants ===

// Q1 aggregation key: (l_returnflag, l_linestatus)
struct AggKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const AggKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& key) const {
        return (static_cast<size_t>(key.returnflag) << 8) | key.linestatus;
    }
};

// Q1 aggregation state
struct AggState {
    int64_t sum_qty = 0;
    int64_t sum_extendedprice = 0;
    int64_t sum_disc_price = 0;           // sum(extendedprice * (1 - discount))
    int64_t sum_charge = 0;               // sum(extendedprice * (1 - discount) * (1 + tax))
    int64_t sum_discount = 0;             // for AVG(discount)
    int64_t count_rows = 0;
};

// Q1 result row
struct ResultRow {
    char returnflag;
    char linestatus;
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double avg_qty;
    double avg_price;
    double avg_disc;
    int64_t count_order;
};

// === File I/O Helpers ===

template <typename T>
void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size == -1) {
        close(fd);
        return nullptr;
    }

    out_size = file_size;
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed: " << path << std::endl;
        return nullptr;
    }

    return ptr;
}

void munmap_file(void* ptr, size_t size) {
    if (ptr && ptr != MAP_FAILED) {
        munmap(ptr, size);
    }
}

// Load dictionary from text file (code=value format)
std::unordered_map<uint8_t, char> load_dictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, char> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;

        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos && eq_pos + 1 < line.length()) {
            uint8_t code = std::stoi(line.substr(0, eq_pos));
            char value = line[eq_pos + 1];
            dict[code] = value;
        }
    }
    f.close();
    return dict;
}

// Convert epoch days to YYYY-MM-DD string
std::string format_date(int32_t days) {
    // Days since 1970-01-01
    int year = 1970;
    int month = 1;
    int day = 1;

    // Simple day-by-day increment (works for prototypes)
    // For production, use Zeller's congruence or similar
    int remaining_days = days;

    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (remaining_days < days_in_year) break;
        remaining_days -= days_in_year;
        year++;
    }

    static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    for (month = 1; month <= 12; month++) {
        int days_this_month = days_in_month[month];
        if (month == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
            days_this_month = 29;
        }

        if (remaining_days < days_this_month) break;
        remaining_days -= days_this_month;
    }

    day = remaining_days + 1;

    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// === Main Query Implementation ===

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Data sizes and paths
    const int64_t num_rows = 59986052;
    const int32_t block_size = 256000;

    std::string table_dir = gendb_dir + "/tables/lineitem/";

    // === LOAD DATA ===
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t shipdate_size = 0;
    int32_t* l_shipdate = (int32_t*)mmap_file<int32_t>(table_dir + "l_shipdate.bin", shipdate_size);
    if (!l_shipdate) {
        std::cerr << "Failed to load l_shipdate" << std::endl;
        return;
    }

    size_t qty_size = 0;
    int64_t* l_quantity = (int64_t*)mmap_file<int64_t>(table_dir + "l_quantity.bin", qty_size);
    if (!l_quantity) {
        std::cerr << "Failed to load l_quantity" << std::endl;
        return;
    }

    size_t price_size = 0;
    int64_t* l_extendedprice = (int64_t*)mmap_file<int64_t>(table_dir + "l_extendedprice.bin", price_size);
    if (!l_extendedprice) {
        std::cerr << "Failed to load l_extendedprice" << std::endl;
        return;
    }

    size_t discount_size = 0;
    int64_t* l_discount = (int64_t*)mmap_file<int64_t>(table_dir + "l_discount.bin", discount_size);
    if (!l_discount) {
        std::cerr << "Failed to load l_discount" << std::endl;
        return;
    }

    size_t tax_size = 0;
    int64_t* l_tax = (int64_t*)mmap_file<int64_t>(table_dir + "l_tax.bin", tax_size);
    if (!l_tax) {
        std::cerr << "Failed to load l_tax" << std::endl;
        return;
    }

    size_t returnflag_size = 0;
    uint8_t* l_returnflag = (uint8_t*)mmap_file<uint8_t>(table_dir + "l_returnflag.bin", returnflag_size);
    if (!l_returnflag) {
        std::cerr << "Failed to load l_returnflag" << std::endl;
        return;
    }

    size_t linestatus_size = 0;
    uint8_t* l_linestatus = (uint8_t*)mmap_file<uint8_t>(table_dir + "l_linestatus.bin", linestatus_size);
    if (!l_linestatus) {
        std::cerr << "Failed to load l_linestatus" << std::endl;
        return;
    }

    // Load dictionaries
    std::unordered_map<uint8_t, char> returnflag_dict = load_dictionary(table_dir + "l_returnflag_dict.txt");
    std::unordered_map<uint8_t, char> linestatus_dict = load_dictionary(table_dir + "l_linestatus_dict.txt");

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
    #endif

    // === FILTER THRESHOLD ===
    // l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
    // 1998-12-01 - 90 days = 1998-09-02
    // Epoch days: 10471
    const int32_t filter_threshold = 10471;

    // === SCAN, FILTER, AGGREGATE ===
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Pre-size aggregation hash table (only 6 groups possible: 3 returnflags × 2 linestatuses)
    std::unordered_map<AggKey, AggState, AggKeyHash> agg_map;
    agg_map.reserve(8);

    // Single-threaded scan for now to ensure correctness
    for (int64_t i = 0; i < num_rows; i++) {
        // Filter predicate
        if (l_shipdate[i] > filter_threshold) {
            continue;
        }

        // Aggregate key
        AggKey key{l_returnflag[i], l_linestatus[i]};

        // Extract values (all are scaled by 100)
        int64_t qty = l_quantity[i];
        int64_t price = l_extendedprice[i];
        int64_t discount = l_discount[i];
        int64_t tax = l_tax[i];

        // Compute aggregates with proper scaled arithmetic
        // All values are scaled by 100, so discount/tax are also in units of 1/100
        // disc_price = price * (100 - discount) / 100
        // charge = disc_price * (100 + tax) / 100
        int64_t disc_price = price * (100 - discount) / 100;
        int64_t charge = disc_price * (100 + tax) / 100;

        // Update aggregates
        auto& state = agg_map[key];
        state.sum_qty += qty;
        state.sum_extendedprice += price;
        state.sum_disc_price += disc_price;
        state.sum_charge += charge;
        state.sum_discount += discount;
        state.count_rows++;
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_agg: %.2f ms\n", ms_scan);
    #endif

    // === BUILD RESULTS ===
    #ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<ResultRow> results;
    results.reserve(agg_map.size());

    for (auto& [key, state] : agg_map) {
        ResultRow row;
        row.returnflag = returnflag_dict[key.returnflag];
        row.linestatus = linestatus_dict[key.linestatus];
        row.sum_qty = static_cast<double>(state.sum_qty) / 100.0;
        row.sum_base_price = static_cast<double>(state.sum_extendedprice) / 100.0;
        row.sum_disc_price = static_cast<double>(state.sum_disc_price) / 100.0;
        row.sum_charge = static_cast<double>(state.sum_charge) / 100.0;
        row.avg_qty = (state.count_rows > 0) ? static_cast<double>(state.sum_qty) / state.count_rows / 100.0 : 0.0;
        row.avg_price = (state.count_rows > 0) ? static_cast<double>(state.sum_extendedprice) / state.count_rows / 100.0 : 0.0;
        row.avg_disc = (state.count_rows > 0) ? static_cast<double>(state.sum_discount) / state.count_rows / 100.0 : 0.0;
        row.count_order = state.count_rows;

        results.push_back(row);
    }

    // Sort by returnflag, linestatus
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) {
            return a.returnflag < b.returnflag;
        }
        return a.linestatus < b.linestatus;
    });

    #ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] build_results: %.2f ms\n", ms_build);
    #endif

    // === WRITE OUTPUT ===
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_file = results_dir + "/Q1.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }

    // Write header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,"
        << "avg_qty,avg_price,avg_disc,count_order\n";

    // Write rows
    for (const auto& row : results) {
        char buf[512];
        snprintf(buf, sizeof(buf),
                 "%c,%c,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%lld\n",
                 row.returnflag,
                 row.linestatus,
                 row.sum_qty,
                 row.sum_base_price,
                 row.sum_disc_price,
                 row.sum_charge,
                 row.avg_qty,
                 row.avg_price,
                 row.avg_disc,
                 (long long)row.count_order);
        out << buf;
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    // === CLEANUP ===
    munmap_file(l_shipdate, shipdate_size);
    munmap_file(l_quantity, qty_size);
    munmap_file(l_extendedprice, price_size);
    munmap_file(l_discount, discount_size);
    munmap_file(l_tax, tax_size);
    munmap_file(l_returnflag, returnflag_size);
    munmap_file(l_linestatus, linestatus_size);

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif

    std::cout << "Q1 execution completed. Results written to " << output_file << std::endl;
}

// === MAIN ===

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_Q1(gendb_dir, results_dir);

    return 0;
}
#endif
