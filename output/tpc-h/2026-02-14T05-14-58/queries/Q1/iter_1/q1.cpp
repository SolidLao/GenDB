#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <thread>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <chrono>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

// Kahan summation for improved floating-point precision
struct KahanSum {
    double sum = 0.0;
    double correction = 0.0;

    void add(double x) {
        double y = x - correction;
        double t = sum + y;
        correction = (t - sum) - y;
        sum = t;
    }

    double value() const { return sum; }
};

struct AggregateGroup {
    int64_t sum_qty = 0;
    KahanSum sum_base_price;
    KahanSum sum_disc_price;
    KahanSum sum_charge;
    KahanSum sum_discount;
    int64_t count_order = 0;
};

// RAII wrapper for mmap'd files
class MmapedFile {
public:
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path, size_t expected_rows, size_t element_size) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << ": " << strerror(errno) << "\n";
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << ": " << strerror(errno) << "\n";
            ::close(fd);
            fd = -1;
            return false;
        }

        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << ": " << strerror(errno) << "\n";
            ::close(fd);
            fd = -1;
            data = nullptr;
            return false;
        }

        // Hint sequential access pattern
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }

    ~MmapedFile() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }
};

struct ResultRow {
    char returnflag;
    char linestatus;
    int64_t sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    int64_t count_order;
    double sum_discount_for_avg;

    bool operator<(const ResultRow& other) const {
        if (returnflag != other.returnflag) return returnflag < other.returnflag;
        return linestatus < other.linestatus;
    }
};

// Parse date from YYYY-MM-DD to days since epoch 1970
int32_t parseDate(const std::string& datestr) {
    int year = std::stoi(datestr.substr(0, 4));
    int month = std::stoi(datestr.substr(5, 2));
    int day = std::stoi(datestr.substr(8, 2));

    int days = 0;
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    int daysInMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        daysInMonth[2] = 29;
    }

    for (int m = 1; m < month; m++) {
        days += daysInMonth[m];
    }

    return days + day;
}

// Parse dictionary mapping from metadata
std::unordered_map<uint8_t, char> parseDictionary(const std::string& dict_str) {
    std::unordered_map<uint8_t, char> dict;
    size_t pos = 0;
    while (pos < dict_str.length()) {
        size_t eq_pos = dict_str.find('=', pos);
        if (eq_pos == std::string::npos) break;
        size_t next_pos = dict_str.find(';', eq_pos);
        if (next_pos == std::string::npos) next_pos = dict_str.length();

        std::string code_str = dict_str.substr(pos, eq_pos - pos);
        std::string value_str = dict_str.substr(eq_pos + 1, next_pos - eq_pos - 1);

        uint8_t code = std::stoi(code_str);
        // Handle values like "A", "N", "R", "F", "O", etc.
        char value = value_str.empty() ? ' ' : value_str[0];
        dict[code] = value;

        pos = next_pos + 1;
    }
    return dict;
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    const int32_t SHIPDATE_CUTOFF = parseDate("1998-09-02");
    const size_t NUM_ROWS = 59986052;  // From lineitem_metadata.txt

    // Load metadata to find dictionary encodings
    std::string metadata_path = gendb_dir + "/lineitem_metadata.txt";
    std::ifstream metadata_file(metadata_path);
    if (!metadata_file.is_open()) {
        std::cerr << "Error: Cannot open " << metadata_path << "\n";
        return;
    }

    std::unordered_map<uint8_t, char> dict_returnflag;
    std::unordered_map<uint8_t, char> dict_linestatus;

    std::string line;
    while (std::getline(metadata_file, line)) {
        if (line.find("dict:l_returnflag:") == 0) {
            std::string dict_str = line.substr(strlen("dict:l_returnflag:"));
            dict_returnflag = parseDictionary(dict_str);
        } else if (line.find("dict:l_linestatus:") == 0) {
            std::string dict_str = line.substr(strlen("dict:l_linestatus:"));
            dict_linestatus = parseDictionary(dict_str);
        }
    }
    metadata_file.close();

    // mmap binary column files
    MmapedFile col_shipdate, col_quantity, col_extendedprice, col_discount, col_tax, col_returnflag, col_linestatus;

    std::string base = gendb_dir + "/lineitem_";
    if (!col_shipdate.open(base + "l_shipdate.col", NUM_ROWS, sizeof(int32_t)) ||
        !col_quantity.open(base + "l_quantity.col", NUM_ROWS, sizeof(int32_t)) ||
        !col_extendedprice.open(base + "l_extendedprice.col", NUM_ROWS, sizeof(int64_t)) ||
        !col_discount.open(base + "l_discount.col", NUM_ROWS, sizeof(int32_t)) ||
        !col_tax.open(base + "l_tax.col", NUM_ROWS, sizeof(int32_t)) ||
        !col_returnflag.open(base + "l_returnflag.col", NUM_ROWS, sizeof(uint8_t)) ||
        !col_linestatus.open(base + "l_linestatus.col", NUM_ROWS, sizeof(uint8_t))) {
        std::cerr << "Error: Failed to mmap one or more column files\n";
        return;
    }

    // Cast to proper types for direct array access
    const int32_t* shipdates = static_cast<const int32_t*>(col_shipdate.data);
    const int32_t* quantities = static_cast<const int32_t*>(col_quantity.data);
    const int64_t* extendedprices = static_cast<const int64_t*>(col_extendedprice.data);
    const int32_t* discounts = static_cast<const int32_t*>(col_discount.data);
    const int32_t* taxes = static_cast<const int32_t*>(col_tax.data);
    const uint8_t* returnflags = static_cast<const uint8_t*>(col_returnflag.data);
    const uint8_t* linestatuses = static_cast<const uint8_t*>(col_linestatus.data);

    // Map for aggregation by (returnflag, linestatus) pair
    std::map<std::pair<char, char>, AggregateGroup> global_agg;

    // Process all rows with filtering
    for (size_t i = 0; i < NUM_ROWS; ++i) {
        // Apply date filter
        if (shipdates[i] > SHIPDATE_CUTOFF) continue;

        // Decode dictionary-encoded columns
        char rf = dict_returnflag[returnflags[i]];
        char ls = dict_linestatus[linestatuses[i]];

        auto key = std::make_pair(rf, ls);
        auto& group = global_agg[key];

        // Convert from internal representation to external
        // Quantities are stored as int32_t, need to cast to double for calculations
        int64_t qty = quantities[i];
        double ext = extendedprices[i] / 100.0;  // Convert from cents to dollars
        double disc = discounts[i] / 10000.0;    // Convert from basis points
        double tax = taxes[i] / 10000.0;         // Convert from basis points

        // Perform aggregations
        group.sum_qty += qty;
        group.sum_base_price.add(ext);

        double disc_price = ext * (1.0 - disc);
        group.sum_disc_price.add(disc_price);

        double charge = disc_price * (1.0 + tax);
        group.sum_charge.add(charge);

        group.sum_discount.add(disc);
        group.count_order++;
    }

    // Build result rows
    std::vector<ResultRow> results;
    for (const auto& [key, group] : global_agg) {
        results.push_back({
            key.first,
            key.second,
            group.sum_qty,
            group.sum_base_price.value(),
            group.sum_disc_price.value(),
            group.sum_charge.value(),
            group.count_order,
            group.sum_discount.value()
        });
    }

    std::sort(results.begin(), results.end());

    // Write results to CSV if requested
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& row : results) {
            double avg_qty = row.count_order > 0 ? (double)row.sum_qty / row.count_order : 0;
            double avg_price = row.count_order > 0 ? row.sum_base_price / row.count_order : 0;
            double avg_disc = row.count_order > 0 ? row.sum_discount_for_avg / row.count_order : 0;

            out << row.returnflag << "," << row.linestatus << ",";
            out << std::fixed << std::setprecision(2);
            out << (double)row.sum_qty << ",";
            out << row.sum_base_price << ",";
            out << row.sum_disc_price << ",";
            out << row.sum_charge << ",";
            out << avg_qty << ","
                << avg_price << ","
                << avg_disc << ","
                << row.count_order << "\n";
        }

        out.close();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    std::cout << "Query returned " << results.size() << " rows\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << duration_ms << " ms\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }

    try {
        run_q1(argv[1], argc > 2 ? argv[2] : "");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
#endif
