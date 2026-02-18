#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <set>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>

// ============================================================================
// Q12 PLAN:
// ============================================================================
// Logical Plan:
//   1. Scan & Filter lineitem (59.9M rows):
//      - l_shipmode IN ('MAIL', 'SHIP')  [dictionary codes]
//      - l_commitdate < l_receiptdate
//      - l_shipdate < l_commitdate
//      - l_receiptdate >= 1994-01-01 (epoch day 8766)
//      - l_receiptdate < 1995-01-01 (epoch day 9131)
//      Estimated output: ~1.2M rows (2% selectivity)
//
//   2. Scan orders (15M rows):
//      - No predicates, full scan
//
//   3. Join on o_orderkey = l_orderkey:
//      - Hash join: build on filtered lineitem (smaller), probe with orders
//
//   4. GROUP BY l_shipmode + aggregate:
//      - Cardinality: 2 groups (MAIL=0, SHIP=1 per dictionary)
//      - Flat array aggregation by shipmode code
//      - Two aggregates: COUNT(CASE WHEN priority IN ('1-URGENT','2-HIGH') THEN 1)
//
// Physical Plan:
//   - lineitem scan: single-threaded filtered scan with in-loop filtering
//   - join: unordered_map for 1:N (o_orderkey maps to vector of lineitem indices)
//   - aggregation: flat array [2] for each shipmode
//   - output: order by l_shipmode, convert dates YYYY-MM-DD, write CSV
//
// Data Structures:
//   - o_orderpriority_dict, l_shipmode_dict: loaded at runtime
//   - ht_orders: hash map keying o_orderkey to order indices
//   - agg_high[2], agg_low[2]: flat arrays indexed by shipmode code
// ============================================================================

struct MmapFile {
    int fd = -1;
    void* data = nullptr;
    size_t size = 0;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            throw std::runtime_error("open failed");
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            throw std::runtime_error("fstat failed");
        }
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            throw std::runtime_error("mmap failed");
        }
    }

    void close() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

template<typename T>
T* cast(void* ptr) {
    return reinterpret_cast<T*>(ptr);
}

// Load dictionary file (format: "code=value" per line)
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << path << std::endl;
        throw std::runtime_error("dictionary open failed");
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Epoch day calculation: 1970-01-01 is day 0
// For 1994-01-01: (1994-1970)*365 + leap days
// For 1995-01-01: (1995-1970)*365 + leap days
int32_t date_to_epoch(int year, int month, int day) {
    int32_t total = 0;
    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        total += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Add days for complete months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }
    for (int m = 1; m < month; ++m) {
        total += days_in_month[m - 1];
    }
    // Add remaining days
    total += (day - 1);
    return total;
}

void epoch_to_date(int32_t epoch, int& year, int& month, int& day) {
    year = 1970;
    day = epoch;
    // Find year
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (day < days_in_year) break;
        day -= days_in_year;
        year++;
    }
    // Find month and day
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }
    month = 1;
    while (month <= 12 && day >= days_in_month[month - 1]) {
        day -= days_in_month[month - 1];
        month++;
    }
    day++;  // 1-indexed
}

void run_q12(const std::string& gendb_dir, const std::string& results_dir) {
    // Timing
    auto t_global_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load dictionaries
    auto orderpriority_dict = load_dictionary(gendb_dir + "/orders/o_orderpriority_dict.txt");
    auto shipmode_dict = load_dictionary(gendb_dir + "/lineitem/l_shipmode_dict.txt");

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", ms);
#endif

    // Build reverse mapping for shipmode: value -> code
    std::unordered_map<std::string, int32_t> shipmode_reverse;
    for (auto& [code, value] : shipmode_dict) {
        shipmode_reverse[value] = code;
    }

    // Date filters
    int32_t receipt_date_min = date_to_epoch(1994, 1, 1);  // 8766
    int32_t receipt_date_max = date_to_epoch(1995, 1, 1);  // 9131

    // Open lineitem columns
    MmapFile li_orderkey_file, li_shipmode_file, li_commitdate_file;
    MmapFile li_receiptdate_file, li_shipdate_file;

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    li_orderkey_file.open(gendb_dir + "/lineitem/l_orderkey.bin");
    li_shipmode_file.open(gendb_dir + "/lineitem/l_shipmode.bin");
    li_commitdate_file.open(gendb_dir + "/lineitem/l_commitdate.bin");
    li_receiptdate_file.open(gendb_dir + "/lineitem/l_receiptdate.bin");
    li_shipdate_file.open(gendb_dir + "/lineitem/l_shipdate.bin");

    int32_t* li_orderkey = cast<int32_t>(li_orderkey_file.data);
    int32_t* li_shipmode = cast<int32_t>(li_shipmode_file.data);
    int32_t* li_commitdate = cast<int32_t>(li_commitdate_file.data);
    int32_t* li_receiptdate = cast<int32_t>(li_receiptdate_file.data);
    int32_t* li_shipdate = cast<int32_t>(li_shipdate_file.data);

    size_t lineitem_count = li_orderkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
#endif

    // Scan & filter lineitem
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<int32_t> filtered_orderkeys;
    std::vector<int32_t> filtered_shipmodes;
    filtered_orderkeys.reserve(lineitem_count / 50);  // Estimate 2% selectivity

    int32_t mail_code = shipmode_reverse.count("MAIL") ? shipmode_reverse["MAIL"] : -1;
    int32_t ship_code = shipmode_reverse.count("SHIP") ? shipmode_reverse["SHIP"] : -1;

    for (size_t i = 0; i < lineitem_count; ++i) {
        int32_t sm = li_shipmode[i];
        if ((sm != mail_code && sm != ship_code)) continue;
        if (li_commitdate[i] >= li_receiptdate[i]) continue;
        if (li_shipdate[i] >= li_commitdate[i]) continue;
        if (li_receiptdate[i] < receipt_date_min) continue;
        if (li_receiptdate[i] >= receipt_date_max) continue;

        filtered_orderkeys.push_back(li_orderkey[i]);
        filtered_shipmodes.push_back(sm);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] scan_filter_lineitem: %.2f ms\n", ms);
#endif

    // Load orders columns
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile o_orderkey_file, o_orderpriority_file;
    o_orderkey_file.open(gendb_dir + "/orders/o_orderkey.bin");
    o_orderpriority_file.open(gendb_dir + "/orders/o_orderpriority.bin");

    int32_t* o_orderkey = cast<int32_t>(o_orderkey_file.data);
    int32_t* o_orderpriority = cast<int32_t>(o_orderpriority_file.data);

    size_t orders_count = o_orderkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // Build hash join on orders
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, std::vector<int32_t>> ht_orders;
    ht_orders.reserve(orders_count);
    for (size_t i = 0; i < orders_count; ++i) {
        ht_orders[o_orderkey[i]].push_back(i);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_hash_join: %.2f ms\n", ms);
#endif

    // Probe join & aggregate
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t agg_high[2] = {0, 0};  // indexed by shipmode code
    int64_t agg_low[2] = {0, 0};

    for (size_t i = 0; i < filtered_orderkeys.size(); ++i) {
        int32_t orderkey = filtered_orderkeys[i];
        int32_t shipmode = filtered_shipmodes[i];

        auto it = ht_orders.find(orderkey);
        if (it != ht_orders.end()) {
            for (int32_t order_idx : it->second) {
                int32_t priority_code = o_orderpriority[order_idx];
                std::string priority_str = orderpriority_dict.count(priority_code)
                                           ? orderpriority_dict[priority_code]
                                           : "";

                // Determine if high or low priority
                bool is_high = (priority_str == "1-URGENT" || priority_str == "2-HIGH");

                // Get shipmode code index (MAIL=?, SHIP=?)
                // We need to map shipmode back to 0 or 1 based on code order
                int shipmode_idx = (shipmode == mail_code) ? 0 : 1;

                if (is_high) {
                    agg_high[shipmode_idx]++;
                } else {
                    agg_low[shipmode_idx]++;
                }
            }
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", ms);
#endif

    // Prepare output
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Create results: (shipmode, high_count, low_count)
    // Only output shipmodes that appear in the filtered lineitem data
    std::vector<std::tuple<std::string, int64_t, int64_t>> results;

    // Collect unique shipmodes from filtered data
    std::vector<std::pair<std::string, int32_t>> shipmode_list;
    std::set<int32_t> seen_codes;
    for (int32_t code : filtered_shipmodes) {
        if (seen_codes.find(code) == seen_codes.end()) {
            seen_codes.insert(code);
            if (shipmode_dict.count(code)) {
                shipmode_list.push_back({shipmode_dict[code], code});
            }
        }
    }
    std::sort(shipmode_list.begin(), shipmode_list.end());

    for (auto& [shipmode_str, code] : shipmode_list) {
        // Map code to index (mail_code -> 0, ship_code -> 1)
        int idx = (code == mail_code) ? 0 : 1;
        results.push_back({shipmode_str, agg_high[idx], agg_low[idx]});
    }

    // Write CSV output
    std::ofstream out(results_dir + "/Q12.csv");
    out << "l_shipmode,high_line_count,low_line_count\n";
    for (auto& [shipmode_str, high_count, low_count] : results) {
        out << shipmode_str << "," << high_count << "," << low_count << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    auto t_global_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_global_end - t_global_start).count();
#ifdef GENDB_PROFILE
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
    run_q12(gendb_dir, results_dir);
    return 0;
}
#endif
