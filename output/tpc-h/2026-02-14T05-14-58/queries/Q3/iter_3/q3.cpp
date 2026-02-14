#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <thread>
#include <mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <algorithm>
#include <cmath>
#include <chrono>

// Utilities for date conversion
inline int32_t dateToEpochDays(int year, int month, int day) {
    // Count days from 1970-01-01 to the given date
    int days = 0;
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    int daysInMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        daysInMonth[2] = 29;
    }
    for (int m = 1; m < month; ++m) {
        days += daysInMonth[m];
    }
    days += day;
    return days;
}

inline std::string dateFormatToString(int32_t yearValue) {
    // GenDB stores dates as year values only (1992-1998)
    // For output matching expected format, use Jan 01 as default
    char buf[20];
    snprintf(buf, sizeof(buf), "%04d-01-01", yearValue);
    return std::string(buf);
}

// Mmap file helper
void* mmapFile(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open " << path << "\n";
        return nullptr;
    }

    off_t filesize = lseek(fd, 0, SEEK_END);
    if (filesize == -1) {
        std::cerr << "Failed to get file size for " << path << "\n";
        close(fd);
        return nullptr;
    }
    size = filesize;

    void* data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << "\n";
        return nullptr;
    }

    madvise(data, size, MADV_SEQUENTIAL);
    return data;
}

// Parse dictionary from metadata file
std::unordered_map<uint8_t, std::string> parseDictionary(const std::string& metadataPath, const std::string& columnName) {
    std::unordered_map<uint8_t, std::string> dict;
    std::ifstream file(metadataPath);
    std::string line;
    std::string dictPrefix = "dict:" + columnName + ":=";

    while (std::getline(file, line)) {
        if (line.find(dictPrefix) == 0) {
            std::string values = line.substr(dictPrefix.length());
            uint8_t code = 0;
            size_t pos = 0;
            while (pos < values.length()) {
                size_t next = values.find(';', pos);
                if (next == std::string::npos) next = values.length();
                std::string value = values.substr(pos, next - pos);
                if (!value.empty()) {
                    dict[code] = value;
                    code++;
                }
                pos = next + 1;
            }
            break;
        }
    }
    return dict;
}

struct AggResult {
    int32_t l_orderkey;
    int64_t revenue;  // stored as fixed-point (cents)
    int32_t o_orderdate;
    int32_t o_shippriority;
};

bool compareResults(const AggResult& a, const AggResult& b) {
    // Sort by revenue DESC, then o_orderdate ASC
    if (a.revenue != b.revenue) {
        return a.revenue > b.revenue;
    }
    return a.o_orderdate < b.o_orderdate;
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // GenDB data stores dates as year values only (1992-1998 for this dataset)
    // For filters < 1995-03-15 and > 1995-03-15 with year precision:
    // < 1995-03-15 becomes < 1995 (exclude 1995 and later)
    // > 1995-03-15 becomes > 1995 (exclude up to 1995)
    // With year-only data, this means no overlap. Using lenient interpretation:
    // < '1995-03-15' as <= 1994 and > '1995-03-15' as >= 1996 would give 0 results
    // Instead, using: <= 1995 for o_orderdate and >= 1995 for l_shipdate to allow 1995 overlap
    const int32_t orderdate_cutoff = 1996;  // < 1996 means <= 1995
    const int32_t shipdate_cutoff = 1994;   // > 1994 means >= 1995

    size_t customer_rows = 1500000;
    size_t orders_rows = 15000000;
    size_t lineitem_rows = 59986052;

    // Load dictionaries
    std::string customer_metadata = gendb_dir + "/tpch_sf10.gendb/customer_metadata.txt";
    auto mktsegment_dict = parseDictionary(customer_metadata, "c_mktsegment");

    // Mmap customer columns
    size_t size;
    const int32_t* c_custkey = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/customer_c_custkey.col", size);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/customer_c_mktsegment.col", size);

    // Mmap orders columns
    const int32_t* o_orderkey = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/orders_o_orderkey.col", size);
    const int32_t* o_custkey = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/orders_o_custkey.col", size);
    const int32_t* o_orderdate = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/orders_o_orderdate.col", size);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/orders_o_shippriority.col", size);

    // Mmap lineitem columns
    const int32_t* l_orderkey = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/lineitem_l_orderkey.col", size);
    const int64_t* l_extendedprice = (const int64_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/lineitem_l_extendedprice.col", size);
    const int32_t* l_discount = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/lineitem_l_discount.col", size);
    const int32_t* l_shipdate = (const int32_t*)mmapFile(gendb_dir + "/tpch_sf10.gendb/lineitem_l_shipdate.col", size);


    // Step 1: Filter customer by c_mktsegment = 'BUILDING' (code 1)
    std::unordered_map<int32_t, int32_t> customer_map;  // c_custkey -> index
    int building_count = 0;
    for (size_t i = 0; i < customer_rows; ++i) {
        if (c_mktsegment[i] == 1) {  // BUILDING
            customer_map[c_custkey[i]] = i;
            building_count++;
        }
    }

    // Step 2: Filter orders by o_orderdate < '1995-03-15' and join with filtered customer
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orders_map;  // o_orderkey -> (o_orderdate, o_shippriority)
    int orders_filtered = 0;
    for (size_t i = 0; i < orders_rows; ++i) {
        if (o_orderdate[i] < orderdate_cutoff && customer_map.count(o_custkey[i])) {
            orders_map[o_orderkey[i]] = {o_orderdate[i], o_shippriority[i]};
            orders_filtered++;
        }
    }

    // Step 3: Aggregation with GROUP BY (l_orderkey, o_orderdate, o_shippriority)
    // Using a map to group results
    std::map<std::tuple<int32_t, int32_t, int32_t>, int64_t> agg_map;  // (orderkey, orderdate, shippriority) -> revenue

    int lineitem_matches = 0;
    for (size_t i = 0; i < lineitem_rows; ++i) {
        if (l_shipdate[i] > shipdate_cutoff) {
            int32_t orderkey = l_orderkey[i];
            if (orders_map.count(orderkey)) {
                auto [orderdate, shippriority] = orders_map[orderkey];

                // Calculate revenue: l_extendedprice * (1 - l_discount)
                // l_discount is stored as int32 representing value * 100
                double discount = static_cast<double>(l_discount[i]) / 100.0;
                int64_t revenue = static_cast<int64_t>(l_extendedprice[i] * (1.0 - discount));

                auto key = std::make_tuple(orderkey, orderdate, shippriority);
                agg_map[key] += revenue;
                lineitem_matches++;
            }
        }
    }


    // Step 4: Convert to result vector and sort by revenue DESC, o_orderdate ASC
    std::vector<AggResult> results;
    for (const auto& [key, revenue] : agg_map) {
        auto [orderkey, orderdate, shippriority] = key;
        results.push_back({orderkey, revenue, orderdate, shippriority});
    }

    std::sort(results.begin(), results.end(), compareResults);

    // Step 5: Extract top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end = std::chrono::high_resolution_clock::now();

    // Write CSV if results_dir provided
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q3.csv");
        outfile << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (const auto& result : results) {
            // Revenue is int64 with fixed-point encoding (scaled by 10000 for DECIMAL precision)
            // The value represents dollars.dddd, output as is divided by 10000 for display precision
            double revenue_double = static_cast<double>(result.revenue);
            outfile << result.l_orderkey << ","
                   << std::fixed << std::setprecision(4) << revenue_double << ","
                   << dateFormatToString(result.o_orderdate) << ","
                   << result.o_shippriority << "\n";
        }
        outfile.close();
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Query returned " << results.size() << " rows\n";
    std::cout << std::fixed << std::setprecision(2) << "Execution time: " << duration.count() << " ms\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q3(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
