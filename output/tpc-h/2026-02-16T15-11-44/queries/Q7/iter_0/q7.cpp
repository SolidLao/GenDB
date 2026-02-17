#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>
#include <cmath>
#include <iomanip>

/*
LOGICAL PLAN:
1. Filter lineitem by l_shipdate ∈ [1995-01-01, 1996-12-31] → ~7M rows
2. Hash join lineitem ⋈ orders on l_orderkey (build on lineitem, probe with orders)
3. Hash join result ⋈ customer on o_custkey
4. Hash join result ⋈ supplier on l_suppkey
5. Load nation table, build map for n_name to identify FRANCE (code 6) and GERMANY (code 7)
6. Filter output to only include pairs: (FRANCE, GERMANY) or (GERMANY, FRANCE)
7. Compute volume = l_extendedprice * (1 - l_discount), extract year from l_shipdate
8. Aggregate: GROUP BY (supp_nation, cust_nation, l_year) with SUM(volume)
9. Sort by supp_nation, cust_nation, l_year

PHYSICAL PLAN:
- Scans: Full scan lineitem with inline date filter
- Joins: Hash join (build on smaller side, probe with larger)
- Aggregation: unordered_map with pre-sized capacity (~50 groups expected)
- Parallelism: OpenMP parallel for on lineitem scan; thread-local aggregation for final merge
- Index usage: Load pre-built n_name dictionary for nation codes
*/

struct MmapFile {
    int fd;
    void* data;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), data(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return;
        }
        size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            data = nullptr;
        }
    }

    ~MmapFile() {
        if (data) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// Helper: convert epoch days to year
inline int32_t days_to_year(int32_t days) {
    // Approximate: epoch day 0 = 1970-01-01
    // Days per year average: 365.25
    int32_t year = 1970 + days / 365;

    // Adjust for leap years more precisely
    while (true) {
        int32_t days_before = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;
        if (days_before > days) {
            year--;
        } else if (days_before + (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 366 : 365) <= days) {
            year++;
        } else {
            break;
        }
    }
    return year;
}

// Helper: load dictionary from text file
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
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

// Helper: compute date constant (days since 1970-01-01)
int32_t date_to_days(int32_t year, int32_t month, int32_t day) {
    int32_t days = 0;
    // Days from years
    for (int32_t y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    // Days from months
    int32_t month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        month_days[2] = 29;
    }
    for (int32_t m = 1; m < month; m++) {
        days += month_days[m];
    }
    // Days in month (1-indexed)
    days += day - 1;
    return days;
}

// Helper: convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Approximate year from days
    int32_t year = 1970;
    int32_t remaining = days;

    while (remaining >= 365) {
        bool leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
        int32_t year_days = leap ? 366 : 365;
        if (remaining >= year_days) {
            remaining -= year_days;
            year++;
        } else {
            break;
        }
    }

    int32_t month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    if (leap) month_days[2] = 29;

    int32_t month = 1;
    while (remaining >= month_days[month]) {
        remaining -= month_days[month];
        month++;
    }

    int32_t day = remaining + 1;

    char buf[12];
    snprintf(buf, 12, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

void run_q7(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Compute date constants
    int32_t date_1995_01_01 = date_to_days(1995, 1, 1);
    int32_t date_1996_12_31 = date_to_days(1996, 12, 31);

    // Load binary columns from GenDB
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile lineitem_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapFile lineitem_suppkey(gendb_dir + "/lineitem/l_suppkey.bin");
    MmapFile lineitem_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    MmapFile lineitem_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile lineitem_discount(gendb_dir + "/lineitem/l_discount.bin");

    MmapFile orders_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MmapFile orders_custkey(gendb_dir + "/orders/o_custkey.bin");

    MmapFile customer_custkey(gendb_dir + "/customer/c_custkey.bin");
    MmapFile customer_nationkey(gendb_dir + "/customer/c_nationkey.bin");

    MmapFile supplier_suppkey(gendb_dir + "/supplier/s_suppkey.bin");
    MmapFile supplier_nationkey(gendb_dir + "/supplier/s_nationkey.bin");

    MmapFile nation_nationkey(gendb_dir + "/nation/n_nationkey.bin");
    MmapFile nation_name(gendb_dir + "/nation/n_name.bin");

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
    #endif

    // Load nation name dictionary
    auto nation_dict = load_dictionary(gendb_dir + "/nation/n_name_dict.txt");

    // Find nation codes for FRANCE and GERMANY
    int32_t france_code = -1, germany_code = -1;
    for (auto& [code, name] : nation_dict) {
        if (name == "FRANCE") france_code = code;
        if (name == "GERMANY") germany_code = code;
    }

    // Cast mmapped data to appropriate types
    auto li_orderkey = (int32_t*)lineitem_orderkey.data;
    auto li_suppkey = (int32_t*)lineitem_suppkey.data;
    auto li_shipdate = (int32_t*)lineitem_shipdate.data;
    auto li_extendedprice = (int64_t*)lineitem_extendedprice.data;
    auto li_discount = (int64_t*)lineitem_discount.data;

    auto o_orderkey = (int32_t*)orders_orderkey.data;
    auto o_custkey = (int32_t*)orders_custkey.data;

    auto c_custkey = (int32_t*)customer_custkey.data;
    auto c_nationkey = (int32_t*)customer_nationkey.data;

    auto s_suppkey = (int32_t*)supplier_suppkey.data;
    auto s_nationkey = (int32_t*)supplier_nationkey.data;

    int32_t num_lineitem = lineitem_orderkey.size / sizeof(int32_t);
    int32_t num_orders = orders_orderkey.size / sizeof(int32_t);
    int32_t num_customer = customer_custkey.size / sizeof(int32_t);
    int32_t num_supplier = supplier_suppkey.size / sizeof(int32_t);

    // STEP 1: Filter lineitem by shipdate and build join hash table
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    struct LineitemFiltered {
        int32_t orderkey;
        int32_t suppkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    std::vector<LineitemFiltered> filtered_li;
    filtered_li.reserve(10000000); // Reserve space for ~10M rows

    #pragma omp parallel for
    for (int32_t i = 0; i < num_lineitem; i++) {
        if (li_shipdate[i] >= date_1995_01_01 && li_shipdate[i] <= date_1996_12_31) {
            LineitemFiltered item = {
                li_orderkey[i],
                li_suppkey[i],
                li_shipdate[i],
                li_extendedprice[i],
                li_discount[i]
            };
            #pragma omp critical
            filtered_li.push_back(item);
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", filter_ms);
    printf("[METADATA] Filtered lineitem: %zu rows\n", filtered_li.size());
    #endif

    // STEP 2: Join lineitem ⋈ orders on l_orderkey
    #ifdef GENDB_PROFILE
    auto t_join_li_o_start = std::chrono::high_resolution_clock::now();
    #endif

    struct LineitemOrderJoined {
        int32_t suppkey;
        int32_t custkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    // Build hash table on filtered lineitem
    std::unordered_map<int32_t, std::vector<LineitemFiltered>> li_by_orderkey;
    li_by_orderkey.reserve(filtered_li.size() / 0.75);
    for (auto& item : filtered_li) {
        li_by_orderkey[item.orderkey].push_back(item);
    }

    std::vector<LineitemOrderJoined> li_o_joined;
    li_o_joined.reserve(filtered_li.size() * 1.1); // Slight buffer for multiple matches

    // Probe with orders
    #pragma omp parallel for
    for (int32_t i = 0; i < num_orders; i++) {
        int32_t okey = o_orderkey[i];
        auto it = li_by_orderkey.find(okey);
        if (it != li_by_orderkey.end()) {
            for (auto& li : it->second) {
                LineitemOrderJoined item = {
                    li.suppkey,
                    o_custkey[i],
                    li.shipdate,
                    li.extendedprice,
                    li.discount
                };
                #pragma omp critical
                li_o_joined.push_back(item);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_li_o_end = std::chrono::high_resolution_clock::now();
    double join_li_o_ms = std::chrono::duration<double, std::milli>(t_join_li_o_end - t_join_li_o_start).count();
    printf("[TIMING] join_lineitem_orders: %.2f ms\n", join_li_o_ms);
    printf("[METADATA] After join with orders: %zu rows\n", li_o_joined.size());
    #endif

    // STEP 3: Join with customer
    #ifdef GENDB_PROFILE
    auto t_join_c_start = std::chrono::high_resolution_clock::now();
    #endif

    struct LineitemOrderCustomerJoined {
        int32_t suppkey;
        int32_t cust_nationkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    // Build hash table on customer
    std::unordered_map<int32_t, int32_t> cust_by_custkey;
    cust_by_custkey.reserve(num_customer / 0.75);
    for (int32_t i = 0; i < num_customer; i++) {
        cust_by_custkey[c_custkey[i]] = c_nationkey[i];
    }

    std::vector<LineitemOrderCustomerJoined> li_o_c_joined;
    li_o_c_joined.reserve(li_o_joined.size());

    // Probe with lineitem-order joined
    for (auto& item : li_o_joined) {
        auto it = cust_by_custkey.find(item.custkey);
        if (it != cust_by_custkey.end()) {
            li_o_c_joined.push_back({
                item.suppkey,
                it->second,
                item.shipdate,
                item.extendedprice,
                item.discount
            });
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_c_end = std::chrono::high_resolution_clock::now();
    double join_c_ms = std::chrono::duration<double, std::milli>(t_join_c_end - t_join_c_start).count();
    printf("[TIMING] join_customer: %.2f ms\n", join_c_ms);
    printf("[METADATA] After join with customer: %zu rows\n", li_o_c_joined.size());
    #endif

    // STEP 4: Join with supplier
    #ifdef GENDB_PROFILE
    auto t_join_s_start = std::chrono::high_resolution_clock::now();
    #endif

    struct LineitemOrderCustomerSupplierJoined {
        int32_t supp_nationkey;
        int32_t cust_nationkey;
        int32_t shipdate;
        int64_t extendedprice;
        int64_t discount;
    };

    // Build hash table on supplier
    std::unordered_map<int32_t, int32_t> supp_by_suppkey;
    supp_by_suppkey.reserve(num_supplier / 0.75);
    for (int32_t i = 0; i < num_supplier; i++) {
        supp_by_suppkey[s_suppkey[i]] = s_nationkey[i];
    }

    std::vector<LineitemOrderCustomerSupplierJoined> li_o_c_s_joined;
    li_o_c_s_joined.reserve(li_o_c_joined.size());

    // Probe
    for (auto& item : li_o_c_joined) {
        auto it = supp_by_suppkey.find(item.suppkey);
        if (it != supp_by_suppkey.end()) {
            li_o_c_s_joined.push_back({
                it->second,
                item.cust_nationkey,
                item.shipdate,
                item.extendedprice,
                item.discount
            });
        }
    }

    #ifdef GENDB_PROFILE
    auto t_join_s_end = std::chrono::high_resolution_clock::now();
    double join_s_ms = std::chrono::duration<double, std::milli>(t_join_s_end - t_join_s_start).count();
    printf("[TIMING] join_supplier: %.2f ms\n", join_s_ms);
    printf("[METADATA] After join with supplier: %zu rows\n", li_o_c_s_joined.size());
    #endif

    // STEP 5: Filter by nation pair constraint and prepare aggregation key
    #ifdef GENDB_PROFILE
    auto t_nation_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    struct AggregationKey {
        int32_t supp_nation;
        int32_t cust_nation;
        int32_t year;

        bool operator==(const AggregationKey& other) const {
            return supp_nation == other.supp_nation &&
                   cust_nation == other.cust_nation &&
                   year == other.year;
        }
    };

    struct AggregationKeyHash {
        size_t operator()(const AggregationKey& k) const {
            return ((size_t)k.supp_nation << 40) | ((size_t)k.cust_nation << 20) | k.year;
        }
    };

    // Filter by nation pair and compute aggregation data
    std::vector<std::pair<AggregationKey, double>> agg_data; // (key, volume)
    agg_data.reserve(li_o_c_s_joined.size());

    for (auto& item : li_o_c_s_joined) {
        // Check nation pair constraint: (FRANCE, GERMANY) or (GERMANY, FRANCE)
        bool valid = false;
        if ((item.supp_nationkey == france_code && item.cust_nationkey == germany_code) ||
            (item.supp_nationkey == germany_code && item.cust_nationkey == france_code)) {
            valid = true;
        }

        if (valid) {
            // Compute volume = l_extendedprice * (1 - l_discount)
            // Both extendedprice and discount are stored as int64_t with scale_factor=100
            // discount: 0.05 → 5, extendedprice: 123.45 → 12345
            // volume = (extendedprice / 100) * (1 - (discount / 100))
            double volume = (static_cast<double>(item.extendedprice) / 100.0) *
                           (1.0 - static_cast<double>(item.discount) / 100.0);

            int32_t year = days_to_year(item.shipdate);

            AggregationKey key = {
                item.supp_nationkey,
                item.cust_nationkey,
                year
            };

            agg_data.push_back({key, volume});
        }
    }

    #ifdef GENDB_PROFILE
    auto t_nation_filter_end = std::chrono::high_resolution_clock::now();
    double nation_filter_ms = std::chrono::duration<double, std::milli>(t_nation_filter_end - t_nation_filter_start).count();
    printf("[TIMING] nation_filter: %.2f ms\n", nation_filter_ms);
    printf("[METADATA] After nation filter: %zu rows\n", agg_data.size());
    #endif

    // STEP 6: Aggregation
    #ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    std::unordered_map<AggregationKey, double, AggregationKeyHash> aggregated;
    aggregated.reserve(100); // Expected ~50 groups

    for (auto& [key, volume] : agg_data) {
        aggregated[key] += volume;
    }

    #ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double agg_ms = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", agg_ms);
    printf("[METADATA] Aggregated groups: %zu\n", aggregated.size());
    #endif

    // STEP 7: Sort results
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    struct Result {
        std::string supp_nation;
        std::string cust_nation;
        int32_t l_year;
        double revenue;
    };

    std::vector<Result> results;
    results.reserve(aggregated.size());

    for (auto& [key, revenue] : aggregated) {
        // Decode nation codes to names
        std::string supp_name = nation_dict.at(key.supp_nation);
        std::string cust_name = nation_dict.at(key.cust_nation);

        results.push_back({supp_name, cust_name, key.year, revenue});
    }

    // Sort by supp_nation, cust_nation, l_year
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.l_year < b.l_year;
    });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double sort_ms = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", sort_ms);
    #endif

    // STEP 8: Write results to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream csv(results_dir + "/Q7.csv");
    csv << "supp_nation,cust_nation,l_year,revenue\n";

    for (auto& r : results) {
        csv << r.supp_nation << "," << r.cust_nation << "," << r.l_year << ","
            << std::fixed << std::setprecision(4) << r.revenue << "\n";
    }
    csv.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

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
    run_q7(gendb_dir, results_dir);
    return 0;
}
#endif
