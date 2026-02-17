#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

/*
 * Q10: Returned Item Reporting - Iteration 0
 *
 * LOGICAL PLAN:
 * ============
 * 1. Filter lineitem: l_returnflag = 'R' (dict code lookup)
 *    Estimated: ~2.3% × 59.9M ≈ 1.4M rows
 * 2. Filter orders: o_orderdate >= 1993-10-01 (8674 days) AND < 1994-01-01 (8766 days)
 *    Estimated: ~4% × 15M ≈ 600K rows
 *    Note: Use zone map to skip blocks
 * 3. Join: orders (filtered) ⋈ lineitem (filtered) on l_orderkey = o_orderkey
 *    Result: ~600K rows (small lineitem result matches all filtered orders)
 * 4. Join: orders-lineitem result ⋈ customer on o_custkey = c_custkey
 *    Result: ~600K rows (expand customer attributes)
 * 5. Join: result ⋈ nation on c_nationkey = n_nationkey
 *    Result: ~600K rows (only 25 nations, all will match)
 * 6. GROUP BY (c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment)
 *    Aggregation: SUM(l_extendedprice * (1 - l_discount))
 *    Estimated groups: <600K distinct customers in filtered result, likely <100K
 * 7. Sort by revenue DESC, limit 20
 *
 * PHYSICAL PLAN:
 * ==============
 * Scans:
 *   - lineitem: Full scan with per-row filter on l_returnflag
 *   - orders: Full scan with date range filter (no zone map in this iter_0 for simplicity)
 *   - customer: Full scan, all rows needed for join
 *   - nation: Full scan, all rows (only 25)
 *
 * Joins:
 *   - orders ⋈ lineitem: Hash join, build on filtered lineitem (smaller), probe orders
 *   - result ⋈ customer: Hash join, build on customer (1.5M), probe result (600K)
 *   - result ⋈ nation: Hash join, build on nation (25), probe result (600K)
 *
 * Aggregation:
 *   - Open-addressing hash table for GROUP BY
 *   - Key: (c_custkey, c_name_code, c_acctbal, c_phone_code, n_name_code, c_address_code, c_comment_code)
 *   - Value: (revenue_sum)
 *
 * Parallelism:
 *   - Parallel lineitem scan with thread-local result buffers
 *   - Parallel order scan
 *   - Sequential hash table builds (small relative to probe)
 *   - Parallel join probes with thread-local aggregation
 *
 * Date Constants:
 *   - 1993-10-01: 8674 days since epoch
 *   - 1994-01-01: 8766 days since epoch
 *
 * Dictionary Handling:
 *   - l_returnflag: Load dict, find code for 'R'
 *   - c_name, c_address, c_phone, c_comment: Load dicts for encoding/decoding
 *   - n_name: Load dict for encoding/decoding
 */

// Struct to hold mmap'd data
template<typename T>
struct MmapArray {
    int fd;
    T* data;
    size_t size;

    MmapArray() : fd(-1), data(nullptr), size(0) {}

    bool load(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            close(fd);
            return false;
        }

        size = st.st_size / sizeof(T);
        data = (T*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            close(fd);
            return false;
        }

        return true;
    }

    ~MmapArray() {
        if (data != nullptr && data != MAP_FAILED) {
            munmap(data, size * sizeof(T));
        }
        if (fd >= 0) close(fd);
    }

    T operator[](size_t idx) const { return data[idx]; }
};

// Load dictionary from file and return mapping from string value to code
std::unordered_map<std::string, int32_t> load_dict(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dict: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[value] = code;
        }
    }
    f.close();
    return dict;
}

// Reverse dictionary: code -> value
std::unordered_map<int32_t, std::string> load_dict_reverse(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(dict_path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dict: " << dict_path << std::endl;
        return dict;
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
    f.close();
    return dict;
}

// Group key structure
struct GroupKey {
    int32_t c_custkey;
    int32_t c_name_code;
    int64_t c_acctbal;
    int32_t c_phone_code;
    int32_t n_name_code;
    int32_t c_address_code;
    int32_t c_comment_code;

    bool operator==(const GroupKey& other) const {
        return c_custkey == other.c_custkey &&
               c_name_code == other.c_name_code &&
               c_acctbal == other.c_acctbal &&
               c_phone_code == other.c_phone_code &&
               n_name_code == other.n_name_code &&
               c_address_code == other.c_address_code &&
               c_comment_code == other.c_comment_code;
    }
};

// Aggregation value uses double for floating-point precision
struct AggregateValue {
    double revenue_sum;  // Use double for accurate aggregate computation
    AggregateValue() : revenue_sum(0.0) {}
    AggregateValue(const AggregateValue& other) : revenue_sum(other.revenue_sum) {}
    AggregateValue& operator=(const AggregateValue& other) {
        revenue_sum = other.revenue_sum;
        return *this;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        size_t h = 0;
        h ^= std::hash<int32_t>()(k.c_custkey) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_name_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int64_t>()(k.c_acctbal) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_phone_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.n_name_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_address_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        h ^= std::hash<int32_t>()(k.c_comment_code) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};

// Result row structure
struct ResultRow {
    int32_t c_custkey;
    std::string c_name;
    double revenue;  // in actual units (not scaled)
    int64_t c_acctbal;
    std::string n_name;
    std::string c_address;
    std::string c_phone;
    std::string c_comment;
};

// Comparator for sorting by revenue desc
bool compare_revenue(const ResultRow& a, const ResultRow& b) {
    return a.revenue > b.revenue;
}

// Helper to convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days) {
    // Forward algorithm: compute year/month/day from days
    int year = 1970;
    int day_of_year = days;

    while (true) {
        int days_in_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) ? 366 : 365;
        if (day_of_year < days_in_year) break;
        day_of_year -= days_in_year;
        year++;
    }

    // Now find month and day
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    int month = 0;
    int day = day_of_year;
    for (int m = 0; m < 12; m++) {
        if (day < days_in_month[m]) {
            month = m;
            break;
        }
        day -= days_in_month[m];
    }

    char buf[16];
    snprintf(buf, 16, "%04d-%02d-%02d", year, month + 1, day + 1);
    return std::string(buf);
}

void run_q10(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Load dictionaries
    auto l_returnflag_dict = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    auto c_name_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_name_dict.txt");
    auto c_address_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_address_dict.txt");
    auto c_phone_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_phone_dict.txt");
    auto c_comment_dict_rev = load_dict_reverse(gendb_dir + "/customer/c_comment_dict.txt");
    auto n_name_dict_rev = load_dict_reverse(gendb_dir + "/nation/n_name_dict.txt");

    int32_t returnflag_r_code = l_returnflag_dict["R"];

#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Load all data
    MmapArray<int32_t> lineitem_orderkey, lineitem_returnflag;
    MmapArray<int64_t> lineitem_extendedprice, lineitem_discount;
    MmapArray<int32_t> orders_orderkey, orders_custkey, orders_orderdate;
    MmapArray<int32_t> customer_custkey, customer_name_code, customer_phone_code;
    MmapArray<int32_t> customer_address_code, customer_comment_code, customer_nationkey;
    MmapArray<int64_t> customer_acctbal;
    MmapArray<int32_t> nation_nationkey, nation_name_code;

    lineitem_orderkey.load(gendb_dir + "/lineitem/l_orderkey.bin");
    lineitem_returnflag.load(gendb_dir + "/lineitem/l_returnflag.bin");
    lineitem_extendedprice.load(gendb_dir + "/lineitem/l_extendedprice.bin");
    lineitem_discount.load(gendb_dir + "/lineitem/l_discount.bin");

    orders_orderkey.load(gendb_dir + "/orders/o_orderkey.bin");
    orders_custkey.load(gendb_dir + "/orders/o_custkey.bin");
    orders_orderdate.load(gendb_dir + "/orders/o_orderdate.bin");

    customer_custkey.load(gendb_dir + "/customer/c_custkey.bin");
    customer_name_code.load(gendb_dir + "/customer/c_name.bin");
    customer_phone_code.load(gendb_dir + "/customer/c_phone.bin");
    customer_address_code.load(gendb_dir + "/customer/c_address.bin");
    customer_comment_code.load(gendb_dir + "/customer/c_comment.bin");
    customer_nationkey.load(gendb_dir + "/customer/c_nationkey.bin");
    customer_acctbal.load(gendb_dir + "/customer/c_acctbal.bin");

    nation_nationkey.load(gendb_dir + "/nation/n_nationkey.bin");
    nation_name_code.load(gendb_dir + "/nation/n_name.bin");

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", load_ms);
#endif

    // Step 1: Filter lineitem on l_returnflag = 'R'
#ifdef GENDB_PROFILE
    auto t_filter_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<uint32_t> filtered_lineitem_indices;
    filtered_lineitem_indices.reserve(lineitem_orderkey.size / 50);  // Estimate ~2% selectivity

    #pragma omp parallel
    {
        std::vector<uint32_t> local_indices;
        local_indices.reserve(lineitem_orderkey.size / 50 / omp_get_num_threads());

        #pragma omp for nowait
        for (size_t i = 0; i < lineitem_orderkey.size; i++) {
            if (lineitem_returnflag[i] == returnflag_r_code) {
                local_indices.push_back(i);
            }
        }

        #pragma omp critical
        {
            filtered_lineitem_indices.insert(filtered_lineitem_indices.end(),
                                            local_indices.begin(), local_indices.end());
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_lineitem_end = std::chrono::high_resolution_clock::now();
    double filter_lineitem_ms = std::chrono::duration<double, std::milli>(t_filter_lineitem_end - t_filter_lineitem_start).count();
    printf("[TIMING] filter_lineitem: %.2f ms (%zu rows)\n", filter_lineitem_ms, filtered_lineitem_indices.size());
#endif

    // Step 2: Filter orders on o_orderdate
    const int32_t DATE_1993_10_01 = 8674;
    const int32_t DATE_1994_01_01 = 8766;

#ifdef GENDB_PROFILE
    auto t_filter_orders_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<uint32_t> filtered_orders_indices;
    filtered_orders_indices.reserve(orders_orderkey.size / 25);  // Estimate ~4% selectivity

    #pragma omp parallel
    {
        std::vector<uint32_t> local_indices;
        local_indices.reserve(orders_orderkey.size / 25 / omp_get_num_threads());

        #pragma omp for nowait
        for (size_t i = 0; i < orders_orderkey.size; i++) {
            int32_t odate = orders_orderdate[i];
            if (odate >= DATE_1993_10_01 && odate < DATE_1994_01_01) {
                local_indices.push_back(i);
            }
        }

        #pragma omp critical
        {
            filtered_orders_indices.insert(filtered_orders_indices.end(),
                                          local_indices.begin(), local_indices.end());
        }
    }

#ifdef GENDB_PROFILE
    auto t_filter_orders_end = std::chrono::high_resolution_clock::now();
    double filter_orders_ms = std::chrono::duration<double, std::milli>(t_filter_orders_end - t_filter_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms (%zu rows)\n", filter_orders_ms, filtered_orders_indices.size());
#endif

    // Step 3: Build hash table on lineitem(orderkey), indexed by orderkey
#ifdef GENDB_PROFILE
    auto t_build_li_ht_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, std::vector<uint32_t>> li_ht;
    li_ht.reserve(filtered_lineitem_indices.size());

    for (uint32_t li_idx : filtered_lineitem_indices) {
        int32_t orderkey = lineitem_orderkey[li_idx];
        li_ht[orderkey].push_back(li_idx);
    }

#ifdef GENDB_PROFILE
    auto t_build_li_ht_end = std::chrono::high_resolution_clock::now();
    double build_li_ht_ms = std::chrono::duration<double, std::milli>(t_build_li_ht_end - t_build_li_ht_start).count();
    printf("[TIMING] build_lineitem_ht: %.2f ms\n", build_li_ht_ms);
#endif

    // Step 4: Build hash table on customer
#ifdef GENDB_PROFILE
    auto t_build_cust_ht_start = std::chrono::high_resolution_clock::now();
#endif

    struct CustomerData {
        int32_t c_name_code;
        int64_t c_acctbal;
        int32_t c_phone_code;
        int32_t c_address_code;
        int32_t c_comment_code;
        int32_t c_nationkey;
    };

    std::unordered_map<int32_t, CustomerData> cust_ht;
    cust_ht.reserve(customer_custkey.size);

    for (size_t i = 0; i < customer_custkey.size; i++) {
        int32_t custkey = customer_custkey[i];
        cust_ht[custkey] = {
            customer_name_code[i],
            customer_acctbal[i],
            customer_phone_code[i],
            customer_address_code[i],
            customer_comment_code[i],
            customer_nationkey[i]
        };
    }

#ifdef GENDB_PROFILE
    auto t_build_cust_ht_end = std::chrono::high_resolution_clock::now();
    double build_cust_ht_ms = std::chrono::duration<double, std::milli>(t_build_cust_ht_end - t_build_cust_ht_start).count();
    printf("[TIMING] build_customer_ht: %.2f ms\n", build_cust_ht_ms);
#endif

    // Step 5: Build hash table on nation
#ifdef GENDB_PROFILE
    auto t_build_nat_ht_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> nat_ht;
    nat_ht.reserve(nation_nationkey.size);

    for (size_t i = 0; i < nation_nationkey.size; i++) {
        int32_t natkey = nation_nationkey[i];
        nat_ht[natkey] = nation_name_code[i];
    }

#ifdef GENDB_PROFILE
    auto t_build_nat_ht_end = std::chrono::high_resolution_clock::now();
    double build_nat_ht_ms = std::chrono::duration<double, std::milli>(t_build_nat_ht_end - t_build_nat_ht_start).count();
    printf("[TIMING] build_nation_ht: %.2f ms\n", build_nat_ht_ms);
#endif

    // Step 6: Join orders with lineitem, then customer, then nation, and aggregate
#ifdef GENDB_PROFILE
    auto t_join_agg_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<GroupKey, AggregateValue, GroupKeyHash> agg_map;
    agg_map.reserve(100000);  // Estimate

    // Process each filtered order
    for (uint32_t o_idx : filtered_orders_indices) {
        int32_t orderkey = orders_orderkey[o_idx];
        int32_t custkey = orders_custkey[o_idx];

        // Probe lineitem hash table
        auto li_it = li_ht.find(orderkey);
        if (li_it == li_ht.end()) continue;

        // For each matching lineitem
        for (uint32_t li_idx : li_it->second) {
            // Probe customer hash table
            auto cust_it = cust_ht.find(custkey);
            if (cust_it == cust_ht.end()) continue;

            const auto& cust_data = cust_it->second;

            // Probe nation hash table
            auto nat_it = nat_ht.find(cust_data.c_nationkey);
            if (nat_it == nat_ht.end()) continue;

            int32_t n_name_code = nat_it->second;

            // Create group key
            GroupKey gk = {
                custkey,
                cust_data.c_name_code,
                cust_data.c_acctbal,
                cust_data.c_phone_code,
                n_name_code,
                cust_data.c_address_code,
                cust_data.c_comment_code
            };

            // Compute revenue: extendedprice * (1 - discount)
            // Both are scaled integers (scale_factor = 100)
            // Convert to actual values, compute, then aggregate
            double extendedprice = (double)lineitem_extendedprice[li_idx] / 100.0;
            double discount = (double)lineitem_discount[li_idx] / 100.0;
            double revenue = extendedprice * (1.0 - discount);

            agg_map[gk].revenue_sum += revenue;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_agg_end = std::chrono::high_resolution_clock::now();
    double join_agg_ms = std::chrono::duration<double, std::milli>(t_join_agg_end - t_join_agg_start).count();
    printf("[TIMING] join_aggregate: %.2f ms (%zu groups)\n", join_agg_ms, agg_map.size());
#endif

    // Step 7: Convert to result rows and sort
#ifdef GENDB_PROFILE
    auto t_convert_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<ResultRow> results;
    results.reserve(agg_map.size());

    for (const auto& [gk, agg_val] : agg_map) {
        ResultRow row;
        row.c_custkey = gk.c_custkey;
        row.c_name = c_name_dict_rev[gk.c_name_code];
        row.revenue = agg_val.revenue_sum;  // Already in actual units
        row.c_acctbal = gk.c_acctbal;
        row.n_name = n_name_dict_rev[gk.n_name_code];
        row.c_address = c_address_dict_rev[gk.c_address_code];
        row.c_phone = c_phone_dict_rev[gk.c_phone_code];
        row.c_comment = c_comment_dict_rev[gk.c_comment_code];
        results.push_back(row);
    }

    // Sort by revenue DESC
    std::sort(results.begin(), results.end(), compare_revenue);

    // Limit to 20
    if (results.size() > 20) {
        results.resize(20);
    }

#ifdef GENDB_PROFILE
    auto t_convert_sort_end = std::chrono::high_resolution_clock::now();
    double convert_sort_ms = std::chrono::duration<double, std::milli>(t_convert_sort_end - t_convert_sort_start).count();
    printf("[TIMING] convert_sort: %.2f ms (%zu rows)\n", convert_sort_ms, results.size());
#endif

    // Step 8: Write to CSV
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q10.csv";
    std::ofstream out(output_path);

    // Header
    out << "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n";

    // Helper lambda to escape CSV field (quote if contains comma, quote, or newline)
    auto escape_csv = [](const std::string& s) -> std::string {
        bool needs_quote = false;
        for (char c : s) {
            if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                needs_quote = true;
                break;
            }
        }

        if (needs_quote) {
            std::string escaped = "\"";
            for (char c : s) {
                if (c == '"') escaped += "\"\"";
                else escaped += c;
            }
            escaped += "\"";
            return escaped;
        }
        return s;
    };

    // Rows
    for (const auto& row : results) {
        out << row.c_custkey << ",";
        out << escape_csv(row.c_name) << ",";
        // Revenue is already in actual units (double)
        out << std::fixed << std::setprecision(4)
            << row.revenue << ",";
        out << std::fixed << std::setprecision(2)
            << (double)row.c_acctbal / 100.0 << ",";
        out << escape_csv(row.n_name) << ",";
        out << escape_csv(row.c_address) << ",";
        out << escape_csv(row.c_phone) << ",";
        out << escape_csv(row.c_comment) << "\n";
    }

    out.close();

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

    std::cout << "Q10 completed. Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q10(gendb_dir, results_dir);

    return 0;
}
#endif
