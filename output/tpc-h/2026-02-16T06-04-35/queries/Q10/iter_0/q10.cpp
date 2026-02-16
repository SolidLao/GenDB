#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <omp.h>
#include <numeric>

// Helper struct for GROUP BY aggregation
struct GroupKey {
    int32_t c_custkey;
    int32_t c_nationkey;
    // We'll include these fields as strings in the map value to avoid composite hashing complexity
};

struct GroupValue {
    std::string c_name;
    std::string c_address;
    std::string c_phone;
    std::string c_comment;
    std::string n_name;
    int64_t c_acctbal;
    int64_t revenue;  // sum of l_extendedprice * (1 - l_discount) as int64_t
};

// Hash function for GroupKey
struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        return ((size_t)k.c_custkey << 32) | ((size_t)k.c_nationkey & 0xFFFFFFFF);
    }
};

struct GroupKeyEqual {
    bool operator()(const GroupKey& a, const GroupKey& b) const {
        return a.c_custkey == b.c_custkey && a.c_nationkey == b.c_nationkey;
    }
};

// Memory-mapped file helper
template<typename T>
class MmapColumn {
public:
    T* data;
    size_t count;
    int fd;
    void* mapped;

    MmapColumn() : data(nullptr), count(0), fd(-1), mapped(nullptr) {}

    bool load(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Error opening " << path << std::endl;
            return false;
        }

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            std::cerr << "Error stat " << path << std::endl;
            close(fd);
            return false;
        }

        count = sb.st_size / sizeof(T);
        mapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (mapped == MAP_FAILED) {
            std::cerr << "Error mmap " << path << std::endl;
            close(fd);
            return false;
        }

        data = (T*)mapped;
        return true;
    }

    ~MmapColumn() {
        if (mapped && mapped != MAP_FAILED) munmap(mapped, count * sizeof(T));
        if (fd >= 0) close(fd);
    }

    T& operator[](size_t i) { return data[i]; }
    size_t size() const { return count; }
};

// Load dictionary for l_returnflag
int8_t loadDictCode(const std::string& dictPath, const std::string& target) {
    std::ifstream dict(dictPath);
    std::string line;
    while (std::getline(dict, line)) {
        if (line.empty()) continue;
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int8_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            if (value == target) return code;
        }
    }
    return -1;  // Not found
}

// Load string column (stored as offset table + string data)
// Layout: [4-byte count] [4-byte padding] [count*4-byte offsets] [string data]
// String data starts at offset 8 + count*4 - 4 (last offset value overlaps with string data start)
// Offsets are END-boundaries: offset[i] points to the end of string i (relative to string data start)
bool loadStringColumn(const std::string& path, std::vector<std::string>& col, size_t count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return false;
    }

    void* mapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        return false;
    }

    const char* data = (const char*)mapped;

    // Read header
    uint32_t num_rows = *((uint32_t*)data);

    // Offset table is at data + 8, each entry is 4 bytes
    const uint32_t* offsets = (const uint32_t*)(data + 8);

    // String data starts at: 8 + num_rows*4 - 4 (offset table overlaps with string data by 4 bytes)
    size_t offset_table_byte_end = 8 + num_rows * 4;
    const char* string_data = data + offset_table_byte_end - 4;
    size_t string_data_size = sb.st_size - (offset_table_byte_end - 4);

    col.resize(count);
    for (size_t i = 0; i < count; i++) {
        uint32_t start = (i == 0) ? 0 : offsets[i - 1];
        uint32_t end = (i < num_rows) ? offsets[i] : string_data_size;
        if (start < string_data_size && end <= string_data_size && start <= end) {
            col[i] = std::string(string_data + start, end - start);
        } else {
            col[i] = "";
        }
    }

    munmap(mapped, sb.st_size);
    close(fd);
    return true;
}

// Convert epoch days to YYYY-MM-DD string
std::string epochDaysToDate(int32_t days) {
    // days since 1970-01-01
    int year = 1970;
    int month = 1;
    int day = days + 1;  // days are 0-indexed

    // Approximate year
    while (day > 365) {
        bool leap = (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
        int daysInYear = leap ? 366 : 365;
        if (day > daysInYear) {
            day -= daysInYear;
            year++;
        } else {
            break;
        }
    }

    // Find month
    const int daysPerMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0);
    if (leap) {
        const_cast<int*>(daysPerMonth)[1] = 29;
    }
    for (month = 1; month <= 12; month++) {
        if (day <= daysPerMonth[month - 1]) break;
        day -= daysPerMonth[month - 1];
    }

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << year << "-"
        << std::setw(2) << month << "-" << std::setw(2) << day;
    return oss.str();
}

void run_q10(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    // Load date constants
    // 1993-10-01 and 1993-10-01 + 3 months = 1993-12-31 (since October+3 months goes to December)
    // Compute epoch days: 1993-10-01
    // Years 1970-1992: 23 years, 5 leap years (1972, 1976, 1980, 1984, 1988, 1992)
    // Days = 18 * 365 + 5 * 366 = 6570 + 1830 = 8400
    // Months Jan-Sep 1993: 31+28+31+30+31+30+31+31+30 = 273
    // Days = 8400 + 273 + 1 (Oct 1) = 8674
    int32_t date_start = 8674;  // 1993-10-01
    int32_t date_end = 8795;    // 1993-12-31 (Oct 31 + Nov 30 + Dec 31 = 92 days later)

    // Load columns
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    // Customer columns
    MmapColumn<int32_t> c_custkey, c_nationkey;
    MmapColumn<int64_t> c_acctbal;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;

    if (!c_custkey.load(gendb_dir + "/customer/c_custkey.bin")) {
        std::cerr << "Failed to load c_custkey" << std::endl;
        return;
    }
    if (!c_nationkey.load(gendb_dir + "/customer/c_nationkey.bin")) {
        std::cerr << "Failed to load c_nationkey" << std::endl;
        return;
    }
    if (!c_acctbal.load(gendb_dir + "/customer/c_acctbal.bin")) {
        std::cerr << "Failed to load c_acctbal" << std::endl;
        return;
    }
    if (!loadStringColumn(gendb_dir + "/customer/c_name.bin", c_name, c_custkey.size())) {
        std::cerr << "Failed to load c_name" << std::endl;
        return;
    }
    if (!loadStringColumn(gendb_dir + "/customer/c_address.bin", c_address, c_custkey.size())) {
        std::cerr << "Failed to load c_address" << std::endl;
        return;
    }
    if (!loadStringColumn(gendb_dir + "/customer/c_phone.bin", c_phone, c_custkey.size())) {
        std::cerr << "Failed to load c_phone" << std::endl;
        return;
    }
    if (!loadStringColumn(gendb_dir + "/customer/c_comment.bin", c_comment, c_custkey.size())) {
        std::cerr << "Failed to load c_comment" << std::endl;
        return;
    }

    size_t customer_count = c_custkey.size();

    // Orders columns
    MmapColumn<int32_t> o_orderkey, o_custkey, o_orderdate;
    if (!o_orderkey.load(gendb_dir + "/orders/o_orderkey.bin")) {
        std::cerr << "Failed to load o_orderkey" << std::endl;
        return;
    }
    if (!o_custkey.load(gendb_dir + "/orders/o_custkey.bin")) {
        std::cerr << "Failed to load o_custkey" << std::endl;
        return;
    }
    if (!o_orderdate.load(gendb_dir + "/orders/o_orderdate.bin")) {
        std::cerr << "Failed to load o_orderdate" << std::endl;
        return;
    }

    size_t orders_count = o_orderkey.size();

    // Lineitem columns
    MmapColumn<int32_t> l_orderkey;
    MmapColumn<int64_t> l_extendedprice, l_discount;
    MmapColumn<int8_t> l_returnflag;

    if (!l_orderkey.load(gendb_dir + "/lineitem/l_orderkey.bin")) {
        std::cerr << "Failed to load l_orderkey" << std::endl;
        return;
    }
    if (!l_extendedprice.load(gendb_dir + "/lineitem/l_extendedprice.bin")) {
        std::cerr << "Failed to load l_extendedprice" << std::endl;
        return;
    }
    if (!l_discount.load(gendb_dir + "/lineitem/l_discount.bin")) {
        std::cerr << "Failed to load l_discount" << std::endl;
        return;
    }
    if (!l_returnflag.load(gendb_dir + "/lineitem/l_returnflag.bin")) {
        std::cerr << "Failed to load l_returnflag" << std::endl;
        return;
    }

    size_t lineitem_count = l_orderkey.size();

    // Nation columns
    MmapColumn<int32_t> n_nationkey;
    std::vector<std::string> n_name;

    if (!n_nationkey.load(gendb_dir + "/nation/n_nationkey.bin")) {
        std::cerr << "Failed to load n_nationkey" << std::endl;
        return;
    }
    if (!loadStringColumn(gendb_dir + "/nation/n_name.bin", n_name, n_nationkey.size())) {
        std::cerr << "Failed to load n_name" << std::endl;
        return;
    }

    size_t nation_count = n_nationkey.size();

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    // Load dictionary for l_returnflag
    int8_t returnflag_R = loadDictCode(gendb_dir + "/lineitem/l_returnflag_dict.txt", "R");
    if (returnflag_R < 0) {
        std::cerr << "Could not find 'R' in l_returnflag dictionary" << std::endl;
        return;
    }

    // Build nation lookup (small dimension)
#ifdef GENDB_PROFILE
    auto t_build_nation_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, std::string> nation_map;
    for (size_t i = 0; i < nation_count; i++) {
        nation_map[n_nationkey[i]] = n_name[i];
    }

#ifdef GENDB_PROFILE
    auto t_build_nation_end = std::chrono::high_resolution_clock::now();
    double ms_build_nation = std::chrono::duration<double, std::milli>(t_build_nation_end - t_build_nation_start).count();
    printf("[TIMING] build_nation_lookup: %.2f ms\n", ms_build_nation);
#endif

    // Build customer lookup (with c_custkey -> customer data)
#ifdef GENDB_PROFILE
    auto t_build_customer_start = std::chrono::high_resolution_clock::now();
#endif

    struct CustomerData {
        std::string name;
        std::string address;
        std::string phone;
        std::string comment;
        int64_t acctbal;
        int32_t nationkey;
    };

    std::unordered_map<int32_t, CustomerData> customer_map;
    for (size_t i = 0; i < customer_count; i++) {
        customer_map[c_custkey[i]] = {
            c_name[i],
            c_address[i],
            c_phone[i],
            c_comment[i],
            c_acctbal[i],
            c_nationkey[i]
        };
    }

#ifdef GENDB_PROFILE
    auto t_build_customer_end = std::chrono::high_resolution_clock::now();
    double ms_build_customer = std::chrono::duration<double, std::milli>(t_build_customer_end - t_build_customer_start).count();
    printf("[TIMING] build_customer_lookup: %.2f ms\n", ms_build_customer);
#endif

    // Build orders lookup (o_orderkey -> o_custkey, filtered by date)
#ifdef GENDB_PROFILE
    auto t_build_orders_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<int32_t, int32_t> orders_map;  // o_orderkey -> o_custkey
    size_t orders_filtered = 0;
    for (size_t i = 0; i < orders_count; i++) {
        if (o_orderdate[i] >= date_start && o_orderdate[i] < date_end) {
            orders_map[o_orderkey[i]] = o_custkey[i];
            orders_filtered++;
        }
    }

#ifdef GENDB_PROFILE
    auto t_build_orders_end = std::chrono::high_resolution_clock::now();
    double ms_build_orders = std::chrono::duration<double, std::milli>(t_build_orders_end - t_build_orders_start).count();
    printf("[TIMING] build_orders_lookup: %.2f ms\n", ms_build_orders);
    printf("[TIMING] orders_filtered_count: %zu\n", orders_filtered);
#endif

    // Scan lineitem with filter and aggregation
#ifdef GENDB_PROFILE
    auto t_scan_lineitem_start = std::chrono::high_resolution_clock::now();
#endif

    std::unordered_map<std::string, GroupValue> agg_map;

    for (size_t i = 0; i < lineitem_count; i++) {
        // Filter: l_returnflag = 'R'
        if (l_returnflag[i] != returnflag_R) continue;

        int32_t order_key = l_orderkey[i];

        // Join with orders
        auto orders_it = orders_map.find(order_key);
        if (orders_it == orders_map.end()) continue;

        int32_t cust_key = orders_it->second;

        // Join with customer
        auto cust_it = customer_map.find(cust_key);
        if (cust_it == customer_map.end()) continue;

        const CustomerData& cust = cust_it->second;

        // Get nation name
        auto nation_it = nation_map.find(cust.nationkey);
        if (nation_it == nation_map.end()) continue;

        const std::string& nat_name = nation_it->second;

        // Create group key
        char group_key[256];
        snprintf(group_key, 256, "%d|%s|%ld|%s|%s|%s|%s",
                 cust_key, cust.name.c_str(), cust.acctbal, cust.phone.c_str(),
                 nat_name.c_str(), cust.address.c_str(), cust.comment.c_str());

        // Compute l_extendedprice * (1 - l_discount)
        // Both are scaled by 100
        // (1 - discount_scaled/100) = (100 - discount_scaled) / 100
        // price * (100 - discount) / 100
        int64_t adjusted_price = l_extendedprice[i] * (100 - l_discount[i]) / 100;

        if (agg_map.find(group_key) == agg_map.end()) {
            agg_map[group_key] = {
                cust.name,
                cust.address,
                cust.phone,
                cust.comment,
                nat_name,
                cust.acctbal,
                adjusted_price
            };
        } else {
            agg_map[group_key].revenue += adjusted_price;
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_scan_lineitem = std::chrono::duration<double, std::milli>(t_scan_lineitem_end - t_scan_lineitem_start).count();
    printf("[TIMING] scan_filter_join_agg: %.2f ms\n", ms_scan_lineitem);
    printf("[TIMING] agg_result_count: %zu\n", agg_map.size());
#endif

    // Sort by revenue DESC
#ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::pair<std::string, GroupValue>> results;
    for (auto& p : agg_map) {
        results.push_back(p);
    }

    std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
        return a.second.revenue > b.second.revenue;
    });

#ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
#endif

    // Write output (LIMIT 20)
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_path = results_dir + "/Q10.csv";
    std::ofstream out(output_path);
    out << "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n";

    size_t limit = std::min((size_t)20, results.size());
    for (size_t i = 0; i < limit; i++) {
        const auto& group_val = results[i].second;

        // Extract c_custkey from group key
        char* key_copy = strdup(results[i].first.c_str());
        int32_t c_custkey_val = std::stoi(std::string(strtok(key_copy, "|")));
        free(key_copy);

        // Format revenue: scaled int64 / 100 to 2 decimal places
        double revenue_double = (double)group_val.revenue / 100.0;

        out << c_custkey_val << ",";
        out << group_val.c_name << ",";
        out << std::fixed << std::setprecision(2) << revenue_double << ",";
        out << std::fixed << std::setprecision(2) << ((double)group_val.c_acctbal / 100.0) << ",";
        out << group_val.n_name << ",";
        out << group_val.c_address << ",";
        out << group_val.c_phone << ",";
        out << group_val.c_comment << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    std::cout << "Query execution completed. Results written to " << output_path << std::endl;
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
