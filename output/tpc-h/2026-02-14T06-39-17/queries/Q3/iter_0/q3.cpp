#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <thread>
#include <mutex>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <queue>
#include <iomanip>
#include <ctime>
#include <chrono>

struct ResultRow {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Memory-mapped file wrapper
class MmapFile {
public:
    MmapFile(const std::string& path) : ptr(nullptr), size(0), fd(-1) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Cannot open file: " + path);
        }
        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            close(fd);
            throw std::runtime_error("Cannot stat file: " + path);
        }
        size = sb.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("Cannot mmap file: " + path);
        }
        madvise(ptr, size, MADV_SEQUENTIAL);
    }

    ~MmapFile() {
        if (ptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }

    const char* data() const { return (const char*)ptr; }
    size_t get_size() const { return size; }

private:
    void* ptr;
    size_t size;
    int fd;
};

// Load dictionary for c_mktsegment
std::unordered_map<uint8_t, std::string> loadDictionary(const std::string& dict_path) {
    std::unordered_map<uint8_t, std::string> dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open dictionary: " + dict_path);
    }
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            uint8_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Convert epoch days to YYYY-MM-DD string
inline std::string epochDaysToString(int32_t days) {
    std::time_t t = (std::time_t)days * 86400LL;
    struct tm* tm_ptr = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_ptr);
    return std::string(buf);
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Constants for date filtering
    const int32_t cutoff_orderdate = 9204;    // 1995-03-15 in epoch days
    const int32_t cutoff_shipdate = 9204;     // 1995-03-15 in epoch days

    // Load dictionaries
    std::string dict_path = gendb_dir + "/customer/c_mktsegment.bin.dict";
    auto mktsegment_dict = loadDictionary(dict_path);
    uint8_t building_code = 0;
    for (auto& [code, value] : mktsegment_dict) {
        if (value == "BUILDING") {
            building_code = code;
            break;
        }
    }

    // mmap files
    MmapFile customer_custkey(gendb_dir + "/customer/c_custkey.bin");
    MmapFile customer_mktsegment(gendb_dir + "/customer/c_mktsegment.bin");

    MmapFile orders_orderkey(gendb_dir + "/orders/o_orderkey.bin");
    MmapFile orders_custkey(gendb_dir + "/orders/o_custkey.bin");
    MmapFile orders_orderdate(gendb_dir + "/orders/o_orderdate.bin");
    MmapFile orders_shippriority(gendb_dir + "/orders/o_shippriority.bin");

    MmapFile lineitem_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    MmapFile lineitem_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MmapFile lineitem_discount(gendb_dir + "/lineitem/l_discount.bin");
    MmapFile lineitem_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");

    const int32_t* c_custkey_data = (const int32_t*)customer_custkey.data();
    const uint8_t* c_mktsegment_data = (const uint8_t*)customer_mktsegment.data();

    const int32_t* o_orderkey_data = (const int32_t*)orders_orderkey.data();
    const int32_t* o_custkey_data = (const int32_t*)orders_custkey.data();
    const int32_t* o_orderdate_data = (const int32_t*)orders_orderdate.data();
    const int32_t* o_shippriority_data = (const int32_t*)orders_shippriority.data();

    const int32_t* l_orderkey_data = (const int32_t*)lineitem_orderkey.data();
    const double* l_extendedprice_data = (const double*)lineitem_extendedprice.data();
    const double* l_discount_data = (const double*)lineitem_discount.data();
    const int32_t* l_shipdate_data = (const int32_t*)lineitem_shipdate.data();

    // Row counts
    size_t customer_rows = customer_custkey.get_size() / sizeof(int32_t);
    size_t orders_rows = orders_orderkey.get_size() / sizeof(int32_t);
    size_t lineitem_rows = lineitem_orderkey.get_size() / sizeof(int32_t);

    // Step 1: Filter customer by c_mktsegment = 'BUILDING'
    std::unordered_map<int32_t, int32_t> customer_custkey_map;
    for (size_t i = 0; i < customer_rows; ++i) {
        if (c_mktsegment_data[i] == building_code) {
            customer_custkey_map[c_custkey_data[i]] = c_custkey_data[i];
        }
    }

    // Step 2: Hash join orders with customer, filter by o_orderdate < cutoff_orderdate
    struct OrderRecord {
        int32_t orderkey;
        int32_t orderdate;
        int32_t shippriority;
    };
    std::unordered_map<int32_t, std::vector<OrderRecord>> orders_map;

    for (size_t i = 0; i < orders_rows; ++i) {
        int32_t custkey = o_custkey_data[i];
        if (customer_custkey_map.count(custkey) && o_orderdate_data[i] < cutoff_orderdate) {
            orders_map[o_orderkey_data[i]].push_back({
                o_orderkey_data[i],
                o_orderdate_data[i],
                o_shippriority_data[i]
            });
        }
    }

    // Step 3: Hash join lineitem with orders, filter by l_shipdate > cutoff_shipdate
    // Group by (l_orderkey, o_orderdate, o_shippriority) and aggregate
    struct GroupKey {
        int32_t l_orderkey;
        int32_t o_orderdate;
        int32_t o_shippriority;

        bool operator<(const GroupKey& other) const {
            if (l_orderkey != other.l_orderkey) return l_orderkey < other.l_orderkey;
            if (o_orderdate != other.o_orderdate) return o_orderdate < other.o_orderdate;
            return o_shippriority < other.o_shippriority;
        }
    };

    std::map<GroupKey, double> aggregates;

    for (size_t i = 0; i < lineitem_rows; ++i) {
        int32_t orderkey = l_orderkey_data[i];

        if (orders_map.count(orderkey) && l_shipdate_data[i] > cutoff_shipdate) {
            double revenue_item = l_extendedprice_data[i] * (1.0 - l_discount_data[i]);

            for (const auto& order : orders_map[orderkey]) {
                GroupKey key = {orderkey, order.orderdate, order.shippriority};
                aggregates[key] += revenue_item;
            }
        }
    }

    // Convert to result rows
    std::vector<ResultRow> result_groups;
    for (const auto& [key, revenue] : aggregates) {
        result_groups.push_back({key.l_orderkey, revenue, key.o_orderdate, key.o_shippriority});
    }

    // Step 4: Sort by revenue DESC, o_orderdate ASC and limit to 10
    std::sort(result_groups.begin(), result_groups.end(),
        [](const ResultRow& a, const ResultRow& b) {
            if (a.revenue != b.revenue) {
                return a.revenue > b.revenue;  // DESC
            }
            return a.o_orderdate < b.o_orderdate;  // ASC
        }
    );

    std::vector<ResultRow> results = result_groups;

    // Keep only top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    // Step 5: Write CSV output
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q3.csv");
        outfile << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (const auto& row : results) {
            outfile << row.l_orderkey << ","
                    << std::fixed << std::setprecision(4) << row.revenue << ","
                    << epochDaysToString(row.o_orderdate) << ","
                    << row.o_shippriority << "\n";
        }
        outfile.close();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Query returned " << (results.size() > 10 ? 10 : results.size()) << " rows\n";
    std::cout << "Execution time: " << duration.count() << " ms\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    try {
        run_q3(argv[1], argc > 2 ? argv[2] : "");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
#endif
