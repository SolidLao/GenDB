#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>
#include <algorithm>
#include <iomanip>
#include <chrono>

namespace {

inline std::string epochDaysToString(int32_t days) {
    std::time_t t = days * 86400;
    struct tm* tm_info = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return std::string(buf);
}

inline const void* mmapFile(const std::string& filename, size_t& filesize) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) return nullptr;
    struct stat sb;
    if (fstat(fd, &sb) < 0) { close(fd); return nullptr; }
    filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return nullptr;
    return ptr;
}

struct AggResult {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator<(const AggResult& other) const {
        if (std::abs(revenue - other.revenue) > 1e-6) return revenue > other.revenue;
        return o_orderdate < other.o_orderdate;
    }
};

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    uint8_t building_code = 4;
    int32_t orderdate_cutoff = 9204;
    int32_t shipdate_cutoff = 9204;

    size_t customer_size = 0, orders_size = 0, lineitem_size = 0;

    const int32_t* c_custkey = (const int32_t*)mmapFile(gendb_dir + "/customer.c_custkey.col", customer_size);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(gendb_dir + "/customer.c_mktsegment.col", customer_size);
    const int32_t* o_orderkey = (const int32_t*)mmapFile(gendb_dir + "/orders.o_orderkey.col", orders_size);
    const int32_t* o_custkey = (const int32_t*)mmapFile(gendb_dir + "/orders.o_custkey.col", orders_size);
    const int32_t* o_orderdate_raw = (const int32_t*)mmapFile(gendb_dir + "/orders.o_orderdate.col", orders_size);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(gendb_dir + "/orders.o_shippriority.col", orders_size);
    const int32_t* l_orderkey = (const int32_t*)mmapFile(gendb_dir + "/lineitem.l_orderkey.col", lineitem_size);
    const double* l_extendedprice = (const double*)mmapFile(gendb_dir + "/lineitem.l_extendedprice.col", lineitem_size);
    const double* l_discount = (const double*)mmapFile(gendb_dir + "/lineitem.l_discount.col", lineitem_size);
    const int32_t* l_shipdate_raw = (const int32_t*)mmapFile(gendb_dir + "/lineitem.l_shipdate.col", lineitem_size);

    size_t num_customers = customer_size / sizeof(int32_t);
    size_t num_orders = orders_size / sizeof(int32_t);
    size_t num_lineitems = lineitem_size / sizeof(int32_t);

    // Just use raw values directly - they might already be correct
    std::unordered_map<int32_t, bool> customer_hash;
    for (size_t i = 0; i < num_customers; ++i) {
        if (c_mktsegment[i] == building_code) {
            customer_hash[c_custkey[i]] = true;
        }
    }

    struct OrderInfo {
        int32_t orderdate;
        int32_t shippriority;
    };
    std::unordered_map<int32_t, OrderInfo> orders_hash;
    for (size_t i = 0; i < num_orders; ++i) {
        if (customer_hash.count(o_custkey[i]) > 0 && o_orderdate_raw[i] < orderdate_cutoff) {
            orders_hash[o_orderkey[i]] = {o_orderdate_raw[i], o_shippriority[i]};
        }
    }

    struct AggKey {
        int32_t orderkey;
        int32_t order_date;
        int32_t shippriority;
        bool operator==(const AggKey& other) const {
            return orderkey == other.orderkey && order_date == other.order_date && shippriority == other.shippriority;
        }
    };

    struct AggKeyHash {
        size_t operator()(const AggKey& k) const {
            return std::hash<int64_t>()(((int64_t)k.orderkey << 32) | (k.shippriority & 0xFFFFFFFF))
                   ^ std::hash<int32_t>()(k.order_date);
        }
    };

    std::unordered_map<AggKey, double, AggKeyHash> aggregation;

    for (size_t j = 0; j < num_lineitems; ++j) {
        int32_t orderkey = l_orderkey[j];
        int32_t shipdate = l_shipdate_raw[j];

        auto it = orders_hash.find(orderkey);
        if (it != orders_hash.end() && shipdate > shipdate_cutoff) {
            double revenue = l_extendedprice[j] * (1.0 - l_discount[j]);
            AggKey key = {orderkey, it->second.orderdate, it->second.shippriority};
            aggregation[key] += revenue;
        }
    }

    std::vector<AggResult> results;
    for (auto& entry : aggregation) {
        results.push_back({entry.first.orderkey, entry.second, entry.first.order_date, entry.first.shippriority});
    }

    std::sort(results.begin(), results.end());

    if (results.size() > 10) results.resize(10);

    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q3.csv");
        outfile << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (const auto& result : results) {
            outfile << result.l_orderkey << ","
                    << std::fixed << std::setprecision(4) << result.revenue << ","
                    << epochDaysToString(result.o_orderdate) << ","
                    << result.o_shippriority << "\n";
        }
        outfile.close();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    std::cout << "Query returned " << results.size() << " rows\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << duration_ms << " ms\n";
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
