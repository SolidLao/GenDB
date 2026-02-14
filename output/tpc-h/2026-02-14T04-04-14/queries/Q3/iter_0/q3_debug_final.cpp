#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <ctime>
#include <cmath>
#include <iomanip>

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
    madvise(ptr, filesize, MADV_SEQUENTIAL);
    return ptr;
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    
    uint8_t building_code = 4;
    int32_t orderdate_cutoff = 9204;
    int32_t shipdate_cutoff = 9204;
    
    size_t customer_size = 0, orders_size = 0, lineitem_size = 0;
    size_t o_orderdate_idx_size = 0;
    
    const int32_t* c_custkey = (const int32_t*)mmapFile(gendb_dir + "/customer.c_custkey.col", customer_size);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(gendb_dir + "/customer.c_mktsegment.col", customer_size);
    
    const int32_t* o_orderkey = (const int32_t*)mmapFile(gendb_dir + "/orders.o_orderkey.col", orders_size);
    const int32_t* o_custkey = (const int32_t*)mmapFile(gendb_dir + "/orders.o_custkey.col", orders_size);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(gendb_dir + "/orders.o_shippriority.col", orders_size);
    const int32_t* o_orderdate_idx = (const int32_t*)mmapFile(gendb_dir + "/orders.o_orderdate.sorted_idx", o_orderdate_idx_size);
    
    size_t num_customers = customer_size / sizeof(int32_t);
    size_t num_orders = orders_size / sizeof(int32_t);
    
    // Build map from row index to date
    std::unordered_map<int32_t, int32_t> o_orderdate_map;
    size_t num_o_idx_entries = o_orderdate_idx_size / (2 * sizeof(int32_t));
    for (size_t i = 0; i < num_o_idx_entries; ++i) {
        int32_t date_val = o_orderdate_idx[2 * i];
        int32_t row_id = o_orderdate_idx[2 * i + 1];
        o_orderdate_map[row_id] = date_val;
    }
    
    // Filter customer
    int building_count = 0;
    std::unordered_map<int32_t, bool> customer_hash;
    for (size_t i = 0; i < num_customers; ++i) {
        if (c_mktsegment[i] == building_code) {
            customer_hash[c_custkey[i]] = true;
            building_count++;
        }
    }
    
    std::cout << "Customers with BUILDING segment: " << building_count << "\n";
    
    // Filter orders
    int orders_pass_filter = 0;
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> orders_hash;
    
    for (size_t i = 0; i < num_orders; ++i) {
        if (customer_hash.count(o_custkey[i]) > 0) {
            int32_t orderdate = o_orderdate_map.count(i) > 0 ? o_orderdate_map[i] : 0;
            if (orderdate < orderdate_cutoff) {
                orders_hash[o_orderkey[i]] = {orderdate, o_shippriority[i]};
                orders_pass_filter++;
            }
        }
    }
    
    std::cout << "Orders pass filter: " << orders_pass_filter << "\n";
    
    // Print sample orders
    std::cout << "\nSample filtered orders (first 5):\n";
    int sample_count = 0;
    for (size_t i = 0; i < num_orders && sample_count < 5; ++i) {
        if (orders_hash.count(o_orderkey[i]) > 0) {
            auto& order = orders_hash[o_orderkey[i]];
            std::cout << "  orderkey=" << o_orderkey[i]
                      << ", orderdate=" << order.first << " (" << epochDaysToString(order.first) << ")"
                      << ", priority=" << order.second << "\n";
            sample_count++;
        }
    }
    
    return 0;
}
