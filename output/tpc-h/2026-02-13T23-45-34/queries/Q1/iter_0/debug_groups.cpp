#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <cmath>
#include <algorithm>

struct AggregateState {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_quantity_for_avg = 0.0;
    double sum_price_for_avg = 0.0;
    double sum_disc_for_avg = 0.0;
    int64_t count = 0;
};

void* mmap_file(const std::string& filename, size_t& size) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) return nullptr;
    off_t file_size = lseek(fd, 0, SEEK_END);
    size = static_cast<size_t>(file_size);
    void* ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    return ptr;
}

int main() {
    const std::string lineitem_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/";
    
    size_t sz_shipdate = 0, sz_returnflag = 0, sz_linestatus = 0;
    
    int32_t* l_shipdate = static_cast<int32_t*>(mmap_file(lineitem_dir + "l_shipdate.bin", sz_shipdate));
    uint8_t* l_returnflag = static_cast<uint8_t*>(mmap_file(lineitem_dir + "l_returnflag.bin", sz_returnflag));
    uint8_t* l_linestatus = static_cast<uint8_t*>(mmap_file(lineitem_dir + "l_linestatus.bin", sz_linestatus));
    
    int64_t num_rows = sz_shipdate / sizeof(int32_t);
    int32_t target_shipdate = 10500;
    
    // Count groups
    std::map<std::pair<uint8_t, uint8_t>, int64_t> group_counts;
    for (int64_t i = 0; i < num_rows && i < 1000000; ++i) {  // Sample first 1M rows
        if (l_shipdate[i] <= target_shipdate) {
            group_counts[{l_returnflag[i], l_linestatus[i]}]++;
        }
    }
    
    std::cout << "Groups found in first 1M rows (raw bytes):\n";
    for (auto& p : group_counts) {
        std::cout << (int)p.first.first << "," << (int)p.first.second << ": " << p.second << " rows\n";
    }
    
    return 0;
}
