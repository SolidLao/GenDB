#include <iostream>
#include <map>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>

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
    
    // Count all unique values
    std::map<std::pair<uint8_t, uint8_t>, int64_t> all_groups;
    for (int64_t i = 0; i < num_rows; ++i) {
        all_groups[{l_returnflag[i], l_linestatus[i]}]++;
    }
    
    std::cout << "ALL groups in dataset:\n";
    for (auto& p : all_groups) {
        std::cout << (int)p.first.first << "," << (int)p.first.second << ": " << p.second << " rows\n";
    }
    
    // Count filtered groups
    std::map<std::pair<uint8_t, uint8_t>, int64_t> filtered_groups;
    for (int64_t i = 0; i < num_rows; ++i) {
        if (l_shipdate[i] <= target_shipdate) {
            filtered_groups[{l_returnflag[i], l_linestatus[i]}]++;
        }
    }
    
    std::cout << "\nFiltered groups (date <= 10500):\n";
    for (auto& p : filtered_groups) {
        std::cout << (int)p.first.first << "," << (int)p.first.second << ": " << p.second << " rows\n";
    }
    
    return 0;
}
