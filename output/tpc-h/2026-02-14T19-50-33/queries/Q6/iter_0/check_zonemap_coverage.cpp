#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <vector>

struct ZoneMapInt32 {
    uint64_t start_row;
    int32_t min_val;
    int32_t max_val;
    uint64_t end_row;
};

void* mmapFile(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    size_out = sb.st_size;
    void* ptr = mmap(nullptr, size_out, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return ptr;
}

int main() {
    const std::string dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/";
    
    size_t zm_size;
    const ZoneMapInt32* zm = (const ZoneMapInt32*)mmapFile(dir + "l_shipdate.zonemap.idx", zm_size);
    size_t num_zones = zm_size / sizeof(ZoneMapInt32);
    
    const int32_t shipdate_min = 8766;
    const int32_t shipdate_max = 9131;
    
    std::cout << "Checking zone map coverage for shipdate range [" << shipdate_min << ", " << shipdate_max << ")\n";
    
    size_t total_rows_in_zones = 0;
    for (size_t i = 0; i < num_zones; ++i) {
        bool overlaps = !(zm[i].max_val < shipdate_min || zm[i].min_val >= shipdate_max);
        if (overlaps) {
            std::cout << "Zone " << i << ": rows [" << zm[i].start_row << ", " << zm[i].end_row 
                     << "), dates [" << zm[i].min_val << ", " << zm[i].max_val << "]" << std::endl;
            total_rows_in_zones += (zm[i].end_row - zm[i].start_row);
        }
    }
    
    std::cout << "\nTotal rows to scan: " << total_rows_in_zones << std::endl;
    
    return 0;
}
