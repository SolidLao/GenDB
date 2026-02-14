#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <iomanip>

struct ZoneMapInt32 {
    uint64_t start_row;
    uint64_t end_row;
    int32_t min_val;
    int32_t max_val;
};

int main() {
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_shipdate.zonemap.idx", O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    
    const ZoneMapInt32* zm = (const ZoneMapInt32*)ptr;
    size_t num_zones = sb.st_size / sizeof(ZoneMapInt32);
    
    std::cout << "Total zones: " << num_zones << std::endl;
    std::cout << "Size per zone: " << sizeof(ZoneMapInt32) << " bytes" << std::endl;
    
    for (size_t i = 0; i < std::min(size_t(5), num_zones); ++i) {
        std::cout << "Zone " << i << ": start=" << zm[i].start_row 
                  << " end=" << zm[i].end_row
                  << " min=" << zm[i].min_val
                  << " max=" << zm[i].max_val << std::endl;
    }
    
    munmap(ptr, sb.st_size);
    return 0;
}
