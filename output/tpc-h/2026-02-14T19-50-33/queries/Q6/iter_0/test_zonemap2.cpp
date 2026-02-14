#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <iomanip>

struct ZoneMapInt32 {
    uint64_t start_row;
    int32_t min_val;
    int32_t max_val;
    uint64_t end_row;
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
    std::cout << "Target range: [8766, 9131)" << std::endl;
    
    int matching_zones = 0;
    for (size_t i = 0; i < std::min(size_t(10), num_zones); ++i) {
        bool overlaps = !(zm[i].max_val < 8766 || zm[i].min_val >= 9131);
        std::cout << "Zone " << i << ": start=" << zm[i].start_row 
                  << " end=" << zm[i].end_row
                  << " min=" << zm[i].min_val
                  << " max=" << zm[i].max_val
                  << (overlaps ? " [MATCH]" : " [SKIP]") << std::endl;
        if (overlaps) matching_zones++;
    }
    
    // Count total matching
    for (size_t i = 0; i < num_zones; ++i) {
        if (!(zm[i].max_val < 8766 || zm[i].min_val >= 9131)) {
            matching_zones++;
        }
    }
    std::cout << "Total matching zones: " << matching_zones << "/" << num_zones << std::endl;
    
    munmap(ptr, sb.st_size);
    return 0;
}
