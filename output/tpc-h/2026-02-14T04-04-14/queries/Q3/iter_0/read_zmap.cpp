#include <iostream>
#include <fstream>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint64_t start_row;
    uint64_t end_row;
};

int main() {
    std::string zmap_file = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders.o_orderdate.zmap";
    
    int fd = open(zmap_file.c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    size_t filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    
    const ZoneMapEntry* zones = (const ZoneMapEntry*)ptr;
    size_t num_zones = filesize / sizeof(ZoneMapEntry);
    
    std::cout << "Zone map entries: " << num_zones << "\n\n";
    
    for (size_t i = 0; i < num_zones && i < 10; ++i) {
        std::cout << "Zone " << i << ":\n";
        std::cout << "  min: " << zones[i].min_val << "\n";
        std::cout << "  max: " << zones[i].max_val << "\n";
        std::cout << "  start_row: " << zones[i].start_row << "\n";
        std::cout << "  end_row: " << zones[i].end_row << "\n\n";
    }
    
    return 0;
}
