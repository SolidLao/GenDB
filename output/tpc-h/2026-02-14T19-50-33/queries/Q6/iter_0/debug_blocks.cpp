#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <iomanip>
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
    
    size_t shipdate_size, discount_size, quantity_size, extendedprice_size;
    const int32_t* l_shipdate = (const int32_t*)mmapFile(dir + "l_shipdate.bin", shipdate_size);
    const double* l_discount = (const double*)mmapFile(dir + "l_discount.bin", discount_size);
    const double* l_quantity = (const double*)mmapFile(dir + "l_quantity.bin", quantity_size);
    const double* l_extendedprice = (const double*)mmapFile(dir + "l_extendedprice.bin", extendedprice_size);
    
    const size_t row_count = shipdate_size / sizeof(int32_t);
    const size_t block_size = 100000;
    const size_t num_blocks = (row_count + block_size - 1) / block_size;
    
    size_t shipdate_zm_size;
    const ZoneMapInt32* shipdate_zm = (const ZoneMapInt32*)mmapFile(dir + "l_shipdate.zonemap.idx", shipdate_zm_size);
    const size_t num_zones = shipdate_zm_size / sizeof(ZoneMapInt32);
    
    const int32_t shipdate_min = 8766;
    const int32_t shipdate_max = 9131;
    const double discount_min = 0.05;
    const double discount_max = 0.07;
    const double quantity_max = 24.0;
    
    // Build skip list using zone maps
    std::vector<bool> skip_block(num_blocks, false);
    for (size_t b = 0; b < num_blocks && b < num_zones; ++b) {
        if (shipdate_zm[b].max_val < shipdate_min || shipdate_zm[b].min_val >= shipdate_max) {
            skip_block[b] = true;
        }
    }
    
    // Count rows in two ways
    double revenue_with_zonemap = 0.0;
    size_t count_with_zonemap = 0;
    
    for (size_t block = 0; block < num_blocks; ++block) {
        if (block < skip_block.size() && skip_block[block]) continue;
        
        const size_t start_row = block * block_size;
        const size_t end_row = std::min(start_row + block_size, row_count);
        
        for (size_t i = start_row; i < end_row; ++i) {
            if (l_shipdate[i] >= shipdate_min &&
                l_shipdate[i] < shipdate_max &&
                l_discount[i] >= discount_min &&
                l_discount[i] <= discount_max &&
                l_quantity[i] < quantity_max) {
                
                revenue_with_zonemap += l_extendedprice[i] * l_discount[i];
                count_with_zonemap++;
            }
        }
    }
    
    double revenue_no_zonemap = 0.0;
    size_t count_no_zonemap = 0;
    
    for (size_t i = 0; i < row_count; ++i) {
        if (l_shipdate[i] >= shipdate_min &&
            l_shipdate[i] < shipdate_max &&
            l_discount[i] >= discount_min &&
            l_discount[i] <= discount_max &&
            l_quantity[i] < quantity_max) {
            
            revenue_no_zonemap += l_extendedprice[i] * l_discount[i];
            count_no_zonemap++;
        }
    }
    
    std::cout << "With zone map:\n";
    std::cout << "  Count: " << count_with_zonemap << "\n";
    std::cout << "  Revenue: " << std::fixed << std::setprecision(4) << revenue_with_zonemap << "\n";
    
    std::cout << "Without zone map:\n";
    std::cout << "  Count: " << count_no_zonemap << "\n";
    std::cout << "  Revenue: " << std::fixed << std::setprecision(4) << revenue_no_zonemap << "\n";
    
    std::cout << "Match: " << (count_with_zonemap == count_no_zonemap ? "YES" : "NO") << "\n";
    
    return 0;
}
