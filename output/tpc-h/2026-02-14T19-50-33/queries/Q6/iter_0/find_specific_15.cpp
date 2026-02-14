#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <iomanip>
#include <vector>
#include <cmath>

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
    
    const int32_t shipdate_min = 8766;
    const int32_t shipdate_max = 9131;
    const double discount_min = 0.05;
    const double discount_max = 0.07;
    const double quantity_max = 24.0;
    
    std::vector<size_t> suspicious_rows;
    
    for (size_t i = 0; i < row_count; ++i) {
        if (l_shipdate[i] >= shipdate_min &&
            l_shipdate[i] < shipdate_max &&
            l_discount[i] >= discount_min &&
            l_discount[i] <= discount_max &&
            l_quantity[i] < quantity_max) {
            
            // Check if quantity is very close to 24.0
            if (l_quantity[i] > 23.99) {
                suspicious_rows.push_back(i);
            }
        }
    }
    
    std::cout << "Rows with quantity very close to 24.0: " << suspicious_rows.size() << std::endl;
    std::cout << "First 20:\n";
    for (size_t j = 0; j < std::min(size_t(20), suspicious_rows.size()); ++j) {
        size_t i = suspicious_rows[j];
        std::cout << "Row " << i << ": quantity=" << std::setprecision(20) << l_quantity[i] 
                 << " (rounded to .02: " << std::fixed << std::setprecision(2) << l_quantity[i] << ")"
                 << std::endl;
    }
    
    return 0;
}
