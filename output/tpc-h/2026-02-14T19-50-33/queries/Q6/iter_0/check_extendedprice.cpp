#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <iomanip>

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
    
    size_t extendedprice_size, discount_size;
    const double* l_extendedprice = (const double*)mmapFile(dir + "l_extendedprice.bin", extendedprice_size);
    const double* l_discount = (const double*)mmapFile(dir + "l_discount.bin", discount_size);
    
    std::cout << "First 10 extendedprice values:\n";
    for (int i = 0; i < 10; ++i) {
        std::cout << i << ": " << std::setprecision(20) << l_extendedprice[i] << std::endl;
    }
    
    std::cout << "\nFirst 10 discount values:\n";
    for (int i = 0; i < 10; ++i) {
        std::cout << i << ": " << std::setprecision(20) << l_discount[i] << std::endl;
    }
    
    // Check first product
    std::cout << "\nFirst product: " << std::setprecision(20) << (l_extendedprice[0] * l_discount[0]) << std::endl;
    
    return 0;
}
