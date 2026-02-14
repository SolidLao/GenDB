#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <set>
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
    
    size_t discount_size;
    const double* l_discount = (const double*)mmapFile(dir + "l_discount.bin", discount_size);
    const size_t row_count = discount_size / sizeof(double);
    
    std::set<double> unique_discounts;
    for (size_t i = 0; i < row_count; ++i) {
        unique_discounts.insert(l_discount[i]);
    }
    
    std::cout << "Unique discount values:\n";
    for (double d : unique_discounts) {
        std::cout << std::setprecision(20) << d << std::endl;
    }
    
    return 0;
}
