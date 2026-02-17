#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iomanip>

template<typename T>
T* mmap_file(const std::string& path, size_t& out_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return nullptr;
    struct stat sb;
    if (fstat(fd, &sb) < 0) { close(fd); return nullptr; }
    out_count = sb.st_size / sizeof(T);
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    return (T*)ptr;
}

int main() {
    size_t count = 0;
    int64_t* extprice = mmap_file<int64_t>("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_extendedprice.bin", count);
    int64_t* discount = mmap_file<int64_t>("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_discount.bin", count);
    
    // Sample a few rows
    for (int i = 0; i < 5; i++) {
        int64_t ep = extprice[i];
        int64_t disc = discount[i];
        int64_t rev = ep - (ep * disc / 100);
        
        double ep_dec = ep / 100.0;
        double disc_dec = disc / 100.0;
        double rev_dec = rev / 100.0;
        
        std::cout << "Row " << i << ":" << std::endl;
        std::cout << "  extprice scaled: " << ep << " (decimal: " << std::fixed << std::setprecision(2) << ep_dec << ")" << std::endl;
        std::cout << "  discount scaled: " << disc << " (decimal: " << std::fixed << std::setprecision(4) << disc_dec << ")" << std::endl;
        std::cout << "  revenue scaled:  " << rev << " (decimal: " << std::fixed << std::setprecision(4) << rev_dec << ")" << std::endl;
        std::cout << "  expected: " << ep_dec * (1.0 - disc_dec) << std::endl << std::endl;
    }
    
    return 0;
}
