#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int main() {
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders.o_orderkey.col", O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    
    const int32_t* data = (const int32_t*)ptr;
    
    std::cout << "First 10 orderkeys:\n";
    for (int i = 0; i < 10; ++i) {
        std::cout << "  [" << i << "]: " << data[i] << "\n";
    }
    
    // Check if orderkeys are sequential or scattered
    std::cout << "\nMin and max (first 1M):\n";
    int32_t min_val = data[0], max_val = data[0];
    for (int i = 0; i < 1000000 && i < 15000000; ++i) {
        if (data[i] < min_val) min_val = data[i];
        if (data[i] > max_val) max_val = data[i];
    }
    std::cout << "  Min: " << min_val << "\n";
    std::cout << "  Max: " << max_val << "\n";
    
    return 0;
}
