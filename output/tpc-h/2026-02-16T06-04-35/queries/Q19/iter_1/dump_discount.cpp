#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cint>

int main() {
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_discount.bin", O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }
    
    struct stat st;
    fstat(fd, &st);
    int64_t* data = (int64_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    
    size_t count = st.st_size / sizeof(int64_t);
    std::cout << "First 20 discount values:\n";
    for (size_t i = 0; i < 20 && i < count; i++) {
        std::cout << "  [" << i << "] = " << data[i] << "\n";
    }
    
    std::cout << "\nMin/Max:\n";
    int64_t minval = data[0], maxval = data[0];
    for (size_t i = 1; i < count; i++) {
        if (data[i] < minval) minval = data[i];
        if (data[i] > maxval) maxval = data[i];
    }
    std::cout << "  Min: " << minval << "\n";
    std::cout << "  Max: " << maxval << "\n";
    
    // Count distribution
    std::cout << "\nDistribution:\n";
    for (int v = 0; v <= 10; v++) {
        int count = 0;
        for (size_t i = 0; i < count; i++) {
            if (data[i] == v) count++;
        }
        if (count > 0) std::cout << "  " << v << ": " << count << " rows\n";
    }
    
    return 0;
}
