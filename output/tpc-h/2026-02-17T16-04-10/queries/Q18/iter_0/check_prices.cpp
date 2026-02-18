#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int main() {
    std::string path = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders/o_totalprice.bin";
    int fd = open(path.c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    
    void* mapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    
    int64_t* data = (int64_t*)mapped;
    size_t count = sb.st_size / sizeof(int64_t);
    
    std::cout << "File size: " << sb.st_size << " bytes, count: " << count << " rows\n";
    std::cout << "First 20 raw values (int64_t):\n";
    for (int i = 0; i < 20; i++) {
        double as_cents = (double)data[i] / 100.0;
        std::cout << "  [" << i << "]: " << data[i] << " (as dollars: " << as_cents << ")\n";
    }
    
    // Find the highest
    int64_t max_val = data[0];
    for (size_t i = 1; i < count; i++) {
        if (data[i] > max_val) max_val = data[i];
    }
    std::cout << "\nMax value: " << max_val << " (as dollars: " << ((double)max_val / 100.0) << ")\n";
    
    munmap(mapped, sb.st_size);
    return 0;
}
