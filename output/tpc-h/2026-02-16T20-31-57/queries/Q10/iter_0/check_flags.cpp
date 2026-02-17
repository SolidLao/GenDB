#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int main() {
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_returnflag.bin", O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    int32_t* data = (int32_t*)mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    size_t count = sb.st_size / sizeof(int32_t);
    
    int64_t count0 = 0, count1 = 0, count2 = 0;
    for (size_t i = 0; i < count; i++) {
        if (data[i] == 0) count0++;
        else if (data[i] == 1) count1++;
        else if (data[i] == 2) count2++;
    }
    
    std::cout << "Count 0 (N): " << count0 << std::endl;
    std::cout << "Count 1 (R): " << count1 << std::endl;
    std::cout << "Count 2 (A): " << count2 << std::endl;
    std::cout << "Total: " << count << std::endl;
    
    close(fd);
    return 0;
}
