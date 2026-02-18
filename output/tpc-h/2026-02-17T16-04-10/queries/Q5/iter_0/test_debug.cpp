#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <unistd.h>

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    // Load first few lineitem discount values
    int fd = open((gendb_dir + "/lineitem/l_discount.bin").c_str(), O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    void* data = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    
    int64_t* disc = (int64_t*)data;
    for (int i = 0; i < 10; ++i) {
        std::cout << "Row " << i << ": discount = " << disc[i] << " (actual " << (double)disc[i]/100 << ")\n";
    }
    
    munmap(data, st.st_size);
    ::close(fd);
    
    return 0;
}
