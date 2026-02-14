#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    int fd = open((gendb_dir + "/orders.o_orderdate.col").c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    size_t filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    
    const int32_t* data = (const int32_t*)ptr;
    size_t num_elements = filesize / sizeof(int32_t);
    
    // Check first 100 elements
    std::cout << "Raw delta values (first 100):\n";
    int distinct_count = 0;
    std::vector<int32_t> samples;
    for (size_t i = 0; i < std::min((size_t)100, num_elements); ++i) {
        samples.push_back(data[i]);
    }
    
    std::sort(samples.begin(), samples.end());
    samples.erase(std::unique(samples.begin(), samples.end()), samples.end());
    
    std::cout << "Distinct values in first 100: " << samples.size() << "\n";
    for (int32_t v : samples) {
        std::cout << "  " << v << "\n";
    }
    
    return 0;
}
