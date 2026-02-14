#include <iostream>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>
#include <iomanip>

inline std::string epochDaysToString(int32_t days) {
    std::time_t t = days * 86400;
    struct tm* tm_info = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return std::string(buf);
}

int main() {
    // The metadata says o_orderdate uses delta encoding
    // But what if it's stored with per-block base values?
    // Block size is 100K
    
    // Theory: the file stores differences, where each row stores
    // the delta from the start of its block
    
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders.o_orderdate.col", O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    
    const int32_t* data = (const int32_t*)ptr;
    
    // What if the first value (8035) is a global base, and all others are deltas?
    int32_t global_base = 8035;
    
    std::cout << "If 8035 is global base:\n";
    std::cout << "Base date: " << epochDaysToString(global_base) << " (1992-01-01)\n\n";
    
    std::cout << "Then reconstructed dates would be:\n";
    int32_t accumulated = global_base;
    for (int i = 0; i < 10; ++i) {
        int32_t stored = data[i];
        int32_t delta = stored - global_base;  // Difference from base?
        std::cout << "  [" << i << "]: stored=" << stored << ", delta=" << delta
                  << ", date=" << epochDaysToString(stored) << "\n";
    }
    
    std::cout << "\nOr maybe each block has a local base stored at the start?\n";
    std::cout << "If block 0 starts at 0 with base 8035:\n";
    
    // Theory 2: First value of each block is the base, rest are offsets
    // Block size = 100K
    const size_t BLOCK_SIZE = 100000;
    
    for (size_t block = 0; block < 3; ++block) {
        size_t block_start = block * BLOCK_SIZE;
        int32_t block_base = data[block_start];
        std::cout << "Block " << block << " base: " << epochDaysToString(block_base) << "\n";
        
        for (size_t i = 0; i < 3; ++i) {
            int32_t raw = data[block_start + i];
            std::cout << "  [" << (block_start + i) << "]: " << epochDaysToString(raw) << "\n";
        }
    }
    
    return 0;
}
