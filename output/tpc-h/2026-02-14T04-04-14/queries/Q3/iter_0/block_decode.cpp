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
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders.o_orderdate.col", O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    void* ptr = mmap(nullptr, sb.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    
    const int32_t* raw_data = (const int32_t*)ptr;
    size_t num_elements = sb.st_size / sizeof(int32_t);
    
    const size_t BLOCK_SIZE = 100000;
    const size_t NUM_BLOCKS = 150;
    
    std::cout << "Block-based delta interpretation:\n";
    std::cout << "Total elements: " << num_elements << "\n";
    std::cout << "Expected rows: 15,000,000\n";
    std::cout << "Block size: 100,000\n";
    std::cout << "Num blocks: 150\n\n";
    
    // Theory: Each block stores values as delta-encoded FROM THE FIRST VALUE IN BLOCK
    // i.e., row[0] in block = absolute value
    //       row[i] in block = base + cumulative_delta[i]
    
    for (size_t block = 0; block < 5; ++block) {
        size_t block_start = block * BLOCK_SIZE;
        int32_t block_base = raw_data[block_start];
        
        std::cout << "Block " << block << ":\n";
        std::cout << "  Base value: " << block_base << " (" << epochDaysToString(block_base) << ")\n";
        
        // Within-block deltas
        std::cout << "  First few row values:\n";
        for (size_t i = 0; i < 5; ++i) {
            int32_t raw = raw_data[block_start + i];
            int32_t delta = (i == 0) ? 0 : (raw - block_base);
            int32_t reconstructed = block_base + delta;
            std::cout << "    row[" << i << "]: raw=" << raw << ", delta=" << delta
                      << ", reconstructed=" << reconstructed
                      << " (" << epochDaysToString(reconstructed) << ")\n";
        }
        std::cout << "\n";
    }
    
    return 0;
}
