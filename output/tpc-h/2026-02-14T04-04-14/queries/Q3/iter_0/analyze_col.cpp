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
    size_t filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    
    const int32_t* data = (const int32_t*)ptr;
    
    std::cout << "Analyzing raw column values:\n\n";
    
    // Print values around key positions
    std::vector<size_t> positions = {0, 100000, 200000, 300000, 400000};
    
    for (size_t pos : positions) {
        std::cout << "Position " << pos << ":\n";
        for (size_t i = pos; i < pos + 5; ++i) {
            std::cout << "  [" << i << "] = " << std::setw(10) << data[i]
                      << " (as date: " << epochDaysToString(data[i]) << ")\n";
        }
        std::cout << "\n";
    }
    
    return 0;
}
