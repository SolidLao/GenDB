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
    
    const int32_t* deltas = (const int32_t*)ptr;
    size_t num_rows = sb.st_size / sizeof(int32_t);
    
    // Reconstruct as per-row delta
    std::vector<int32_t> decoded(num_rows);
    decoded[0] = deltas[0];
    for (size_t i = 1; i < num_rows; ++i) {
        decoded[i] = decoded[i-1] + deltas[i];
    }
    
    // Expected from ground truth Q3 results:
    // First result has o_orderdate = 1995-02-23 = 9184 days
    std::cout << "Decoded dates (sample):\n";
    std::cout << "  [0] = " << epochDaysToString(decoded[0]) << " (" << decoded[0] << ")\n";
    std::cout << "  [100] = " << epochDaysToString(decoded[100]) << " (" << decoded[100] << ")\n";
    std::cout << "  [1000] = " << epochDaysToString(decoded[1000]) << " (" << decoded[1000] << ")\n";
    std::cout << "  [10000] = " << epochDaysToString(decoded[10000]) << " (" << decoded[10000] << ")\n";
    
    // Look for dates around 1995-03-15 (day 9204) and 1995-02-23 (day 9184)
    std::cout << "\nSearching for 1995-03-15 (9204)...\n";
    int count = 0;
    for (size_t i = 0; i < num_rows && count < 3; ++i) {
        if (decoded[i] == 9204) {
            std::cout << "  Found at row " << i << "\n";
            count++;
        }
    }
    if (count == 0) {
        std::cout << "  NOT FOUND\n";
        // Try nearby
        std::cout << "  Max decoded value: " << decoded[num_rows-1] << "\n";
        std::cout << "  Max as date: " << epochDaysToString(decoded[num_rows-1]) << "\n";
    }
    
    return 0;
}
