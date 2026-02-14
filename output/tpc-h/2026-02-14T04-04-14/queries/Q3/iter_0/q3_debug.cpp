#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <ctime>
#include <algorithm>
#include <iomanip>
#include <chrono>

inline std::string epochDaysToString(int32_t days) {
    const int64_t SECONDS_PER_DAY = 86400;
    std::time_t t = days * SECONDS_PER_DAY;
    struct tm* tm_info = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return std::string(buf);
}

inline const void* mmapFile(const std::string& filename, size_t& filesize) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filename << std::endl;
        return nullptr;
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }
    filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << filename << std::endl;
        return nullptr;
    }
    madvise(ptr, filesize, MADV_SEQUENTIAL);
    return ptr;
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    
    size_t orders_size = 0;
    const int32_t* o_orderdate_delta = (const int32_t*)mmapFile(
        gendb_dir + "/orders.o_orderdate.col", orders_size);
    size_t num_orders = orders_size / sizeof(int32_t);
    
    // Decode
    std::vector<int32_t> o_orderdate(num_orders);
    o_orderdate[0] = o_orderdate_delta[0];
    for (size_t i = 1; i < num_orders; ++i) {
        o_orderdate[i] = o_orderdate[i-1] + o_orderdate_delta[i];
    }
    
    // Print first 10
    for (int i = 0; i < 10; ++i) {
        std::cout << "orders[" << i << "]: raw_delta=" << o_orderdate_delta[i]
                  << ", decoded=" << o_orderdate[i]
                  << ", date=" << epochDaysToString(o_orderdate[i]) << std::endl;
    }
    
    return 0;
}
