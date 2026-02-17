#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

int main() {
    // Test date conversion
    int32_t date_1995_01_01_epoch = 0;
    for (int y = 1970; y < 1995; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        date_1995_01_01_epoch += leap ? 366 : 365;
    }
    
    std::cout << "1995-01-01 epoch days: " << date_1995_01_01_epoch << std::endl;
    
    // Sample some orders dates
    int fd = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders/o_orderdate.bin", O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open orders file\n";
        return 1;
    }
    
    int32_t dates[100];
    read(fd, dates, sizeof(dates));
    close(fd);
    
    std::cout << "Sample order dates: ";
    for (int i = 0; i < 20; i++) {
        std::cout << dates[i] << " ";
    }
    std::cout << std::endl;
    
    return 0;
}
