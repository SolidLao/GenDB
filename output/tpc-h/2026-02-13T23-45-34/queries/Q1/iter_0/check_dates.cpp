#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>

int main() {
    const std::string lineitem_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/";
    
    int fd = open((lineitem_dir + "l_shipdate.bin").c_str(), O_RDONLY);
    off_t file_size = lseek(fd, 0, SEEK_END);
    int32_t* dates = static_cast<int32_t*>(mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0));
    close(fd);
    
    int64_t num_rows = file_size / sizeof(int32_t);
    int32_t min_date = dates[0], max_date = dates[0];
    int32_t sep2_1998 = -1;
    
    for (int64_t i = 0; i < num_rows; ++i) {
        min_date = std::min(min_date, dates[i]);
        max_date = std::max(max_date, dates[i]);
    }
    
    std::cout << "Min date: " << min_date << " Max date: " << max_date << std::endl;
    std::cout << "Target 1998-09-02 should be around: 10472 (from epoch 1970)" << std::endl;
    
    // Count rows with date <= different thresholds
    for (int32_t threshold = 10000; threshold <= 10600; threshold += 50) {
        int64_t count = 0;
        for (int64_t i = 0; i < num_rows; ++i) {
            if (dates[i] <= threshold) count++;
        }
        double pct = 100.0 * count / num_rows;
        std::cout << "Date <= " << threshold << ": " << count << " rows (" << pct << "%)" << std::endl;
    }
    
    munmap(dates, file_size);
    return 0;
}
