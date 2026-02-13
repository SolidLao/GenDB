#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>

inline int32_t date_to_days(int year, int month, int day) {
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 719469;
}

int main() {
    std::string dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem";
    size_t row_count = 59985990;
    
    int fd_ship = open((dir + "/l_shipdate.bin").c_str(), O_RDONLY);
    int fd_disc = open((dir + "/l_discount.bin").c_str(), O_RDONLY);
    int fd_qty = open((dir + "/l_quantity.bin").c_str(), O_RDONLY);
    int fd_price = open((dir + "/l_extendedprice.bin").c_str(), O_RDONLY);
    
    int32_t* shipdate = (int32_t*)mmap(nullptr, row_count * 4, PROT_READ, MAP_PRIVATE, fd_ship, 0);
    int64_t* discount = (int64_t*)mmap(nullptr, row_count * 8, PROT_READ, MAP_PRIVATE, fd_disc, 0);
    int64_t* quantity = (int64_t*)mmap(nullptr, row_count * 8, PROT_READ, MAP_PRIVATE, fd_qty, 0);
    int64_t* price = (int64_t*)mmap(nullptr, row_count * 8, PROT_READ, MAP_PRIVATE, fd_price, 0);
    
    int32_t date_start = date_to_days(1994, 1, 1);
    int32_t date_end = date_to_days(1995, 1, 1);
    
    // Count all matches with both filter variants
    int64_t revenue1 = 0, count1 = 0;  // Using >= and <=
    int64_t revenue2 = 0, count2 = 0;  // Using > and <
    
    for (size_t i = 0; i < row_count; ++i) {
        int32_t sd = shipdate[i];
        int64_t disc = discount[i];
        int64_t qty = quantity[i];
        
        // Variant 1: >= 5 AND <= 7
        if (sd >= date_start && sd < date_end && disc >= 5 && disc <= 7 && qty < 2400) {
            revenue1 += price[i] * disc;
            count1++;
        }
        
        // Variant 2: > 4 AND < 8  
        if (sd >= date_start && sd < date_end && disc > 4 && disc < 8 && qty < 2400) {
            revenue2 += price[i] * disc;
            count2++;
        }
    }
    
    std::cout << "Variant 1 (disc >= 5 && disc <= 7): count=" << count1 
              << " revenue=" << (revenue1 / 10000.0) << std::endl;
    std::cout << "Variant 2 (disc > 4 && disc < 8): count=" << count2 
              << " revenue=" << (revenue2 / 10000.0) << std::endl;
    
    std::cout << "\nExpected: 1230113636.01" << std::endl;
    std::cout << "Current:  1230108915.60" << std::endl;
    
    return 0;
}
