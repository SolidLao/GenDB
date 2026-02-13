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
    
    std::cout << "Date range: " << date_start << " to " << date_end << std::endl;
    
    // Sample first few matching rows
    int matches = 0;
    int64_t revenue = 0;
    for (size_t i = 0; i < row_count && matches < 10; ++i) {
        if (shipdate[i] >= date_start && shipdate[i] < date_end &&
            discount[i] >= 5 && discount[i] <= 7 &&
            quantity[i] < 2400) {
            std::cout << "Row " << i << ": shipdate=" << shipdate[i] 
                      << " discount=" << discount[i] 
                      << " quantity=" << quantity[i]
                      << " price=" << price[i] << std::endl;
            revenue += price[i] * discount[i];
            matches++;
        }
    }
    
    std::cout << "Sample revenue (first 10 matches): " << revenue << std::endl;
    
    return 0;
}
