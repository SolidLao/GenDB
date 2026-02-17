#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

int main() {
    // Check lineitem extended price and discount values
    int fd_price = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_extendedprice.bin", O_RDONLY);
    int fd_discount = open("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_discount.bin", O_RDONLY);
    
    int64_t prices[20], discounts[20];
    read(fd_price, prices, sizeof(prices));
    read(fd_discount, discounts, sizeof(discounts));
    
    close(fd_price);
    close(fd_discount);
    
    std::cout << "Sample prices and discounts (scale_factor: 2):\n";
    for (int i = 0; i < 20; i++) {
        double price_actual = static_cast<double>(prices[i]) / 2.0;
        double discount_actual = static_cast<double>(discounts[i]) / 2.0;
        std::cout << "Stored: " << prices[i] << ", " << discounts[i] 
                  << " | Actual: " << price_actual << ", " << discount_actual << "\n";
    }
    
    return 0;
}
