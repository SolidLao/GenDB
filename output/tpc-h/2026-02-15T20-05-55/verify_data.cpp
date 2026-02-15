#include <iostream>
#include <fstream>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

int main() {
    std::cout << "=== Data Verification ===" << std::endl;

    // Check lineitem shipdate column (should be days since epoch)
    std::string shipdate_file = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/tables/lineitem/l_shipdate.bin";
    int fd = open(shipdate_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open shipdate file" << std::endl;
        return 1;
    }

    size_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    const int32_t* dates = static_cast<const int32_t*>(mapped);

    size_t num_dates = file_size / sizeof(int32_t);
    std::cout << "\nLinitem shipdate column:" << std::endl;
    std::cout << "  File size: " << file_size << " bytes" << std::endl;
    std::cout << "  Number of dates: " << num_dates << std::endl;

    // Check first 10 dates
    std::cout << "  First 10 dates (as epoch days):" << std::endl;
    for (int i = 0; i < 10 && i < num_dates; ++i) {
        std::cout << "    [" << i << "] = " << dates[i];

        // Verify it's a reasonable epoch day (1992-2000 range)
        // 1992-01-01 = 8035 days, 1999-12-31 = 10958 days
        if (dates[i] >= 8000 && dates[i] <= 11000) {
            std::cout << " ✓ (valid date range)";
        } else {
            std::cout << " ✗ (INVALID)";
        }
        std::cout << std::endl;
    }

    // Check last 10 dates
    std::cout << "  Last 10 dates (as epoch days):" << std::endl;
    size_t start_idx = (num_dates > 10) ? (num_dates - 10) : 0;
    for (size_t i = start_idx; i < num_dates; ++i) {
        std::cout << "    [" << i << "] = " << dates[i];
        if (dates[i] >= 8000 && dates[i] <= 11000) {
            std::cout << " ✓";
        } else {
            std::cout << " ✗";
        }
        std::cout << std::endl;
    }

    munmap(mapped, file_size);
    close(fd);

    // Check decimal column (extendedprice)
    std::string price_file = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/tables/lineitem/l_extendedprice.bin";
    fd = open(price_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open price file" << std::endl;
        return 1;
    }

    file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    const int64_t* prices = static_cast<const int64_t*>(mapped);

    size_t num_prices = file_size / sizeof(int64_t);
    std::cout << "\nLineitem extendedprice column (scaled by 100):" << std::endl;
    std::cout << "  File size: " << file_size << " bytes" << std::endl;
    std::cout << "  Number of prices: " << num_prices << std::endl;

    std::cout << "  First 10 prices (as scaled int64):" << std::endl;
    for (int i = 0; i < 10 && i < num_prices; ++i) {
        double actual = static_cast<double>(prices[i]) / 100.0;
        std::cout << "    [" << i << "] = " << prices[i] << " (actual: " << actual << ")";

        // Reasonable range for TPC-H (products can be expensive, but not unlimited)
        if (prices[i] > 0 && prices[i] < 10000000) {
            std::cout << " ✓";
        } else {
            std::cout << " ✗";
        }
        std::cout << std::endl;
    }

    std::cout << "  Last 10 prices (as scaled int64):" << std::endl;
    start_idx = (num_prices > 10) ? (num_prices - 10) : 0;
    for (size_t i = start_idx; i < num_prices; ++i) {
        double actual = static_cast<double>(prices[i]) / 100.0;
        std::cout << "    [" << i << "] = " << prices[i] << " (actual: " << actual << ")";
        if (prices[i] > 0 && prices[i] < 10000000) {
            std::cout << " ✓";
        } else {
            std::cout << " ✗";
        }
        std::cout << std::endl;
    }

    munmap(mapped, file_size);
    close(fd);

    // Check dictionary encoding
    std::cout << "\nLineitem l_returnflag dictionary:" << std::endl;
    std::ifstream dict_file("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/tables/lineitem/l_returnflag_dict.txt");
    std::string line;
    while (std::getline(dict_file, line)) {
        std::cout << "  " << line << std::endl;
    }
    dict_file.close();

    std::cout << "\nVerification complete!" << std::endl;
    return 0;
}
