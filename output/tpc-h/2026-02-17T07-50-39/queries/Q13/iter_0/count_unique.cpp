#include <iostream>
#include <unordered_set>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>

template<typename T>
T* mmap_column(const std::string& path, size_t expected_rows, size_t& actual_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    actual_rows = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return static_cast<T*>(addr);
}

int main() {
    size_t num_orders = 0;
    int32_t* o_custkey = mmap_column<int32_t>("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/orders/o_custkey.bin", 15000000, num_orders);
    
    std::unordered_set<int32_t> unique_custs;
    for (size_t i = 0; i < num_orders; i++) {
        unique_custs.insert(o_custkey[i]);
    }
    
    std::cout << "Total orders: " << num_orders << std::endl;
    std::cout << "Unique customers in orders: " << unique_custs.size() << std::endl;
    std::cout << "Expected customers with 0 orders: " << (1500000 - unique_custs.size()) << std::endl;
    
    return 0;
}
