#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <algorithm>

template<typename T>
T* mmap_column(const std::string& path, size_t& actual_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    actual_rows = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return static_cast<T*>(addr);
}

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    size_t num_customers = 0, num_orders = 0;
    int32_t* c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", num_customers);
    int32_t* o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", num_orders);
    
    std::cout << "First 10 customer keys: ";
    for (int i = 0; i < 10; i++) std::cout << c_custkey[i] << " ";
    std::cout << std::endl;
    
    std::cout << "First 10 order customer keys: ";
    for (int i = 0; i < 10; i++) std::cout << o_custkey[i] << " ";
    std::cout << std::endl;
    
    std::cout << "Customer key range: [" << *std::min_element(c_custkey, c_custkey + num_customers) 
              << ", " << *std::max_element(c_custkey, c_custkey + num_customers) << "]" << std::endl;
    std::cout << "Order custkey range: [" << *std::min_element(o_custkey, o_custkey + num_orders) 
              << ", " << *std::max_element(o_custkey, o_custkey + num_orders) << "]" << std::endl;
    
    return 0;
}
