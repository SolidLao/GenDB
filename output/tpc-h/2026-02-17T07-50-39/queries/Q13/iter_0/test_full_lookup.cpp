#include <iostream>
#include <fstream>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>

template<typename T>
T* mmap_column(const std::string& path, size_t expected_rows, size_t& actual_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat " << path << std::endl;
        close(fd);
        exit(1);
    }
    actual_rows = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        close(fd);
        exit(1);
    }
    close(fd);
    return static_cast<T*>(addr);
}

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    // Load customer keys
    size_t num_customers = 0;
    int32_t* c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", 1500000, num_customers);
    std::cout << "Loaded " << num_customers << " customers" << std::endl;
    
    // Load hash index
    std::string path = gendb_dir + "/indexes/orders_custkey_hash.bin";
    int fd = open(path.c_str(), O_RDONLY);
    
    uint32_t num_unique, table_size;
    read(fd, &num_unique, sizeof(num_unique));
    read(fd, &table_size, sizeof(table_size));
    
    std::vector<int32_t> keys(table_size);
    std::vector<uint32_t> offsets(table_size);
    std::vector<uint32_t> counts(table_size);
    
    for (uint32_t i = 0; i < table_size; i++) {
        read(fd, &keys[i], sizeof(int32_t));
        read(fd, &offsets[i], sizeof(uint32_t));
        read(fd, &counts[i], sizeof(uint32_t));
    }
    close(fd);
    
    // Test first 10 customers
    int found = 0, not_found = 0;
    for (int c = 0; c < 10; c++) {
        int32_t custkey = c_custkey[c];
        uint32_t hash = static_cast<uint32_t>(custkey) * 2654435761U;
        uint32_t slot = hash % table_size;
        
        bool found_key = false;
        for (uint32_t i = 0; i < table_size; i++) {
            uint32_t idx = (slot + i) % table_size;
            if (keys[idx] == -1) break;
            if (keys[idx] == custkey) {
                std::cout << "Customer " << c << " (key=" << custkey << "): found with " << counts[idx] << " orders" << std::endl;
                found_key = true;
                found++;
                break;
            }
        }
        if (!found_key) {
            std::cout << "Customer " << c << " (key=" << custkey << "): NOT FOUND" << std::endl;
            not_found++;
        }
    }
    std::cout << "Found: " << found << ", Not found: " << not_found << std::endl;
    
    return 0;
}
