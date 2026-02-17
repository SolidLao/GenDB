#include <iostream>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>

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
    
    // Load customer keys
    size_t num_customers = 0;
    int32_t* c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", num_customers);
    
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
    
    std::cout << "num_unique: " << num_unique << ", table_size: " << table_size << std::endl;
    
    // Sample 1000 customers to see how many we can find
    int found = 0, not_found = 0;
    int max_probes = 0;
    
    for (int c = 0; c < 1000; c++) {
        int32_t custkey = c_custkey[c];
        uint32_t hash = static_cast<uint32_t>(custkey) * 2654435761U;
        uint32_t slot = hash % table_size;
        
        bool found_key = false;
        int probes = 0;
        for (uint32_t i = 0; i < table_size; i++) {
            uint32_t idx = (slot + i) % table_size;
            probes++;
            if (keys[idx] == -1) break;
            if (keys[idx] == custkey) {
                found_key = true;
                found++;
                if (probes > max_probes) max_probes = probes;
                break;
            }
        }
        if (!found_key) {
            not_found++;
        }
    }
    
    std::cout << "Out of 1000 customers: Found: " << found << ", Not found: " << not_found << std::endl;
    std::cout << "Max probes needed: " << max_probes << std::endl;
    std::cout << "Expected ~667 found, ~333 not found (based on 999982/1500000 ratio)" << std::endl;
    
    return 0;
}
