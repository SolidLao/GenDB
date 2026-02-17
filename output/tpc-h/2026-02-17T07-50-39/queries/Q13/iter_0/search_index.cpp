#include <iostream>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <cstdint>

int main() {
    std::string path = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/indexes/orders_custkey_hash.bin";
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
    
    // Search for keys 1-10 in the entire table
    for (int32_t search_key = 1; search_key <= 10; search_key++) {
        bool found = false;
        for (uint32_t i = 0; i < table_size; i++) {
            if (keys[i] == search_key) {
                std::cout << "Key " << search_key << " found at slot " << i << " with count " << counts[i] << std::endl;
                found = true;
                break;
            }
        }
        if (!found) {
            std::cout << "Key " << search_key << " NOT FOUND in index" << std::endl;
        }
    }
    
    return 0;
}
