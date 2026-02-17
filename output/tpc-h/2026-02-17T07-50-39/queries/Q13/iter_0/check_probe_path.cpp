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
    
    // Check probe path for key=2
    int32_t search_key = 2;
    uint32_t hash = static_cast<uint32_t>(search_key) * 2654435761U;
    uint32_t slot = hash % table_size;
    
    std::cout << "Probing for key " << search_key << " starting at slot " << slot << std::endl;
    for (uint32_t i = 0; i < 30; i++) {
        uint32_t idx = (slot + i) % table_size;
        std::cout << "Probe " << i << " slot " << idx << ": key=" << keys[idx] << ", count=" << counts[idx];
        if (keys[idx] == search_key) {
            std::cout << " <-- FOUND!";
        }
        if (keys[idx] == -1) {
            std::cout << " <-- EMPTY SLOT, search would stop here";
        }
        std::cout << std::endl;
    }
    
    return 0;
}
