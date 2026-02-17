#include <iostream>
#include <fstream>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <cstdint>

int main() {
    std::string path = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/indexes/orders_custkey_hash.bin";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return 1;
    }

    uint32_t num_unique, table_size;
    read(fd, &num_unique, sizeof(num_unique));
    read(fd, &table_size, sizeof(table_size));

    std::cout << "num_unique: " << num_unique << std::endl;
    std::cout << "table_size: " << table_size << std::endl;

    // Load all keys, offsets, counts
    std::vector<int32_t> keys(table_size);
    std::vector<uint32_t> offsets(table_size);
    std::vector<uint32_t> counts(table_size);
    
    for (uint32_t i = 0; i < table_size; i++) {
        read(fd, &keys[i], sizeof(int32_t));
        read(fd, &offsets[i], sizeof(uint32_t));
        read(fd, &counts[i], sizeof(uint32_t));
    }

    // Test lookup for customer key = 1 (first customer)
    int32_t test_key = 1;
    uint32_t hash = static_cast<uint32_t>(test_key) * 2654435761U;
    uint32_t slot = hash % table_size;
    
    std::cout << "Looking up key " << test_key << std::endl;
    std::cout << "Initial hash: " << hash << std::endl;
    std::cout << "Initial slot: " << slot << std::endl;
    
    for (uint32_t i = 0; i < 20; i++) {
        uint32_t idx = (slot + i) % table_size;
        std::cout << "Probe " << i << " at slot " << idx << ": key=" << keys[idx] << ", count=" << counts[idx] << std::endl;
        if (keys[idx] == test_key) {
            std::cout << "FOUND at probe " << i << ": offset=" << offsets[idx] << ", count=" << counts[idx] << std::endl;
            break;
        }
        if (keys[idx] == -1) {
            std::cout << "Empty slot, key not found" << std::endl;
            break;
        }
    }

    close(fd);
    return 0;
}
