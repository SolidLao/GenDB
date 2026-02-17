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

    // Count empty vs non-empty slots
    int empty_count = 0;
    int nonempty_count = 0;
    
    for (uint32_t i = 0; i < table_size; i++) {
        int32_t key;
        uint32_t offset, count;
        read(fd, &key, sizeof(key));
        read(fd, &offset, sizeof(offset));
        read(fd, &count, sizeof(count));
        
        if (count == 0) {
            empty_count++;
            if (empty_count <= 5) {
                std::cout << "Empty slot " << i << ": key=" << key << ", offset=" << offset << ", count=" << count << std::endl;
            }
        } else {
            nonempty_count++;
        }
    }

    std::cout << "Empty slots: " << empty_count << std::endl;
    std::cout << "Non-empty slots: " << nonempty_count << std::endl;

    close(fd);
    return 0;
}
