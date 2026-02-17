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

    // Read first few entries
    for (int i = 0; i < 10; i++) {
        int32_t key;
        uint32_t offset, count;
        read(fd, &key, sizeof(key));
        read(fd, &offset, sizeof(offset));
        read(fd, &count, sizeof(count));
        std::cout << "Entry " << i << ": key=" << key << ", offset=" << offset << ", count=" << count << std::endl;
    }

    close(fd);
    return 0;
}
