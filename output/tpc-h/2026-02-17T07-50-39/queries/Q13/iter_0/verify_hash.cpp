#include <iostream>
#include <cstdint>

int main() {
    int32_t key = 2;
    uint32_t hash = static_cast<uint32_t>(key) * 2654435761U;
    uint32_t table_size = 1048576;
    uint32_t slot = hash % table_size;
    
    std::cout << "Key: " << key << std::endl;
    std::cout << "Hash: " << hash << std::endl;
    std::cout << "Table size: " << table_size << std::endl;
    std::cout << "Initial slot: " << slot << std::endl;
    std::cout << "Expected slot: 979826" << std::endl;
    std::cout << "Distance: " << ((979826 - slot + table_size) % table_size) << " probes needed" << std::endl;
    
    return 0;
}
