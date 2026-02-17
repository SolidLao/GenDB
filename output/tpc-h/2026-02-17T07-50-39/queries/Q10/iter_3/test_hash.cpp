#include <cstdio>
#include <cstdint>

int main() {
    uint32_t table_size = 2097152;
    int32_t key = 225184;
    
    // Simple modulo
    size_t idx1 = (size_t)key % table_size;
    printf("Simple modulo: %zu\n", idx1);
    
    // Multiplicative hash (our code)
    size_t idx2 = (((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & (table_size - 1);
    printf("Multiplicative hash: %zu\n", idx2);
    
    // Test key 1115125
    key = 1115125;
    idx1 = (size_t)key % table_size;
    idx2 = (((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & (table_size - 1);
    printf("\nKey=%d:\n", key);
    printf("  Simple modulo: %zu\n", idx1);
    printf("  Multiplicative hash: %zu\n", idx2);
    
    return 0;
}
