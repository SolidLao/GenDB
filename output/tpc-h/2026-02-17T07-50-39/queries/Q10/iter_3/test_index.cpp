#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>

struct HashSingleEntry {
    int32_t key;
    uint32_t position;
};

int main() {
    const char* path = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/indexes/customer_custkey_hash.bin";
    int fd = open(path, O_RDONLY);
    
    uint32_t num_entries, table_size;
    read(fd, &num_entries, sizeof(num_entries));
    read(fd, &table_size, sizeof(table_size));
    
    printf("Index: num_entries=%u, table_size=%u\n", num_entries, table_size);
    
    HashSingleEntry* entries = new HashSingleEntry[table_size];
    read(fd, entries, table_size * sizeof(HashSingleEntry));
    close(fd);
    
    printf("First 10 entries:\n");
    for (uint32_t i = 0; i < 10 && i < table_size; i++) {
        printf("  [%u] key=%d, pos=%u\n", i, entries[i].key, entries[i].position);
    }
    
    // Test lookup for custkey=1115125
    int32_t test_key = 1115125;
    size_t idx = (size_t)test_key % table_size;
    printf("\nLookup for key=%d, initial idx=%zu:\n", test_key, idx);
    for (size_t probe = 0; probe < 20; probe++) {
        size_t pos = (idx + probe) % table_size;
        printf("  probe %zu: [%zu] key=%d, pos=%u\n", probe, pos, entries[pos].key, entries[pos].position);
        if (entries[pos].key == -1) {
            printf("  -> Empty slot, key not found\n");
            break;
        }
        if (entries[pos].key == test_key) {
            printf("  -> FOUND!\n");
            break;
        }
    }
    
    delete[] entries;
    return 0;
}
