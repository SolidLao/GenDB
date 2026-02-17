#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>

int main() {
    const char* path = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/customer/c_custkey.bin";
    int fd = open(path, O_RDONLY);
    struct stat sb;
    fstat(fd, &sb);
    
    const int32_t* data = (const int32_t*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    size_t count = sb.st_size / sizeof(int32_t);
    
    printf("First 10 custkeys: ");
    for (int i = 0; i < 10 && i < (int)count; i++) {
        printf("%d ", data[i]);
    }
    printf("\n");
    
    // Check if specific values exist
    int32_t test_keys[] = {1115125, 1485193, 213595, 1130219, 1046014};
    for (int i = 0; i < 5; i++) {
        bool found = false;
        for (size_t j = 0; j < count && j < 10000; j++) {
            if (data[j] == test_keys[i]) {
                printf("Found %d at position %zu\n", test_keys[i], j);
                found = true;
                break;
            }
        }
        if (!found) {
            printf("Not found in first 10000: %d\n", test_keys[i]);
        }
    }
    
    munmap((void*)data, sb.st_size);
    close(fd);
    return 0;
}
