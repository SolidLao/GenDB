#include <iostream>
#include <vector>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

bool load_string_column(const std::string& path, std::vector<std::string>& col, size_t expected_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << path << std::endl;
        return false;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return false;
    }

    void* mapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) {
        std::cerr << "Error: mmap failed for " << path << std::endl;
        return false;
    }

    const char* data = (const char*)mapped;
    const char* end_ptr = data + sb.st_size;

    col.resize(expected_rows);
    const char* ptr = data;
    size_t idx = 0;

    while (ptr < end_ptr && idx < expected_rows) {
        // Read 4-byte length
        if (ptr + 4 > end_ptr) break;
        uint32_t len = *((const uint32_t*)ptr);
        ptr += 4;

        // Read string data
        if (ptr + len > end_ptr) break;
        col[idx] = std::string(ptr, len);
        ptr += len;
        idx++;
    }

    munmap(mapped, sb.st_size);
    return true;
}

int main() {
    std::vector<std::string> c_name;
    if (!load_string_column("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/customer/c_name.bin", c_name, 10)) {
        std::cerr << "Failed to load" << std::endl;
        return 1;
    }
    std::cout << "Loaded " << c_name.size() << " rows" << std::endl;
    for (int i = 0; i < 5; i++) {
        std::cout << "  [" << i << "]: " << c_name[i] << std::endl;
    }
    return 0;
}
