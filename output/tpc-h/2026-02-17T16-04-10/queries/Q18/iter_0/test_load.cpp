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

    std::cout << "File size: " << sb.st_size << std::endl;

    void* mapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) {
        std::cerr << "Error: mmap failed for " << path << std::endl;
        return false;
    }

    const char* data = (const char*)mapped;

    // Read header: number of rows
    uint32_t num_rows = *((uint32_t*)data);
    std::cout << "Num rows: " << num_rows << std::endl;

    // Offset table is at data + 8, each entry is 4 bytes (END-boundaries)
    const uint32_t* offsets = (const uint32_t*)(data + 8);

    // String data starts at: 8 + num_rows*4 - 4 (offset table overlaps with string data by 4 bytes)
    size_t offset_table_byte_end = 8 + num_rows * 4;
    const char* string_data = data + offset_table_byte_end - 4;
    size_t string_data_size = sb.st_size - (offset_table_byte_end - 4);

    std::cout << "String data starts at offset: " << (offset_table_byte_end - 4) << ", size: " << string_data_size << std::endl;

    col.resize(expected_rows);
    for (size_t i = 0; i < expected_rows && i < (size_t)num_rows; i++) {
        uint32_t start = (i == 0) ? 0 : offsets[i - 1];
        uint32_t end = offsets[i];
        if (start <= end && end <= string_data_size) {
            col[i] = std::string(string_data + start, end - start);
        } else {
            col[i] = "";
        }
        if (i < 5) {
            std::cout << "Row " << i << ": start=" << start << " end=" << end << " len=" << (end-start) << " data=\"" << col[i] << "\"" << std::endl;
        }
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
    return 0;
}
