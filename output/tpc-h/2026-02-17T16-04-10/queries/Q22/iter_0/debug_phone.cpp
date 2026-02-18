#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_set>
#include <cstring>
#include <cstdint>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
    const std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    const std::string customer_dir = gendb_dir + "/customer";

    // Load customer data
    int fd_phone = open((customer_dir + "/c_phone.bin").c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd_phone, &sb);
    size_t phone_size = sb.st_size;
    void* phone_data = mmap(nullptr, phone_size, PROT_READ, MAP_SHARED, fd_phone, 0);
    const char* c_phone_raw = static_cast<char*>(phone_data);

    // Print first 20 phone numbers
    size_t offset = 0;
    for (int i = 0; i < 20 && offset < phone_size; i++) {
        const char* start = c_phone_raw + offset;
        size_t len = std::strlen(start);
        std::string code = std::string(start, len);
        std::cout << "Phone " << i << ": '" << code << "' (first 2: '" << code.substr(0, 2) << "')" << std::endl;
        offset += len + 1;
    }

    return 0;
}
