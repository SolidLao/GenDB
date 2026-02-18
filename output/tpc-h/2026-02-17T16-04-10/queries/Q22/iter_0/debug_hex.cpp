#include <iostream>
#include <cstdint>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static constexpr size_t PHONE_FIELD_WIDTH = 19;

int main() {
    const std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    const std::string customer_dir = gendb_dir + "/customer";

    int fd_phone = open((customer_dir + "/c_phone.bin").c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd_phone, &sb);
    size_t phone_size = sb.st_size;
    void* phone_data = mmap(nullptr, phone_size, PROT_READ, MAP_SHARED, fd_phone, 0);
    const unsigned char* c_phone_raw = static_cast<unsigned char*>(phone_data);

    // Print first 20 bytes (first phone number)
    std::cout << "First phone number (hex): ";
    for (int i = 0; i < PHONE_FIELD_WIDTH; i++) {
        printf("%02x ", c_phone_raw[i]);
    }
    std::cout << std::endl;

    std::cout << "As ASCII: ";
    for (int i = 0; i < PHONE_FIELD_WIDTH; i++) {
        unsigned char c = c_phone_raw[i];
        if (c >= 32 && c < 127) {
            std::cout << (char)c;
        } else {
            std::cout << ".";
        }
    }
    std::cout << std::endl;

    return 0;
}
