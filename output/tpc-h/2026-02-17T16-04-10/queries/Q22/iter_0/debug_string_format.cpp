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

    // Parse first 20 records
    for (int i = 0; i < 20; i++) {
        const unsigned char* ptr = c_phone_raw + (i * PHONE_FIELD_WIDTH);
        
        // First 4 bytes are little-endian length
        uint32_t len = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);
        
        // Next len bytes are the string
        std::string phone(reinterpret_cast<const char*>(ptr + 4), len);
        
        std::cout << "Customer " << i << ": len=" << len << " phone='" << phone << "'";
        if (phone.length() >= 2) {
            std::cout << " code='" << phone.substr(0, 2) << "'";
        }
        std::cout << std::endl;
    }

    return 0;
}
