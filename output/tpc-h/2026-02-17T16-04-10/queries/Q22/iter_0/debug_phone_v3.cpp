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

static constexpr size_t PHONE_FIELD_WIDTH = 19;

std::string extract_phone(const char* phone_raw, int32_t idx) {
    const char* ptr = phone_raw + (idx * PHONE_FIELD_WIDTH);
    std::string result(ptr, PHONE_FIELD_WIDTH);

    // Trim leading and trailing whitespace
    size_t start = result.find_first_not_of(" \0");
    if (start == std::string::npos) {
        return "";
    }

    size_t end = result.find_last_not_of(" \0");
    return result.substr(start, end - start + 1);
}

int main(int argc, char* argv[]) {
    const std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    const std::string customer_dir = gendb_dir + "/customer";

    int fd_phone = open((customer_dir + "/c_phone.bin").c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd_phone, &sb);
    size_t phone_size = sb.st_size;
    void* phone_data = mmap(nullptr, phone_size, PROT_READ, MAP_SHARED, fd_phone, 0);
    const char* c_phone_raw = static_cast<char*>(phone_data);

    int fd_acctbal = open((customer_dir + "/c_acctbal.bin").c_str(), O_RDONLY);
    fstat(fd_acctbal, &sb);
    size_t acctbal_size = sb.st_size;
    void* acctbal_data = mmap(nullptr, acctbal_size, PROT_READ, MAP_SHARED, fd_acctbal, 0);
    const int64_t* c_acctbal = static_cast<int64_t*>(acctbal_data);

    std::unordered_set<std::string> valid_codes = {"13", "31", "23", "29", "30", "18", "17"};

    // Print first 20 phone numbers using fixed-width extraction
    for (int i = 0; i < 20; i++) {
        std::string phone = extract_phone(c_phone_raw, i);
        int64_t acctbal = c_acctbal[i];
        std::cout << "Customer " << i << ": phone='" << phone << "' acctbal=" << acctbal;
        if (phone.length() >= 2) {
            std::string code = phone.substr(0, 2);
            std::cout << " code='" << code << "'";
            if (valid_codes.count(code) > 0) {
                std::cout << " VALID_CODE";
                if (acctbal > 0) {
                    std::cout << " POSITIVE";
                }
            }
        }
        std::cout << std::endl;
    }

    return 0;
}
