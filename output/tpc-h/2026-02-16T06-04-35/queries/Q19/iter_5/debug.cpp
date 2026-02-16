#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

std::unordered_map<int8_t, std::string> load_dict(const std::string& dict_path) {
    std::unordered_map<int8_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int8_t code = std::stoi(line.substr(0, eq));
        std::string value = line.substr(eq + 1);
        dict[code] = value;
    }
    f.close();
    return dict;
}

int8_t find_dict_code(const std::unordered_map<int8_t, std::string>& dict,
                      const std::string& value) {
    for (const auto& [code, val] : dict) {
        if (val == value) return code;
    }
    return -1;
}

template <typename T>
T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "Failed to stat " << path << std::endl;
        close(fd);
        return nullptr;
    }
    size_t file_size = st.st_size;
    count = file_size / sizeof(T);
    T* data = (T*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }
    return data;
}

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    
    auto l_shipmode_dict = load_dict(gendb_dir + "/lineitem/l_shipmode_dict.txt");
    
    int8_t code_air = find_dict_code(l_shipmode_dict, "AIR");
    int8_t code_reg_air = find_dict_code(l_shipmode_dict, "REG AIR");
    
    printf("Dictionary loaded:\n");
    for (const auto& [code, value] : l_shipmode_dict) {
        printf("  %d = %s\n", (int)code, value.c_str());
    }
    
    printf("\nFound codes:\n");
    printf("  AIR = %d\n", (int)code_air);
    printf("  REG AIR = %d\n", (int)code_reg_air);
    printf("  AIR REG = %d\n", (int)find_dict_code(l_shipmode_dict, "AIR REG"));
    
    // Load a bit of data to check
    size_t li_count = 0;
    int8_t* l_shipmode = mmap_column<int8_t>(gendb_dir + "/lineitem/l_shipmode.bin", li_count);
    
    // Count shipmode values
    int counts[10] = {0};
    for (size_t i = 0; i < li_count && i < 1000000; ++i) {
        if (l_shipmode[i] >= 0 && l_shipmode[i] < 10) {
            counts[l_shipmode[i]]++;
        }
    }
    
    printf("\nFirst 1M lineitem shipmode value counts:\n");
    for (int i = 0; i < 10; ++i) {
        if (counts[i] > 0) {
            printf("  Code %d: %d rows\n", i, counts[i]);
        }
    }
    
    return 0;
}
