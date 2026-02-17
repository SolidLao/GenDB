#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>

int main() {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream in("/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem/l_returnflag_dict.txt");
    std::string line;
    int32_t code = 0;
    while (std::getline(in, line)) {
        dict[line] = code;
        std::cout << "Code " << code << " -> " << line << std::endl;
        code++;
    }
    in.close();
    
    std::cout << "Dictionary for 'R': " << dict["R"] << std::endl;
    return 0;
}
