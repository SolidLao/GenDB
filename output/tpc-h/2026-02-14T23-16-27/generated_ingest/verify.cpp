#include <iostream>
#include <fstream>
#include <vector>
#include <cmath>

int main() {
    std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";

    std::cout << "=== VERIFICATION: DATE COLUMNS ===\n";
    
    // Check orders.o_orderdate (should be between ~8000-10592 for years 1992-1998)
    std::ifstream f(gendb_dir + "/orders/o_orderdate.bin", std::ios::binary);
    std::vector<int32_t> dates(10);
    f.read((char*)dates.data(), 10 * sizeof(int32_t));
    f.close();
    
    std::cout << "Orders o_orderdate (first 10 values, should be ~8000-10000 epoch days):\n";
    for (int i = 0; i < 10; i++) {
        std::cout << "  [" << i << "] = " << dates[i] << " days";
        if (dates[i] < 3000 || dates[i] > 20000) {
            std::cout << " **ERROR**";
        }
        std::cout << "\n";
    }

    // Check lineitem.l_shipdate
    f.open(gendb_dir + "/lineitem/l_shipdate.bin", std::ios::binary);
    f.read((char*)dates.data(), 10 * sizeof(int32_t));
    f.close();
    
    std::cout << "\nLineitem l_shipdate (first 10 values, should be ~8000-10000 epoch days):\n";
    for (int i = 0; i < 10; i++) {
        std::cout << "  [" << i << "] = " << dates[i] << " days";
        if (dates[i] < 3000 || dates[i] > 20000) {
            std::cout << " **ERROR**";
        }
        std::cout << "\n";
    }

    std::cout << "\n=== VERIFICATION: DECIMAL COLUMNS ===\n";
    
    // Check orders.o_totalprice (should be non-zero with scale=100)
    std::ifstream f2(gendb_dir + "/orders/o_totalprice.bin", std::ios::binary);
    std::vector<int64_t> decimals(10);
    f2.read((char*)decimals.data(), 10 * sizeof(int64_t));
    f2.close();
    
    std::cout << "Orders o_totalprice (first 10 values, scaled by 100, all should be non-zero):\n";
    int non_zero = 0;
    for (int i = 0; i < 10; i++) {
        std::cout << "  [" << i << "] = " << decimals[i];
        double actual = (double)decimals[i] / 100.0;
        std::cout << " (actual: $" << actual << ")";
        if (decimals[i] != 0) non_zero++;
        std::cout << "\n";
    }
    if (non_zero != 10) {
        std::cout << "**WARNING**: Only " << non_zero << "/10 decimal values are non-zero\n";
    }

    // Check lineitem.l_extendedprice
    f2.open(gendb_dir + "/lineitem/l_extendedprice.bin", std::ios::binary);
    f2.read((char*)decimals.data(), 10 * sizeof(int64_t));
    f2.close();
    
    std::cout << "\nLineitem l_extendedprice (first 10 values, scaled by 100, all should be non-zero):\n";
    non_zero = 0;
    for (int i = 0; i < 10; i++) {
        std::cout << "  [" << i << "] = " << decimals[i];
        double actual = (double)decimals[i] / 100.0;
        std::cout << " (actual: $" << actual << ")";
        if (decimals[i] != 0) non_zero++;
        std::cout << "\n";
    }
    if (non_zero != 10) {
        std::cout << "**WARNING**: Only " << non_zero << "/10 decimal values are non-zero\n";
    }

    std::cout << "\n=== VERIFICATION: DICTIONARY ENCODING ===\n";
    std::ifstream dict_f(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    std::string line;
    std::cout << "Lineitem l_returnflag dictionary:\n";
    while (std::getline(dict_f, line)) {
        std::cout << "  " << line << "\n";
    }
    dict_f.close();

    std::cout << "\n=== VERIFICATION: FILE SIZES ===\n";
    std::cout << "Lineitem l_shipdate.bin: ";
    std::ifstream fs(gendb_dir + "/lineitem/l_shipdate.bin", std::ios::binary | std::ios::ate);
    std::cout << fs.tellg() / sizeof(int32_t) << " rows\n";
    fs.close();
    
    std::cout << "Orders o_orderdate.bin: ";
    fs.open(gendb_dir + "/orders/o_orderdate.bin", std::ios::binary | std::ios::ate);
    std::cout << fs.tellg() / sizeof(int32_t) << " rows\n";
    fs.close();

    std::cout << "\n=== VERIFICATION COMPLETE ===\n";

    return 0;
}
