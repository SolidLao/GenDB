#include "index/index.h"
#include <iostream>

int main() {
    gendb::RobinHoodMap<gendb::Q3GroupKey, double, gendb::Q3GroupKeyHash> map1;
    gendb::RobinHoodMap<gendb::Q3GroupKey, double, gendb::Q3GroupKeyHash> map2;
    
    gendb::Q3GroupKey key1{100, 9000, 0};
    gendb::Q3GroupKey key2{100, 9000, 0};  // Same as key1
    gendb::Q3GroupKey key3{200, 9001, 0};  // Different
    
    map1[key1] += 100.0;
    map2[key2] += 50.0;
    map2[key3] += 200.0;
    
    std::cout << "Map1 size: " << map1.size() << std::endl;
    std::cout << "Map2 size: " << map2.size() << std::endl;
    
    // Merge map2 into map1
    gendb::RobinHoodMap<gendb::Q3GroupKey, double, gendb::Q3GroupKeyHash> result;
    for (auto pair : map1) {
        result[pair.first] += pair.second;
    }
    for (auto pair : map2) {
        result[pair.first] += pair.second;
    }
    
    std::cout << "Result size: " << result.size() << " (should be 2)" << std::endl;
    for (auto pair : result) {
        std::cout << "Key(" << pair.first.l_orderkey << "," << pair.first.o_orderdate << "," << pair.first.o_shippriority << ") = " << pair.second << std::endl;
    }
    
    return 0;
}
