#include "operators/robin_hood_map.h"
#include <iostream>

struct TestKey {
    int a, b;
    bool operator==(const TestKey& other) const {
        return a == other.a && b == other.b;
    }
};

struct TestHash {
    size_t operator()(const TestKey& k) const {
        return std::hash<int>()(k.a) ^ std::hash<int>()(k.b);
    }
};

int main() {
    gendb::RobinHoodMap<TestKey, double, TestHash> map;
    
    map[TestKey{1, 2}] += 100.0;
    map[TestKey{1, 2}] += 50.0;
    map[TestKey{3, 4}] += 200.0;
    
    std::cout << "Map size: " << map.size() << std::endl;
    
    for (const auto& [key, value] : map) {
        std::cout << "Key(" << key.a << "," << key.b << ") = " << value << std::endl;
    }
    
    return 0;
}
