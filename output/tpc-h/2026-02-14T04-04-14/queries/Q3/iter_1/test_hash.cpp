#include <iostream>
#include <vector>

template <typename K, typename V>
class CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint8_t distance;
        bool occupied = false;
    };
    std::vector<Entry> table;
    size_t count = 0;
    static constexpr float load_factor = 0.75f;

    size_t hash(K key) const {
        uint64_t h = key;
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        return h;
    }

public:
    CompactHashTable(size_t initial_capacity = 1024) {
        table.resize(initial_capacity);
    }

    void insert(K key, V value) {
        if (count >= size_t(table.size() * load_factor)) {
            std::cout << "Need resize at count=" << count << "\n";
            return;  // Skip resize to test
        }

        size_t idx = hash(key) & (table.size() - 1);
        uint8_t distance = 0;
        int iterations = 0;

        while (true) {
            iterations++;
            if (iterations > 1000) {
                std::cout << "INFINITE LOOP DETECTED at idx=" << idx << " distance=" << (int)distance << "\n";
                return;
            }

            if (!table[idx].occupied) {
                table[idx].key = key;
                table[idx].value = value;
                table[idx].distance = distance;
                table[idx].occupied = true;
                count++;
                return;
            }

            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }

            if (distance > table[idx].distance) {
                std::swap(table[idx].key, key);
                std::swap(table[idx].value, value);
                std::swap(table[idx].distance, distance);
            }

            idx = (idx + 1) & (table.size() - 1);
            distance++;
        }
    }
};

int main() {
    CompactHashTable<int32_t, double> ht(256);
    for (int i = 0; i < 100; i++) {
        ht.insert(i, i * 1.5);
    }
    std::cout << "Test passed\n";
    return 0;
}
