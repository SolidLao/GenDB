#include <iostream>
#include <vector>
#include <unordered_map>
#include <chrono>

template <typename K, typename V>
class CompactHashTable {
    struct Entry {
        K key;
        V value;
        uint16_t distance;
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

    void resize(size_t new_capacity) {
        std::cout << "Resizing from " << table.size() << " to " << new_capacity << std::endl;
        std::vector<Entry> old_table = std::move(table);
        table.resize(new_capacity);
        count = 0;
        for (auto& e : old_table) {
            if (e.occupied) {
                insert(e.key, e.value);
            }
        }
    }

public:
    CompactHashTable(size_t initial_capacity = 1024) {
        table.resize(initial_capacity);
    }

    void insert(K key, V value) {
        if (count >= size_t(table.size() * load_factor)) {
            resize(table.size() * 2);
        }

        size_t idx = hash(key) & (table.size() - 1);
        uint16_t distance = 0;

        int probe_count = 0;
        while (true) {
            probe_count++;
            if (probe_count > 100000) {
                std::cout << "INFINITE LOOP DETECTED at key=" << key << " table_size=" << table.size() << std::endl;
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

    V* find(K key) {
        size_t idx = hash(key) & (table.size() - 1);
        uint16_t distance = 0;

        while (true) {
            if (!table[idx].occupied) return nullptr;
            if (table[idx].key == key) return &table[idx].value;
            if (distance > table[idx].distance) return nullptr;

            idx = (idx + 1) & (table.size() - 1);
            distance++;
        }
    }

    size_t size() const { return count; }
};

int main() {
    CompactHashTable<int32_t, int32_t> ht(100);
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int32_t i = 0; i < 1000000; i++) {
        ht.insert(i, i * 2);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    
    std::cout << "Inserted 1M items in " << duration << " seconds" << std::endl;
    std::cout << "Table size: " << ht.size() << std::endl;
    
    return 0;
}
