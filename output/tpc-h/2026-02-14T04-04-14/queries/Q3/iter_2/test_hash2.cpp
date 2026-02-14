#include <iostream>
#include <vector>
#include <iomanip>

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
        std::cout << "Resize: " << table.size() << " -> " << new_capacity << ", rehashing " << count << " entries\n";
        std::vector<Entry> old_table = std::move(table);
        table.resize(new_capacity);
        count = 0;
        for (auto& e : old_table) {
            if (e.occupied) {
                std::cout << "  Reinserting key=" << e.key << std::endl;
                insert(e.key, e.value);
            }
        }
        std::cout << "Resize complete, count=" << count << std::endl;
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

        std::cout << "Insert key=" << key << ", hash_idx=" << idx << ", table_size=" << table.size() << std::endl;

        int probe_count = 0;
        while (true) {
            probe_count++;
            if (probe_count > 20) {
                std::cout << "  After " << probe_count << " probes at key=" << key << ", idx=" << idx 
                          << ", occupied=" << (int)table[idx].occupied 
                          << ", dist=" << table[idx].distance << "\n";
                if (probe_count > 100) {
                    std::cout << "  INFINITE LOOP!\n";
                    return;
                }
            }

            if (!table[idx].occupied) {
                std::cout << "  Found empty at idx=" << idx << ", distance=" << (int)distance << std::endl;
                table[idx].key = key;
                table[idx].value = value;
                table[idx].distance = distance;
                table[idx].occupied = true;
                count++;
                return;
            }

            if (table[idx].key == key) {
                std::cout << "  Found existing key at idx=" << idx << std::endl;
                table[idx].value = value;
                return;
            }

            if (distance > table[idx].distance) {
                std::cout << "  Displacement: idx=" << idx << ", our_dist=" << (int)distance 
                          << " > their_dist=" << table[idx].distance << std::endl;
                std::swap(table[idx].key, key);
                std::swap(table[idx].value, value);
                std::swap(table[idx].distance, distance);
            }

            idx = (idx + 1) & (table.size() - 1);
            distance++;
        }
    }

    size_t size() const { return count; }
};

int main() {
    CompactHashTable<int32_t, int32_t> ht(10);  // Small table to trigger resize quickly
    
    for (int32_t i = 0; i < 15; i++) {
        std::cout << "\n=== Inserting " << i << " ===\n";
        ht.insert(i, i * 2);
    }

    std::cout << "\nFinal table size: " << ht.size() << std::endl;
    return 0;
}
