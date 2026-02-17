# Join Order Sampling Program Template

## Purpose
For multi-table joins (2+ JOINs), empirically determine the best join order by
sampling ~100K rows per table and measuring intermediate result sizes.

## When to Use
- Query has 2+ JOINs
- Join operation dominates execution time (>40% of total)
- Heuristic join ordering may be suboptimal

## Template: sampling_join_order.cpp

```cpp
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <chrono>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

// Helper: mmap a binary column file
template<typename T>
struct MappedColumn {
    const T* data;
    size_t count;
    size_t file_size;

    MappedColumn(const std::string& path, size_t row_count) : count(row_count) {
        int fd = open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st); file_size = st.st_size;
        data = (const T*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
    }
    ~MappedColumn() { munmap((void*)data, file_size); }
};

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    const size_t SAMPLE_SIZE = 100000; // rows to sample per table

    // 1. Load columns (sample first SAMPLE_SIZE rows)
    // Example for a 3-table join: table_A, table_B, table_C
    // MappedColumn<int32_t> table_A_key(gendb_dir + "/table_A.gendb/key.col", table_A_rows);
    // ... load all join key columns and filter columns

    // 2. Apply single-table predicates to get filtered row indices
    // std::vector<uint32_t> filtered_A, filtered_B, filtered_C;
    // for (size_t i = 0; i < std::min(SAMPLE_SIZE, table_A_rows); i++) {
    //     if (table_A_filter[i] == target_value) filtered_A.push_back(i);
    // }

    // 3. Test each join order
    struct JoinOrder {
        std::string name;
        size_t intermediate_1; // rows after first join
        size_t intermediate_2; // rows after second join (final)
        size_t total_intermediate;
    };
    std::vector<JoinOrder> orders;

    // Order A: table_A -> table_B -> table_C
    {
        // Build hash on filtered table_A keys
        std::unordered_set<int32_t> A_keys;
        for (auto idx : filtered_A) A_keys.insert(table_A_key.data[idx]);

        // Probe with filtered table_B
        std::vector<uint32_t> intermediate;
        for (auto idx : filtered_B) {
            if (A_keys.count(table_B_fk.data[idx])) intermediate.push_back(idx);
        }
        size_t inter1 = intermediate.size();

        // Build hash on intermediate keys
        std::unordered_set<int32_t> B_keys;
        for (auto idx : intermediate) B_keys.insert(table_B_key.data[idx]);

        // Probe with filtered table_C
        size_t inter2 = 0;
        for (auto idx : filtered_C) {
            if (B_keys.count(table_C_fk.data[idx])) inter2++;
        }

        orders.push_back({"A->B->C", inter1, inter2, inter1 + inter2});
    }

    // Order B: table_B -> table_A -> table_C
    // ... similar pattern

    // Order C: table_C -> table_B -> table_A
    // ... similar pattern

    // 4. Find best order (smallest total intermediate)
    std::sort(orders.begin(), orders.end(),
        [](const JoinOrder& a, const JoinOrder& b) { return a.total_intermediate < b.total_intermediate; });

    // 5. Output JSON result
    std::cout << "{" << std::endl;
    std::cout << "  \"best_order\": \"" << orders[0].name << "\"," << std::endl;
    std::cout << "  \"orders\": [" << std::endl;
    for (size_t i = 0; i < orders.size(); i++) {
        std::cout << "    {\"name\": \"" << orders[i].name
                  << "\", \"intermediate_1\": " << orders[i].intermediate_1
                  << ", \"intermediate_2\": " << orders[i].intermediate_2
                  << ", \"total\": " << orders[i].total_intermediate << "}";
        if (i + 1 < orders.size()) std::cout << ",";
        std::cout << std::endl;
    }
    std::cout << "  ]" << std::endl;
    std::cout << "}" << std::endl;

    return 0;
}
```

## Handling Encoded Columns During Sampling
- **Dictionary-encoded**: Load dictionary from metadata, translate filter values to codes
- **Delta-encoded**: Must decode (cumulative sum) before sampling
- **Date columns**: Compare as epoch days (int32_t)
- **Decimal columns**: Compare as scaled int64_t

## For 4+ Table Joins
Use greedy heuristic instead of exhaustive enumeration:
1. Start with the most selective single-table filter result
2. Greedily add the table that produces the smallest intermediate
3. Repeat until all tables joined

## Expected Runtime
- SF10 with 100K sample: < 2 seconds
- SF100 with 100K sample: < 5 seconds
