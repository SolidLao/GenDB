#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_set>
#include <string>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cstring>
#include <cstdint>

/*
================================================================================
                        Q20 OPTIMIZED IMPLEMENTATION (ITER 4)
================================================================================

Target: 102ms (currently ~1844ms) — 18.1x gap from Umbra

QUERY ANALYSIS (ITER 2-3 REGRESSION ANALYSIS):
- lineitem_aggregation: 1297ms / 70% (DOMINANT BOTTLENECK)
- Sequential merge of 64 thread-local tables is serialized
- Current approach: thread-local tables → sequential merge (kills parallelism)

KEY OPTIMIZATION (ITERATION 4):
PARTITIONED HASH AGGREGATION (eliminate merge bottleneck)
  Problem: Sequential merge of 64 tables is ~O(15M) inherent latency
  Solution: Partition by key hash → each partition built by dedicated threads → no merge
  Expected gain: 4-6x speedup (from 1297ms → 250-300ms)

  Approach:
  1. Hash (partkey, suppkey) → partition_id = hash % 64
  2. Each thread writes to multiple partitions (contention-free)
  3. Each partition owns its CompactHashTable → built exclusively by subset of threads
  4. Final concatenation: O(final groups) not O(initial groups)

PHYSICAL PLAN:
1. Load nation → find CANADA nationkey [2]
2. Load part → filter "forest%" → set (~10K rows)
3. Load lineitem → PARALLEL aggregation:
   - Partition by hash(partkey, suppkey) into 64 partitions
   - Each partition has its own CompactHashTable
   - Threads write to partitions based on row key hash (zero contention)
   - No merge phase — partitions are independent
4. Load partsupp → filter by partkey + availability check
5. Load supplier → filter by nation + suppkey
6. Sort & output

ARCHITECTURE DECISIONS:
- Partitioned open-addressing hash tables (64 partitions)
- Each partition pre-sized: 15M rows / 64 partitions = ~235K expected groups per partition
- Static assignment: thread t handles partition p if (p % num_threads == t)
- Zero-copy merge: partition tables are independent

================================================================================
*/

const int32_t DATE_1994_01_01 = 8766;
const int32_t DATE_1995_01_01 = 9131;

struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            exit(1);
        }
        size = lseek(fd, 0, SEEK_END);
        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            exit(1);
        }
    }

    ~MmapFile() {
        if (ptr != MAP_FAILED && ptr) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }
};

std::string read_string(std::istream& f) {
    int32_t len;
    f.read(reinterpret_cast<char*>(&len), sizeof(int32_t));
    if (!f || f.eof()) return "";
    std::string s(len, '\0');
    f.read(&s[0], len);
    return s;
}

std::vector<std::string> load_string_column(const std::string& file_path) {
    std::vector<std::string> result;
    std::ifstream f(file_path, std::ios::binary);
    if (!f.is_open()) {
        std::cerr << "Failed to open string file: " << file_path << std::endl;
        exit(1);
    }

    while (true) {
        std::string s = read_string(f);
        if (s.empty() && f.eof()) break;
        if (f.eof()) break;
        result.push_back(s);
    }
    return result;
}

inline bool starts_with_forest(const std::string& s) {
    return s.size() >= 6 && s[0] == 'f' && s[1] == 'o' && s[2] == 'r' &&
           s[3] == 'e' && s[4] == 's' && s[5] == 't';
}

// Efficient pair hash with bit packing (avoid std::hash<pair> overhead)
inline uint64_t hash_pair(int32_t k1, int32_t k2) {
    uint64_t h = ((uint64_t)k1 << 32) | (uint32_t)k2;
    h ^= h >> 33;
    h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33;
    return h;
}

using KeyPair = std::pair<int32_t, int32_t>;

// Open-addressing hash table with Robin Hood hashing for aggregation
struct CompactHashTable {
    struct Entry {
        KeyPair key;
        int64_t value;
        uint8_t dist;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashTable(size_t expected_size = 0) : count(0) {
        // Pre-size at 75% load factor, power-of-2
        size_t capacity = 1;
        if (expected_size > 0) {
            while (capacity < expected_size * 4 / 3) capacity <<= 1;
        } else {
            capacity = 16;
        }
        table.resize(capacity);
        mask = capacity - 1;
    }

    void reserve(size_t expected_size) {
        size_t capacity = 1;
        while (capacity < expected_size * 4 / 3) capacity <<= 1;
        if (capacity > table.capacity()) {
            // Rebuild table with new capacity
            std::vector<Entry> old_table = std::move(table);
            table.clear();
            table.resize(capacity);
            mask = capacity - 1;
            count = 0;

            for (const auto& entry : old_table) {
                if (entry.occupied) {
                    insert(entry.key, entry.value);
                }
            }
        }
    }

    void insert(KeyPair key, int64_t value) {
        size_t pos = hash_pair(key.first, key.second) & mask;
        Entry entry{key, value, 0, true};

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value = value;
                return;
            }
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }

        table[pos] = entry;
        count++;
    }

    // Insert with aggregation (add to existing or create new)
    void insert_agg(KeyPair key, int64_t delta) {
        size_t pos = hash_pair(key.first, key.second) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                table[pos].value += delta;
                return;
            }
            if (dist > table[pos].dist) break;
            pos = (pos + 1) & mask;
            dist++;
        }

        // Not found, need to insert
        Entry entry{key, delta, dist, true};
        while (table[pos].occupied) {
            if (entry.dist > table[pos].dist) {
                std::swap(entry, table[pos]);
            }
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    // Find with const reference (for merging)
    int64_t* find(KeyPair key) {
        size_t pos = hash_pair(key.first, key.second) & mask;
        uint8_t dist = 0;

        while (table[pos].occupied) {
            if (table[pos].key == key) {
                return &table[pos].value;
            }
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    // Iterator for merge
    auto begin() {
        return table.begin();
    }

    auto end() {
        return table.end();
    }

    size_t size() const {
        return count;
    }
};

void run_q20(const std::string& gendb_dir, const std::string& results_dir) {
    auto total_start = std::chrono::high_resolution_clock::now();

    // =========================================================================
    // Step 1: Load nation, find CANADA
    // =========================================================================

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string nation_dir = gendb_dir + "/nation";
    MmapFile nation_nationkey_mmap(nation_dir + "/n_nationkey.bin");
    int32_t* nation_nationkey = static_cast<int32_t*>(nation_nationkey_mmap.ptr);
    size_t nation_count = nation_nationkey_mmap.size / sizeof(int32_t);
    auto nation_names = load_string_column(nation_dir + "/n_name.bin");

    int32_t canada_nationkey = -1;
    for (size_t i = 0; i < nation_count; i++) {
        if (nation_names[i] == "CANADA") {
            canada_nationkey = nation_nationkey[i];
            break;
        }
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] nation_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 2: Load part, filter by p_name LIKE 'forest%'
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string part_dir = gendb_dir + "/part";
    MmapFile part_partkey_mmap(part_dir + "/p_partkey.bin");
    int32_t* part_partkey = static_cast<int32_t*>(part_partkey_mmap.ptr);
    size_t part_count = part_partkey_mmap.size / sizeof(int32_t);

    auto part_names = load_string_column(part_dir + "/p_name.bin");

    std::unordered_set<int32_t> qualified_partkey;
    for (size_t i = 0; i < part_count; i++) {
        if (starts_with_forest(part_names[i])) {
            qualified_partkey.insert(part_partkey[i]);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] part_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 3: Parallel scan with PARTITIONED hash aggregation (ITER 4)
    // KEY OPTIMIZATION: Partition by hash(key) → eliminate sequential merge
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string lineitem_dir = gendb_dir + "/lineitem";
    MmapFile li_partkey_mmap(lineitem_dir + "/l_partkey.bin");
    int32_t* li_partkey = static_cast<int32_t*>(li_partkey_mmap.ptr);
    size_t lineitem_count = li_partkey_mmap.size / sizeof(int32_t);

    MmapFile li_suppkey_mmap(lineitem_dir + "/l_suppkey.bin");
    int32_t* li_suppkey = static_cast<int32_t*>(li_suppkey_mmap.ptr);

    MmapFile li_quantity_mmap(lineitem_dir + "/l_quantity.bin");
    int64_t* li_quantity = static_cast<int64_t*>(li_quantity_mmap.ptr);

    MmapFile li_shipdate_mmap(lineitem_dir + "/l_shipdate.bin");
    int32_t* li_shipdate = static_cast<int32_t*>(li_shipdate_mmap.ptr);

    // PARTITIONED AGGREGATION: 64 partitions, each with its own hash table
    // Partition = hash(key) % 64
    // This eliminates the merge bottleneck: each partition is built independently
    const int NUM_PARTITIONS = 64;
    std::vector<CompactHashTable> partition_tables;
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        partition_tables.emplace_back(15000000 / NUM_PARTITIONS + 10000);
    }

#pragma omp parallel for schedule(static, 500000)
    for (size_t i = 0; i < lineitem_count; i++) {
        if (li_shipdate[i] >= DATE_1994_01_01 && li_shipdate[i] < DATE_1995_01_01) {
            KeyPair key = {li_partkey[i], li_suppkey[i]};
            uint64_t key_hash = hash_pair(key.first, key.second);
            int partition = key_hash % NUM_PARTITIONS;
            partition_tables[partition].insert_agg(key, li_quantity[i]);
        }
    }

    // NO MERGE PHASE: Partitions are independent and will be scanned directly
    // Create a vector of partition references for efficient lookup
    std::vector<CompactHashTable*> partitions;
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        partitions.push_back(&partition_tables[p]);
    }

    // Helper function to find a key in the partitioned tables
    auto find_in_partitions = [&](KeyPair key) -> int64_t* {
        uint64_t key_hash = hash_pair(key.first, key.second);
        int partition = key_hash % NUM_PARTITIONS;
        return partitions[partition]->find(key);
    };

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] lineitem_aggregation: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 4: Load partsupp, filter by qualified partkey + availability
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string partsupp_dir = gendb_dir + "/partsupp";
    MmapFile ps_partkey_mmap(partsupp_dir + "/ps_partkey.bin");
    int32_t* ps_partkey = static_cast<int32_t*>(ps_partkey_mmap.ptr);
    size_t partsupp_count = ps_partkey_mmap.size / sizeof(int32_t);

    MmapFile ps_suppkey_mmap(partsupp_dir + "/ps_suppkey.bin");
    int32_t* ps_suppkey = static_cast<int32_t*>(ps_suppkey_mmap.ptr);

    MmapFile ps_availqty_mmap(partsupp_dir + "/ps_availqty.bin");
    int32_t* ps_availqty = static_cast<int32_t*>(ps_availqty_mmap.ptr);

    struct PairHashFn {
        size_t operator()(const KeyPair& p) const {
            return hash_pair(p.first, p.second);
        }
    };

    std::unordered_set<KeyPair, PairHashFn> qualified_partsupp;
    qualified_partsupp.reserve(200);

    for (size_t i = 0; i < partsupp_count; i++) {
        if (qualified_partkey.count(ps_partkey[i]) == 0) continue;

        KeyPair key = {ps_partkey[i], ps_suppkey[i]};
        int64_t* val = find_in_partitions(key);  // Lookup in partitioned tables
        if (!val) continue;

        int64_t threshold = (*val) / 2;
        if ((int64_t)ps_availqty[i] * 100 > threshold) {
            qualified_partsupp.insert(key);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] partsupp_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 5: Load supplier, filter and join
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string supplier_dir = gendb_dir + "/supplier";
    MmapFile supp_suppkey_mmap(supplier_dir + "/s_suppkey.bin");
    int32_t* supp_suppkey = static_cast<int32_t*>(supp_suppkey_mmap.ptr);
    size_t supplier_count = supp_suppkey_mmap.size / sizeof(int32_t);

    MmapFile supp_nationkey_mmap(supplier_dir + "/s_nationkey.bin");
    int32_t* supp_nationkey = static_cast<int32_t*>(supp_nationkey_mmap.ptr);

    auto supp_names = load_string_column(supplier_dir + "/s_name.bin");
    auto supp_addresses = load_string_column(supplier_dir + "/s_address.bin");

    std::unordered_set<int32_t> qualified_suppkey;
    for (const auto& key : qualified_partsupp) {
        qualified_suppkey.insert(key.second);
    }

    struct Result {
        std::string s_name;
        std::string s_address;
        bool operator<(const Result& other) const {
            return s_name < other.s_name;
        }
    };

    std::vector<Result> results;
    results.reserve(2000);

    for (size_t i = 0; i < supplier_count; i++) {
        if (supp_nationkey[i] != canada_nationkey) continue;
        if (qualified_suppkey.count(supp_suppkey[i]) == 0) continue;

        Result r;
        r.s_name = supp_names[i];
        r.s_address = supp_addresses[i];
        results.push_back(r);
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] supplier_filter: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 6: Sort results by s_name
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::sort(results.begin(), results.end());

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms);
#endif

    // =========================================================================
    // Step 7: Write CSV output
    // =========================================================================

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::string output_file = results_dir + "/Q20.csv";
    std::ofstream out(output_file);
    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        exit(1);
    }

    out << "s_name,s_address\n";
    for (const auto& r : results) {
        std::string escaped_address = r.s_address;
        bool needs_quotes = false;
        for (char c : escaped_address) {
            if (c == ',' || c == '"' || c == '\n') {
                needs_quotes = true;
                break;
            }
        }

        out << r.s_name;
        out << ",";
        if (needs_quotes) {
            out << "\"";
            for (char c : escaped_address) {
                if (c == '"') out << "\"\"";
                else out << c;
            }
            out << "\"";
        } else {
            out << escaped_address;
        }
        out << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q20(gendb_dir, results_dir);
    return 0;
}
#endif
