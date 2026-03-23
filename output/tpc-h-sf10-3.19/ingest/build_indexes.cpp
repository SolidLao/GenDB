#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <cassert>
#include <omp.h>

namespace fs = std::filesystem;

// ============================================================
// Helpers
// ============================================================

template<typename T>
std::vector<T> load_bin(const std::string& path, size_t count) {
    std::vector<T> data(count);
    std::ifstream f(path, std::ios::binary);
    if (!f) { std::cerr << "Cannot open " << path << "\n"; return data; }
    f.read((char*)data.data(), count * sizeof(T));
    return data;
}

uint64_t load_row_count(const std::string& table_dir) {
    uint64_t count = 0;
    std::ifstream f(table_dir + "/meta.bin", std::ios::binary);
    f.read((char*)&count, sizeof(count));
    return count;
}

template<typename T>
void write_bin(const std::string& path, const T* data, size_t count) {
    std::ofstream f(path, std::ios::binary);
    f.write((const char*)data, count * sizeof(T));
}

// ============================================================
// 1. Zone Map on lineitem.l_shipdate
// Format: For each block of BLOCK_SIZE rows, store (min_date, max_date) as two int32_t.
// Total entries: ceil(N / BLOCK_SIZE) * 2 int32_t values.
// File: indexes/lineitem_l_shipdate_zonemap.bin
//   Header: uint64_t num_blocks, uint32_t block_size
//   Body: int32_t[num_blocks * 2] (min, max pairs)
// ============================================================
void build_shipdate_zonemap(const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/lineitem";
    uint64_t N = load_row_count(table_dir);
    auto shipdate = load_bin<int32_t>(table_dir + "/l_shipdate.bin", N);

    const uint32_t BLOCK_SIZE = 65536;
    uint64_t num_blocks = (N + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // Each block: (min_date, max_date)
    std::vector<int32_t> zonemap(num_blocks * 2);

    #pragma omp parallel for
    for (uint64_t b = 0; b < num_blocks; b++) {
        uint64_t start = b * BLOCK_SIZE;
        uint64_t end = std::min(start + BLOCK_SIZE, N);
        int32_t mn = shipdate[start], mx = shipdate[start];
        for (uint64_t i = start + 1; i < end; i++) {
            if (shipdate[i] < mn) mn = shipdate[i];
            if (shipdate[i] > mx) mx = shipdate[i];
        }
        zonemap[b * 2] = mn;
        zonemap[b * 2 + 1] = mx;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_dir + "/lineitem_l_shipdate_zonemap.bin", std::ios::binary);
    f.write((char*)&num_blocks, sizeof(num_blocks));
    f.write((char*)&BLOCK_SIZE, sizeof(BLOCK_SIZE));
    f.write((char*)zonemap.data(), zonemap.size() * sizeof(int32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  shipdate zonemap: " << num_blocks << " blocks in "
              << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 2. Grouped index on lineitem.l_orderkey (sorted data)
// Since lineitem is sorted by l_orderkey, store for each unique orderkey:
//   (start_row, count)
// Format: Dense array indexed by orderkey.
//   File: indexes/lineitem_l_orderkey_grouped.bin
//   Header: uint64_t num_entries (= max_orderkey + 1)
//   Body: struct { uint32_t start; uint32_t count; } [num_entries]
//   For orderkeys with no lineitems: start=0, count=0
// ============================================================
void build_orderkey_grouped(const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/lineitem";
    uint64_t N = load_row_count(table_dir);
    auto orderkey = load_bin<int32_t>(table_dir + "/l_orderkey.bin", N);

    // Find max orderkey
    int32_t max_ok = 0;
    #pragma omp parallel for reduction(max:max_ok)
    for (uint64_t i = 0; i < N; i++) {
        if (orderkey[i] > max_ok) max_ok = orderkey[i];
    }

    uint64_t num_entries = (uint64_t)max_ok + 1;
    // Allocate start/count arrays
    std::vector<uint32_t> starts(num_entries, 0);
    std::vector<uint32_t> counts(num_entries, 0);

    // Since data is sorted by l_orderkey, scan linearly
    uint64_t i = 0;
    while (i < N) {
        int32_t key = orderkey[i];
        uint32_t start = (uint32_t)i;
        uint32_t count = 0;
        while (i < N && orderkey[i] == key) { ++count; ++i; }
        starts[key] = start;
        counts[key] = count;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    // Write interleaved: [start0, count0, start1, count1, ...]
    std::vector<uint32_t> interleaved(num_entries * 2);
    for (uint64_t j = 0; j < num_entries; j++) {
        interleaved[j * 2] = starts[j];
        interleaved[j * 2 + 1] = counts[j];
    }

    std::ofstream f(idx_dir + "/lineitem_l_orderkey_grouped.bin", std::ios::binary);
    f.write((char*)&num_entries, sizeof(num_entries));
    f.write((char*)interleaved.data(), interleaved.size() * sizeof(uint32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  orderkey grouped index: " << num_entries << " entries (max_orderkey=" << max_ok
              << ") in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 3. Dense PK lookup for orders: o_orderkey -> row_id
// Format: Dense array indexed by orderkey.
//   File: indexes/orders_o_orderkey_lookup.bin
//   Header: uint64_t num_entries (= max_orderkey + 1)
//   Body: int32_t[num_entries] where arr[key] = row_id, -1 if missing
// ============================================================
void build_orders_pk_lookup(const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/orders";
    uint64_t N = load_row_count(table_dir);
    auto orderkey = load_bin<int32_t>(table_dir + "/o_orderkey.bin", N);

    int32_t max_ok = 0;
    #pragma omp parallel for reduction(max:max_ok)
    for (uint64_t i = 0; i < N; i++) {
        if (orderkey[i] > max_ok) max_ok = orderkey[i];
    }

    uint64_t num_entries = (uint64_t)max_ok + 1;
    std::vector<int32_t> lookup(num_entries, -1);

    #pragma omp parallel for
    for (uint64_t i = 0; i < N; i++) {
        lookup[orderkey[i]] = (int32_t)i;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_dir + "/orders_o_orderkey_lookup.bin", std::ios::binary);
    f.write((char*)&num_entries, sizeof(num_entries));
    f.write((char*)lookup.data(), lookup.size() * sizeof(int32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  orders PK lookup: " << num_entries << " entries in "
              << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 4. Hash index for partsupp: (ps_partkey, ps_suppkey) -> row_id
// Open-addressing hash table with linear probing.
// Format:
//   File: indexes/partsupp_pk_hash.bin
//   Header: uint64_t capacity
//   Body: struct { int32_t partkey; int32_t suppkey; int32_t row_id; int32_t padding; } [capacity]
//   Empty slot: partkey == -1
//
// Hash function: ((uint64_t)partkey * 2654435761ULL) ^ ((uint64_t)suppkey * 40499ULL)
// ============================================================
struct PSHashEntry {
    int32_t partkey;
    int32_t suppkey;
    int32_t row_id;
    int32_t padding;
};

void build_partsupp_hash(const std::string& storage_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/partsupp";
    uint64_t N = load_row_count(table_dir);
    auto partkey = load_bin<int32_t>(table_dir + "/ps_partkey.bin", N);
    auto suppkey = load_bin<int32_t>(table_dir + "/ps_suppkey.bin", N);

    // Capacity: next power of 2 above N * 2
    uint64_t cap = 1;
    while (cap < N * 2) cap <<= 1;

    std::vector<PSHashEntry> table(cap);
    for (uint64_t i = 0; i < cap; i++) {
        table[i].partkey = -1;
        table[i].suppkey = -1;
        table[i].row_id = -1;
        table[i].padding = 0;
    }

    uint64_t mask = cap - 1;
    for (uint64_t i = 0; i < N; i++) {
        uint64_t h = ((uint64_t)(uint32_t)partkey[i] * 2654435761ULL) ^ ((uint64_t)(uint32_t)suppkey[i] * 40499ULL);
        uint64_t slot = h & mask;
        while (table[slot].partkey != -1) {
            slot = (slot + 1) & mask;
        }
        table[slot].partkey = partkey[i];
        table[slot].suppkey = suppkey[i];
        table[slot].row_id = (int32_t)i;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_dir + "/partsupp_pk_hash.bin", std::ios::binary);
    f.write((char*)&cap, sizeof(cap));
    f.write((char*)table.data(), cap * sizeof(PSHashEntry));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  partsupp hash index: " << N << " entries, capacity " << cap
              << " in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// MAIN
// ============================================================
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <storage_dir>\n";
        return 1;
    }
    std::string storage_dir = argv[1];

    auto t0 = std::chrono::high_resolution_clock::now();
    std::cout << "Building indexes...\n";
    std::cout << "  Storage: " << storage_dir << "\n";
    std::cout << "  Threads: " << omp_get_max_threads() << "\n\n";

    // Build indexes (some can run concurrently)
    build_shipdate_zonemap(storage_dir);
    build_orderkey_grouped(storage_dir);
    build_orders_pk_lookup(storage_dir);
    build_partsupp_hash(storage_dir);

    auto t1 = std::chrono::high_resolution_clock::now();
    double total = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "\nTotal index building time: " << total << "s\n";
    return 0;
}
