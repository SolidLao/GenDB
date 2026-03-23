#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <filesystem>
#include <algorithm>
#include <chrono>
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

bool file_exists(const std::string& path) {
    return fs::exists(path);
}

// ============================================================
// 1. Zone Map on orders.o_orderdate
// Same format as lineitem_l_shipdate_zonemap:
//   Header: uint64_t num_blocks, uint32_t block_size
//   Body: int32_t[num_blocks * 2] (min, max pairs)
// ============================================================
void build_orderdate_zonemap(const std::string& storage_dir) {
    std::string idx_path = storage_dir + "/indexes/orders_o_orderdate_zonemap.bin";
    if (file_exists(idx_path)) {
        std::cout << "  [SKIP] orders_o_orderdate_zonemap already exists\n";
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/orders";
    uint64_t N = load_row_count(table_dir);
    auto orderdate = load_bin<int32_t>(table_dir + "/o_orderdate.bin", N);

    const uint32_t BLOCK_SIZE = 65536;
    uint64_t num_blocks = (N + BLOCK_SIZE - 1) / BLOCK_SIZE;
    std::vector<int32_t> zonemap(num_blocks * 2);

    #pragma omp parallel for
    for (uint64_t b = 0; b < num_blocks; b++) {
        uint64_t start = b * BLOCK_SIZE;
        uint64_t end = std::min(start + (uint64_t)BLOCK_SIZE, N);
        int32_t mn = orderdate[start], mx = orderdate[start];
        for (uint64_t i = start + 1; i < end; i++) {
            if (orderdate[i] < mn) mn = orderdate[i];
            if (orderdate[i] > mx) mx = orderdate[i];
        }
        zonemap[b * 2] = mn;
        zonemap[b * 2 + 1] = mx;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_path, std::ios::binary);
    f.write((char*)&num_blocks, sizeof(num_blocks));
    f.write((char*)&BLOCK_SIZE, sizeof(BLOCK_SIZE));
    f.write((char*)zonemap.data(), zonemap.size() * sizeof(int32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  orders_o_orderdate_zonemap: " << num_blocks << " blocks in "
              << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 2. Zone Map on lineitem.l_receiptdate
// Same format as shipdate zonemap
// ============================================================
void build_receiptdate_zonemap(const std::string& storage_dir) {
    std::string idx_path = storage_dir + "/indexes/lineitem_l_receiptdate_zonemap.bin";
    if (file_exists(idx_path)) {
        std::cout << "  [SKIP] lineitem_l_receiptdate_zonemap already exists\n";
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/lineitem";
    uint64_t N = load_row_count(table_dir);
    auto receiptdate = load_bin<int32_t>(table_dir + "/l_receiptdate.bin", N);

    const uint32_t BLOCK_SIZE = 65536;
    uint64_t num_blocks = (N + BLOCK_SIZE - 1) / BLOCK_SIZE;
    std::vector<int32_t> zonemap(num_blocks * 2);

    #pragma omp parallel for
    for (uint64_t b = 0; b < num_blocks; b++) {
        uint64_t start = b * BLOCK_SIZE;
        uint64_t end = std::min(start + (uint64_t)BLOCK_SIZE, N);
        int32_t mn = receiptdate[start], mx = receiptdate[start];
        for (uint64_t i = start + 1; i < end; i++) {
            if (receiptdate[i] < mn) mn = receiptdate[i];
            if (receiptdate[i] > mx) mx = receiptdate[i];
        }
        zonemap[b * 2] = mn;
        zonemap[b * 2 + 1] = mx;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_path, std::ios::binary);
    f.write((char*)&num_blocks, sizeof(num_blocks));
    f.write((char*)&BLOCK_SIZE, sizeof(BLOCK_SIZE));
    f.write((char*)zonemap.data(), zonemap.size() * sizeof(int32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  lineitem_l_receiptdate_zonemap: " << num_blocks << " blocks in "
              << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 3. Dense PK lookup for customer: c_custkey -> row_id
// Same format as orders_o_orderkey_lookup:
//   Header: uint64_t num_entries (= max_custkey + 1)
//   Body: int32_t[num_entries] where arr[key] = row_id, -1 if missing
// ============================================================
void build_customer_pk_lookup(const std::string& storage_dir) {
    std::string idx_path = storage_dir + "/indexes/customer_c_custkey_lookup.bin";
    if (file_exists(idx_path)) {
        std::cout << "  [SKIP] customer_c_custkey_lookup already exists\n";
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/customer";
    uint64_t N = load_row_count(table_dir);
    auto custkey = load_bin<int32_t>(table_dir + "/c_custkey.bin", N);

    int32_t max_ck = 0;
    #pragma omp parallel for reduction(max:max_ck)
    for (uint64_t i = 0; i < N; i++) {
        if (custkey[i] > max_ck) max_ck = custkey[i];
    }

    uint64_t num_entries = (uint64_t)max_ck + 1;
    std::vector<int32_t> lookup(num_entries, -1);

    #pragma omp parallel for
    for (uint64_t i = 0; i < N; i++) {
        lookup[custkey[i]] = (int32_t)i;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_path, std::ios::binary);
    f.write((char*)&num_entries, sizeof(num_entries));
    f.write((char*)lookup.data(), lookup.size() * sizeof(int32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  customer_c_custkey_lookup: " << num_entries << " entries in "
              << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 4. Dense PK lookup for supplier: s_suppkey -> row_id
// Same format as orders_o_orderkey_lookup
// ============================================================
void build_supplier_pk_lookup(const std::string& storage_dir) {
    std::string idx_path = storage_dir + "/indexes/supplier_s_suppkey_lookup.bin";
    if (file_exists(idx_path)) {
        std::cout << "  [SKIP] supplier_s_suppkey_lookup already exists\n";
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/supplier";
    uint64_t N = load_row_count(table_dir);
    auto suppkey = load_bin<int32_t>(table_dir + "/s_suppkey.bin", N);

    int32_t max_sk = 0;
    #pragma omp parallel for reduction(max:max_sk)
    for (uint64_t i = 0; i < N; i++) {
        if (suppkey[i] > max_sk) max_sk = suppkey[i];
    }

    uint64_t num_entries = (uint64_t)max_sk + 1;
    std::vector<int32_t> lookup(num_entries, -1);

    #pragma omp parallel for
    for (uint64_t i = 0; i < N; i++) {
        lookup[suppkey[i]] = (int32_t)i;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_path, std::ios::binary);
    f.write((char*)&num_entries, sizeof(num_entries));
    f.write((char*)lookup.data(), lookup.size() * sizeof(int32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  supplier_s_suppkey_lookup: " << num_entries << " entries in "
              << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 5. Dense PK lookup for part: p_partkey -> row_id
// Same format as orders_o_orderkey_lookup
// ============================================================
void build_part_pk_lookup(const std::string& storage_dir) {
    std::string idx_path = storage_dir + "/indexes/part_p_partkey_lookup.bin";
    if (file_exists(idx_path)) {
        std::cout << "  [SKIP] part_p_partkey_lookup already exists\n";
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/part";
    uint64_t N = load_row_count(table_dir);
    auto partkey = load_bin<int32_t>(table_dir + "/p_partkey.bin", N);

    int32_t max_pk = 0;
    #pragma omp parallel for reduction(max:max_pk)
    for (uint64_t i = 0; i < N; i++) {
        if (partkey[i] > max_pk) max_pk = partkey[i];
    }

    uint64_t num_entries = (uint64_t)max_pk + 1;
    std::vector<int32_t> lookup(num_entries, -1);

    #pragma omp parallel for
    for (uint64_t i = 0; i < N; i++) {
        lookup[partkey[i]] = (int32_t)i;
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_path, std::ios::binary);
    f.write((char*)&num_entries, sizeof(num_entries));
    f.write((char*)lookup.data(), lookup.size() * sizeof(int32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  part_p_partkey_lookup: " << num_entries << " entries in "
              << std::chrono::duration<double>(t1-t0).count() << "s\n";
}

// ============================================================
// 6. Grouped index on partsupp.ps_partkey (sorted data)
// Since partsupp is sorted by (ps_partkey, ps_suppkey), store for each unique partkey:
//   (start_row, count)
// Format: Dense array indexed by partkey.
//   File: indexes/partsupp_ps_partkey_grouped.bin
//   Header: uint64_t num_entries (= max_partkey + 1)
//   Body: struct { uint32_t start; uint32_t count; } [num_entries]
// ============================================================
void build_partsupp_partkey_grouped(const std::string& storage_dir) {
    std::string idx_path = storage_dir + "/indexes/partsupp_ps_partkey_grouped.bin";
    if (file_exists(idx_path)) {
        std::cout << "  [SKIP] partsupp_ps_partkey_grouped already exists\n";
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();
    std::string table_dir = storage_dir + "/partsupp";
    uint64_t N = load_row_count(table_dir);
    auto partkey = load_bin<int32_t>(table_dir + "/ps_partkey.bin", N);

    // Find max partkey
    int32_t max_pk = 0;
    #pragma omp parallel for reduction(max:max_pk)
    for (uint64_t i = 0; i < N; i++) {
        if (partkey[i] > max_pk) max_pk = partkey[i];
    }

    uint64_t num_entries = (uint64_t)max_pk + 1;
    std::vector<uint32_t> starts(num_entries, 0);
    std::vector<uint32_t> counts(num_entries, 0);

    // Linear scan (data is sorted by ps_partkey)
    uint64_t i = 0;
    while (i < N) {
        int32_t key = partkey[i];
        uint32_t start = (uint32_t)i;
        uint32_t count = 0;
        while (i < N && partkey[i] == key) { ++count; ++i; }
        starts[key] = start;
        counts[key] = count;
    }

    // Write interleaved: [start0, count0, start1, count1, ...]
    std::vector<uint32_t> interleaved(num_entries * 2);
    for (uint64_t j = 0; j < num_entries; j++) {
        interleaved[j * 2] = starts[j];
        interleaved[j * 2 + 1] = counts[j];
    }

    std::string idx_dir = storage_dir + "/indexes";
    fs::create_directories(idx_dir);

    std::ofstream f(idx_path, std::ios::binary);
    f.write((char*)&num_entries, sizeof(num_entries));
    f.write((char*)interleaved.data(), interleaved.size() * sizeof(uint32_t));

    auto t1 = std::chrono::high_resolution_clock::now();
    std::cout << "  partsupp_ps_partkey_grouped: " << num_entries << " entries (max_partkey=" << max_pk
              << ") in " << std::chrono::duration<double>(t1-t0).count() << "s\n";
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
    std::cout << "Building extended indexes...\n";
    std::cout << "  Storage: " << storage_dir << "\n";
    std::cout << "  Threads: " << omp_get_max_threads() << "\n\n";

    // Build all new indexes (skip any that already exist)
    build_orderdate_zonemap(storage_dir);
    build_receiptdate_zonemap(storage_dir);
    build_customer_pk_lookup(storage_dir);
    build_supplier_pk_lookup(storage_dir);
    build_part_pk_lookup(storage_dir);
    build_partsupp_partkey_grouped(storage_dir);

    auto t1 = std::chrono::high_resolution_clock::now();
    double total = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "\nTotal extended index building time: " << total << "s\n";
    return 0;
}
