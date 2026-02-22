// build_indexes.cpp — TPC-H index construction from binary columnar data
// Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <climits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ── mmap column reader ────────────────────────────────────────────────────────
template<typename T>
struct ColMap {
    const T* data = nullptr;
    size_t   n    = 0;
    int      fd   = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(("open:"+path).c_str()); return false; }
        struct stat st; fstat(fd, &st);
        size_t sz = (size_t)st.st_size;
        n = sz / sizeof(T);
        data = (const T*)mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); close(fd); return false; }
        madvise((void*)data, sz, MADV_SEQUENTIAL);
        return true;
    }
    ~ColMap() {
        if (data && data != MAP_FAILED) munmap((void*)data, n * sizeof(T));
        if (fd >= 0) close(fd);
    }
};

// ── Hash utilities ────────────────────────────────────────────────────────────
// Multiply-shift hash (NOT std::hash — avoids identity clustering)
inline uint32_t msh32(int32_t key, uint32_t mask) {
    return (uint32_t)(((uint64_t)(uint32_t)key * 2654435761ULL) >> 32) & mask;
}
inline uint32_t msh64(int64_t key, uint32_t mask) {
    uint64_t k = (uint64_t)key;
    k = (k ^ (k >> 30)) * 0xbf58476d1ce4e5b9ULL;
    k = (k ^ (k >> 27)) * 0x94d049bb133111ebULL;
    k = k ^ (k >> 31);
    return (uint32_t)k & mask;
}

// Power-of-2 ceiling
uint32_t next_pow2(uint32_t n) {
    uint32_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

// ── Single-value hash index ───────────────────────────────────────────────────
// Stores one (key → row_idx) per unique key (PK columns).
// File format:
//   uint32_t num_rows
//   uint32_t capacity      (power of 2)
//   Entry[capacity]:  { int32_t key; uint32_t row_idx }
//   empty slot: key == INT32_MIN
struct SvEntry { int32_t key; uint32_t row_idx; };
static const int32_t EMPTY_KEY32 = INT32_MIN;

void build_single_value_hash(const std::string& col_path, const std::string& out_path) {
    ColMap<int32_t> col;
    if (!col.open(col_path)) return;
    uint32_t N   = (uint32_t)col.n;
    uint32_t cap = next_pow2((uint32_t)(N / 0.5)); // load factor ~0.5
    uint32_t mask = cap - 1;

    std::vector<SvEntry> ht(cap, {EMPTY_KEY32, 0u});

    for (uint32_t i = 0; i < N; i++) {
        int32_t k = col.data[i];
        uint32_t slot = msh32(k, mask);
        while (ht[slot].key != EMPTY_KEY32) slot = (slot + 1) & mask;
        ht[slot] = {k, i};
    }

    int fd = open(out_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open:"+out_path).c_str()); return; }
    write(fd, &N,   sizeof(uint32_t));
    write(fd, &cap, sizeof(uint32_t));
    write(fd, ht.data(), cap * sizeof(SvEntry));
    close(fd);
    std::cout << "  [single_hash] " << out_path << "  rows=" << N << " cap=" << cap << "\n";
}

// ── Multi-value hash index ────────────────────────────────────────────────────
// Stores (key → [row_idx, ...]) for FK columns with duplicates.
// Construction: sort positions by key, find group boundaries, build HT.
// File format:
//   uint32_t num_positions  (= N)
//   uint32_t num_unique
//   uint32_t capacity       (power of 2)
//   uint32_t positions[num_positions]   (sorted by key)
//   MvEntry[capacity]: { int32_t key; uint32_t offset; uint32_t count }
//   empty: key == INT32_MIN
struct MvEntry { int32_t key; uint32_t offset; uint32_t count; };

void build_multi_value_hash(const std::string& col_path, const std::string& out_path) {
    ColMap<int32_t> col;
    if (!col.open(col_path)) return;
    uint32_t N = (uint32_t)col.n;

    // Sort positions by key (sort-based grouping — no unordered_map<K,vector>)
    std::vector<uint32_t> positions(N);
    std::iota(positions.begin(), positions.end(), 0u);
    std::sort(positions.begin(), positions.end(), [&](uint32_t a, uint32_t b){
        return col.data[a] < col.data[b];
    });

    // Scan for group boundaries → count unique keys
    uint32_t num_unique = 0;
    {
        int32_t prev = EMPTY_KEY32;
        for (uint32_t i = 0; i < N; i++) {
            if (col.data[positions[i]] != prev) { num_unique++; prev = col.data[positions[i]]; }
        }
    }

    uint32_t cap  = next_pow2((uint32_t)(num_unique / 0.5));
    uint32_t mask = cap - 1;
    std::vector<MvEntry> ht(cap, {EMPTY_KEY32, 0u, 0u});

    // Second scan: insert group (key, offset, count) into hash table
    {
        int32_t  cur_key    = EMPTY_KEY32;
        uint32_t cur_offset = 0;
        uint32_t cur_count  = 0;

        auto flush = [&]() {
            if (cur_count == 0) return;
            uint32_t slot = msh32(cur_key, mask);
            while (ht[slot].key != EMPTY_KEY32) slot = (slot + 1) & mask;
            ht[slot] = {cur_key, cur_offset, cur_count};
        };

        for (uint32_t i = 0; i < N; i++) {
            int32_t k = col.data[positions[i]];
            if (k != cur_key) {
                flush();
                cur_key    = k;
                cur_offset = i;
                cur_count  = 0;
            }
            cur_count++;
        }
        flush();
    }

    int fd = open(out_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open:"+out_path).c_str()); return; }
    write(fd, &N,          sizeof(uint32_t));
    write(fd, &num_unique, sizeof(uint32_t));
    write(fd, &cap,        sizeof(uint32_t));
    write(fd, positions.data(), N * sizeof(uint32_t));
    write(fd, ht.data(), cap * sizeof(MvEntry));
    close(fd);
    std::cout << "  [multi_hash]  " << out_path << "  rows=" << N
              << " unique=" << num_unique << " cap=" << cap << "\n";
}

// ── Composite single-value hash index ─────────────────────────────────────────
// Key = (int64_t)partkey << 32 | (uint32_t)suppkey → single row_idx
// File format:
//   uint32_t num_rows
//   uint32_t capacity
//   CsEntry[capacity]: { int64_t key; uint32_t row_idx; uint32_t _pad; }
//   empty: key == INT64_MIN
struct CsEntry { int64_t key; uint32_t row_idx; uint32_t _pad; };
static const int64_t EMPTY_KEY64 = INT64_MIN;

void build_composite_hash(const std::string& col1_path, const std::string& col2_path,
                           const std::string& out_path) {
    ColMap<int32_t> col1, col2;
    if (!col1.open(col1_path) || !col2.open(col2_path)) return;
    uint32_t N    = (uint32_t)col1.n;
    uint32_t cap  = next_pow2((uint32_t)(N / 0.5));
    uint32_t mask = cap - 1;

    std::vector<CsEntry> ht(cap, {EMPTY_KEY64, 0u, 0u});

    for (uint32_t i = 0; i < N; i++) {
        int64_t k = ((int64_t)col1.data[i] << 32) | (uint32_t)col2.data[i];
        uint32_t slot = msh64(k, mask);
        while (ht[slot].key != EMPTY_KEY64) slot = (slot + 1) & mask;
        ht[slot] = {k, i, 0u};
    }

    int fd = open(out_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open:"+out_path).c_str()); return; }
    write(fd, &N,   sizeof(uint32_t));
    write(fd, &cap, sizeof(uint32_t));
    write(fd, ht.data(), cap * sizeof(CsEntry));
    close(fd);
    std::cout << "  [comp_hash]   " << out_path << "  rows=" << N << " cap=" << cap << "\n";
}

// ── main ──────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc != 2) { std::cerr << "Usage: build_indexes <gendb_dir>\n"; return 1; }
    std::string db = argv[1];

    std::cout << "=== Building indexes in " << db << " ===\n";

    // Run independent indexes in parallel with OpenMP sections
    // Groups: [lineitem], [orders PK + FK + PK-FK], [customer + part + supplier + partsupp]
    #pragma omp parallel sections num_threads(8)
    {
        #pragma omp section
        {
            // lineitem: multi-value hash on l_orderkey
            std::cout << "[lineitem] building l_orderkey hash...\n" << std::flush;
            build_multi_value_hash(
                db+"/lineitem/l_orderkey.bin",
                db+"/indexes/lineitem_orderkey_hash.bin");
            // Zone maps for lineitem/orders are already written by ingest.
        }

        #pragma omp section
        {
            // orders: single-value hash on o_orderkey (PK)
            std::cout << "[orders] building o_orderkey hash...\n" << std::flush;
            build_single_value_hash(
                db+"/orders/o_orderkey.bin",
                db+"/indexes/orders_orderkey_hash.bin");
        }

        #pragma omp section
        {
            // orders: multi-value hash on o_custkey (FK)
            std::cout << "[orders] building o_custkey hash...\n" << std::flush;
            build_multi_value_hash(
                db+"/orders/o_custkey.bin",
                db+"/indexes/orders_custkey_hash.bin");
        }

        #pragma omp section
        {
            // customer: single-value hash on c_custkey (PK)
            std::cout << "[customer] building c_custkey hash...\n" << std::flush;
            build_single_value_hash(
                db+"/customer/c_custkey.bin",
                db+"/indexes/customer_custkey_hash.bin");
        }

        #pragma omp section
        {
            // part: single-value hash on p_partkey (PK)
            std::cout << "[part] building p_partkey hash...\n" << std::flush;
            build_single_value_hash(
                db+"/part/p_partkey.bin",
                db+"/indexes/part_partkey_hash.bin");
        }

        #pragma omp section
        {
            // supplier: single-value hash on s_suppkey (PK, 100K rows)
            std::cout << "[supplier] building s_suppkey hash...\n" << std::flush;
            build_single_value_hash(
                db+"/supplier/s_suppkey.bin",
                db+"/indexes/supplier_suppkey_hash.bin");
        }

        #pragma omp section
        {
            // partsupp: composite hash on (ps_partkey, ps_suppkey)
            std::cout << "[partsupp] building composite hash...\n" << std::flush;
            build_composite_hash(
                db+"/partsupp/ps_partkey.bin",
                db+"/partsupp/ps_suppkey.bin",
                db+"/indexes/partsupp_composite_hash.bin");
        }
    }

    std::cout << "\n=== Index building complete ===\n";
    return 0;
}
