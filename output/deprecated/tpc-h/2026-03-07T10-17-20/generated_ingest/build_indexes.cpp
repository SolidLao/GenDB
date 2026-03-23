// build_indexes.cpp — Build zone maps and hash indexes from binary columnar data.
//
// Indexes built:
//   lineitem/l_shipdate_zone_map.bin   — zone map (block=100000) on l_shipdate (sorted)
//   orders/o_orderdate_zone_map.bin    — zone map (block=100000) on o_orderdate (sorted)
//   orders/o_orderkey_hash.bin         — hash map: o_orderkey -> orders row index
//   customer/c_custkey_hash.bin        — hash map: c_custkey -> customer row index
//   supplier/s_suppkey_hash.bin        — hash map: s_suppkey -> supplier row index
//   partsupp/ps_composite_hash.bin     — hash map: (ps_partkey,ps_suppkey) -> partsupp row index
//
// Zone map file layout (int32_t throughout):
//   [0] num_blocks  [1] block_size  [2..2+num_blocks-1] min[]  [2+N..] max[]
//
// Hash map (int32->int32) file layout:
//   [0..7]   int64_t capacity
//   [8..15]  int64_t num_entries
//   [16..]   int32_t keys[capacity]    (sentinel = -1)
//   [16+cap*4..] int32_t values[capacity]
//
// Hash map (int64->int32) file layout:
//   [0..7]   int64_t capacity
//   [8..15]  int64_t num_entries
//   [16..]   int64_t keys[capacity]    (sentinel = -1LL)
//   [16+cap*8..] int32_t values[capacity]
//
// Usage: ./build_indexes <gendb_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include <vector>
#include <string>
#include <thread>
#include <cassert>
#include <sys/stat.h>
#include <errno.h>

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

static int64_t file_size(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        fprintf(stderr, "stat failed: %s\n", path.c_str()); exit(1);
    }
    return (int64_t)st.st_size;
}

template<typename T>
static std::vector<T> read_col(const std::string& path) {
    int64_t sz = file_size(path);
    int64_t n  = sz / (int64_t)sizeof(T);
    std::vector<T> v(n);
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open: %s\n", path.c_str()); exit(1); }
    fread(v.data(), sizeof(T), n, f);
    fclose(f);
    return v;
}

static void write_raw(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot open for write: %s\n", path.c_str()); exit(1); }
    fwrite(data, 1, bytes, f);
    fclose(f);
}

// ---------------------------------------------------------------------------
// Hash functions (splitmix style)
// ---------------------------------------------------------------------------

static inline uint32_t hash32(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;
    return x;
}

static inline uint64_t hash64(uint64_t x) {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}

// ---------------------------------------------------------------------------
// Zone map builder
// ---------------------------------------------------------------------------

// Builds a zone map for a SORTED int32_t column.
// File layout: int32_t num_blocks, int32_t block_size, int32_t min[N], int32_t max[N]
static void build_zone_map(const std::string& col_path, const std::string& out_path,
                            int32_t block_size) {
    auto col = read_col<int32_t>(col_path);
    int64_t nrows = (int64_t)col.size();
    int64_t num_blocks = (nrows + block_size - 1) / block_size;

    std::vector<int32_t> mins(num_blocks), maxs(num_blocks);
    for (int64_t b = 0; b < num_blocks; b++) {
        int64_t lo = b * block_size;
        int64_t hi = std::min(lo + block_size, nrows);
        // Column is sorted, so min=first, max=last in block
        mins[b] = col[lo];
        maxs[b] = col[hi - 1];
    }

    // Write: header + mins[] + maxs[]
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write zone map: %s\n", out_path.c_str()); exit(1); }
    int32_t nb32 = (int32_t)num_blocks;
    fwrite(&nb32,       sizeof(int32_t), 1,         f);
    fwrite(&block_size, sizeof(int32_t), 1,         f);
    fwrite(mins.data(), sizeof(int32_t), num_blocks, f);
    fwrite(maxs.data(), sizeof(int32_t), num_blocks, f);
    fclose(f);

    fprintf(stdout, "zone_map %s: %ld blocks of %d rows\n",
            out_path.c_str(), (long)num_blocks, block_size);
}

// ---------------------------------------------------------------------------
// Hash map builders (int32 -> int32)
// ---------------------------------------------------------------------------

// Open-addressing hash map with linear probing.
// Capacity must be power of 2.  Sentinel key = -1.
// File: int64_t capacity | int64_t num_entries | int32_t keys[cap] | int32_t values[cap]
static void build_hash32(const std::string& key_path, const std::string& out_path,
                          int64_t capacity) {
    auto keys_col = read_col<int32_t>(key_path);
    int64_t N = (int64_t)keys_col.size();

    // Allocate hash table
    std::vector<int32_t> htkeys(capacity, -1);
    std::vector<int32_t> htvals(capacity, -1);
    int64_t mask = capacity - 1;

    for (int64_t row = 0; row < N; row++) {
        int32_t k = keys_col[row];
        uint64_t h = (uint64_t)hash32((uint32_t)k) & (uint64_t)mask;
        while (htkeys[h] != -1) {
            h = (h + 1) & (uint64_t)mask;
        }
        htkeys[h] = k;
        htvals[h] = (int32_t)row;
    }

    // Write file
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write hash: %s\n", out_path.c_str()); exit(1); }
    int64_t meta[2] = {capacity, N};
    fwrite(meta,           sizeof(int64_t), 2,        f);
    fwrite(htkeys.data(),  sizeof(int32_t), capacity, f);
    fwrite(htvals.data(),  sizeof(int32_t), capacity, f);
    fclose(f);

    fprintf(stdout, "hash32 %s: %ld entries, capacity %ld (load=%.2f)\n",
            out_path.c_str(), (long)N, (long)capacity, (double)N / capacity);
}

// ---------------------------------------------------------------------------
// Hash map builder (int64 composite key -> int32)
// ---------------------------------------------------------------------------

// Key: ((int64_t)ps_partkey << 32) | (uint32_t)ps_suppkey
// File: int64_t capacity | int64_t num_entries | int64_t keys[cap] | int32_t values[cap]
static void build_hash64_composite(const std::string& partkey_path,
                                    const std::string& suppkey_path,
                                    const std::string& out_path,
                                    int64_t capacity) {
    auto partkeys = read_col<int32_t>(partkey_path);
    auto suppkeys = read_col<int32_t>(suppkey_path);
    int64_t N = (int64_t)partkeys.size();
    assert(N == (int64_t)suppkeys.size());

    std::vector<int64_t> htkeys(capacity, -1LL);
    std::vector<int32_t> htvals(capacity, -1);
    int64_t mask = capacity - 1;

    for (int64_t row = 0; row < N; row++) {
        int64_t k = ((int64_t)partkeys[row] << 32) | (uint32_t)suppkeys[row];
        uint64_t h = hash64((uint64_t)k) & (uint64_t)mask;
        while (htkeys[h] != -1LL) {
            h = (h + 1) & (uint64_t)mask;
        }
        htkeys[h] = k;
        htvals[h] = (int32_t)row;
    }

    // Write: meta | keys[] | values[]
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write composite hash: %s\n", out_path.c_str()); exit(1); }
    int64_t meta[2] = {capacity, N};
    fwrite(meta,          sizeof(int64_t), 2,        f);
    fwrite(htkeys.data(), sizeof(int64_t), capacity, f);
    fwrite(htvals.data(), sizeof(int32_t), capacity, f);
    fclose(f);

    fprintf(stdout, "hash64 %s: %ld entries, capacity %ld (load=%.2f)\n",
            out_path.c_str(), (long)N, (long)capacity, (double)N / capacity);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string g = argv[1];  // gendb_dir

    fprintf(stdout, "Building indexes in parallel...\n");

    // Zone map: lineitem.l_shipdate (block=100000, sorted)
    std::thread t1([&]{
        build_zone_map(g + "/lineitem/l_shipdate.bin",
                       g + "/lineitem/l_shipdate_zone_map.bin", 100000);
    });

    // Zone map: orders.o_orderdate (block=100000, sorted)
    std::thread t2([&]{
        build_zone_map(g + "/orders/o_orderdate.bin",
                       g + "/orders/o_orderdate_zone_map.bin", 100000);
    });

    // Hash: orders.o_orderkey -> row index   capacity=2^25=33554432
    std::thread t3([&]{
        build_hash32(g + "/orders/o_orderkey.bin",
                     g + "/orders/o_orderkey_hash.bin",
                     33554432LL);
    });

    // Hash: customer.c_custkey -> row index  capacity=2^22=4194304
    std::thread t4([&]{
        build_hash32(g + "/customer/c_custkey.bin",
                     g + "/customer/c_custkey_hash.bin",
                     4194304LL);
    });

    // Hash: supplier.s_suppkey -> row index  capacity=2^18=262144
    std::thread t5([&]{
        build_hash32(g + "/supplier/s_suppkey.bin",
                     g + "/supplier/s_suppkey_hash.bin",
                     262144LL);
    });

    // Hash: (ps_partkey, ps_suppkey) composite -> row index  capacity=2^24=16777216
    std::thread t6([&]{
        build_hash64_composite(g + "/partsupp/ps_partkey.bin",
                               g + "/partsupp/ps_suppkey.bin",
                               g + "/partsupp/ps_composite_hash.bin",
                               16777216LL);
    });

    t1.join(); t2.join(); t3.join(); t4.join(); t5.join(); t6.join();

    fprintf(stdout, "All indexes built successfully.\n");
    return 0;
}
