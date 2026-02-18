// TPC-H Index Builder
// Builds: lineitem_shipdate_zonemap, lineitem_orderkey_hash,
//         orders_orderkey_hash, orders_custkey_hash, customer_custkey_hash
// Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <cassert>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ── mmap column helper ────────────────────────────────────────────────────────
template<typename T>
struct ColMap {
    T* data = nullptr;
    size_t count = 0;
    int fd = -1;
    size_t byte_size = 0;
    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st; fstat(fd, &st);
        byte_size = (size_t)st.st_size;
        count = byte_size / sizeof(T);
        void* p = mmap(nullptr, byte_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (p == MAP_FAILED) { perror("mmap"); close(fd); fd=-1; return false; }
        madvise(p, byte_size, MADV_SEQUENTIAL);
        data = (T*)p;
        return true;
    }
    void close_file() {
        if (data) munmap(data, byte_size);
        if (fd >= 0) close(fd);
    }
};

// ── zone map builder ──────────────────────────────────────────────────────────
// Layout: [uint32_t num_blocks] then per block:
//   [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count]  = 20 bytes

struct ZoneEntry {
    int32_t  min_val;
    int32_t  max_val;
    uint64_t row_start;
    uint32_t row_count;
};

static void build_zone_map(const std::string& col_file, const std::string& out_file,
                            uint32_t block_size) {
    printf("[zone_map] building from %s...\n", col_file.c_str()); fflush(stdout);
    ColMap<int32_t> col;
    if (!col.open(col_file)) { fprintf(stderr,"Cannot open %s\n", col_file.c_str()); exit(1); }

    size_t nrows = col.count;
    size_t nblocks = (nrows + block_size - 1) / block_size;
    std::vector<ZoneEntry> zones(nblocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < nblocks; b++) {
        size_t rs = b * block_size;
        size_t re = std::min(rs + block_size, nrows);
        int32_t mn = col.data[rs], mx = col.data[rs];
        for (size_t i = rs+1; i < re; i++) {
            int32_t v = col.data[i];
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
        zones[b] = { mn, mx, (uint64_t)rs, (uint32_t)(re - rs) };
    }
    col.close_file();

    FILE* fp = fopen(out_file.c_str(), "wb");
    if (!fp) { perror(out_file.c_str()); exit(1); }
    uint32_t nb = (uint32_t)nblocks;
    fwrite(&nb, 4, 1, fp);
    fwrite(zones.data(), sizeof(ZoneEntry), nblocks, fp);
    fclose(fp);
    printf("[zone_map] done: %zu blocks written to %s\n", nblocks, out_file.c_str()); fflush(stdout);
}

// ── multi-value hash index builder ───────────────────────────────────────────
// Key column: int32_t  (e.g. l_orderkey, o_custkey, c_custkey)
//
// Binary layout:
//   [uint32_t num_unique]        — number of unique keys
//   [uint32_t table_size]        — hash table capacity (power of 2)
//   Hash slots (table_size entries):
//     [int32_t key, uint32_t offset, uint32_t count]  = 12 bytes each
//     (key=INT32_MIN means empty slot)
//   Positions section:
//     [uint32_t total_positions]
//     [uint32_t pos_0, pos_1, ...]

static uint32_t next_pow2(uint32_t x) {
    if (x == 0) return 1;
    x--; x|=x>>1; x|=x>>2; x|=x>>4; x|=x>>8; x|=x>>16; x++;
    return x;
}

static uint64_t hash_key(int32_t k) {
    return (uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL;
}

static void build_hash_multi(const std::string& col_file, const std::string& out_file) {
    printf("[hash_multi] building from %s...\n", col_file.c_str()); fflush(stdout);
    ColMap<int32_t> col;
    if (!col.open(col_file)) { fprintf(stderr,"Cannot open %s\n", col_file.c_str()); exit(1); }

    size_t nrows = col.count;
    printf("[hash_multi] %zu rows\n", nrows); fflush(stdout);

    // Step 1: count unique keys via histogram
    // Sort keys to group by value → O(N log N) but simple and parallelizable
    std::vector<uint32_t> sorted_pos(nrows);
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < nrows; i++) sorted_pos[i] = (uint32_t)i;
    // parallel sort by key value
    const int32_t* kdata = col.data;
    std::sort(sorted_pos.begin(), sorted_pos.end(),
              [kdata](uint32_t a, uint32_t b){ return kdata[a] < kdata[b]; });

    // Step 2: count unique keys
    uint32_t num_unique = 0;
    {
        int32_t prev = INT32_MIN;
        for (size_t i = 0; i < nrows; i++) {
            int32_t k = col.data[sorted_pos[i]];
            if (k != prev) { num_unique++; prev = k; }
        }
    }
    printf("[hash_multi] unique keys: %u\n", num_unique); fflush(stdout);

    // Step 3: build positions array (already in sorted_pos, grouped by key)
    // Step 4: build hash table (load factor ~0.5)
    uint32_t table_size = next_pow2(num_unique * 2);
    uint32_t mask = table_size - 1;

    struct Slot { int32_t key; uint32_t offset; uint32_t count; };
    std::vector<Slot> ht(table_size, {INT32_MIN, 0, 0});

    // Populate hash table from sorted groups
    auto insert_slot = [&](int32_t k, uint32_t off, uint32_t cnt) {
        uint32_t h = (uint32_t)(hash_key(k) >> 32) & mask;
        while (ht[h].key != INT32_MIN) h = (h + 1) & mask;
        ht[h] = {k, off, cnt};
    };

    size_t i = 0;
    while (i < nrows) {
        int32_t k = col.data[sorted_pos[i]];
        uint32_t start = (uint32_t)i;
        while (i < nrows && col.data[sorted_pos[i]] == k) i++;
        uint32_t cnt = (uint32_t)(i - start);
        insert_slot(k, start, cnt);
    }
    col.close_file();

    // Write binary file
    FILE* fp = fopen(out_file.c_str(), "wb");
    if (!fp) { perror(out_file.c_str()); exit(1); }
    fwrite(&num_unique,   4, 1, fp);
    fwrite(&table_size,   4, 1, fp);
    fwrite(ht.data(),     sizeof(Slot), table_size, fp);
    uint32_t total_pos = (uint32_t)nrows;
    fwrite(&total_pos,    4, 1, fp);
    fwrite(sorted_pos.data(), 4, nrows, fp);
    fclose(fp);
    printf("[hash_multi] done: %u unique, table_size=%u, written to %s\n",
           num_unique, table_size, out_file.c_str()); fflush(stdout);
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: build_indexes <gendb_dir>\n"); return 1; }
    std::string base = argv[1];

    std::string idx_dir = base + "/indexes";
    mkdir(idx_dir.c_str(), 0755);

    // ── 1. lineitem shipdate zone map ─────────────────────────────────────────
    build_zone_map(
        base + "/lineitem/l_shipdate.bin",
        idx_dir + "/lineitem_shipdate_zonemap.bin",
        65536
    );

    // ── 2. lineitem orderkey hash (multi-value: many lines per order) ─────────
    build_hash_multi(
        base + "/lineitem/l_orderkey.bin",
        idx_dir + "/lineitem_orderkey_hash.bin"
    );

    // ── 3. orders orderkey hash (unique per row — still use multi for uniform interface) ──
    build_hash_multi(
        base + "/orders/o_orderkey.bin",
        idx_dir + "/orders_orderkey_hash.bin"
    );

    // ── 4. orders custkey hash (multi-value: multiple orders per customer) ────
    build_hash_multi(
        base + "/orders/o_custkey.bin",
        idx_dir + "/orders_custkey_hash.bin"
    );

    // ── 5. customer custkey hash (unique per row) ─────────────────────────────
    build_hash_multi(
        base + "/customer/c_custkey.bin",
        idx_dir + "/customer_custkey_hash.bin"
    );

    printf("\n=== Index building complete ===\n"); fflush(stdout);
    return 0;
}
