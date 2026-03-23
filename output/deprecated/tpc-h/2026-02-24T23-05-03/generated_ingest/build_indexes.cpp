// build_indexes.cpp — Build zone maps and hash indexes from binary column data
//
// Zone maps:
//   lineitem/indexes/l_shipdate_zone_map.bin     (Q1: <=date, Q6: BETWEEN, Q3: >date)
//   orders/indexes/o_orderdate_zone_map.bin       (Q3: <date, Q9: EXTRACT YEAR)
//   Format: [uint32_t num_blocks][per block: int32_t min, int32_t max, uint32_t count]
//   Block size: 65536 rows
//
// Hash indexes (open-addressing, linear probing, ~50% load factor):
//   orders/indexes/orders_pk_hash.bin       — o_orderkey  → row_idx
//   customer/indexes/customer_pk_hash.bin   — c_custkey   → row_idx
//   supplier/indexes/supplier_pk_hash.bin   — s_suppkey   → row_idx
//   partsupp/indexes/partsupp_pk_hash.bin   — (ps_partkey,ps_suppkey) → row_idx
//
// Hash index file layout: [uint32_t cap][slot0][slot1]...[slot_{cap-1}]
// Single-key slot: { int32_t key (INT32_MIN = empty); uint32_t row_idx }   8 bytes
// Composite slot:  { int32_t partkey (INT32_MIN = empty); int32_t suppkey; uint32_t row_idx }  12 bytes
//
// Hash function (single key):   h = ((uint32_t)key * 2654435761u) & mask
// Hash function (composite):    uint64_t k = ((uint64_t)(uint32_t)pk<<32)|(uint32_t)sk
//                                h = (uint32_t)((k * 11400714819323198485ull) >> 32) & mask
// Probe: for (uint32_t pr=0; pr < cap; pr++) { idx=(h+pr)&mask; if empty→insert/stop }   (C24)
//
// Capacities (next_power_of_2(rows * 2) for ≤50% load factor — C9):
//   orders:   15000000*2=30M → 2^25=33554432
//   customer: 1500000*2=3M  → 2^22=4194304
//   supplier: 100000*2=200K → 2^18=262144
//   partsupp: 8000000*2=16M → 2^24=16777216

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <climits>
#include <algorithm>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ── I/O helpers ───────────────────────────────────────────────────────────

static const void* mmap_ro(const std::string& path, size_t& out_sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: "+path).c_str()); out_sz=0; return nullptr; }
    struct stat st; fstat(fd, &st); out_sz = (size_t)st.st_size;
    void* p = mmap(nullptr, out_sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); out_sz=0; return nullptr; }
    return p;
}

static void write_bin(const std::string& path, const void* data, size_t bytes) {
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open: "+path).c_str()); return; }
    size_t done = 0;
    while (done < bytes) {
        ssize_t n = write(fd, (const char*)data + done, bytes - done);
        if (n <= 0) { perror("write"); break; }
        done += (size_t)n;
    }
    close(fd);
}

static void mkdirp(const std::string& path) {
    std::string cmd = "mkdir -p \"" + path + "\"";
    (void)system(cmd.c_str());
}

// ── Zone map builder ──────────────────────────────────────────────────────
// Format: [uint32_t num_blocks][{int32_t min, int32_t max, uint32_t count}*]
static void build_zone_map(const std::string& col_file, const std::string& out_file,
                           uint32_t block_size) {
    size_t sz; const void* raw = mmap_ro(col_file, sz);
    if (!raw) return;
    const int32_t* col = (const int32_t*)raw;
    size_t nrows = sz / 4;

    struct Block { int32_t mn, mx; uint32_t cnt; };
    size_t num_blocks = (nrows + block_size - 1) / block_size;
    std::vector<Block> blocks(num_blocks);

    #pragma omp parallel for schedule(static)
    for (size_t b = 0; b < num_blocks; b++) {
        size_t from = b * block_size;
        size_t to   = std::min(from + (size_t)block_size, nrows);
        int32_t mn = col[from], mx = col[from];
        for (size_t i = from+1; i < to; i++) {
            if (col[i] < mn) mn = col[i];
            if (col[i] > mx) mx = col[i];
        }
        blocks[b] = {mn, mx, (uint32_t)(to - from)};
    }
    munmap((void*)raw, sz);

    // Write: [uint32_t num_blocks][Block*]
    std::vector<uint8_t> out(4 + num_blocks * 12);
    uint32_t nb = (uint32_t)num_blocks;
    memcpy(out.data(), &nb, 4);
    memcpy(out.data()+4, blocks.data(), num_blocks * 12);
    write_bin(out_file, out.data(), out.size());
    printf("[zone_map] %s: %zu blocks\n", out_file.c_str(), num_blocks);
}

// ── Single-key PK hash index builder ─────────────────────────────────────
// Slot: { int32_t key=INT32_MIN(empty); uint32_t row_idx }
// File: [uint32_t cap][PKSlot[cap]]
struct PKSlot { int32_t key; uint32_t row_idx; };

static uint32_t pk_hash(int32_t key, uint32_t mask) {
    return ((uint32_t)key * 2654435761u) & mask;
}

static void build_pk_hash(const std::string& key_col_file, const std::string& out_file,
                          uint32_t cap) {
    size_t sz; const void* raw = mmap_ro(key_col_file, sz);
    if (!raw) return;
    const int32_t* keys = (const int32_t*)raw;
    size_t nrows = sz / 4;
    uint32_t mask = cap - 1;

    // Allocate and initialize with sentinel — C20: use std::fill, NOT memset
    std::vector<PKSlot> ht(cap);
    std::fill(ht.begin(), ht.end(), PKSlot{INT32_MIN, 0});

    // Sequential insert (order-preserving, deterministic)
    for (uint32_t row = 0; row < (uint32_t)nrows; row++) {
        int32_t k = keys[row];
        uint32_t h = pk_hash(k, mask);
        // C24: bounded probing
        for (uint32_t pr = 0; pr < cap; pr++) {
            uint32_t idx = (h + pr) & mask;
            if (ht[idx].key == INT32_MIN) {
                ht[idx].key     = k;
                ht[idx].row_idx = row;
                break;
            }
        }
    }
    munmap((void*)raw, sz);

    // Write [uint32_t cap][PKSlot[cap]]
    std::vector<uint8_t> out(4 + (size_t)cap * sizeof(PKSlot));
    memcpy(out.data(), &cap, 4);
    memcpy(out.data()+4, ht.data(), (size_t)cap * sizeof(PKSlot));
    write_bin(out_file, out.data(), out.size());
    printf("[hash_idx] %s: cap=%u rows=%zu\n", out_file.c_str(), cap, nrows);
}

// ── Composite PK hash index (partsupp) ───────────────────────────────────
// Slot: { int32_t partkey=INT32_MIN(empty); int32_t suppkey; uint32_t row_idx }
// File: [uint32_t cap][PSSlot[cap]]
struct PSSlot { int32_t partkey; int32_t suppkey; uint32_t row_idx; };

static uint32_t ps_hash(int32_t pk, int32_t sk, uint32_t mask) {
    uint64_t k = ((uint64_t)(uint32_t)pk << 32) | (uint32_t)sk;
    return (uint32_t)((k * 11400714819323198485ull) >> 32) & mask;
}

static void build_partsupp_hash(const std::string& db_dir, uint32_t cap) {
    size_t sz_pk, sz_sk;
    const void* raw_pk = mmap_ro(db_dir+"/partsupp/ps_partkey.bin", sz_pk);
    const void* raw_sk = mmap_ro(db_dir+"/partsupp/ps_suppkey.bin",  sz_sk);
    if (!raw_pk || !raw_sk) {
        if (raw_pk) munmap((void*)raw_pk, sz_pk);
        if (raw_sk) munmap((void*)raw_sk, sz_sk);
        return;
    }

    const int32_t* pk_col = (const int32_t*)raw_pk;
    const int32_t* sk_col = (const int32_t*)raw_sk;
    size_t nrows = sz_pk / 4;
    uint32_t mask = cap - 1;

    // C20: use std::fill, NOT memset
    std::vector<PSSlot> ht(cap);
    std::fill(ht.begin(), ht.end(), PSSlot{INT32_MIN, 0, 0});

    // C24: bounded probing
    for (uint32_t row = 0; row < (uint32_t)nrows; row++) {
        int32_t pk = pk_col[row], sk = sk_col[row];
        uint32_t h = ps_hash(pk, sk, mask);
        for (uint32_t pr = 0; pr < cap; pr++) {
            uint32_t idx = (h + pr) & mask;
            if (ht[idx].partkey == INT32_MIN) {
                ht[idx].partkey  = pk;
                ht[idx].suppkey  = sk;
                ht[idx].row_idx  = row;
                break;
            }
        }
    }
    munmap((void*)raw_pk, sz_pk);
    munmap((void*)raw_sk, sz_sk);

    std::string out_path = db_dir+"/partsupp/indexes/partsupp_pk_hash.bin";
    std::vector<uint8_t> out(4 + (size_t)cap * sizeof(PSSlot));
    memcpy(out.data(), &cap, 4);
    memcpy(out.data()+4, ht.data(), (size_t)cap * sizeof(PSSlot));
    write_bin(out_path, out.data(), out.size());
    printf("[hash_idx] %s: cap=%u rows=%zu\n", out_path.c_str(), cap, nrows);
}

// ── Main ──────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: build_indexes <db_dir>\n"); return 1; }
    std::string db = argv[1];
    printf("Building indexes in %s\n", db.c_str());
    double t0 = omp_get_wtime();

    mkdirp(db + "/lineitem/indexes");
    mkdirp(db + "/orders/indexes");
    mkdirp(db + "/customer/indexes");
    mkdirp(db + "/supplier/indexes");
    mkdirp(db + "/partsupp/indexes");

    // Capacities: next_power_of_2(rows * 2) — C9
    const uint32_t CAP_ORDERS   = 33554432u;  // 2^25, 15M rows → 30M → 2^25
    const uint32_t CAP_CUSTOMER = 4194304u;   // 2^22,  1.5M rows → 3M → 2^22
    const uint32_t CAP_SUPPLIER = 262144u;    // 2^18,  100K rows → 200K → 2^18
    const uint32_t CAP_PARTSUPP = 16777216u;  // 2^24,  8M rows → 16M → 2^24
    const uint32_t BLOCK_SIZE   = 65536u;

    // Build all indexes in parallel
    #pragma omp parallel sections num_threads(6)
    {
        #pragma omp section
        build_zone_map(db+"/lineitem/l_shipdate.bin",
                       db+"/lineitem/indexes/l_shipdate_zone_map.bin", BLOCK_SIZE);

        #pragma omp section
        build_zone_map(db+"/orders/o_orderdate.bin",
                       db+"/orders/indexes/o_orderdate_zone_map.bin", BLOCK_SIZE);

        #pragma omp section
        build_pk_hash(db+"/orders/o_orderkey.bin",
                      db+"/orders/indexes/orders_pk_hash.bin", CAP_ORDERS);

        #pragma omp section
        build_pk_hash(db+"/customer/c_custkey.bin",
                      db+"/customer/indexes/customer_pk_hash.bin", CAP_CUSTOMER);

        #pragma omp section
        build_pk_hash(db+"/supplier/s_suppkey.bin",
                      db+"/supplier/indexes/supplier_pk_hash.bin", CAP_SUPPLIER);

        #pragma omp section
        build_partsupp_hash(db, CAP_PARTSUPP);
    }

    printf("Index building done in %.1f s\n", omp_get_wtime() - t0);
    return 0;
}
