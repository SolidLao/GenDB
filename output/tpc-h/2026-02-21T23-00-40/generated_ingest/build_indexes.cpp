// build_indexes.cpp — build zone maps and hash indexes from binary columnar data
// Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <numeric>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

// ── utilities ─────────────────────────────────────────────────────────────────

static void die(const char* msg) { perror(msg); std::exit(1); }

static void mkdir_p(const std::string& path) {
    std::string cmd = "mkdir -p " + path;
    if (std::system(cmd.c_str()) != 0) die(("mkdir_p: " + path).c_str());
}

// mmap a binary column file; returns ptr and element count
template<typename T>
static std::pair<const T*, size_t> mmap_col(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die(("open col: " + path).c_str());
    struct stat st; fstat(fd, &st);
    size_t bytes = (size_t)st.st_size;
    size_t count = bytes / sizeof(T);
    if (count == 0) { close(fd); return {nullptr, 0}; }
    void* p = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) die(("mmap col: " + path).c_str());
    madvise(p, bytes, MADV_SEQUENTIAL);
    close(fd);
    return {(const T*)p, count};
}

template<typename T>
static void munmap_col(const T* ptr, size_t count) {
    if (ptr) munmap((void*)ptr, count * sizeof(T));
}

// ── zone map ──────────────────────────────────────────────────────────────────
// File format:
//   [uint32_t num_blocks]
//   [int32_t min, int32_t max, uint32_t start_row] * num_blocks
// Data must be sorted by the indexed column for zone maps to be useful.

struct ZoneBlock { int32_t min_val, max_val; uint32_t start_row; };

static void build_zonemap(const std::string& col_path,
                          const std::string& out_path,
                          uint32_t block_size) {
    auto [col, N] = mmap_col<int32_t>(col_path);
    if (!col || N == 0) {
        fprintf(stderr, "build_zonemap: empty column %s\n", col_path.c_str());
        return;
    }

    std::vector<ZoneBlock> blocks;
    blocks.reserve((N + block_size - 1) / block_size);

    for (uint32_t start = 0; start < (uint32_t)N; start += block_size) {
        uint32_t end = std::min((uint32_t)N, start + block_size);
        int32_t mn = col[start], mx = col[start];
        for (uint32_t i = start+1; i < end; ++i) {
            if (col[i] < mn) mn = col[i];
            if (col[i] > mx) mx = col[i];
        }
        blocks.push_back({mn, mx, start});
    }
    munmap_col(col, N);

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) die(("fopen zonemap: " + out_path).c_str());
    uint32_t nb = (uint32_t)blocks.size();
    fwrite(&nb, sizeof(uint32_t), 1, f);
    fwrite(blocks.data(), sizeof(ZoneBlock), blocks.size(), f);
    fclose(f);

    printf("[zonemap] %s: %u blocks\n", out_path.c_str(), nb);
    fflush(stdout);
}

// ── single-key hash index ─────────────────────────────────────────────────────
// File format:
//   [uint32_t capacity]  (power of 2)
//   [int32_t key, uint32_t row_idx] * capacity
//   empty slot: key = INT32_MIN
//
// Hash function: multiply-shift on key
// Load factor: ~0.5

struct HashEntry32 { int32_t key; uint32_t row_idx; };

static uint32_t next_pow2(uint64_t n) {
    uint64_t p = 1;
    while (p < n) p <<= 1;
    return (uint32_t)p;
}

static inline uint32_t hash32(int32_t key, uint32_t cap_mask) {
    return (uint32_t)((uint32_t)key * 2654435761u) & cap_mask;
}

static void build_hash32(const std::string& col_path,
                         const std::string& out_path) {
    auto [col, N] = mmap_col<int32_t>(col_path);
    if (!col || N == 0) {
        fprintf(stderr, "build_hash32: empty column %s\n", col_path.c_str());
        return;
    }

    uint32_t cap = next_pow2((uint64_t)N * 2);
    uint32_t mask = cap - 1;
    std::vector<HashEntry32> table(cap, {INT32_MIN, 0u});

    // sort-based construction: sort (key, row_idx) pairs then insert
    // to avoid pathological hash patterns
    std::vector<std::pair<int32_t,uint32_t>> pairs(N);
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < N; ++i)
        pairs[i] = {col[i], (uint32_t)i};
    std::sort(pairs.begin(), pairs.end());

    // insert sorted pairs into open-addressing table
    for (auto& [k, r] : pairs) {
        uint32_t h = hash32(k, mask);
        while (table[h].key != INT32_MIN) h = (h + 1) & mask;
        table[h] = {k, r};
    }
    munmap_col(col, N);

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) die(("fopen hash32: " + out_path).c_str());
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(table.data(), sizeof(HashEntry32), cap, f);
    fclose(f);

    printf("[hash32] %s: cap=%u rows=%zu\n", out_path.c_str(), cap, N);
    fflush(stdout);
}

// ── composite hash index (int64 key) ─────────────────────────────────────────
// composite key = (int64_t)partkey * 100003 + suppkey
// File format:
//   [uint32_t capacity]
//   [int64_t key, uint32_t row_idx, uint32_t _pad] * capacity
//   empty slot: key = INT64_MIN

struct HashEntry64 { int64_t key; uint32_t row_idx; uint32_t _pad; };

static inline uint32_t hash64(int64_t key, uint32_t mask) {
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    return (uint32_t)(h >> 32) & mask;
}

static void build_composite_hash(const std::string& partkey_path,
                                  const std::string& suppkey_path,
                                  const std::string& out_path) {
    auto [pk_col, N]  = mmap_col<int32_t>(partkey_path);
    auto [sk_col, N2] = mmap_col<int32_t>(suppkey_path);
    if (N != N2 || !pk_col || N == 0) {
        fprintf(stderr, "build_composite_hash: size mismatch or empty\n");
        return;
    }

    uint32_t cap  = next_pow2((uint64_t)N * 2);
    uint32_t mask = cap - 1;
    std::vector<HashEntry64> table(cap, {INT64_MIN, 0u, 0u});

    std::vector<std::pair<int64_t,uint32_t>> pairs(N);
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < N; ++i)
        pairs[i] = {(int64_t)pk_col[i] * 100003LL + sk_col[i], (uint32_t)i};
    std::sort(pairs.begin(), pairs.end());

    for (auto& [k, r] : pairs) {
        uint32_t h = hash64(k, mask);
        while (table[h].key != INT64_MIN) h = (h + 1) & mask;
        table[h] = {k, r, 0u};
    }
    munmap_col(pk_col, N);
    munmap_col(sk_col, N2);

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) die(("fopen hash64: " + out_path).c_str());
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(table.data(), sizeof(HashEntry64), cap, f);
    fclose(f);

    printf("[hash64] %s: cap=%u rows=%zu\n", out_path.c_str(), cap, N);
    fflush(stdout);
}

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string db(argv[1]);

    // create index subdirectories
    for (auto t : {"lineitem","orders","customer","part","supplier","partsupp"})
        mkdir_p(db + "/" + std::string(t) + "/indexes");

    std::string li = db + "/lineitem/";
    std::string or_ = db + "/orders/";
    std::string cu = db + "/customer/";
    std::string pa = db + "/part/";
    std::string ps = db + "/partsupp/";
    std::string su = db + "/supplier/";

    printf("Building indexes in %s\n", db.c_str()); fflush(stdout);

    // Build all indexes in parallel where independent
    std::vector<std::thread> tasks;

    // lineitem: zone map on l_shipdate (data sorted by shipdate)
    tasks.emplace_back([&]{
        build_zonemap(li+"l_shipdate.bin",
                      li+"indexes/shipdate_zonemap.bin", 100000);
    });

    // orders: zone map on o_orderdate + hash on o_orderkey
    tasks.emplace_back([&]{
        build_zonemap(or_+"o_orderdate.bin",
                      or_+"indexes/orderdate_zonemap.bin", 100000);
    });
    tasks.emplace_back([&]{
        build_hash32(or_+"o_orderkey.bin",
                     or_+"indexes/orderkey_hash.bin");
    });

    // customer: hash on c_custkey
    tasks.emplace_back([&]{
        build_hash32(cu+"c_custkey.bin",
                     cu+"indexes/custkey_hash.bin");
    });

    // part: hash on p_partkey
    tasks.emplace_back([&]{
        build_hash32(pa+"p_partkey.bin",
                     pa+"indexes/partkey_hash.bin");
    });

    // supplier: hash on s_suppkey (100K rows — still useful for Q9)
    tasks.emplace_back([&]{
        build_hash32(su+"s_suppkey.bin",
                     su+"indexes/suppkey_hash.bin");
    });

    // partsupp: composite hash on (ps_partkey, ps_suppkey)
    tasks.emplace_back([&]{
        build_composite_hash(ps+"ps_partkey.bin",
                             ps+"ps_suppkey.bin",
                             ps+"indexes/composite_hash.bin");
    });

    for (auto& t : tasks) t.join();

    printf("\nAll indexes built successfully.\n");
    return 0;
}
