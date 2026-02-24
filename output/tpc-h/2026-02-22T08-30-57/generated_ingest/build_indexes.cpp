// build_indexes.cpp — Build zone maps and hash indexes from binary columns
// Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp
// Usage: ./build_indexes <gendb_dir>
//
// Zone map format (int32_t):  [uint32_t num_blocks][int32_t min, max per block]
// Zone map format (double):   [uint32_t num_blocks][double  min, max per block]
// Hash index format (int32_t): [uint32_t ht_size][uint32_t num_positions]
//                              [ht_size x {int32_t key, uint32_t offset, uint32_t count}]
//                              [num_positions x uint32_t row_index]
// Hash index format (int64_t): same but key is int64_t (16 bytes/slot vs 12)
// Empty slot sentinel: INT32_MIN / INT64_MIN

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <chrono>
#include <omp.h>

static constexpr uint32_t BLOCK_SIZE = 100000;
static constexpr size_t   WRITE_BUF  = 4ULL * 1024 * 1024;

// ============================================================
// mmap helpers
// ============================================================
template<typename T>
static std::pair<const T*, uint32_t> mmap_col(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(p, sz, MADV_SEQUENTIAL);
    close(fd);
    return {(const T*)p, (uint32_t)(sz / sizeof(T))};
}

static void write_buf(int fd, const void* data, size_t bytes) {
    const char* ptr = (const char*)data;
    size_t rem = bytes;
    while (rem > 0) {
        size_t ch = std::min(rem, WRITE_BUF);
        ssize_t w = ::write(fd, ptr, ch); if (w <= 0) { perror("write"); exit(1); }
        ptr += w; rem -= (size_t)w;
    }
}

static int open_out(const std::string& path) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    return fd;
}

static uint32_t next_pow2(uint32_t x) {
    uint32_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static inline uint32_t hash32(int32_t key, uint32_t mask) {
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
}

static inline uint32_t hash64(int64_t key, uint32_t mask) {
    uint64_t h = (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    h ^= h >> 33;
    return (uint32_t)(h & (uint64_t)mask);
}

// ============================================================
// Zone map: int32_t column
// ============================================================
static void build_zone_i32(const std::string& col_path, const std::string& out_path) {
    auto [data, n] = mmap_col<int32_t>(col_path);
    uint32_t nb = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    std::vector<int32_t> zones(nb * 2);  // [min, max] per block

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < nb; b++) {
        uint32_t lo = b * BLOCK_SIZE, hi = std::min(lo + BLOCK_SIZE, n);
        int32_t mn = data[lo], mx = data[lo];
        for (uint32_t i = lo+1; i < hi; i++) {
            if (data[i] < mn) mn = data[i];
            if (data[i] > mx) mx = data[i];
        }
        zones[b*2]   = mn;
        zones[b*2+1] = mx;
    }

    int fd = open_out(out_path);
    write_buf(fd, &nb, 4);
    write_buf(fd, zones.data(), (size_t)nb * 8);
    close(fd);
    munmap((void*)data, (size_t)n * sizeof(int32_t));
    printf("  zone_i32 %s: %u blocks\n", out_path.c_str(), nb); fflush(stdout);
}

// ============================================================
// Zone map: double column
// ============================================================
static void build_zone_f64(const std::string& col_path, const std::string& out_path) {
    auto [data, n] = mmap_col<double>(col_path);
    uint32_t nb = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    std::vector<double> zones(nb * 2);

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < nb; b++) {
        uint32_t lo = b * BLOCK_SIZE, hi = std::min(lo + BLOCK_SIZE, n);
        double mn = data[lo], mx = data[lo];
        for (uint32_t i = lo+1; i < hi; i++) {
            if (data[i] < mn) mn = data[i];
            if (data[i] > mx) mx = data[i];
        }
        zones[b*2]   = mn;
        zones[b*2+1] = mx;
    }

    int fd = open_out(out_path);
    write_buf(fd, &nb, 4);
    write_buf(fd, zones.data(), (size_t)nb * 16);
    close(fd);
    munmap((void*)data, (size_t)n * sizeof(double));
    printf("  zone_f64 %s: %u blocks\n", out_path.c_str(), nb); fflush(stdout);
}

// ============================================================
// Hash index: single int32_t key
// Layout: struct HtSlot { int32_t key; uint32_t offset; uint32_t count; } -- 12 bytes
// ============================================================
static void build_hash_i32(const std::string& col_path, const std::string& out_path) {
    auto t0 = std::chrono::steady_clock::now();
    auto [keys, n] = mmap_col<int32_t>(col_path);

    // 1. Create and sort (key, row_idx) pairs
    std::vector<std::pair<int32_t,uint32_t>> pairs(n);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < n; i++) pairs[i] = {keys[i], i};

    std::sort(pairs.begin(), pairs.end());

    // 2. Scan groups: build positions array and entries list
    struct Entry { int32_t key; uint32_t offset, count; };
    std::vector<Entry>    entries;  entries.reserve(n / 4 + 1);
    std::vector<uint32_t> positions(n);

    {
        uint32_t i = 0;
        while (i < n) {
            int32_t  cur_key = pairs[i].first;
            uint32_t off     = i;
            uint32_t cnt     = 0;
            while (i < n && pairs[i].first == cur_key) {
                positions[off + cnt] = pairs[i].second;
                cnt++; i++;
            }
            entries.push_back({cur_key, off, cnt});
        }
    }
    { std::vector<std::pair<int32_t,uint32_t>> tmp; pairs.swap(tmp); }

    uint32_t num_keys = (uint32_t)entries.size();
    uint32_t ht_size  = next_pow2(num_keys * 2 + 1);
    uint32_t mask     = ht_size - 1;

    // 3. Build open-addressing hash table — C20: use std::fill NOT memset for sentinel
    struct HtSlot { int32_t key; uint32_t offset; uint32_t count; };
    std::vector<HtSlot> ht(ht_size);
    std::fill(ht.begin(), ht.end(), HtSlot{INT32_MIN, 0, 0});

    // C24: bounded probing loop
    for (auto& e : entries) {
        uint32_t h = hash32(e.key, mask);
        for (uint32_t probe = 0; probe < ht_size; probe++) {
            if (ht[h].key == INT32_MIN) { ht[h] = {e.key, e.offset, e.count}; break; }
            h = (h + 1) & mask;
        }
    }

    // 4. Write: [ht_size][num_positions][hash table][positions]
    int fd = open_out(out_path);
    uint32_t hdr[2] = {ht_size, n};
    write_buf(fd, hdr, 8);
    write_buf(fd, ht.data(),        (size_t)ht_size * sizeof(HtSlot));
    write_buf(fd, positions.data(), (size_t)n * 4);
    close(fd);

    munmap((void*)keys, (size_t)n * sizeof(int32_t));
    auto t1 = std::chrono::steady_clock::now();
    printf("  hash_i32 %s: %u rows, %u keys, ht_size=%u in %.1fs\n",
           out_path.c_str(), n, num_keys, ht_size,
           std::chrono::duration<double>(t1-t0).count()); fflush(stdout);
}

// ============================================================
// Hash index: composite int64_t key = (col1 << 32) | (uint32_t)col2
// Layout: struct HtSlot64 { int64_t key; uint32_t offset; uint32_t count; } -- 16 bytes
// ============================================================
static void build_hash_i64_composite(const std::string& col1_path,
                                     const std::string& col2_path,
                                     const std::string& out_path) {
    auto t0 = std::chrono::steady_clock::now();
    auto [col1, n1] = mmap_col<int32_t>(col1_path);
    auto [col2, n2] = mmap_col<int32_t>(col2_path);
    uint32_t n = std::min(n1, n2);

    // 1. Create and sort composite key pairs
    std::vector<std::pair<int64_t,uint32_t>> pairs(n);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < n; i++) {
        int64_t k = ((int64_t)col1[i] << 32) | (uint32_t)col2[i];
        pairs[i] = {k, i};
    }

    std::sort(pairs.begin(), pairs.end());

    // 2. Scan groups
    struct Entry { int64_t key; uint32_t offset, count; };
    std::vector<Entry>    entries;  entries.reserve(n + 1);
    std::vector<uint32_t> positions(n);

    {
        uint32_t i = 0;
        while (i < n) {
            int64_t  cur_key = pairs[i].first;
            uint32_t off     = i;
            uint32_t cnt     = 0;
            while (i < n && pairs[i].first == cur_key) {
                positions[off + cnt] = pairs[i].second;
                cnt++; i++;
            }
            entries.push_back({cur_key, off, cnt});
        }
    }
    { std::vector<std::pair<int64_t,uint32_t>> tmp; pairs.swap(tmp); }

    uint32_t num_keys = (uint32_t)entries.size();
    uint32_t ht_size  = next_pow2(num_keys * 2 + 1);
    uint32_t mask     = ht_size - 1;

    // 3. Build hash table — C20: use std::fill NOT memset for sentinel
    struct HtSlot64 { int64_t key; uint32_t offset; uint32_t count; };
    std::vector<HtSlot64> ht(ht_size);
    std::fill(ht.begin(), ht.end(), HtSlot64{INT64_MIN, 0, 0});

    // C24: bounded probing loop
    for (auto& e : entries) {
        uint32_t h = hash64(e.key, mask);
        for (uint32_t probe = 0; probe < ht_size; probe++) {
            if (ht[h].key == INT64_MIN) { ht[h] = {e.key, e.offset, e.count}; break; }
            h = (h + 1) & mask;
        }
    }

    // 4. Write
    int fd = open_out(out_path);
    uint32_t hdr[2] = {ht_size, n};
    write_buf(fd, hdr, 8);
    write_buf(fd, ht.data(),        (size_t)ht_size * sizeof(HtSlot64));
    write_buf(fd, positions.data(), (size_t)n * 4);
    close(fd);

    munmap((void*)col1, (size_t)n1 * 4);
    munmap((void*)col2, (size_t)n2 * 4);
    auto t1 = std::chrono::steady_clock::now();
    printf("  hash_i64c %s: %u rows, %u keys, ht_size=%u in %.1fs\n",
           out_path.c_str(), n, num_keys, ht_size,
           std::chrono::duration<double>(t1-t0).count()); fflush(stdout);
}

// ============================================================
// MAIN
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    std::string db = argv[1];
    std::string ix = db + "/indexes";

    printf("Building indexes in %s\n", ix.c_str()); fflush(stdout);

    // Zone maps (fast, parallel) — on sorted columns zone maps are maximally effective
    printf("\n--- Zone Maps ---\n"); fflush(stdout);
    #pragma omp parallel sections
    {
        #pragma omp section
        { build_zone_i32(db+"/lineitem/l_shipdate.bin",
                         ix+"/lineitem_shipdate_zonemap.bin"); }
        #pragma omp section
        { build_zone_f64(db+"/lineitem/l_quantity.bin",
                         ix+"/lineitem_quantity_zonemap.bin"); }
        #pragma omp section
        { build_zone_f64(db+"/lineitem/l_discount.bin",
                         ix+"/lineitem_discount_zonemap.bin"); }
        #pragma omp section
        { build_zone_i32(db+"/orders/o_orderdate.bin",
                         ix+"/orders_orderdate_zonemap.bin"); }
    }

    // Small hash indexes (parallel: customer, part, supplier, orders.custkey)
    printf("\n--- Small Hash Indexes ---\n"); fflush(stdout);
    #pragma omp parallel sections
    {
        #pragma omp section
        { build_hash_i32(db+"/customer/c_custkey.bin",
                         ix+"/customer_custkey_hash.bin"); }
        #pragma omp section
        { build_hash_i32(db+"/part/p_partkey.bin",
                         ix+"/part_partkey_hash.bin"); }
        #pragma omp section
        { build_hash_i32(db+"/supplier/s_suppkey.bin",
                         ix+"/supplier_suppkey_hash.bin"); }
        #pragma omp section
        { build_hash_i32(db+"/orders/o_custkey.bin",
                         ix+"/orders_custkey_hash.bin"); }
    }

    // Composite key index for partsupp (unique composite PK)
    printf("\n--- Partsupp Composite Hash ---\n"); fflush(stdout);
    build_hash_i64_composite(db+"/partsupp/ps_partkey.bin",
                              db+"/partsupp/ps_suppkey.bin",
                              ix+"/partsupp_keys_hash.bin");

    // Large hash indexes sequentially (memory pressure: orders then lineitem)
    printf("\n--- Orders Orderkey Hash ---\n"); fflush(stdout);
    build_hash_i32(db+"/orders/o_orderkey.bin",
                   ix+"/orders_orderkey_hash.bin");

    printf("\n--- Lineitem Orderkey Hash (largest) ---\n"); fflush(stdout);
    build_hash_i32(db+"/lineitem/l_orderkey.bin",
                   ix+"/lineitem_orderkey_hash.bin");

    printf("\nAll indexes built successfully.\n");
    return 0;
}
