// build_indexes.cpp — TPC-H Index Builder
// Compiles with: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp
// Usage: ./build_indexes <gendb_dir>
//
// Builds:
//   Zone maps:  lineitem/l_shipdate, l_quantity, l_discount; orders/o_orderdate
//   Hash idx:   lineitem/l_orderkey, orders/o_orderkey, customer/c_custkey,
//               part/p_partkey, supplier/s_suppkey, partsupp composite(ps_partkey,ps_suppkey)
//
// Zone map file format (per column):
//   [uint32_t num_blocks]
//   [T min, T max, uint32_t nrows_in_block] * num_blocks
//
// Hash index file format (multi-value, sort-based grouping):
//   [uint32_t magic=0x48494458]
//   [uint32_t num_positions]
//   [uint32_t num_unique_keys]
//   [uint32_t capacity]       -- power of 2
//   [uint32_t positions[num_positions]]        -- positions grouped by key (sorted)
//   [Slot hash_table[capacity]]               -- open addressing, empty=sentinel
//   Slot<int32_t>: {int32_t key; uint32_t offset; uint32_t count;}   12 bytes
//   Slot<uint64_t>: {uint64_t key; uint32_t offset; uint32_t count;} 16 bytes
//   Empty slot sentinel: key = INT32_MIN (0x80000000) or UINT64_MAX

#include <algorithm>
#include <cassert>
#include <climits>
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
#include <omp.h>

static constexpr uint32_t BLOCK_SIZE  = 100000;
static constexpr uint32_t HASH_MAGIC  = 0x48494458u;  // 'HIDX'

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────
static size_t get_nrows(const std::string& path, size_t elem_sz) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) { perror(path.c_str()); exit(1); }
    return st.st_size / elem_sz;
}

struct MmapRO {
    const void* data = nullptr;
    size_t size = 0;
    int fd = -1;
    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st; fstat(fd, &st); size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { ::close(fd); fd=-1; return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }
    ~MmapRO() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) ::close(fd);
    }
};

static uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    return n + 1;
}

// Multiply-shift hash for int32_t key
static inline uint32_t hash_i32(int32_t key, int shift) {
    return (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> shift);
}

// Multiply-shift hash for uint64_t key
static inline uint32_t hash_u64(uint64_t key, int shift) {
    // MurmurHash64 mix
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33;
    return (uint32_t)(key >> shift);
}

static void write_file(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 4*1024*1024);
    fwrite(data, 1, bytes, f);
    fclose(f);
}

// ─────────────────────────────────────────────────────────────────────────────
// Zone map builder for int32_t column
// ─────────────────────────────────────────────────────────────────────────────
static void build_zonemap_i32(const std::string& col_path, const std::string& out_path) {
    MmapRO mf; mf.open(col_path);
    size_t nrows = mf.size / sizeof(int32_t);
    const int32_t* col = (const int32_t*)mf.data;

    uint32_t num_blocks = (uint32_t)((nrows + BLOCK_SIZE - 1) / BLOCK_SIZE);
    // Each block: int32_t min, int32_t max, uint32_t nrows  → 12 bytes
    std::vector<int32_t> bmins(num_blocks), bmaxs(num_blocks);
    std::vector<uint32_t> brows(num_blocks);

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < num_blocks; b++) {
        uint32_t start = b * BLOCK_SIZE;
        uint32_t end   = std::min((uint64_t)start + BLOCK_SIZE, (uint64_t)nrows);
        int32_t mn = col[start], mx = col[start];
        for (uint32_t i = start+1; i < end; i++) {
            if (col[i] < mn) mn = col[i];
            if (col[i] > mx) mx = col[i];
        }
        bmins[b] = mn; bmaxs[b] = mx; brows[b] = end - start;
    }

    // Write: [num_blocks][min,max,nrows * num_blocks]
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", out_path.c_str()); exit(1); }
    fwrite(&num_blocks, 4, 1, f);
    for (uint32_t b = 0; b < num_blocks; b++) {
        fwrite(&bmins[b], 4, 1, f);
        fwrite(&bmaxs[b], 4, 1, f);
        fwrite(&brows[b], 4, 1, f);
    }
    fclose(f);
    printf("  zonemap_i32 %s: %u blocks\n", out_path.c_str(), num_blocks);
}

// ─────────────────────────────────────────────────────────────────────────────
// Zone map builder for double column
// ─────────────────────────────────────────────────────────────────────────────
static void build_zonemap_f64(const std::string& col_path, const std::string& out_path) {
    MmapRO mf; mf.open(col_path);
    size_t nrows = mf.size / sizeof(double);
    const double* col = (const double*)mf.data;

    uint32_t num_blocks = (uint32_t)((nrows + BLOCK_SIZE - 1) / BLOCK_SIZE);
    std::vector<double> bmins(num_blocks), bmaxs(num_blocks);
    std::vector<uint32_t> brows(num_blocks);

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < num_blocks; b++) {
        uint32_t start = b * BLOCK_SIZE;
        uint32_t end   = (uint32_t)std::min((uint64_t)start + BLOCK_SIZE, (uint64_t)nrows);
        double mn = col[start], mx = col[start];
        for (uint32_t i = start+1; i < end; i++) {
            if (col[i] < mn) mn = col[i];
            if (col[i] > mx) mx = col[i];
        }
        bmins[b] = mn; bmaxs[b] = mx; brows[b] = end - start;
    }

    // Write: [num_blocks][double min, double max, uint32_t nrows] per block
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", out_path.c_str()); exit(1); }
    fwrite(&num_blocks, 4, 1, f);
    for (uint32_t b = 0; b < num_blocks; b++) {
        fwrite(&bmins[b], 8, 1, f);
        fwrite(&bmaxs[b], 8, 1, f);
        fwrite(&brows[b], 4, 1, f);
    }
    fclose(f);
    printf("  zonemap_f64 %s: %u blocks\n", out_path.c_str(), num_blocks);
}

// ─────────────────────────────────────────────────────────────────────────────
// Hash index builder for int32_t key column (multi-value)
// Sort-based grouping — no unordered_map
// ─────────────────────────────────────────────────────────────────────────────
struct SlotI32 { int32_t key; uint32_t offset; uint32_t count; };

static void build_hash_i32(const std::string& col_path, const std::string& out_path) {
    printf("  hash_i32 %s ...\n", out_path.c_str()); fflush(stdout);
    MmapRO mf; mf.open(col_path);
    uint32_t N = (uint32_t)(mf.size / sizeof(int32_t));
    const int32_t* col = (const int32_t*)mf.data;

    // Step 1: create and sort position array by key
    std::vector<uint32_t> positions(N);
    std::iota(positions.begin(), positions.end(), 0u);
    std::sort(positions.begin(), positions.end(), [&](uint32_t a, uint32_t b) {
        return col[a] < col[b];
    });

    // Step 2: scan for unique keys → build (key, offset, count) list
    std::vector<SlotI32> entries;
    entries.reserve(N / 4 + 1);
    uint32_t i = 0;
    while (i < N) {
        int32_t key = col[positions[i]];
        uint32_t start = i;
        while (i < N && col[positions[i]] == key) i++;
        entries.push_back({key, start, i - start});
    }
    uint32_t num_unique = (uint32_t)entries.size();

    // Step 3: build open-addressing hash table (load ~0.5)
    uint32_t capacity = next_pow2(num_unique * 2);
    int shift = 64 - __builtin_ctz(capacity);
    std::vector<SlotI32> ht(capacity, {INT32_MIN, 0, 0});
    for (auto& e : entries) {
        uint32_t slot = hash_i32(e.key, shift) & (capacity - 1);
        while (ht[slot].key != INT32_MIN) slot = (slot + 1) & (capacity - 1);
        ht[slot] = e;
    }

    // Step 4: write file
    // Header: magic(4), num_positions(4), num_unique(4), capacity(4) = 16 bytes
    // positions array: N * 4 bytes
    // hash table: capacity * 12 bytes
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", out_path.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 4*1024*1024);
    fwrite(&HASH_MAGIC, 4, 1, f);
    fwrite(&N, 4, 1, f);
    fwrite(&num_unique, 4, 1, f);
    fwrite(&capacity, 4, 1, f);
    fwrite(positions.data(), 4, N, f);
    fwrite(ht.data(), sizeof(SlotI32), capacity, f);
    fclose(f);
    printf("  hash_i32 %s done: %u rows, %u unique, cap=%u\n",
           out_path.c_str(), N, num_unique, capacity);
}

// ─────────────────────────────────────────────────────────────────────────────
// Hash index builder for composite (int32_t, int32_t) → uint64_t key
// ─────────────────────────────────────────────────────────────────────────────
struct SlotU64 { uint64_t key; uint32_t offset; uint32_t count; };

static void build_hash_composite(const std::string& col1_path, const std::string& col2_path,
                                  const std::string& out_path) {
    printf("  hash_composite %s ...\n", out_path.c_str()); fflush(stdout);
    MmapRO mf1; mf1.open(col1_path);
    MmapRO mf2; mf2.open(col2_path);
    uint32_t N = (uint32_t)(mf1.size / sizeof(int32_t));
    const int32_t* col1 = (const int32_t*)mf1.data;
    const int32_t* col2 = (const int32_t*)mf2.data;

    // Encode composite key: high 32 bits = col1, low 32 bits = col2
    std::vector<uint64_t> keys(N);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < N; i++) {
        keys[i] = ((uint64_t)(uint32_t)col1[i] << 32) | (uint32_t)col2[i];
    }

    // Sort positions by composite key
    std::vector<uint32_t> positions(N);
    std::iota(positions.begin(), positions.end(), 0u);
    std::sort(positions.begin(), positions.end(), [&](uint32_t a, uint32_t b) {
        return keys[a] < keys[b];
    });

    // Find unique keys
    std::vector<SlotU64> entries;
    entries.reserve(N);
    uint32_t i = 0;
    while (i < N) {
        uint64_t key = keys[positions[i]];
        uint32_t start = i;
        while (i < N && keys[positions[i]] == key) i++;
        entries.push_back({key, start, i - start});
    }
    uint32_t num_unique = (uint32_t)entries.size();

    // Build hash table
    uint32_t capacity = next_pow2(num_unique * 2);
    int shift = 64 - __builtin_ctz(capacity);
    const uint64_t EMPTY_KEY = UINT64_MAX;
    std::vector<SlotU64> ht(capacity, {EMPTY_KEY, 0, 0});
    for (auto& e : entries) {
        uint32_t slot = hash_u64(e.key, shift) & (capacity - 1);
        while (ht[slot].key != EMPTY_KEY) slot = (slot + 1) & (capacity - 1);
        ht[slot] = e;
    }

    // Write file
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", out_path.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 4*1024*1024);
    fwrite(&HASH_MAGIC, 4, 1, f);
    fwrite(&N, 4, 1, f);
    fwrite(&num_unique, 4, 1, f);
    fwrite(&capacity, 4, 1, f);
    fwrite(positions.data(), 4, N, f);
    fwrite(ht.data(), sizeof(SlotU64), capacity, f);
    fclose(f);
    printf("  hash_composite %s done: %u rows, %u unique, cap=%u\n",
           out_path.c_str(), N, num_unique, capacity);
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    std::string db = argv[1];
    std::string idx = db + "/indexes";

    // Ensure indexes directory exists
    {
        struct stat st;
        if (stat(idx.c_str(), &st) != 0) {
            mkdir(idx.c_str(), 0755);
        }
    }

    printf("=== Building indexes in %s ===\n", idx.c_str()); fflush(stdout);
    auto t0 = std::chrono::steady_clock::now();

    // ── Zone maps (built in parallel using threads) ──────────────────────────
    printf("[zone maps]\n"); fflush(stdout);
    std::vector<std::thread> zone_threads;

    // lineitem/l_shipdate (int32_t) — primary filter column, sorted
    zone_threads.emplace_back([&](){
        build_zonemap_i32(db+"/lineitem/l_shipdate.bin",
                          idx+"/lineitem_shipdate_zonemap.bin");
    });
    // orders/o_orderdate (int32_t) — sorted
    zone_threads.emplace_back([&](){
        build_zonemap_i32(db+"/orders/o_orderdate.bin",
                          idx+"/orders_orderdate_zonemap.bin");
    });
    // lineitem/l_quantity (double)
    zone_threads.emplace_back([&](){
        build_zonemap_f64(db+"/lineitem/l_quantity.bin",
                          idx+"/lineitem_quantity_zonemap.bin");
    });
    // lineitem/l_discount (double)
    zone_threads.emplace_back([&](){
        build_zonemap_f64(db+"/lineitem/l_discount.bin",
                          idx+"/lineitem_discount_zonemap.bin");
    });

    for (auto& th : zone_threads) th.join();
    printf("[zone maps] done\n"); fflush(stdout);

    // ── Hash indexes (built in parallel) ─────────────────────────────────────
    printf("[hash indexes]\n"); fflush(stdout);
    std::vector<std::thread> hash_threads;

    // lineitem.l_orderkey — multi-value (FK, ~4 rows per key)
    hash_threads.emplace_back([&](){
        build_hash_i32(db+"/lineitem/l_orderkey.bin",
                       idx+"/lineitem_orderkey_hash.bin");
    });
    // orders.o_orderkey — PK (1:1)
    hash_threads.emplace_back([&](){
        build_hash_i32(db+"/orders/o_orderkey.bin",
                       idx+"/orders_orderkey_hash.bin");
    });
    // customer.c_custkey — PK
    hash_threads.emplace_back([&](){
        build_hash_i32(db+"/customer/c_custkey.bin",
                       idx+"/customer_custkey_hash.bin");
    });
    // part.p_partkey — PK
    hash_threads.emplace_back([&](){
        build_hash_i32(db+"/part/p_partkey.bin",
                       idx+"/part_partkey_hash.bin");
    });
    // supplier.s_suppkey — PK (100K rows > 10K threshold)
    hash_threads.emplace_back([&](){
        build_hash_i32(db+"/supplier/s_suppkey.bin",
                       idx+"/supplier_suppkey_hash.bin");
    });
    // partsupp composite (ps_partkey, ps_suppkey) — composite PK
    hash_threads.emplace_back([&](){
        build_hash_composite(db+"/partsupp/ps_partkey.bin",
                             db+"/partsupp/ps_suppkey.bin",
                             idx+"/partsupp_partkey_suppkey_hash.bin");
    });

    for (auto& th : hash_threads) th.join();
    printf("[hash indexes] done\n"); fflush(stdout);

    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    printf("=== All indexes built in %.1fs ===\n", elapsed);

    // Verify index files exist and are non-empty
    auto check = [](const std::string& p) {
        struct stat st;
        if (stat(p.c_str(), &st) != 0 || st.st_size == 0) {
            fprintf(stderr, "VERIFY FAIL: %s missing or empty\n", p.c_str());
            exit(1);
        }
        printf("  OK: %s (%zu bytes)\n", p.c_str(), (size_t)st.st_size);
    };
    check(idx+"/lineitem_shipdate_zonemap.bin");
    check(idx+"/orders_orderdate_zonemap.bin");
    check(idx+"/lineitem_quantity_zonemap.bin");
    check(idx+"/lineitem_discount_zonemap.bin");
    check(idx+"/lineitem_orderkey_hash.bin");
    check(idx+"/orders_orderkey_hash.bin");
    check(idx+"/customer_custkey_hash.bin");
    check(idx+"/part_partkey_hash.bin");
    check(idx+"/supplier_suppkey_hash.bin");
    check(idx+"/partsupp_partkey_suppkey_hash.bin");

    printf("=== Index verification PASSED ===\n");
    return 0;
}
