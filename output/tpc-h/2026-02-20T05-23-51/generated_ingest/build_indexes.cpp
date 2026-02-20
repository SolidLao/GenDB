// build_indexes.cpp — GenDB TPC-H index construction from binary columnar data
// Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp
#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <future>
#include <iostream>
#include <numeric>
#include <parallel/algorithm>   // __gnu_parallel::sort
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

static const uint32_t BLOCK_SIZE  = 100000;
static const int32_t  EMPTY32     = INT32_MIN;
static const int64_t  EMPTY64     = INT64_MIN;

// ─────────────────────────── mmap column ───────────────────────────────────

template<typename T>
struct ColMap {
    const T* data = nullptr;
    size_t   count = 0;
    size_t   sz    = 0;

    explicit ColMap(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st{}; fstat(fd, &st);
        sz    = (size_t)st.st_size;
        count = sz / sizeof(T);
        data  = (const T*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (data == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        madvise((void*)data, sz, MADV_SEQUENTIAL);
    }
    ~ColMap() { if (data && data != MAP_FAILED) munmap((void*)data, sz); }
    ColMap(const ColMap&) = delete;
};

// ─────────────────────────── Write helpers ──────────────────────────────────

template<typename T>
void fwrite_vec(FILE* f, const std::vector<T>& v) {
    fwrite(v.data(), sizeof(T), v.size(), f);
}

template<typename T>
void fwrite_ptr(FILE* f, const T* p, size_t n) {
    fwrite(p, sizeof(T), n, f);
}

FILE* open_index(const std::string& path) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create index: " + path);
    setvbuf(f, nullptr, _IOFBF, 4 * 1024 * 1024);
    return f;
}

// ─────────────────────────── Hash utilities ──────────────────────────────────

inline uint32_t hash32(int32_t key) {
    // Multiply-shift hash — avoids clustering unlike std::hash (identity on int)
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32);
}

inline uint32_t hash64(int64_t key) {
    uint64_t x = (uint64_t)key;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x ^= (x >> 31);
    return (uint32_t)x;
}

inline uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    n--; n|=n>>1; n|=n>>2; n|=n>>4; n|=n>>8; n|=n>>16; return n+1;
}

// ─────────────────────────── Zone Map builder ────────────────────────────────
//
// Format: [uint32_t num_blocks]
//         [struct { int32_t min; int32_t max; uint32_t block_size; } × num_blocks]
//
// Usage: skip block b if col_max[b] < lower_bound OR col_min[b] >= upper_bound

struct ZMEntry { int32_t vmin; int32_t vmax; uint32_t bsize; };

void build_zone_map(const std::string& col_path, const std::string& out_path,
                    const std::string& label) {
    auto t0 = std::chrono::steady_clock::now();
    std::cout << "  ZM " << label << " ..." << std::flush;

    ColMap<int32_t> col(col_path);
    size_t n = col.count;
    uint32_t num_blocks = (uint32_t)((n + BLOCK_SIZE - 1) / BLOCK_SIZE);

    std::vector<ZMEntry> zm(num_blocks);

    #pragma omp parallel for schedule(dynamic, 16)
    for (uint32_t b = 0; b < num_blocks; b++) {
        uint32_t beg = b * BLOCK_SIZE;
        uint32_t end = (uint32_t)std::min((size_t)(beg + BLOCK_SIZE), n);
        int32_t vmin = INT32_MAX, vmax = INT32_MIN;
        for (uint32_t i = beg; i < end; i++) {
            int32_t v = col.data[i];
            if (v < vmin) vmin = v;
            if (v > vmax) vmax = v;
        }
        zm[b] = {vmin, vmax, end - beg};
    }

    FILE* f = open_index(out_path);
    fwrite(&num_blocks, sizeof(uint32_t), 1, f);
    fwrite_vec(f, zm);
    fclose(f);

    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now()-t0).count();
    std::cout << " " << num_blocks << " blocks, " << sec << "s\n";
}

// ─────────────────────────── Multi-value Hash (int32 key) ────────────────────
//
// Format: [uint32_t num_positions]
//         [uint32_t positions[num_positions]]
//         [uint32_t capacity]
//         [struct { int32_t key; uint32_t offset; uint32_t count; } × capacity]
// Empty slot: key == INT32_MIN
//
// Lookup: h = hash32(k) & (cap-1); linear probe until key==k or key==EMPTY32
//         Found: positions[offset..offset+count-1] are row indices for that key

struct HEntry32 { int32_t key; uint32_t offset; uint32_t count; };

void build_hash_int32(const std::string& col_path, const std::string& out_path,
                      const std::string& label, bool use_parallel_sort) {
    auto t0 = std::chrono::steady_clock::now();
    std::cout << "  HASH32 " << label << " ..." << std::flush;

    ColMap<int32_t> col(col_path);
    uint32_t n = (uint32_t)col.count;

    // Step 1: create positions array [0..n-1]
    std::vector<uint32_t> pos(n);
    std::iota(pos.begin(), pos.end(), 0u);

    // Step 2: sort positions by key value (sort-based grouping)
    if (use_parallel_sort) {
        __gnu_parallel::sort(pos.begin(), pos.end(),
            [&](uint32_t a, uint32_t b){ return col.data[a] < col.data[b]; });
    } else {
        std::sort(pos.begin(), pos.end(),
            [&](uint32_t a, uint32_t b){ return col.data[a] < col.data[b]; });
    }
    std::cout << " sorted" << std::flush;

    // Step 3: scan sorted positions to find group boundaries → unique keys
    std::vector<HEntry32> entries;
    entries.reserve(n / 4 + 1024); // rough estimate of unique keys
    for (uint32_t i = 0; i < n; ) {
        int32_t k = col.data[pos[i]];
        uint32_t j = i;
        while (j < n && col.data[pos[j]] == k) j++;
        entries.push_back({k, i, j - i});
        i = j;
    }
    uint32_t num_unique = (uint32_t)entries.size();

    // Step 4: build open-addressing hash table (linear probing, load ~0.5)
    uint32_t cap = next_pow2(num_unique * 2);
    std::vector<HEntry32> ht(cap, {EMPTY32, 0, 0});
    for (const auto& e : entries) {
        uint32_t h = hash32(e.key) & (cap - 1);
        while (ht[h].key != EMPTY32) h = (h + 1) & (cap - 1);
        ht[h] = e;
    }

    // Step 5: write file
    FILE* f = open_index(out_path);
    fwrite(&n, sizeof(uint32_t), 1, f);
    fwrite_vec(f, pos);
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite_vec(f, ht);
    fclose(f);

    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now()-t0).count();
    std::cout << " " << num_unique << " unique keys, cap=" << cap << ", " << sec << "s\n";
}

// ─────────────────────────── Composite Hash (int64 key) ──────────────────────
//
// Format: [uint32_t num_positions]
//         [uint32_t positions[num_positions]]
//         [uint32_t capacity]
//         [struct { int64_t key; uint32_t offset; uint32_t count; } × capacity]
// Empty slot: key == INT64_MIN
// Key encoding: (int64_t)ps_partkey << 32 | (uint32_t)ps_suppkey

struct HEntry64 { int64_t key; uint32_t offset; uint32_t count; };

void build_hash_composite(const std::string& partkey_path, const std::string& suppkey_path,
                          const std::string& out_path, const std::string& label) {
    auto t0 = std::chrono::steady_clock::now();
    std::cout << "  HASH64 " << label << " ..." << std::flush;

    ColMap<int32_t> pk(partkey_path), sk(suppkey_path);
    assert(pk.count == sk.count);
    uint32_t n = (uint32_t)pk.count;

    // Build composite int64 key array
    std::vector<int64_t> keys(n);
    #pragma omp parallel for
    for (uint32_t i = 0; i < n; i++) {
        keys[i] = ((int64_t)pk.data[i] << 32) | (uint32_t)sk.data[i];
    }

    // Sort positions by composite key
    std::vector<uint32_t> pos(n);
    std::iota(pos.begin(), pos.end(), 0u);
    std::sort(pos.begin(), pos.end(),
        [&](uint32_t a, uint32_t b){ return keys[a] < keys[b]; });
    std::cout << " sorted" << std::flush;

    // Scan boundaries
    std::vector<HEntry64> entries;
    entries.reserve(n);
    for (uint32_t i = 0; i < n; ) {
        int64_t k = keys[pos[i]];
        uint32_t j = i;
        while (j < n && keys[pos[j]] == k) j++;
        entries.push_back({k, i, j - i});
        i = j;
    }
    uint32_t num_unique = (uint32_t)entries.size();

    // Build hash table
    uint32_t cap = next_pow2(num_unique * 2);
    std::vector<HEntry64> ht(cap, {EMPTY64, 0, 0});
    for (const auto& e : entries) {
        uint32_t h = hash64(e.key) & (cap - 1);
        while (ht[h].key != EMPTY64) h = (h + 1) & (cap - 1);
        ht[h] = e;
    }

    FILE* f = open_index(out_path);
    fwrite(&n, sizeof(uint32_t), 1, f);
    fwrite_vec(f, pos);
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite_vec(f, ht);
    fclose(f);

    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now()-t0).count();
    std::cout << " " << num_unique << " unique keys, cap=" << cap << ", " << sec << "s\n";
}

// ─────────────────────────── Main ───────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) { std::cerr << "Usage: build_indexes <db_dir>\n"; return 1; }
    std::string db = argv[1];
    std::string idx = db + "/indexes";
    fs::create_directories(idx);

    std::cout << "Building indexes in " << db << "\n";
    auto t0 = std::chrono::steady_clock::now();

    // Build all indexes in parallel using std::async (outer) + OpenMP (inner)
    // Groups of independent indexes run concurrently.

    // Group A: zone maps (very fast, can overlap with hash builds)
    auto fa1 = std::async(std::launch::async, [&]{
        build_zone_map(db+"/lineitem/l_shipdate.bin",
                       idx+"/lineitem_shipdate_zonemap.bin", "lineitem.l_shipdate");
    });
    auto fa2 = std::async(std::launch::async, [&]{
        build_zone_map(db+"/orders/o_orderdate.bin",
                       idx+"/orders_orderdate_zonemap.bin", "orders.o_orderdate");
    });

    // Group B: lineitem hash — largest, use parallel sort
    auto fb1 = std::async(std::launch::async, [&]{
        build_hash_int32(db+"/lineitem/l_orderkey.bin",
                         idx+"/lineitem_orderkey_hash.bin", "lineitem.l_orderkey",
                         true /*parallel_sort*/);
    });

    // Group C: orders hashes
    auto fc1 = std::async(std::launch::async, [&]{
        build_hash_int32(db+"/orders/o_orderkey.bin",
                         idx+"/orders_orderkey_hash.bin", "orders.o_orderkey",
                         false);
    });
    auto fc2 = std::async(std::launch::async, [&]{
        build_hash_int32(db+"/orders/o_custkey.bin",
                         idx+"/orders_custkey_hash.bin", "orders.o_custkey",
                         false);
    });

    // Group D: smaller tables
    auto fd1 = std::async(std::launch::async, [&]{
        build_hash_int32(db+"/customer/c_custkey.bin",
                         idx+"/customer_custkey_hash.bin", "customer.c_custkey",
                         false);
    });
    auto fd2 = std::async(std::launch::async, [&]{
        build_hash_int32(db+"/part/p_partkey.bin",
                         idx+"/part_partkey_hash.bin", "part.p_partkey",
                         false);
    });
    auto fd3 = std::async(std::launch::async, [&]{
        build_hash_int32(db+"/supplier/s_suppkey.bin",
                         idx+"/supplier_suppkey_hash.bin", "supplier.s_suppkey",
                         false);
    });
    auto fd4 = std::async(std::launch::async, [&]{
        build_hash_composite(db+"/partsupp/ps_partkey.bin",
                             db+"/partsupp/ps_suppkey.bin",
                             idx+"/partsupp_composite_hash.bin",
                             "partsupp.(ps_partkey,ps_suppkey)");
    });

    // Wait for all
    fa1.get(); fa2.get();
    fb1.get();
    fc1.get(); fc2.get();
    fd1.get(); fd2.get(); fd3.get(); fd4.get();

    double sec = std::chrono::duration<double>(std::chrono::steady_clock::now()-t0).count();
    std::cout << "All indexes built in " << sec << "s\n";

    // Verify index files exist and are non-empty
    std::vector<std::string> idx_files = {
        idx+"/lineitem_shipdate_zonemap.bin",
        idx+"/lineitem_orderkey_hash.bin",
        idx+"/orders_orderdate_zonemap.bin",
        idx+"/orders_orderkey_hash.bin",
        idx+"/orders_custkey_hash.bin",
        idx+"/customer_custkey_hash.bin",
        idx+"/part_partkey_hash.bin",
        idx+"/supplier_suppkey_hash.bin",
        idx+"/partsupp_composite_hash.bin",
    };
    for (const auto& p : idx_files) {
        struct stat st{};
        if (stat(p.c_str(), &st) != 0 || st.st_size == 0)
            std::cerr << "WARN: index missing or empty: " << p << "\n";
        else
            std::cout << "  [OK] " << fs::path(p).filename().string()
                      << " (" << st.st_size/1024 << " KB)\n";
    }
    return 0;
}
