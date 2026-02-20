// build_indexes.cpp — TPC-H index construction from binary columnar data
// Compile: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include <string>
#include <vector>
#include <algorithm>
#include <numeric>
#include <thread>
#include <stdexcept>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <parallel/algorithm>

namespace fs = std::filesystem;

static const uint32_t BLOCK_SIZE = 100000;
static const size_t   WRITE_BUF  = 4 * 1024 * 1024;

// ─── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    const void* data = nullptr; size_t size = 0; int fd = -1;
    MmapFile() = default;
    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st; fstat(fd, &st); size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        madvise((void*)data, size, MADV_SEQUENTIAL);
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
    template<typename T> const T* as() const { return (const T*)data; }
    size_t count32() const { return size / sizeof(int32_t); }
};

static void write_file(const std::string& path, const void* buf, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    setvbuf(f, nullptr, _IOFBF, WRITE_BUF);
    const char* p = (const char*)buf; size_t rem = bytes;
    while (rem) { size_t w = fwrite(p, 1, rem, f); p += w; rem -= w; }
    fclose(f);
}

static inline uint32_t next_pow2(uint64_t n) {
    uint32_t v = 1; while ((uint64_t)v < n) v <<= 1; return v;
}

// Multiply-shift hash (NEVER std::hash on integers — often identity)
static inline uint32_t mhash(int32_t key, uint32_t cap_mask) {
    return (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & cap_mask;
}

// ─── Zone Map ─────────────────────────────────────────────────────────────────
// Format: [uint32_t num_blocks][num_blocks × {int32_t min, int32_t max, uint32_t row_count}]
struct ZoneEntry { int32_t min_val, max_val; uint32_t row_count; };

static void build_zone_map(const std::string& col_path, const std::string& out_path,
                            uint32_t block_size = BLOCK_SIZE) {
    MmapFile mf(col_path);
    uint32_t nrows = (uint32_t)(mf.size / sizeof(int32_t));
    const int32_t* col = mf.as<int32_t>();
    uint32_t nblocks = (nrows + block_size - 1) / block_size;

    std::vector<ZoneEntry> zones(nblocks);

    #pragma omp parallel for schedule(dynamic, 4)
    for (uint32_t b = 0; b < nblocks; ++b) {
        uint32_t start = b * block_size;
        uint32_t end   = std::min(start + block_size, nrows);
        int32_t mn = col[start], mx = col[start];
        for (uint32_t i = start + 1; i < end; ++i) {
            if (col[i] < mn) mn = col[i];
            if (col[i] > mx) mx = col[i];
        }
        zones[b] = {mn, mx, end - start};
    }

    // Write: [uint32_t num_blocks][ZoneEntry × nblocks]
    size_t total = sizeof(uint32_t) + nblocks * sizeof(ZoneEntry);
    std::vector<uint8_t> buf(total);
    memcpy(buf.data(), &nblocks, 4);
    memcpy(buf.data() + 4, zones.data(), nblocks * sizeof(ZoneEntry));
    write_file(out_path, buf.data(), buf.size());
    fprintf(stderr, "[zone_map] %s: %u blocks\n", out_path.c_str(), nblocks);
}

// ─── Multivalue Hash Index ────────────────────────────────────────────────────
// For lineitem.l_orderkey: key → {offset, count} into positions array
// Format: [uint32_t cap][uint32_t n_pos]
//         [cap × {int32_t key, uint32_t offset, uint32_t count}]  (empty: key=INT32_MIN)
//         [n_pos × uint32_t positions]
struct MHEntry { int32_t key; uint32_t offset; uint32_t count; };

static void build_multivalue_hash(const std::string& col_path, const std::string& out_path) {
    MmapFile mf(col_path);
    uint32_t nrows = (uint32_t)(mf.size / sizeof(int32_t));
    const int32_t* col = mf.as<int32_t>();

    fprintf(stderr, "[mhash] building positions array (%u rows)...\n", nrows);

    // Sort-based grouping (no std::unordered_map<K, vector<uint32_t>>)
    std::vector<uint32_t> positions(nrows);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < nrows; ++i) positions[i] = i;

    fprintf(stderr, "[mhash] sorting positions by key...\n");
    __gnu_parallel::sort(positions.begin(), positions.end(),
        [&](uint32_t a, uint32_t b){ return col[a] < col[b]; });

    fprintf(stderr, "[mhash] scanning for group boundaries...\n");
    // Scan to find unique keys and their (offset, count)
    std::vector<MHEntry> entries;
    entries.reserve(16000000); // ~15M unique
    uint32_t i = 0;
    while (i < nrows) {
        int32_t key = col[positions[i]];
        uint32_t start = i;
        while (i < nrows && col[positions[i]] == key) ++i;
        entries.push_back({key, start, i - start});
    }
    uint32_t n_unique = (uint32_t)entries.size();
    fprintf(stderr, "[mhash] %u unique keys\n", n_unique);

    // Build open-addressing hash table
    uint32_t cap = next_pow2((uint64_t)n_unique * 2); // load ~0.5
    uint32_t mask = cap - 1;
    std::vector<MHEntry> ht(cap, {INT32_MIN, 0, 0});

    for (auto& e : entries) {
        uint32_t slot = mhash(e.key, mask);
        while (ht[slot].key != INT32_MIN) slot = (slot + 1) & mask;
        ht[slot] = e;
    }

    // Write: cap, n_pos, hash_table, positions
    uint32_t n_pos = nrows;
    size_t total = 4 + 4 + (size_t)cap * sizeof(MHEntry) + (size_t)n_pos * 4;
    std::vector<uint8_t> buf(total);
    uint8_t* p = buf.data();
    memcpy(p, &cap,   4); p += 4;
    memcpy(p, &n_pos, 4); p += 4;
    memcpy(p, ht.data(), (size_t)cap * sizeof(MHEntry)); p += (size_t)cap * sizeof(MHEntry);
    memcpy(p, positions.data(), (size_t)n_pos * 4);
    write_file(out_path, buf.data(), buf.size());
    fprintf(stderr, "[mhash] written %s (cap=%u, %.1fMB)\n",
            out_path.c_str(), cap, total / 1e6);
}

// ─── Unique Hash Index ────────────────────────────────────────────────────────
// Maps key → row_idx (key is unique per row)
// Format: [uint32_t cap][cap × {int32_t key, uint32_t row_idx}]  (empty: key=INT32_MIN)
struct UHEntry { int32_t key; uint32_t row_idx; };

static void build_unique_hash(const std::string& col_path, const std::string& out_path) {
    MmapFile mf(col_path);
    uint32_t nrows = (uint32_t)(mf.size / sizeof(int32_t));
    const int32_t* col = mf.as<int32_t>();

    uint32_t cap = next_pow2((uint64_t)nrows * 2); // load ~0.5
    uint32_t mask = cap - 1;
    std::vector<UHEntry> ht(cap, {INT32_MIN, UINT32_MAX});

    // Build single-threaded (hash table insertions have dependencies)
    for (uint32_t i = 0; i < nrows; ++i) {
        int32_t key = col[i];
        uint32_t slot = mhash(key, mask);
        while (ht[slot].key != INT32_MIN) slot = (slot + 1) & mask;
        ht[slot] = {key, i};
    }

    size_t total = 4 + (size_t)cap * sizeof(UHEntry);
    std::vector<uint8_t> buf(total);
    memcpy(buf.data(), &cap, 4);
    memcpy(buf.data() + 4, ht.data(), (size_t)cap * sizeof(UHEntry));
    write_file(out_path, buf.data(), buf.size());
    fprintf(stderr, "[uhash] written %s (cap=%u, %.1fMB)\n",
            out_path.c_str(), cap, total / 1e6);
}

// ─── main ─────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1;
    }
    std::string base = argv[1];
    std::string idx_dir = base + "/indexes";
    fs::create_directories(idx_dir);

    omp_set_num_threads(omp_get_max_threads());
    fprintf(stderr, "Using %d OpenMP threads\n", omp_get_max_threads());

    // Build zone maps in parallel
    std::thread t_zm1([&]() {
        build_zone_map(base + "/lineitem/l_shipdate.bin",
                       idx_dir + "/lineitem_shipdate_zonemap.bin");
    });
    std::thread t_zm2([&]() {
        build_zone_map(base + "/orders/o_orderdate.bin",
                       idx_dir + "/orders_orderdate_zonemap.bin");
    });
    t_zm1.join(); t_zm2.join();

    // Build unique hash indexes in parallel
    std::thread t_uh1([&]() {
        build_unique_hash(base + "/orders/o_orderkey.bin",
                          idx_dir + "/orders_orderkey_hash.bin");
    });
    std::thread t_uh2([&]() {
        build_unique_hash(base + "/customer/c_custkey.bin",
                          idx_dir + "/customer_custkey_hash.bin");
    });
    t_uh1.join(); t_uh2.join();

    // Build multivalue hash on lineitem.l_orderkey (largest, last)
    build_multivalue_hash(base + "/lineitem/l_orderkey.bin",
                          idx_dir + "/lineitem_orderkey_hash.bin");

    fprintf(stderr, "Index building complete.\n");
    return 0;
}
