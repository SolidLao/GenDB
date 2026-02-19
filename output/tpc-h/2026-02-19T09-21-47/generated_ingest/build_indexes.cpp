// build_indexes.cpp
// Reads binary column files (output of ingest) and builds zone maps + hash indexes.
//
// Compilation: g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o build_indexes build_indexes.cpp
// Usage: ./build_indexes <gendb_dir>
//   gendb_dir e.g. /home/jl4492/GenDB/benchmarks/tpc-h/gendb/sf10.gendb

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <cassert>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

struct ZoneMapEntry {
    double   min_val;
    double   max_val;
    uint32_t row_count;
    uint32_t _pad; // = 0
}; // 24 bytes

struct HashSlot {
    int32_t  key;      // INT32_MIN = empty sentinel
    uint32_t row_pos;
}; // 8 bytes

struct HashSlot64 {
    int64_t  key;      // INT64_MIN = empty sentinel
    uint32_t row_pos;
    uint32_t _pad;
}; // 16 bytes

// ---------------------------------------------------------------------------
// MmapCol helper
// ---------------------------------------------------------------------------

struct MmapCol {
    void*  data;
    size_t size;
    int    fd;

    MmapCol(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            fprintf(stderr, "ERROR: cannot open %s\n", path.c_str());
            exit(1);
        }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            fprintf(stderr, "ERROR: mmap failed for %s\n", path.c_str());
            close(fd);
            exit(1);
        }
        madvise(data, size, MADV_SEQUENTIAL);
    }

    template<typename T> T*     as()    { return (T*)data; }
    template<typename T> size_t count() { return size / sizeof(T); }

    ~MmapCol() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }

    // No copy
    MmapCol(const MmapCol&) = delete;
    MmapCol& operator=(const MmapCol&) = delete;
};

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

static void ensure_dir(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) == 0) return; // already exists
    if (mkdir(path.c_str(), 0755) != 0) {
        fprintf(stderr, "ERROR: cannot create directory %s\n", path.c_str());
        exit(1);
    }
}

static size_t get_row_count_int32(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        fprintf(stderr, "ERROR: cannot stat %s\n", path.c_str());
        exit(1);
    }
    return (size_t)st.st_size / sizeof(int32_t);
}

using Clock = std::chrono::steady_clock;
using ms    = std::chrono::milliseconds;

static double elapsed_ms(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

// ---------------------------------------------------------------------------
// Zone map builder — int32_t column, stored as double min/max
// ---------------------------------------------------------------------------

static void build_zonemap_int32(const std::string& col_path,
                                 const std::string& out_path,
                                 const char*         label) {
    printf("Building %s zonemap...\n", label);
    auto t0 = Clock::now();

    MmapCol col(col_path);
    const int32_t* data = col.as<int32_t>();
    size_t N = col.count<int32_t>();

    constexpr uint32_t BLOCK = 100000;
    uint32_t num_blocks = (uint32_t)((N + BLOCK - 1) / BLOCK);

    std::vector<ZoneMapEntry> zones(num_blocks);

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < num_blocks; b++) {
        size_t start = (size_t)b * BLOCK;
        size_t end   = std::min(start + BLOCK, N);
        int32_t mn = data[start], mx = data[start];
        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < mn) mn = data[i];
            if (data[i] > mx) mx = data[i];
        }
        zones[b].min_val   = (double)mn;
        zones[b].max_val   = (double)mx;
        zones[b].row_count = (uint32_t)(end - start);
        zones[b]._pad      = 0;
    }

    // Write: [uint32_t num_blocks][ZoneMapEntry * num_blocks]
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "ERROR: cannot open %s\n", out_path.c_str()); exit(1); }
    fwrite(&num_blocks, sizeof(uint32_t), 1, f);
    fwrite(zones.data(), sizeof(ZoneMapEntry), num_blocks, f);
    fclose(f);

    printf("  Done %s zonemap: %u blocks, %.1f ms\n", label, num_blocks, elapsed_ms(t0));
}

// ---------------------------------------------------------------------------
// Zone map builder — double column
// ---------------------------------------------------------------------------

static void build_zonemap_double(const std::string& col_path,
                                  const std::string& out_path,
                                  const char*         label) {
    printf("Building %s zonemap...\n", label);
    auto t0 = Clock::now();

    MmapCol col(col_path);
    const double* data = col.as<double>();
    size_t N = col.count<double>();

    constexpr uint32_t BLOCK = 100000;
    uint32_t num_blocks = (uint32_t)((N + BLOCK - 1) / BLOCK);

    std::vector<ZoneMapEntry> zones(num_blocks);

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < num_blocks; b++) {
        size_t start = (size_t)b * BLOCK;
        size_t end   = std::min(start + BLOCK, N);
        double mn = data[start], mx = data[start];
        for (size_t i = start + 1; i < end; i++) {
            if (data[i] < mn) mn = data[i];
            if (data[i] > mx) mx = data[i];
        }
        zones[b].min_val   = mn;
        zones[b].max_val   = mx;
        zones[b].row_count = (uint32_t)(end - start);
        zones[b]._pad      = 0;
    }

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "ERROR: cannot open %s\n", out_path.c_str()); exit(1); }
    fwrite(&num_blocks, sizeof(uint32_t), 1, f);
    fwrite(zones.data(), sizeof(ZoneMapEntry), num_blocks, f);
    fclose(f);

    printf("  Done %s zonemap: %u blocks, %.1f ms\n", label, num_blocks, elapsed_ms(t0));
}

// ---------------------------------------------------------------------------
// Hash index builder — int32_t key → row_pos (serial, safe for PK columns)
// ---------------------------------------------------------------------------

static void build_hash32(const int32_t* keys, uint32_t N,
                          const std::string& out_path,
                          const char* label) {
    printf("Building %s hash index (N=%u)...\n", label, N);
    auto t0 = Clock::now();

    // Capacity = next power of 2 >= 2*N  (load factor ~0.5)
    uint32_t cap_bits = 1;
    uint32_t cap = 2;
    while (cap < 2u * N) { cap <<= 1; cap_bits++; }

    // Allocate: all slots empty (key = INT32_MIN)
    std::vector<HashSlot> table(cap, {INT32_MIN, 0u});

    // Serial insertion — safe and fast enough for 15M entries with unique keys
    for (uint32_t i = 0; i < N; i++) {
        int32_t k = keys[i];
        uint32_t h = (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> (64 - cap_bits));
        h &= (cap - 1);
        while (table[h].key != INT32_MIN) {
            h = (h + 1u) & (cap - 1u);
        }
        table[h].key     = k;
        table[h].row_pos = i;
    }

    // Write: [uint32_t capacity][uint32_t num_entries][HashSlot * capacity]
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "ERROR: cannot open %s\n", out_path.c_str()); exit(1); }
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(&N,   sizeof(uint32_t), 1, f);
    fwrite(table.data(), sizeof(HashSlot), cap, f);
    fclose(f);

    printf("  Done %s hash: cap=%u, %.1f ms\n", label, cap, elapsed_ms(t0));
}

// ---------------------------------------------------------------------------
// Hash index builder — composite int64_t key → row_pos (serial)
// ---------------------------------------------------------------------------

static void build_hash64(const int64_t* keys, uint32_t N,
                          const std::string& out_path,
                          const char* label) {
    printf("Building %s hash64 index (N=%u)...\n", label, N);
    auto t0 = Clock::now();

    uint32_t cap_bits = 1;
    uint32_t cap = 2;
    while (cap < 2u * N) { cap <<= 1; cap_bits++; }

    std::vector<HashSlot64> table(cap, {INT64_MIN, 0u, 0u});

    for (uint32_t i = 0; i < N; i++) {
        int64_t k = keys[i];
        uint32_t h = (uint32_t)(((uint64_t)k * 0x9E3779B97F4A7C15ULL) >> (64 - cap_bits));
        h &= (cap - 1u);
        while (table[h].key != INT64_MIN) {
            h = (h + 1u) & (cap - 1u);
        }
        table[h].key     = k;
        table[h].row_pos = i;
        table[h]._pad    = 0;
    }

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) { fprintf(stderr, "ERROR: cannot open %s\n", out_path.c_str()); exit(1); }
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(&N,   sizeof(uint32_t), 1, f);
    fwrite(table.data(), sizeof(HashSlot64), cap, f);
    fclose(f);

    printf("  Done %s hash64: cap=%u, %.1f ms\n", label, cap, elapsed_ms(t0));
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        fprintf(stderr, "  gendb_dir e.g. /path/to/sf10.gendb\n");
        return 1;
    }

    std::string base = argv[1];
    // Strip trailing slash
    while (!base.empty() && base.back() == '/') base.pop_back();

    printf("Building indexes in: %s\n\n", base.c_str());
    auto t_total = Clock::now();

    // Create indexes subdirectories
    ensure_dir(base + "/lineitem/indexes");
    ensure_dir(base + "/orders/indexes");
    ensure_dir(base + "/customer/indexes");
    ensure_dir(base + "/part/indexes");
    ensure_dir(base + "/supplier/indexes");
    ensure_dir(base + "/partsupp/indexes");

    // -----------------------------------------------------------------------
    // 1. Zone maps (built in parallel via OpenMP sections)
    // -----------------------------------------------------------------------
    printf("=== Zone Maps ===\n");

    // l_shipdate zonemap (int32_t, sorted)
    build_zonemap_int32(
        base + "/lineitem/l_shipdate.bin",
        base + "/lineitem/indexes/l_shipdate_zonemap.bin",
        "l_shipdate");

    // l_discount zonemap (double)
    build_zonemap_double(
        base + "/lineitem/l_discount.bin",
        base + "/lineitem/indexes/l_discount_zonemap.bin",
        "l_discount");

    // l_quantity zonemap (double)
    build_zonemap_double(
        base + "/lineitem/l_quantity.bin",
        base + "/lineitem/indexes/l_quantity_zonemap.bin",
        "l_quantity");

    // o_orderdate zonemap (int32_t, sorted)
    build_zonemap_int32(
        base + "/orders/o_orderdate.bin",
        base + "/orders/indexes/o_orderdate_zonemap.bin",
        "o_orderdate");

    printf("\n=== Hash Indexes ===\n");

    // -----------------------------------------------------------------------
    // 2. Hash indexes
    // -----------------------------------------------------------------------

    // orders/o_orderkey_hash.bin
    {
        size_t N = get_row_count_int32(base + "/orders/o_orderkey.bin");
        printf("  orders: %zu rows\n", N);
        MmapCol col(base + "/orders/o_orderkey.bin");
        build_hash32(col.as<int32_t>(), (uint32_t)N,
                     base + "/orders/indexes/o_orderkey_hash.bin",
                     "o_orderkey");
    }

    // customer/c_custkey_hash.bin
    {
        size_t N = get_row_count_int32(base + "/customer/c_custkey.bin");
        printf("  customer: %zu rows\n", N);
        MmapCol col(base + "/customer/c_custkey.bin");
        build_hash32(col.as<int32_t>(), (uint32_t)N,
                     base + "/customer/indexes/c_custkey_hash.bin",
                     "c_custkey");
    }

    // part/p_partkey_hash.bin
    {
        size_t N = get_row_count_int32(base + "/part/p_partkey.bin");
        printf("  part: %zu rows\n", N);
        MmapCol col(base + "/part/p_partkey.bin");
        build_hash32(col.as<int32_t>(), (uint32_t)N,
                     base + "/part/indexes/p_partkey_hash.bin",
                     "p_partkey");
    }

    // supplier/s_suppkey_hash.bin
    {
        size_t N = get_row_count_int32(base + "/supplier/s_suppkey.bin");
        printf("  supplier: %zu rows\n", N);
        MmapCol col(base + "/supplier/s_suppkey.bin");
        build_hash32(col.as<int32_t>(), (uint32_t)N,
                     base + "/supplier/indexes/s_suppkey_hash.bin",
                     "s_suppkey");
    }

    // partsupp/ps_key_hash.bin — composite key
    {
        size_t N_ps = get_row_count_int32(base + "/partsupp/ps_partkey.bin");
        size_t N_ss = get_row_count_int32(base + "/partsupp/ps_suppkey.bin");
        if (N_ps != N_ss) {
            fprintf(stderr, "ERROR: partsupp partkey/suppkey size mismatch (%zu vs %zu)\n",
                    N_ps, N_ss);
            return 1;
        }
        printf("  partsupp: %zu rows\n", N_ps);

        MmapCol col_pk(base + "/partsupp/ps_partkey.bin");
        MmapCol col_sk(base + "/partsupp/ps_suppkey.bin");
        const int32_t* pk = col_pk.as<int32_t>();
        const int32_t* sk = col_sk.as<int32_t>();
        uint32_t N = (uint32_t)N_ps;

        // Build composite key array
        printf("  Assembling composite keys for partsupp...\n");
        auto t0c = Clock::now();
        std::vector<int64_t> comp_keys(N);
        #pragma omp parallel for schedule(static)
        for (uint32_t i = 0; i < N; i++) {
            comp_keys[i] = ((int64_t)pk[i] << 32) | (uint32_t)sk[i];
        }
        printf("  Composite key assembly: %.1f ms\n", elapsed_ms(t0c));

        build_hash64(comp_keys.data(), N,
                     base + "/partsupp/indexes/ps_key_hash.bin",
                     "ps_key");
    }

    printf("\nAll indexes built successfully. Total time: %.1f ms\n", elapsed_ms(t_total));
    return 0;
}
