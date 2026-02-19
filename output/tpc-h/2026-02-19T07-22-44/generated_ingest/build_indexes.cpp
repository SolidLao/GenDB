// build_indexes.cpp — TPC-H index builder
// Usage: ./build_indexes <gendb_dir>
// Compile: g++ -O3 -march=native -std=c++17 -fopenmp -o build_indexes build_indexes.cpp

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <cassert>

#include <string>
#include <vector>
#include <algorithm>
#include <numeric>
#include <filesystem>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

namespace fs = std::filesystem;

// ─── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(("open " + path).c_str()); return false; }
        struct stat st;
        if (fstat(fd, &st) < 0) { perror("fstat"); ::close(fd); fd = -1; return false; }
        size = (size_t)st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); ::close(fd); fd = -1; data = nullptr; return false; }
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }

    void close() {
        if (data && data != MAP_FAILED) { munmap(data, size); data = nullptr; }
        if (fd >= 0) { ::close(fd); fd = -1; }
    }

    ~MmapFile() { close(); }
};

// ─── next power of 2 (>= n) ──────────────────────────────────────────────────
static inline uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    return n + 1;
}

// ─── write helper ─────────────────────────────────────────────────────────────
static void write_all(FILE* fp, const void* buf, size_t sz) {
    size_t written = fwrite(buf, 1, sz, fp);
    if (written != sz) { perror("fwrite"); exit(1); }
}

// ─── Index 1 & 2: Zone map ────────────────────────────────────────────────────
//
// Binary format:
//   [uint32_t num_blocks]
//   per block: [int32_t min, int32_t max, uint32_t actual_block_size]
//
static void build_zonemap(const std::string& col_path,
                           const std::string& out_path,
                           const char* label) {
    double t0 = omp_get_wtime();

    MmapFile mf;
    if (!mf.open(col_path)) { fprintf(stderr, "Cannot open %s\n", col_path.c_str()); exit(1); }

    const int32_t* arr = reinterpret_cast<const int32_t*>(mf.data);
    uint32_t row_count  = (uint32_t)(mf.size / sizeof(int32_t));

    static const uint32_t BLOCK_SIZE = 100000;
    uint32_t num_blocks = (row_count + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // Per-block min/max/actual_size
    struct ZoneEntry { int32_t min_val; int32_t max_val; uint32_t actual_size; };
    std::vector<ZoneEntry> zones(num_blocks);

    #pragma omp parallel for schedule(static)
    for (uint32_t b = 0; b < num_blocks; b++) {
        uint32_t start = b * BLOCK_SIZE;
        uint32_t end   = std::min(start + BLOCK_SIZE, row_count);
        uint32_t actual = end - start;

        int32_t mn = arr[start];
        int32_t mx = arr[start];
        for (uint32_t i = start + 1; i < end; i++) {
            if (arr[i] < mn) mn = arr[i];
            if (arr[i] > mx) mx = arr[i];
        }
        zones[b] = {mn, mx, actual};
    }

    mf.close();

    // Ensure output directory exists
    fs::create_directories(fs::path(out_path).parent_path());

    FILE* fp = fopen(out_path.c_str(), "wb");
    if (!fp) { perror(("fopen " + out_path).c_str()); exit(1); }

    write_all(fp, &num_blocks, sizeof(uint32_t));
    for (uint32_t b = 0; b < num_blocks; b++) {
        write_all(fp, &zones[b].min_val,    sizeof(int32_t));
        write_all(fp, &zones[b].max_val,    sizeof(int32_t));
        write_all(fp, &zones[b].actual_size, sizeof(uint32_t));
    }
    fclose(fp);

    double t1 = omp_get_wtime();
    printf("%s zonemap: %u rows, %u blocks  %.3fs\n", label, row_count, num_blocks, t1 - t0);

    // Verify file was written
    struct stat st;
    if (stat(out_path.c_str(), &st) != 0 || st.st_size == 0) {
        fprintf(stderr, "VERIFY FAILED: %s not written properly\n", out_path.c_str());
        exit(1);
    }
    printf("  -> %s  (%ld bytes)\n", out_path.c_str(), (long)st.st_size);
}

// ─── Index 3: Single-value hash index on orders.o_orderkey ───────────────────
//
// Bucket layout: struct { int32_t key; uint32_t pos; } — 8 bytes
// Empty marker: key = INT32_MIN
// Binary format: [uint32_t capacity] [capacity × {int32_t key, uint32_t pos}]
//
struct OrderBucket {
    int32_t  key;
    uint32_t pos;
};

static void build_orders_orderkey_hash(const std::string& gendb_dir) {
    double t0 = omp_get_wtime();

    std::string col_path = gendb_dir + "/orders/o_orderkey.bin";
    std::string out_path = gendb_dir + "/indexes/orders_orderkey_hash.bin";

    MmapFile mf;
    if (!mf.open(col_path)) { fprintf(stderr, "Cannot open %s\n", col_path.c_str()); exit(1); }

    const int32_t* arr = reinterpret_cast<const int32_t*>(mf.data);
    uint32_t N = (uint32_t)(mf.size / sizeof(int32_t));

    // capacity = next power of 2 >= N * 2  (load factor ~0.5)
    uint32_t capacity = next_pow2(N * 2);

    // Step 1: create (key, pos) pairs
    std::vector<std::pair<int32_t, uint32_t>> pairs(N);
    #pragma omp parallel for schedule(static)
    for (uint32_t i = 0; i < N; i++) {
        pairs[i] = {arr[i], i};
    }

    mf.close();

    // Step 2: sort by key (enables deterministic sequential insertion)
    std::sort(pairs.begin(), pairs.end(),
              [](const std::pair<int32_t,uint32_t>& a,
                 const std::pair<int32_t,uint32_t>& b){ return a.first < b.first; });

    // Step 3: build hash table
    std::vector<OrderBucket> ht(capacity, {INT32_MIN, 0u});
    uint32_t mask = capacity - 1;

    for (uint32_t i = 0; i < N; i++) {
        int32_t  key = pairs[i].first;
        uint32_t pos = pairs[i].second;
        uint32_t bucket = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (ht[bucket].key != INT32_MIN) {
            bucket = (bucket + 1) & mask;
        }
        ht[bucket] = {key, pos};
    }

    // Write
    fs::create_directories(fs::path(out_path).parent_path());
    FILE* fp = fopen(out_path.c_str(), "wb");
    if (!fp) { perror(("fopen " + out_path).c_str()); exit(1); }

    write_all(fp, &capacity, sizeof(uint32_t));
    write_all(fp, ht.data(), (size_t)capacity * sizeof(OrderBucket));
    fclose(fp);

    double t1 = omp_get_wtime();
    printf("orders orderkey hash: %u rows, %u buckets  %.3fs\n", N, capacity, t1 - t0);

    struct stat st;
    if (stat(out_path.c_str(), &st) != 0 || st.st_size == 0) {
        fprintf(stderr, "VERIFY FAILED: %s not written properly\n", out_path.c_str());
        exit(1);
    }
    printf("  -> %s  (%ld bytes)\n", out_path.c_str(), (long)st.st_size);
}

// ─── Index 4: Multi-value hash index on lineitem.l_orderkey ──────────────────
//
// Bucket layout: struct { int32_t key; uint32_t offset; uint32_t count; } — 12 bytes
// Empty marker: key = INT32_MIN
// Binary format:
//   [uint32_t capacity]
//   [uint32_t num_positions]
//   [capacity × {int32_t key, uint32_t offset, uint32_t count}]
//   [num_positions × uint32_t positions]
//
struct LineitemBucket {
    int32_t  key;
    uint32_t offset;
    uint32_t count;
};

static void build_lineitem_orderkey_hash(const std::string& gendb_dir) {
    double t0 = omp_get_wtime();

    std::string col_path = gendb_dir + "/lineitem/l_orderkey.bin";
    std::string out_path = gendb_dir + "/indexes/lineitem_orderkey_hash.bin";

    MmapFile mf;
    if (!mf.open(col_path)) { fprintf(stderr, "Cannot open %s\n", col_path.c_str()); exit(1); }

    const int32_t* arr = reinterpret_cast<const int32_t*>(mf.data);
    uint32_t N = (uint32_t)(mf.size / sizeof(int32_t));

    // Step 1: positions array [0..N-1]
    std::vector<uint32_t> positions(N);
    std::iota(positions.begin(), positions.end(), 0u);

    // Step 2: sort positions by key value (serial std::sort as per spec)
    std::sort(positions.begin(), positions.end(),
              [&](uint32_t a, uint32_t b){ return arr[a] < arr[b]; });

    // Step 3: scan sorted positions to find group boundaries
    // Collect (key, offset, count)
    struct Group { int32_t key; uint32_t offset; uint32_t count; };
    std::vector<Group> groups;
    groups.reserve(1 << 20); // ~1M unique orders at SF10

    {
        uint32_t i = 0;
        while (i < N) {
            int32_t  key   = arr[positions[i]];
            uint32_t start = i;
            while (i < N && arr[positions[i]] == key) i++;
            groups.push_back({key, start, i - start});
        }
    }

    uint32_t num_unique = (uint32_t)groups.size();

    // Step 4: build open-addressing hash table
    uint32_t capacity = next_pow2(num_unique * 2);
    uint32_t mask     = capacity - 1;

    std::vector<LineitemBucket> ht(capacity, {INT32_MIN, 0u, 0u});

    for (uint32_t g = 0; g < num_unique; g++) {
        int32_t  key    = groups[g].key;
        uint32_t offset = groups[g].offset;
        uint32_t count  = groups[g].count;
        uint32_t bucket = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (ht[bucket].key != INT32_MIN) {
            bucket = (bucket + 1) & mask;
        }
        ht[bucket] = {key, offset, count};
    }

    mf.close();

    // Write
    fs::create_directories(fs::path(out_path).parent_path());
    FILE* fp = fopen(out_path.c_str(), "wb");
    if (!fp) { perror(("fopen " + out_path).c_str()); exit(1); }

    write_all(fp, &capacity,     sizeof(uint32_t));
    write_all(fp, &N,            sizeof(uint32_t));  // num_positions
    write_all(fp, ht.data(),     (size_t)capacity * sizeof(LineitemBucket));
    write_all(fp, positions.data(), (size_t)N * sizeof(uint32_t));
    fclose(fp);

    double t1 = omp_get_wtime();
    printf("lineitem orderkey hash: %u rows, %u unique keys, %u buckets  %.3fs\n",
           N, num_unique, capacity, t1 - t0);

    struct stat st;
    if (stat(out_path.c_str(), &st) != 0 || st.st_size == 0) {
        fprintf(stderr, "VERIFY FAILED: %s not written properly\n", out_path.c_str());
        exit(1);
    }
    printf("  -> %s  (%ld bytes)\n", out_path.c_str(), (long)st.st_size);
}

// ─── main ─────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];

    double t_total = omp_get_wtime();

    // Index 1: Zone map on lineitem.l_shipdate
    build_zonemap(gendb_dir + "/lineitem/l_shipdate.bin",
                  gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin",
                  "lineitem.l_shipdate");

    // Index 2: Zone map on orders.o_orderdate
    build_zonemap(gendb_dir + "/orders/o_orderdate.bin",
                  gendb_dir + "/indexes/orders_orderdate_zonemap.bin",
                  "orders.o_orderdate");

    // Index 3: Single-value hash index on orders.o_orderkey
    build_orders_orderkey_hash(gendb_dir);

    // Index 4: Multi-value hash index on lineitem.l_orderkey
    build_lineitem_orderkey_hash(gendb_dir);

    double t_end = omp_get_wtime();
    printf("\nTotal index build time: %.3fs\n", t_end - t_total);

    return 0;
}
