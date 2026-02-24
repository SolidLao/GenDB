// build_indexes.cpp — SEC EDGAR SF3 index builder
// Reads binary columnar files and builds:
//   1. num/indexes/ddate_zone_map.bin  — zone map on num.ddate (YYYYMMDD ints)
//   2. sub/indexes/adsh_row_map.bin    — direct-address array: adsh_code -> sub_row_index
//   3. tag/indexes/tagver_hash.bin     — hash table: (tag_code,ver_code) -> tag_row_index
//
// Rules:
//   C9:  next_power_of_2(count*2) for hash table capacity
//   C20: std::fill for multi-byte sentinels, not memset
//   C24: bounded probing (for-loop with probe < cap)

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <limits>

namespace fs = std::filesystem;

// ─── Helpers ──────────────────────────────────────────────────────────────────
static auto now() { return std::chrono::steady_clock::now(); }
static double ms_since(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration<double,std::milli>(now()-t0).count();
}

static uint32_t next_power_of_2(uint64_t n) {
    if (n == 0) return 1;
    uint64_t p = 1;
    while (p < n) p <<= 1;
    return (uint32_t)p;
}

// mmap a column file, return pointer + row count
template<typename T>
struct ColView {
    const T* data = nullptr;
    size_t   rows = 0;
    size_t   fsize = 0;
    int      fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::cerr << "Cannot open: " << path << "\n"; return false; }
        struct stat sb; fstat(fd, &sb);
        fsize = sb.st_size;
        rows = fsize / sizeof(T);
        data = (const T*)mmap(nullptr, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); ::close(fd); return false; }
        madvise((void*)data, fsize, MADV_SEQUENTIAL);
        return true;
    }
    void close_view() {
        if (data && data != MAP_FAILED) munmap((void*)data, fsize);
        if (fd >= 0) ::close(fd);
        data = nullptr; fd = -1;
    }
};

static void write_file(const std::string& path, const void* data, size_t bytes) {
    std::ofstream f(path, std::ios::binary);
    f.write((const char*)data, bytes);
}

// ─── 1. Zone map for num.ddate ─────────────────────────────────────────────────
// ddate is YYYYMMDD integer (int32_t).
// Zone map format: [uint32_t num_blocks][{int32_t min, int32_t max, uint32_t block_size}...]
// Block size: 100000 rows
void build_num_ddate_zone_map(const std::string& gendb_dir)
{
    auto t0 = now();
    std::string col_path = gendb_dir + "/num/ddate.bin";
    ColView<int32_t> col;
    if (!col.open(col_path)) return;

    const uint32_t BLOCK_SIZE = 100000;
    uint32_t num_blocks = (uint32_t)((col.rows + BLOCK_SIZE - 1) / BLOCK_SIZE);

    struct ZoneEntry { int32_t min_val; int32_t max_val; uint32_t block_size; };
    std::vector<ZoneEntry> zones;
    zones.reserve(num_blocks);

    for (size_t blk = 0; blk < num_blocks; ++blk) {
        size_t row_start = blk * BLOCK_SIZE;
        size_t row_end   = std::min(row_start + BLOCK_SIZE, col.rows);
        int32_t bmin = col.data[row_start];
        int32_t bmax = col.data[row_start];
        for (size_t r = row_start + 1; r < row_end; ++r) {
            int32_t v = col.data[r];
            if (v < bmin) bmin = v;
            if (v > bmax) bmax = v;
        }
        zones.push_back({bmin, bmax, (uint32_t)(row_end - row_start)});
    }

    col.close_view();

    // Write: [uint32_t num_blocks][ZoneEntry * num_blocks]
    fs::create_directories(gendb_dir + "/num/indexes");
    std::string out_path = gendb_dir + "/num/indexes/ddate_zone_map.bin";
    std::ofstream out(out_path, std::ios::binary);
    out.write((char*)&num_blocks, sizeof(uint32_t));
    out.write((char*)zones.data(), zones.size() * sizeof(ZoneEntry));

    std::cout << "[zone_map] num/ddate: " << num_blocks << " blocks in " << ms_since(t0) << "ms\n";
}

// ─── 2. Sub adsh_row_map (direct-address array) ────────────────────────────────
// sub/adsh.bin contains adsh codes (int32_t) indexed by row.
// Since adsh_code == row_index (PK property, sub processed first in ingest),
// arr[adsh_code] == adsh_code. But we build it explicitly for completeness.
// Format: int32_t array of size = max_adsh_code+1; arr[code] = row_index (-1 if absent)
void build_sub_adsh_row_map(const std::string& gendb_dir)
{
    auto t0 = now();
    std::string col_path = gendb_dir + "/sub/adsh.bin";
    ColView<int32_t> col;
    if (!col.open(col_path)) return;

    // Find max code
    int32_t max_code = -1;
    for (size_t i = 0; i < col.rows; ++i)
        if (col.data[i] > max_code) max_code = col.data[i];

    // Allocate direct-address array
    size_t arr_size = (size_t)(max_code + 1);
    std::vector<int32_t> row_map(arr_size, -1);
    for (size_t i = 0; i < col.rows; ++i)
        row_map[(size_t)col.data[i]] = (int32_t)i;

    col.close_view();

    fs::create_directories(gendb_dir + "/sub/indexes");
    write_file(gendb_dir + "/sub/indexes/adsh_row_map.bin",
               row_map.data(), row_map.size() * sizeof(int32_t));

    std::cout << "[adsh_row_map] sub: " << col.rows << " rows; map_size=" << arr_size
              << " in " << ms_since(t0) << "ms\n";
}

// ─── 3. Tag (tag_code, version_code) -> row hash table ────────────────────────
// Hash table format:
//   Header:  [uint64_t ht_size]
//   Slots:   [{int32_t tag_code, int32_t ver_code, int32_t row_idx, int32_t valid}] x ht_size
// Open addressing with linear probing. Sentinel: valid=0
// C9: ht_size = next_power_of_2(num_rows * 2) for ≤50% load
// C24: bounded probing (probe < ht_size)
struct TagVerSlot {
    int32_t tag_code;
    int32_t ver_code;
    int32_t row_idx;
    int32_t valid;   // 0 = empty, 1 = occupied
};

void build_tag_tagver_hash(const std::string& gendb_dir)
{
    auto t0 = now();
    ColView<int32_t> tag_col, ver_col;
    if (!tag_col.open(gendb_dir + "/tag/tag.bin")) return;
    if (!ver_col.open(gendb_dir + "/tag/version.bin")) return;
    size_t nrows = tag_col.rows;

    // C9: capacity = next_power_of_2(nrows * 2)
    uint64_t cap = next_power_of_2((uint64_t)nrows * 2);

    std::vector<TagVerSlot> ht(cap);
    // C20: use std::fill for initialization (not memset for multi-byte sentinels)
    std::fill(ht.begin(), ht.end(), TagVerSlot{0, 0, -1, 0});

    uint64_t mask = cap - 1;
    for (size_t i = 0; i < nrows; ++i) {
        int32_t tc = tag_col.data[i];
        int32_t vc = ver_col.data[i];
        // Hash: combine tag_code and ver_code
        uint64_t h = ((uint64_t)(uint32_t)tc * 2654435761ULL) ^ ((uint64_t)(uint32_t)vc * 2246822519ULL);
        uint64_t probe = h & mask;
        // C24: bounded probing
        for (uint64_t p = 0; p < cap; ++p) {
            uint64_t slot = (probe + p) & mask;
            if (!ht[slot].valid) {
                ht[slot] = {tc, vc, (int32_t)i, 1};
                break;
            }
        }
    }

    tag_col.close_view();
    ver_col.close_view();

    fs::create_directories(gendb_dir + "/tag/indexes");
    std::string out_path = gendb_dir + "/tag/indexes/tagver_hash.bin";
    std::ofstream out(out_path, std::ios::binary);
    out.write((char*)&cap, sizeof(uint64_t));
    out.write((char*)ht.data(), ht.size() * sizeof(TagVerSlot));

    std::cout << "[tagver_hash] tag: " << nrows << " rows; ht_size=" << cap
              << " in " << ms_since(t0) << "ms\n";
}

// ─── main ──────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: build_indexes <gendb_dir>\n";
        return 1;
    }
    std::string gendb_dir = argv[1];

    auto t0 = now();
    std::cout << "=== Building indexes for " << gendb_dir << " ===\n";

    build_num_ddate_zone_map(gendb_dir);
    build_sub_adsh_row_map(gendb_dir);
    build_tag_tagver_hash(gendb_dir);

    std::cout << "=== Index building complete in " << ms_since(t0) << "ms ===\n";
    return 0;
}
