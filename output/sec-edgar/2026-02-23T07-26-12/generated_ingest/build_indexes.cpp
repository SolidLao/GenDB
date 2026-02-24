// build_indexes.cpp — Build hash indexes and zone maps for sf3.gendb
//
// Indexes built:
//   sub_adsh_hash.bin        : adsh_code → sub row_id  (for FK joins)
//   tag_tv_hash.bin          : (tag_code, version_code) → tag row_id
//   pre_atv_hash.bin         : (adsh,tag,version) codes → presence (for Q24 anti-join)
//   sub_fy_zone_map.bin      : zone map on sub.fy
//   sub_sic_zone_map.bin     : zone map on sub.sic
//   num_uom_zone_map.bin     : zone map on num.uom (sorted, very effective)
//   num_ddate_zone_map.bin   : zone map on num.ddate (sorted within uom)
//   pre_stmt_zone_map.bin    : zone map on pre.stmt (sorted)
//
// Hash index file layout: [uint64_t ht_cap] [Slot × ht_cap]
// Empty slot marker: key field = INT32_MIN (use std::fill, NOT memset — rule C20)
// Bounded probing: for loop with probe < cap (rule C24)
// Capacity: next_power_of_2(row_count * 2) for ≤50% load factor (rule C9)
//
// Zone map file layout: [uint32_t num_blocks] [ZoneBlock × num_blocks]
//   ZoneBlock<T>: { T min; T max; uint32_t row_count; }

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

using namespace std;

// ============================================================================
// Utilities
// ============================================================================
static uint64_t next_pow2(uint64_t n) {
    --n; n|=n>>1; n|=n>>2; n|=n>>4; n|=n>>8; n|=n>>16; n|=n>>32;
    return n + 1;
}

template<typename T>
static const T* mmap_col(const string& path, size_t& nrows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); nrows = 0; return nullptr; }
    struct stat sb; fstat(fd, &sb);
    nrows = sb.st_size / sizeof(T);
    const T* d = (const T*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (d == MAP_FAILED) { perror("mmap"); nrows = 0; return nullptr; }
    madvise((void*)d, sb.st_size, MADV_SEQUENTIAL);
    return d;
}

static bool write_binary(const string& path, const void* data, size_t bytes) {
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(path.c_str()); return false; }
    const char* p = (const char*)data; size_t written = 0;
    while (written < bytes) {
        ssize_t r = ::write(fd, p+written, bytes-written);
        if (r <= 0) { perror("write"); close(fd); return false; }
        written += r;
    }
    close(fd);
    return true;
}

// ============================================================================
// Hash functions
// ============================================================================
static inline uint64_t h1(int32_t k) {
    return (uint64_t)(uint32_t)k * 2654435761ULL;
}
static inline uint64_t h2(int32_t a, int32_t b) {
    return (uint64_t)(uint32_t)a * 2654435761ULL
         ^ (uint64_t)(uint32_t)b * 40503ULL;
}
static inline uint64_t h3(int32_t a, int32_t b, int32_t c) {
    return (uint64_t)(uint32_t)a * 2654435761ULL
         ^ (uint64_t)(uint32_t)b * 40503ULL
         ^ (uint64_t)(uint32_t)c * 48271ULL;
}

// ============================================================================
// Hash index: sub.adsh → row_id
// Slot: { int32_t adsh_code; int32_t row_id; }
// Empty: adsh_code == INT32_MIN
// ============================================================================
struct SubAdsSlot { int32_t adsh_code; int32_t row_id; };

static void build_sub_adsh_hash(const string& db_dir) {
    size_t nrows;
    const int32_t* adsh = mmap_col<int32_t>(db_dir + "/sub/adsh.bin", nrows);
    if (!adsh) return;
    cout << "[sub_adsh_hash] " << nrows << " rows..." << flush;

    uint64_t cap = next_pow2((uint64_t)nrows * 2);
    vector<SubAdsSlot> ht(cap);
    fill(ht.begin(), ht.end(), SubAdsSlot{INT32_MIN, -1}); // C20: use fill not memset

    for (int32_t i = 0; i < (int32_t)nrows; ++i) {
        uint64_t h = h1(adsh[i]) & (cap - 1);
        for (uint64_t probe = 0; probe < cap; ++probe) { // C24: bounded probing
            uint64_t idx = (h + probe) & (cap - 1);
            if (ht[idx].adsh_code == INT32_MIN) {
                ht[idx] = {adsh[i], i}; break;
            }
        }
    }

    munmap((void*)adsh, nrows * sizeof(int32_t));

    // File: [uint64_t cap] [SubAdsSlot × cap]
    string path = db_dir + "/indexes/sub_adsh_hash.bin";
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    ::write(fd, &cap, sizeof(cap));
    ::write(fd, ht.data(), cap * sizeof(SubAdsSlot));
    close(fd);
    cout << " cap=" << cap << " done." << endl;
}

// ============================================================================
// Hash index: tag.(tag,version) → row_id
// Slot: { int32_t tag_code; int32_t version_code; int32_t row_id; int32_t _pad; }
// Empty: tag_code == INT32_MIN
// ============================================================================
struct TagTVSlot { int32_t tag_code; int32_t version_code; int32_t row_id; int32_t _pad; };

static void build_tag_tv_hash(const string& db_dir) {
    size_t nrows, nrows2;
    const int32_t* tag_col = mmap_col<int32_t>(db_dir + "/tag/tag.bin",     nrows);
    const int32_t* ver_col = mmap_col<int32_t>(db_dir + "/tag/version.bin", nrows2);
    if (!tag_col || !ver_col || nrows != nrows2) {
        cerr << "[tag_tv_hash] ERROR loading columns\n"; return;
    }
    cout << "[tag_tv_hash] " << nrows << " rows..." << flush;

    uint64_t cap = next_pow2((uint64_t)nrows * 2);
    vector<TagTVSlot> ht(cap);
    fill(ht.begin(), ht.end(), TagTVSlot{INT32_MIN, -1, -1, 0});

    for (int32_t i = 0; i < (int32_t)nrows; ++i) {
        uint64_t h = h2(tag_col[i], ver_col[i]) & (cap - 1);
        for (uint64_t probe = 0; probe < cap; ++probe) {
            uint64_t idx = (h + probe) & (cap - 1);
            if (ht[idx].tag_code == INT32_MIN) {
                ht[idx] = {tag_col[i], ver_col[i], i, 0}; break;
            }
        }
    }

    munmap((void*)tag_col, nrows * sizeof(int32_t));
    munmap((void*)ver_col, nrows2 * sizeof(int32_t));

    string path = db_dir + "/indexes/tag_tv_hash.bin";
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    ::write(fd, &cap, sizeof(cap));
    ::write(fd, ht.data(), cap * sizeof(TagTVSlot));
    close(fd);
    cout << " cap=" << cap << " done." << endl;
}

// ============================================================================
// Hash index: pre.(adsh,tag,version) → existence (for Q24 anti-join)
// Slot: { int32_t adsh; int32_t tag; int32_t version; int32_t row_id; }
// Empty: adsh == INT32_MIN
// Multiple pre rows with same (adsh,tag,version) → store first occurrence
// ============================================================================
struct PreATVSlot { int32_t adsh; int32_t tag; int32_t version; int32_t row_id; };

static void build_pre_atv_hash(const string& db_dir) {
    size_t nr_a, nr_t, nr_v;
    const int32_t* adsh = mmap_col<int32_t>(db_dir + "/pre/adsh.bin",   nr_a);
    const int32_t* tag  = mmap_col<int32_t>(db_dir + "/pre/tag.bin",    nr_t);
    const int32_t* ver  = mmap_col<int32_t>(db_dir + "/pre/version.bin",nr_v);
    if (!adsh || !tag || !ver || !(nr_a == nr_t && nr_t == nr_v)) {
        cerr << "[pre_atv_hash] ERROR loading columns\n"; return;
    }
    size_t nrows = nr_a;
    cout << "[pre_atv_hash] " << nrows << " rows..." << flush;

    uint64_t cap = next_pow2((uint64_t)nrows * 2);
    cout << " cap=" << cap << " (" << (cap*sizeof(PreATVSlot)/(1024*1024)) << "MB)..." << flush;

    // Use mmap(MAP_ANONYMOUS) + parallel init for large table (P22)
    size_t nbytes = cap * sizeof(PreATVSlot);
    PreATVSlot* ht = (PreATVSlot*)mmap(nullptr, nbytes,
                                        PROT_READ|PROT_WRITE,
                                        MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (ht == MAP_FAILED) { perror("mmap anonymous"); return; }

    // Parallel page-fault initialization (P22)
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < cap; ++i) {
        ht[i] = PreATVSlot{INT32_MIN, INT32_MIN, INT32_MIN, -1};
    }

    // Sequential insert (correctness; parallel CAS would race on multi-field slots)
    for (int32_t i = 0; i < (int32_t)nrows; ++i) {
        uint64_t h = h3(adsh[i], tag[i], ver[i]) & (cap - 1);
        for (uint64_t probe = 0; probe < cap; ++probe) {
            uint64_t idx = (h + probe) & (cap - 1);
            if (ht[idx].adsh == INT32_MIN) {
                ht[idx] = {adsh[i], tag[i], ver[i], i}; break;
            }
            // If same key already present, skip (keep first occurrence)
            if (ht[idx].adsh == adsh[i] && ht[idx].tag == tag[i] && ht[idx].version == ver[i])
                break;
        }
    }

    munmap((void*)adsh, nr_a * sizeof(int32_t));
    munmap((void*)tag,  nr_t * sizeof(int32_t));
    munmap((void*)ver,  nr_v * sizeof(int32_t));

    string path = db_dir + "/indexes/pre_atv_hash.bin";
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    ::write(fd, &cap, sizeof(cap));
    // Write in chunks to handle large file
    const char* p = (const char*)ht;
    size_t written = 0;
    while (written < nbytes) {
        ssize_t r = ::write(fd, p+written, min((size_t)256*1024*1024, nbytes-written));
        if (r <= 0) { perror("write pre_atv"); break; }
        written += r;
    }
    close(fd);
    munmap(ht, nbytes);
    cout << " done." << endl;
}

// ============================================================================
// Zone map builder
// ============================================================================
template<typename T>
struct ZoneBlock { T min_val; T max_val; uint32_t row_count; };

template<typename T>
static void build_zone_map(const string& col_path, const string& out_path, uint32_t block_size) {
    size_t nrows;
    const T* col = mmap_col<T>(col_path, nrows);
    if (!col) return;
    cout << "[zone_map] " << out_path << " rows=" << nrows << " block=" << block_size << "..." << flush;

    vector<ZoneBlock<T>> blocks;
    for (size_t i = 0; i < nrows; i += block_size) {
        size_t end = min(i + block_size, nrows);
        T mn = col[i], mx = col[i];
        for (size_t j = i+1; j < end; ++j) { mn = min(mn, col[j]); mx = max(mx, col[j]); }
        blocks.push_back({mn, mx, (uint32_t)(end - i)});
    }
    munmap((void*)col, nrows * sizeof(T));

    uint32_t nb = (uint32_t)blocks.size();
    int fd = open(out_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    ::write(fd, &nb, sizeof(nb));
    ::write(fd, blocks.data(), nb * sizeof(ZoneBlock<T>));
    close(fd);
    cout << " " << nb << " blocks. done." << endl;
}

// ============================================================================
// main
// ============================================================================
int main(int argc, char* argv[]) {
    if (argc < 2) { cerr << "Usage: " << argv[0] << " <gendb_dir>\n"; return 1; }
    string db = argv[1];

    // --- Hash Indexes ---
    build_sub_adsh_hash(db);
    build_tag_tv_hash(db);
    build_pre_atv_hash(db);

    // --- Zone Maps ---
    // sub: sorted by (fy, sic)
    build_zone_map<int32_t>(db+"/sub/fy.bin",    db+"/indexes/sub_fy_zone_map.bin",  10000);
    build_zone_map<int32_t>(db+"/sub/sic.bin",   db+"/indexes/sub_sic_zone_map.bin", 10000);

    // num: sorted by (uom, ddate)
    build_zone_map<int16_t>(db+"/num/uom.bin",   db+"/indexes/num_uom_zone_map.bin",   100000);
    build_zone_map<int32_t>(db+"/num/ddate.bin", db+"/indexes/num_ddate_zone_map.bin", 100000);

    // pre: sorted by (stmt)
    build_zone_map<int16_t>(db+"/pre/stmt.bin",  db+"/indexes/pre_stmt_zone_map.bin",  100000);

    cout << "[build_indexes] all done." << endl;
    return 0;
}
