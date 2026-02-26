// build_indexes.cpp — Build indexes for sf3.gendb
// Indexes: num zone maps, pre zone maps, tag PK hash,
//          pre existence hash (Q24 anti-join), pre key sorted (Q4/Q6)

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cassert>
#include <climits>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <numeric>
#include <filesystem>
#include <stdexcept>
#include <unordered_map>

namespace fs = std::filesystem;

// ============================================================
// Read utilities
// ============================================================
template<typename T>
static std::vector<T> read_col(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "WARNING: cannot open %s\n", path.c_str()); return {}; }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    std::vector<T> v(sz / sizeof(T));
    if (!v.empty()) fread(v.data(), sizeof(T), v.size(), f);
    fclose(f);
    return v;
}

// Read varlen column into vector<string>
static std::vector<std::string> read_varlen(const std::string& off_path,
                                             const std::string& dat_path) {
    auto offsets = read_col<int64_t>(off_path);
    if (offsets.empty()) return {};
    uint32_t N = (uint32_t)(offsets.size() - 1);

    FILE* fd = fopen(dat_path.c_str(), "rb");
    if (!fd) return std::vector<std::string>(N);
    fseek(fd, 0, SEEK_END);
    int64_t data_sz = ftell(fd);
    fseek(fd, 0, SEEK_SET);
    std::vector<char> data(data_sz > 0 ? data_sz : 1);
    if (data_sz > 0) fread(data.data(), 1, data_sz, fd);
    fclose(fd);

    std::vector<std::string> strs(N);
    for (uint32_t i = 0; i < N; i++) {
        int64_t s = offsets[i], e = offsets[i+1];
        if (e > s && s >= 0 && e <= data_sz)
            strs[i].assign(data.data() + s, (size_t)(e - s));
    }
    return strs;
}

// ============================================================
// Hash function: FNV-64a
// ============================================================
static inline uint64_t fnv64(const void* data, size_t len) {
    uint64_t h = 14695981039346656037ULL;
    const uint8_t* p = (const uint8_t*)data;
    for (size_t i = 0; i < len; i++) { h ^= p[i]; h *= 1099511628211ULL; }
    // Ensure non-zero (0 is empty sentinel)
    return h ? h : 1;
}

// hash 3 int32_t values
static inline uint64_t hash3i(int32_t a, int32_t b, int32_t c) {
    uint64_t h = 14695981039346656037ULL;
    uint32_t ua = (uint32_t)a, ub = (uint32_t)b, uc = (uint32_t)c;
    h ^= ua; h *= 1099511628211ULL;
    h ^= (ua>>8); h *= 1099511628211ULL;
    h ^= (ua>>16); h *= 1099511628211ULL;
    h ^= (ua>>24); h *= 1099511628211ULL;
    h ^= ub; h *= 1099511628211ULL;
    h ^= (ub>>8); h *= 1099511628211ULL;
    h ^= (ub>>16); h *= 1099511628211ULL;
    h ^= (ub>>24); h *= 1099511628211ULL;
    h ^= uc; h *= 1099511628211ULL;
    h ^= (uc>>8); h *= 1099511628211ULL;
    h ^= (uc>>16); h *= 1099511628211ULL;
    h ^= (uc>>24); h *= 1099511628211ULL;
    return h ? h : 1;
}

static uint32_t next_pow2(uint32_t n) {
    n--;
    n |= n>>1; n |= n>>2; n |= n>>4; n |= n>>8; n |= n>>16;
    return n+1;
}

// ============================================================
// Zone maps for num: per-block min/max of uom and ddate
// Block size = 100000 rows
// Format: [n_blocks:int32][per block: uom_min:int8 uom_max:int8 ddate_min:int32 ddate_max:int32]
// ============================================================
static void build_num_zonemaps(const std::string& db) {
    fprintf(stderr, "[idx] building num zone maps...\n");
    auto uom   = read_col<int8_t> (db+"/num/uom.bin");
    auto ddate = read_col<int32_t>(db+"/num/ddate.bin");
    uint32_t N = (uint32_t)uom.size();
    if (N == 0) { fprintf(stderr, "[idx] num empty\n"); return; }

    const uint32_t BS = 100000;
    uint32_t n_blocks = (N + BS - 1) / BS;

    FILE* f = fopen((db+"/indexes/num_zonemaps.bin").c_str(), "wb");
    fwrite(&n_blocks, 4, 1, f);

    for (uint32_t b = 0; b < n_blocks; b++) {
        uint32_t lo = b * BS, hi = std::min(lo + BS, N);
        int8_t  umin = uom[lo], umax = uom[lo];
        int32_t dmin = ddate[lo], dmax = ddate[lo];
        for (uint32_t i = lo+1; i < hi; i++) {
            if (uom[i]   < umin) umin = uom[i];
            if (uom[i]   > umax) umax = uom[i];
            if (ddate[i] < dmin) dmin = ddate[i];
            if (ddate[i] > dmax) dmax = ddate[i];
        }
        fwrite(&umin, 1, 1, f);
        fwrite(&umax, 1, 1, f);
        fwrite(&dmin, 4, 1, f);
        fwrite(&dmax, 4, 1, f);
    }
    fclose(f);
    fprintf(stderr, "[idx] num zone maps: %u blocks\n", n_blocks);
}

// ============================================================
// Zone maps for pre: per-block min/max of stmt and adsh
// Format: [n_blocks:int32][per block: stmt_min:int8 stmt_max:int8 adsh_min:int32 adsh_max:int32]
// ============================================================
static void build_pre_zonemaps(const std::string& db) {
    fprintf(stderr, "[idx] building pre zone maps...\n");
    auto stmt = read_col<int8_t> (db+"/pre/stmt.bin");
    auto adsh = read_col<int32_t>(db+"/pre/adsh.bin");
    uint32_t N = (uint32_t)stmt.size();
    if (N == 0) { fprintf(stderr, "[idx] pre empty\n"); return; }

    const uint32_t BS = 100000;
    uint32_t n_blocks = (N + BS - 1) / BS;

    FILE* f = fopen((db+"/indexes/pre_zonemaps.bin").c_str(), "wb");
    fwrite(&n_blocks, 4, 1, f);

    for (uint32_t b = 0; b < n_blocks; b++) {
        uint32_t lo = b * BS, hi = std::min(lo + BS, N);
        int8_t  smin = stmt[lo], smax = stmt[lo];
        int32_t amin = adsh[lo], amax = adsh[lo];
        for (uint32_t i = lo+1; i < hi; i++) {
            if (stmt[i] < smin) smin = stmt[i];
            if (stmt[i] > smax) smax = stmt[i];
            if (adsh[i] < amin) amin = adsh[i];
            if (adsh[i] > amax) amax = adsh[i];
        }
        fwrite(&smin, 1, 1, f);
        fwrite(&smax, 1, 1, f);
        fwrite(&amin, 4, 1, f);
        fwrite(&amax, 4, 1, f);
    }
    fclose(f);
    fprintf(stderr, "[idx] pre zone maps: %u blocks\n", n_blocks);
}

// ============================================================
// Zone maps for tag: per-block min/max of abstract
// Format: [n_blocks:int32][per block: abstract_min:int8 abstract_max:int8]
// ============================================================
static void build_tag_zonemaps(const std::string& db) {
    fprintf(stderr, "[idx] building tag zone maps...\n");
    auto abst = read_col<int8_t>(db+"/tag/abstract.bin");
    uint32_t N = (uint32_t)abst.size();
    if (N == 0) return;

    const uint32_t BS = 100000;
    uint32_t n_blocks = (N + BS - 1) / BS;

    FILE* f = fopen((db+"/indexes/tag_zonemaps.bin").c_str(), "wb");
    fwrite(&n_blocks, 4, 1, f);

    for (uint32_t b = 0; b < n_blocks; b++) {
        uint32_t lo = b * BS, hi = std::min(lo + BS, N);
        int8_t amin = abst[lo], amax = abst[lo];
        for (uint32_t i = lo+1; i < hi; i++) {
            if (abst[i] < amin) amin = abst[i];
            if (abst[i] > amax) amax = abst[i];
        }
        fwrite(&amin, 1, 1, f);
        fwrite(&amax, 1, 1, f);
    }
    fclose(f);
    fprintf(stderr, "[idx] tag zone maps: %u blocks\n", n_blocks);
}

// ============================================================
// Tag PK hash index: (tag_str, version_str) -> row_id
// Robin Hood open-addressing hash.
// Format: [capacity:uint32][num_entries:uint32]
//         [slots: capacity × {key_hash:uint64, row_id:int32, _pad:int32}]
// Empty slot: row_id = INT32_MIN (key_hash may be anything)
// At query time: hash the key string, probe slots until key_hash matches or empty.
// ============================================================
#pragma pack(push, 1)
struct TagHashSlot {
    uint64_t key_hash;
    int32_t  row_id;
    int32_t  _pad;
};
#pragma pack(pop)

static void build_tag_pk_hash(const std::string& db) {
    fprintf(stderr, "[idx] building tag PK hash...\n");
    auto tag_strs = read_varlen(db+"/tag/tag.offsets", db+"/tag/tag.data");
    auto ver_strs = read_varlen(db+"/tag/version.offsets", db+"/tag/version.data");
    uint32_t N = (uint32_t)tag_strs.size();
    fprintf(stderr, "[idx] tag rows: %u\n", N);

    // Build combined key hash for each row
    uint32_t cap = next_pow2(N * 2);
    if (cap < 4) cap = 4;

    std::vector<TagHashSlot> slots(cap, {0, INT32_MIN, 0});

    for (uint32_t row = 0; row < N; row++) {
        const std::string& t = tag_strs[row];
        const std::string& v = ver_strs[row];
        // Compute hash of "tag\x00version"
        uint64_t h1 = fnv64(t.data(), t.size());
        uint64_t h2 = fnv64(v.data(), v.size());
        uint64_t kh = h1 ^ (h2 * 0x9e3779b97f4a7c15ULL);
        if (!kh) kh = 1;

        uint32_t pos = (uint32_t)(kh & (cap - 1));
        // Linear probing
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t idx = (pos + probe) & (cap - 1);
            if (slots[idx].row_id == INT32_MIN) {
                slots[idx].key_hash = kh;
                slots[idx].row_id   = (int32_t)row;
                break;
            }
            // Collision: continue probing (first-entry-wins for duplicate keys, PK should be unique)
        }
    }

    FILE* f = fopen((db+"/indexes/tag_pk_hash.bin").c_str(), "wb");
    fwrite(&cap, 4, 1, f);
    fwrite(&N,   4, 1, f);
    fwrite(slots.data(), sizeof(TagHashSlot), cap, f);
    fclose(f);
    fprintf(stderr, "[idx] tag PK hash: cap=%u, N=%u, load=%.2f\n", cap, N, (double)N/cap);
}

// ============================================================
// pre existence hash: (adsh_code, tag_code, ver_code) -> exists
// For Q24 anti-join: LEFT JOIN pre ON ... WHERE pre.adsh IS NULL
// Robin Hood open-addressing.
// Format: [capacity:uint32][num_entries:uint32]
//         [slots: capacity × {adsh:int32, tag:int32, ver:int32}]
// Empty slot: adsh = INT32_MIN
// ============================================================
#pragma pack(push, 1)
struct PreExistSlot {
    int32_t adsh, tag, ver;
};
#pragma pack(pop)

static void build_pre_existence_hash(const std::string& db) {
    fprintf(stderr, "[idx] building pre existence hash...\n");
    auto adsh = read_col<int32_t>(db+"/pre/adsh.bin");
    auto tag  = read_col<int32_t>(db+"/pre/tag.bin");
    auto ver  = read_col<int32_t>(db+"/pre/version.bin");
    uint32_t N = (uint32_t)adsh.size();
    fprintf(stderr, "[idx] pre rows: %u\n", N);

    uint32_t cap = next_pow2(N * 2);
    std::vector<PreExistSlot> slots(cap, {INT32_MIN, 0, 0});

    uint64_t collisions = 0;
    for (uint32_t row = 0; row < N; row++) {
        if (adsh[row] == -1) continue; // skip orphan rows
        uint64_t kh = hash3i(adsh[row], tag[row], ver[row]);
        uint32_t pos = (uint32_t)(kh & (cap - 1));
        bool inserted = false;
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t idx = (pos + probe) & (cap - 1);
            if (slots[idx].adsh == INT32_MIN) {
                slots[idx] = {adsh[row], tag[row], ver[row]};
                inserted = true;
                break;
            }
            // Check duplicate (same key already in table)
            if (slots[idx].adsh == adsh[row] && slots[idx].tag == tag[row] && slots[idx].ver == ver[row]) {
                inserted = true; // already exists
                break;
            }
            ++collisions;
        }
        if (!inserted) fprintf(stderr, "WARNING: pre existence hash full at row %u\n", row);
    }

    uint32_t n_entries = N;
    FILE* f = fopen((db+"/indexes/pre_existence_hash.bin").c_str(), "wb");
    fwrite(&cap,       4, 1, f);
    fwrite(&n_entries, 4, 1, f);
    fwrite(slots.data(), sizeof(PreExistSlot), cap, f);
    fclose(f);
    fprintf(stderr, "[idx] pre existence hash: cap=%u, N=%u, load=%.2f, collisions=%lu\n",
            cap, N, (double)N/cap, collisions);
}

// ============================================================
// pre key sorted index: sorted array for (adsh,tag,ver) -> row_id
// Allows equal-range binary search at query time (multiple pre rows per key)
// Format: [n:uint32][entries: {adsh:int32, tag:int32, ver:int32, row_id:int32}*n]
// Sorted by (adsh, tag, ver)
// ============================================================
#pragma pack(push, 1)
struct PreKeyEntry {
    int32_t adsh, tag, ver, row_id;
};
#pragma pack(pop)

static void build_pre_key_sorted(const std::string& db) {
    fprintf(stderr, "[idx] building pre key sorted index...\n");
    auto adsh = read_col<int32_t>(db+"/pre/adsh.bin");
    auto tag  = read_col<int32_t>(db+"/pre/tag.bin");
    auto ver  = read_col<int32_t>(db+"/pre/version.bin");
    uint32_t N = (uint32_t)adsh.size();

    std::vector<PreKeyEntry> entries(N);
    for (uint32_t i = 0; i < N; i++)
        entries[i] = {adsh[i], tag[i], ver[i], (int32_t)i};

    std::sort(entries.begin(), entries.end(), [](const PreKeyEntry& a, const PreKeyEntry& b){
        if (a.adsh != b.adsh) return a.adsh < b.adsh;
        if (a.tag  != b.tag)  return a.tag  < b.tag;
        return a.ver < b.ver;
    });

    FILE* f = fopen((db+"/indexes/pre_key_sorted.bin").c_str(), "wb");
    fwrite(&N, 4, 1, f);
    fwrite(entries.data(), sizeof(PreKeyEntry), N, f);
    fclose(f);
    fprintf(stderr, "[idx] pre key sorted: %u entries\n", N);
}

// ============================================================
// sub adsh hash: adsh_code -> row_id
// Since sub is sorted by adsh, adsh_code == row_id; the hash is trivially
// identity. We still write a compact hash table for code generators.
// Format: [capacity:uint32][num_entries:uint32]
//         [slots: capacity × {key_hash:uint64, row_id:int32, _pad:int32}]
// Key is FNV64 of the 20-byte adsh string.
// ============================================================
static void build_sub_adsh_hash(const std::string& db) {
    fprintf(stderr, "[idx] building sub adsh hash...\n");

    FILE* af = fopen((db+"/sub/adsh.bin").c_str(), "rb");
    if (!af) { fprintf(stderr, "[idx] sub/adsh.bin not found\n"); return; }
    fseek(af, 0, SEEK_END);
    long sz = ftell(af);
    fseek(af, 0, SEEK_SET);
    uint32_t N = (uint32_t)(sz / 20);
    std::vector<char> raw(sz);
    fread(raw.data(), 1, sz, af);
    fclose(af);

    uint32_t cap = next_pow2(N * 2);
    std::vector<TagHashSlot> slots(cap, {0, INT32_MIN, 0});

    for (uint32_t row = 0; row < N; row++) {
        uint64_t kh = fnv64(raw.data() + row*20, 20);
        uint32_t pos = (uint32_t)(kh & (cap - 1));
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t idx = (pos + probe) & (cap - 1);
            if (slots[idx].row_id == INT32_MIN) {
                slots[idx].key_hash = kh;
                slots[idx].row_id   = (int32_t)row;
                break;
            }
        }
    }

    FILE* f = fopen((db+"/indexes/sub_adsh_hash.bin").c_str(), "wb");
    fwrite(&cap, 4, 1, f);
    fwrite(&N,   4, 1, f);
    fwrite(slots.data(), sizeof(TagHashSlot), cap, f);
    fclose(f);
    fprintf(stderr, "[idx] sub adsh hash: cap=%u, N=%u\n", cap, N);
}

// ============================================================
// num adsh inverted index: adsh_code -> list of row_ids in num
// Format: [n_entries:int32]
//         [entries: {adsh_code:int32, offset_in_rowids:int32, count:int32}*n_entries sorted by adsh_code]
//         [rowids: int32*total_rows]
// ============================================================
static void build_num_adsh_index(const std::string& db) {
    fprintf(stderr, "[idx] building num adsh inverted index...\n");
    auto adsh = read_col<int32_t>(db+"/num/adsh.bin");
    uint32_t N = (uint32_t)adsh.size();

    // Group row_ids by adsh_code
    std::unordered_map<int32_t, std::vector<int32_t>> groups;
    groups.reserve(100000);
    for (uint32_t i = 0; i < N; i++) {
        if (adsh[i] >= 0) groups[adsh[i]].push_back((int32_t)i);
    }

    // Sort groups by adsh_code for binary search
    std::vector<std::pair<int32_t, std::vector<int32_t>>> sorted_groups(groups.begin(), groups.end());
    std::sort(sorted_groups.begin(), sorted_groups.end(),
              [](const auto& a, const auto& b){ return a.first < b.first; });

    uint32_t n_entries = (uint32_t)sorted_groups.size();
    // Write header entries and rowids
    struct AdshEntry { int32_t adsh_code, offset, count; };
    std::vector<AdshEntry> header(n_entries);
    std::vector<int32_t>   rowids;
    rowids.reserve(N);

    int32_t off = 0;
    for (uint32_t i = 0; i < n_entries; i++) {
        header[i].adsh_code = sorted_groups[i].first;
        header[i].offset    = off;
        header[i].count     = (int32_t)sorted_groups[i].second.size();
        for (int32_t r : sorted_groups[i].second) rowids.push_back(r);
        off += header[i].count;
    }

    FILE* f = fopen((db+"/indexes/num_adsh_index.bin").c_str(), "wb");
    fwrite(&n_entries, 4, 1, f);
    fwrite(header.data(), sizeof(AdshEntry), n_entries, f);
    uint32_t total_rowids = (uint32_t)rowids.size();
    fwrite(&total_rowids, 4, 1, f);
    fwrite(rowids.data(), 4, total_rowids, f);
    fclose(f);
    fprintf(stderr, "[idx] num adsh index: %u unique adsh, %u total rows\n", n_entries, N);
}

// ============================================================
// main
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string db = argv[1];
    fs::create_directories(db + "/indexes");

    try {
        build_num_zonemaps(db);
        build_pre_zonemaps(db);
        build_tag_zonemaps(db);
        build_tag_pk_hash(db);
        build_pre_existence_hash(db);
        build_pre_key_sorted(db);
        build_sub_adsh_hash(db);
        build_num_adsh_index(db);
    } catch (const std::exception& e) {
        fprintf(stderr, "ERROR: %s\n", e.what());
        return 1;
    }

    fprintf(stderr, "\n=== Index building complete ===\n");
    return 0;
}
