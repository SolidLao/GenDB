// build_indexes.cpp — SEC EDGAR SF3 index building
//
// Reads binary columnar data from gendb_dir, builds three hash indexes:
//   1. sub/indexes/sub_adsh_hash.bin   — adsh_code → sub_row_id
//   2. tag/indexes/tag_pair_hash.bin   — (tag_code, ver_code) → tag_row_id
//   3. pre/indexes/pre_triple_hash.bin — (adsh_code, tag_code, ver_code) → first_row_id in sorted pre
//
// Hash function (must match query code exactly):
//   hash_int32(k)       = (uint64_t)k * 0x9E3779B97F4A7C15ULL
//   hash_combine(h1,h2) = h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1<<6) + (h1>>2))
//
// Slot formats (16 bytes each for cache alignment):
//   SubADSHSlot:   { int32_t adsh_code; int32_t row_id; int32_t _pad[2]; }
//   TagPairSlot:   { int32_t tag_code; int32_t ver_code; int32_t row_id; int32_t _pad; }
//   PreTripleSlot: { int32_t adsh_code; int32_t tag_code; int32_t ver_code; int32_t row_id; }
//
// File layout: [uint32_t capacity][Slot * capacity]
// Empty sentinel: first field (key) == INT32_MIN

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <cassert>
#include <cmath>
#include <string>
#include <vector>
#include <chrono>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

// ─────────────────────────────────────────────────────────────
// Timing
// ─────────────────────────────────────────────────────────────
static double now_sec() {
    auto t = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(t.time_since_epoch()).count();
}

// ─────────────────────────────────────────────────────────────
// Hash functions (verbatim — must match query code)
// ─────────────────────────────────────────────────────────────
static inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
}

static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
    return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
}

// ─────────────────────────────────────────────────────────────
// next_power_of_2
// ─────────────────────────────────────────────────────────────
static uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    uint32_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

// ─────────────────────────────────────────────────────────────
// Read binary column file
// ─────────────────────────────────────────────────────────────
template<typename T>
static std::vector<T> read_col(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { perror(path.c_str()); exit(1); }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    size_t n = sz / sizeof(T);
    std::vector<T> data(n);
    if (n > 0) fread(data.data(), sizeof(T), n, f);
    fclose(f);
    return data;
}

// ─────────────────────────────────────────────────────────────
// Slot structures — 16 bytes each (cache line aligned)
// C20: use std::fill with sentinel struct, NOT memset
// ─────────────────────────────────────────────────────────────

struct SubADSHSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t row_id;
    int32_t _pad0;
    int32_t _pad1;
};
static_assert(sizeof(SubADSHSlot) == 16, "SubADSHSlot must be 16 bytes");

struct TagPairSlot {
    int32_t tag_code;   // INT32_MIN = empty
    int32_t ver_code;
    int32_t row_id;
    int32_t _pad;
};
static_assert(sizeof(TagPairSlot) == 16, "TagPairSlot must be 16 bytes");

struct PreTripleSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t tag_code;
    int32_t ver_code;
    int32_t row_id;
};
static_assert(sizeof(PreTripleSlot) == 16, "PreTripleSlot must be 16 bytes");

// ─────────────────────────────────────────────────────────────
// Write hash table to file
// Layout: [uint32_t cap][Slot * cap]
// ─────────────────────────────────────────────────────────────
template<typename Slot>
static void write_hash(const std::string& path, const std::vector<Slot>& table, uint32_t cap) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(table.data(), sizeof(Slot), table.size(), f);
    fclose(f);
}

// ─────────────────────────────────────────────────────────────
// Build sub_adsh_hash: adsh_code → sub_row_id
// cap = next_pow2(86135 * 2) = 262144
// ─────────────────────────────────────────────────────────────
static void build_sub_adsh_hash(const std::string& db_dir) {
    double t0 = now_sec();
    fprintf(stderr, "[index] Building sub_adsh_hash...\n");

    auto adsh = read_col<int32_t>(db_dir + "/sub/adsh.bin");
    size_t N = adsh.size();

    uint32_t cap = next_pow2((uint32_t)(N * 2));
    uint32_t mask = cap - 1;
    fprintf(stderr, "[index] sub_adsh_hash: N=%zu cap=%u\n", N, cap);

    // C20: use std::fill, NOT memset, for INT32_MIN sentinel
    std::vector<SubADSHSlot> table(cap);
    SubADSHSlot empty_slot;
    empty_slot.adsh_code = INT32_MIN;
    empty_slot.row_id    = -1;
    empty_slot._pad0     = 0;
    empty_slot._pad1     = 0;
    std::fill(table.begin(), table.end(), empty_slot);

    for (uint32_t row = 0; row < (uint32_t)N; row++) {
        int32_t key = adsh[row];
        uint32_t pos = (uint32_t)(hash_int32(key) & mask);
        // C24: bounded linear probe
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t slot = (pos + probe) & mask;
            if (table[slot].adsh_code == INT32_MIN) {
                table[slot].adsh_code = key;
                table[slot].row_id    = (int32_t)row;
                break;
            }
        }
    }

    std::string out_path = db_dir + "/sub/indexes/sub_adsh_hash.bin";
    write_hash(out_path, table, cap);
    fprintf(stderr, "[index] sub_adsh_hash done in %.2fs → %s (%.1f MB)\n",
            now_sec()-t0, out_path.c_str(),
            (4.0 + cap * sizeof(SubADSHSlot)) / (1<<20));
}

// ─────────────────────────────────────────────────────────────
// Build tag_pair_hash: (tag_code, ver_code) → tag_row_id
// cap = next_pow2(1070662 * 2) = 4194304
// ─────────────────────────────────────────────────────────────
static void build_tag_pair_hash(const std::string& db_dir) {
    double t0 = now_sec();
    fprintf(stderr, "[index] Building tag_pair_hash...\n");

    auto tag_col = read_col<int32_t>(db_dir + "/tag/tag.bin");
    auto ver_col = read_col<int32_t>(db_dir + "/tag/version.bin");
    size_t N = tag_col.size();
    assert(ver_col.size() == N);

    uint32_t cap = next_pow2((uint32_t)(N * 2));
    uint32_t mask = cap - 1;
    fprintf(stderr, "[index] tag_pair_hash: N=%zu cap=%u\n", N, cap);

    std::vector<TagPairSlot> table(cap);
    TagPairSlot empty_slot;
    empty_slot.tag_code = INT32_MIN;
    empty_slot.ver_code = -1;
    empty_slot.row_id   = -1;
    empty_slot._pad     = 0;
    std::fill(table.begin(), table.end(), empty_slot);

    for (uint32_t row = 0; row < (uint32_t)N; row++) {
        int32_t tk = tag_col[row];
        int32_t vk = ver_col[row];
        uint64_t h = hash_combine(hash_int32(tk), hash_int32(vk));
        uint32_t pos = (uint32_t)(h & mask);
        // C24: bounded linear probe
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t slot = (pos + probe) & mask;
            if (table[slot].tag_code == INT32_MIN) {
                table[slot].tag_code = tk;
                table[slot].ver_code = vk;
                table[slot].row_id   = (int32_t)row;
                break;
            }
            // Duplicate (tag, version) — tag PK should be unique, keep first
            if (table[slot].tag_code == tk && table[slot].ver_code == vk) break;
        }
    }

    std::string out_path = db_dir + "/tag/indexes/tag_pair_hash.bin";
    write_hash(out_path, table, cap);
    fprintf(stderr, "[index] tag_pair_hash done in %.2fs → %s (%.1f MB)\n",
            now_sec()-t0, out_path.c_str(),
            (4.0 + cap * sizeof(TagPairSlot)) / (1<<20));
}

// ─────────────────────────────────────────────────────────────
// Build pre_triple_hash: (adsh_code, tag_code, ver_code) → first_row_id in sorted pre
// cap = next_pow2(9600799 * 2) = 33554432
//
// Since pre is sorted by (adsh_code, tag_code, ver_code), the hash stores
// only the FIRST row_id for each unique key. Query code scans forward from
// first_row_id while the triple matches.
// ─────────────────────────────────────────────────────────────
static void build_pre_triple_hash(const std::string& db_dir) {
    double t0 = now_sec();
    fprintf(stderr, "[index] Building pre_triple_hash...\n");

    auto adsh_col = read_col<int32_t>(db_dir + "/pre/adsh.bin");
    auto tag_col  = read_col<int32_t>(db_dir + "/pre/tag.bin");
    auto ver_col  = read_col<int32_t>(db_dir + "/pre/version.bin");
    size_t N = adsh_col.size();
    assert(tag_col.size() == N && ver_col.size() == N);

    uint32_t cap = next_pow2((uint32_t)(N * 2));
    uint32_t mask = cap - 1;
    fprintf(stderr, "[index] pre_triple_hash: N=%zu cap=%u (%.0f MB)\n",
            N, cap, (4.0 + (double)cap * sizeof(PreTripleSlot)) / (1<<20));

    std::vector<PreTripleSlot> table(cap);
    PreTripleSlot empty_slot;
    empty_slot.adsh_code = INT32_MIN;
    empty_slot.tag_code  = -1;
    empty_slot.ver_code  = -1;
    empty_slot.row_id    = -1;
    std::fill(table.begin(), table.end(), empty_slot);

    size_t distinct_keys = 0;
    int32_t prev_adsh = INT32_MIN, prev_tag = INT32_MIN, prev_ver = INT32_MIN;

    for (uint32_t row = 0; row < (uint32_t)N; row++) {
        int32_t ak = adsh_col[row];
        int32_t tk = tag_col[row];
        int32_t vk = ver_col[row];

        // Since pre is sorted, skip duplicate keys (only insert first occurrence)
        if (ak == prev_adsh && tk == prev_tag && vk == prev_ver) continue;
        prev_adsh = ak; prev_tag = tk; prev_ver = vk;
        distinct_keys++;

        uint64_t h = hash_combine(hash_combine(hash_int32(ak), hash_int32(tk)), hash_int32(vk));
        uint32_t pos = (uint32_t)(h & mask);
        // C24: bounded linear probe
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t slot = (pos + probe) & mask;
            if (table[slot].adsh_code == INT32_MIN) {
                table[slot].adsh_code = ak;
                table[slot].tag_code  = tk;
                table[slot].ver_code  = vk;
                table[slot].row_id    = (int32_t)row;
                break;
            }
        }

        if ((distinct_keys & 0xFFFFF) == 0) {
            fprintf(stderr, "[index] pre_triple_hash: %.1fM distinct keys...\r",
                    distinct_keys/1e6);
        }
    }
    fprintf(stderr, "\n[index] pre_triple_hash: %zu distinct (adsh,tag,version) keys\n",
            distinct_keys);

    std::string out_path = db_dir + "/pre/indexes/pre_triple_hash.bin";
    write_hash(out_path, table, cap);
    fprintf(stderr, "[index] pre_triple_hash done in %.2fs → %s (%.0f MB)\n",
            now_sec()-t0, out_path.c_str(),
            (4.0 + (double)cap * sizeof(PreTripleSlot)) / (1<<20));
}

// ─────────────────────────────────────────────────────────────
// Spot-check: verify a few date-like and numeric columns
// ─────────────────────────────────────────────────────────────
static void verify(const std::string& db_dir) {
    fprintf(stderr, "[verify] Spot-checking binary columns...\n");

    // Check num.ddate has values > 3000 (they are YYYYMMDD integers like 20221231)
    {
        auto ddate = read_col<int32_t>(db_dir + "/num/ddate.bin");
        int ok = 0, bad = 0;
        for (size_t i = 0; i < std::min((size_t)10000, ddate.size()); i++) {
            if (ddate[i] > 3000) ok++; else bad++;
        }
        fprintf(stderr, "[verify] num.ddate[0..10000]: %d > 3000 (expected), %d <= 3000\n", ok, bad);
    }

    // Check num.value has non-NaN values
    {
        auto val = read_col<double>(db_dir + "/num/value.bin");
        int non_null = 0, null_count = 0;
        for (size_t i = 0; i < std::min((size_t)10000, val.size()); i++) {
            if (!std::isnan(val[i])) non_null++; else null_count++;
        }
        fprintf(stderr, "[verify] num.value[0..10000]: %d non-NULL, %d NULL\n",
                non_null, null_count);
        // Show a sample max
        double max_v = -1e300;
        for (size_t i = 0; i < std::min((size_t)100000, val.size()); i++) {
            if (!std::isnan(val[i]) && val[i] > max_v) max_v = val[i];
        }
        fprintf(stderr, "[verify] num.value max in first 100K rows: %.3e\n", max_v);
    }

    // Check sub.fy has non-zero values
    {
        auto fy = read_col<int32_t>(db_dir + "/sub/fy.bin");
        int non_null = 0, null_count = 0;
        for (size_t i = 0; i < fy.size(); i++) {
            if (fy[i] != INT32_MIN) non_null++; else null_count++;
        }
        fprintf(stderr, "[verify] sub.fy: %d non-NULL, %d NULL\n", non_null, null_count);
    }

    // Check pre column sizes match
    {
        auto padsh = read_col<int32_t>(db_dir + "/pre/adsh.bin");
        auto pstmt = read_col<int16_t>(db_dir + "/pre/stmt.bin");
        auto ptag  = read_col<int32_t>(db_dir + "/pre/tag.bin");
        fprintf(stderr, "[verify] pre: adsh=%zu, stmt=%zu, tag=%zu rows\n",
                padsh.size(), pstmt.size(), ptag.size());
        // Check that adsh is sorted (non-decreasing)
        bool sorted_ok = true;
        for (size_t i = 1; i < std::min((size_t)100000, padsh.size()); i++) {
            if (padsh[i] < padsh[i-1]) { sorted_ok = false; break; }
        }
        fprintf(stderr, "[verify] pre: first 100K adsh_code values sorted=%s\n",
                sorted_ok ? "YES" : "NO (BUG!)");
    }

    // Check num row count
    {
        auto nadsh = read_col<int32_t>(db_dir + "/num/adsh.bin");
        fprintf(stderr, "[verify] num: %zu rows\n", nadsh.size());
    }

    // Verify pre_triple_hash has entries
    {
        FILE* f = fopen((db_dir + "/pre/indexes/pre_triple_hash.bin").c_str(), "rb");
        if (f) {
            uint32_t cap;
            fread(&cap, 4, 1, f);
            fclose(f);
            fprintf(stderr, "[verify] pre_triple_hash: cap=%u\n", cap);
        }
    }

    fprintf(stderr, "[verify] Done\n");
}

// ─────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string db_dir(argv[1]);
    double t_start = now_sec();
    fprintf(stderr, "[index] Building indexes for %s\n", db_dir.c_str());

    build_sub_adsh_hash(db_dir);
    build_tag_pair_hash(db_dir);
    build_pre_triple_hash(db_dir);

    verify(db_dir);

    fprintf(stderr, "[index] ALL indexes built in %.1fs\n", now_sec()-t_start);
    return 0;
}
