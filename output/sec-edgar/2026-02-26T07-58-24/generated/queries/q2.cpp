// Q2: SEC-EDGAR query — decorrelated MAX subquery
// SELECT s.name, n.tag, n.value
// FROM num n JOIN sub s ON n.adsh = s.adsh
// JOIN (SELECT adsh, tag, MAX(value) FROM num WHERE uom='pure' AND value IS NOT NULL GROUP BY adsh,tag) m
//   ON n.adsh=m.adsh AND n.tag=m.tag AND n.value=m.max_value
// WHERE n.uom='pure' AND s.fy=2022 AND n.value IS NOT NULL
// ORDER BY n.value DESC, s.name, n.tag LIMIT 100
//
// Iter 1 optimizations:
//   1. mmap tag_numpre.dict + name.dict with offset/length arrays (no per-string heap alloc)
//   2. Single-threaded Phase 1 + Phase 2 (only ~2 qualifying blocks; OMP overhead > benefit)
//   3. malloc+memset for hash map (avoids mmap(MAP_ANONYMOUS) page-fault overhead)

#include <cstdio>
#include <cstdint>
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <string>
#include <string_view>
#include <algorithm>
#include <iostream>
#include <limits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include "timing_utils.h"

namespace {

// ===== Hash function for (adsh_code, tag_code) pair =====
static inline uint64_t hash_int32x2(int32_t a, int32_t b) {
    uint64_t ha = (uint64_t)(uint32_t)a * 0x9E3779B97F4A7C15ULL;
    uint64_t hb = (uint64_t)(uint32_t)b * 0x9E3779B97F4A7C15ULL;
    return ha ^ (hb * 0x517CC1B727220A95ULL + 0x6C62272E07BB0142ULL + (ha << 6) + (ha >> 2));
}

static inline uint64_t pack_key(int32_t adsh, int32_t tag) {
    return ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tag;
}

// ===== Open-addressing hash map: {adsh_code,tag_code} -> max_value =====
// Empty sentinel: key == UINT64_MAX
struct MaxSlot {
    uint64_t key;   // packed adsh<<32|tag, UINT64_MAX = empty
    double   value; // max value
};

static inline void max_map_update(MaxSlot* __restrict__ ht, uint32_t mask,
                                   int32_t adsh, int32_t tag, double val) {
    uint64_t key = pack_key(adsh, tag);
    uint32_t pos = (uint32_t)(hash_int32x2(adsh, tag) & mask);
    while (true) {
        if (__builtin_expect(ht[pos].key == UINT64_MAX, 0)) {
            ht[pos].key   = key;
            ht[pos].value = val;
            return;
        }
        if (ht[pos].key == key) {
            if (val > ht[pos].value) ht[pos].value = val;
            return;
        }
        pos = (pos + 1) & mask;
    }
}

static inline double max_map_lookup(const MaxSlot* __restrict__ ht, uint32_t mask,
                                     int32_t adsh, int32_t tag) {
    uint64_t key = pack_key(adsh, tag);
    uint32_t pos = (uint32_t)(hash_int32x2(adsh, tag) & mask);
    while (true) {
        if (__builtin_expect(ht[pos].key == UINT64_MAX, 0))
            return std::numeric_limits<double>::quiet_NaN();
        if (ht[pos].key == key) return ht[pos].value;
        pos = (pos + 1) & mask;
    }
}

// ===== Utility: mmap file =====
static void* mmap_file(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); size_out = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    size_out = (size_t)st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); size_out = 0; return nullptr; }
    return p;
}

// ===== Mmap-backed dict: [n:uint32][len:uint16, bytes...]*n =====
// Builds offset/length index arrays pointing into the mmap buffer.
// Zero heap allocation per string entry.
struct MmapDict {
    void*     buf      = nullptr;
    size_t    buf_size = 0;
    uint32_t  n        = 0;
    uint32_t* offsets  = nullptr;  // byte offset of string i's data in buf
    uint16_t* lengths  = nullptr;  // byte length of string i

    bool load(const std::string& path) {
        buf = mmap_file(path, buf_size);
        if (!buf) return false;
        n = *(const uint32_t*)buf;
        offsets = (uint32_t*)malloc((n + 1) * sizeof(uint32_t));
        lengths = (uint16_t*)malloc(n       * sizeof(uint16_t));
        if (!offsets || !lengths) return false;
        uint32_t cur_off = 4; // skip the uint32 count header
        for (uint32_t i = 0; i < n; i++) {
            uint16_t len = *(const uint16_t*)((const char*)buf + cur_off);
            cur_off += 2;
            offsets[i] = cur_off;
            lengths[i] = len;
            cur_off += len;
        }
        offsets[n] = cur_off;
        return true;
    }

    std::string_view get(uint32_t i) const {
        return std::string_view((const char*)buf + offsets[i], lengths[i]);
    }

    ~MmapDict() {
        free(offsets);
        free(lengths);
        if (buf) munmap(buf, buf_size);
    }
};

// ===== Load tiny binary dict into std::vector<std::string> (uom only, 201 entries) =====
static std::vector<std::string> load_binary_dict_small(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { perror(path.c_str()); return dict; }
    uint32_t n = 0;
    if (fread(&n, 4, 1, f) != 1) { fclose(f); return dict; }
    dict.reserve(n);
    for (uint32_t i = 0; i < n; i++) {
        uint16_t len = 0;
        if (fread(&len, 2, 1, f) != 1) break;
        std::string s(len, '\0');
        if (len > 0) fread(&s[0], 1, len, f);
        dict.push_back(std::move(s));
    }
    fclose(f);
    return dict;
}

// ===== Candidate for top-100 output =====
struct Candidate {
    double  value;
    int32_t adsh_code;
    int32_t tag_code;
    int32_t name_code;
};

// ===== Zone map entry (10 bytes each, packed) =====
#pragma pack(push, 1)
struct ZoneEntry {
    int8_t  uom_min;
    int8_t  uom_max;
    int32_t ddate_min;
    int32_t ddate_max;
};
#pragma pack(pop)

// ===== Main query =====
} // end anonymous namespace

void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ===== DATA LOADING =====
    size_t num_uom_sz = 0, num_value_sz = 0, num_adsh_sz = 0, num_tag_sz = 0;
    size_t sub_fy_sz = 0, sub_name_sz = 0, zonemaps_sz = 0;

    const int8_t*  num_uom      = nullptr;
    const double*  num_value    = nullptr;
    const int32_t* num_adsh     = nullptr;
    const int32_t* num_tag      = nullptr;
    const int16_t* sub_fy       = nullptr;
    const int32_t* sub_name_arr = nullptr;
    const void*    zonemaps_raw = nullptr;

    MmapDict tag_dict, name_dict;
    int8_t pure_code = -1;
    size_t num_rows  = 0;

    std::vector<int> qualifying_blocks;

    {
        GENDB_PHASE("data_loading");

        // Load tiny UOM dict (201 entries) to resolve pure_code
        {
            auto uom_dict = load_binary_dict_small(gendb_dir + "/shared/uom.dict");
            for (int i = 0; i < (int)uom_dict.size(); i++) {
                if (uom_dict[i] == "pure") { pure_code = (int8_t)i; break; }
            }
        }
        if (pure_code < 0) {
            std::cerr << "ERROR: 'pure' not found in uom.dict\n";
            return;
        }

        // mmap tag_numpre.dict — zero per-string heap allocation
        if (!tag_dict.load(gendb_dir + "/shared/tag_numpre.dict")) {
            std::cerr << "ERROR: failed to load tag_numpre.dict\n";
            return;
        }

        // mmap name.dict — zero per-string heap allocation
        if (!name_dict.load(gendb_dir + "/sub/name.dict")) {
            std::cerr << "ERROR: failed to load name.dict\n";
            return;
        }

        // mmap num columns
        num_uom   = (const int8_t*) mmap_file(gendb_dir + "/num/uom.bin",   num_uom_sz);
        num_value = (const double*)  mmap_file(gendb_dir + "/num/value.bin",  num_value_sz);
        num_adsh  = (const int32_t*) mmap_file(gendb_dir + "/num/adsh.bin",   num_adsh_sz);
        num_tag   = (const int32_t*) mmap_file(gendb_dir + "/num/tag.bin",    num_tag_sz);

        // mmap sub columns (small, fits in RAM easily)
        sub_fy      = (const int16_t*) mmap_file(gendb_dir + "/sub/fy.bin",   sub_fy_sz);
        sub_name_arr = (const int32_t*) mmap_file(gendb_dir + "/sub/name.bin", sub_name_sz);

        num_rows = num_uom_sz / sizeof(int8_t);

        // Prefetch sub columns immediately (small, frequently accessed)
        if (sub_fy)       madvise((void*)sub_fy,       sub_fy_sz,   MADV_WILLNEED);
        if (sub_name_arr) madvise((void*)sub_name_arr,  sub_name_sz, MADV_WILLNEED);

        // mmap zone maps
        zonemaps_raw = mmap_file(gendb_dir + "/indexes/num_zonemaps.bin", zonemaps_sz);

        // ===== Zone map filtering: skip blocks where pure_code is out of [uom_min, uom_max] =====
        if (zonemaps_raw) {
            int32_t n_blocks = *(const int32_t*)zonemaps_raw;
            const ZoneEntry* zones = (const ZoneEntry*)((const char*)zonemaps_raw + 4);
            for (int b = 0; b < n_blocks; b++) {
                if (pure_code < zones[b].uom_min || pure_code > zones[b].uom_max) continue;
                qualifying_blocks.push_back(b);
            }
        } else {
            int32_t n_blocks = (int32_t)((num_rows + 99999) / 100000);
            for (int b = 0; b < n_blocks; b++) qualifying_blocks.push_back(b);
        }

        // Advise sequential read on qualifying blocks
        for (int b : qualifying_blocks) {
            size_t off   = (size_t)b * 100000;
            size_t count = std::min((size_t)100000, num_rows - off);
            madvise((void*)(num_uom   + off), count * sizeof(int8_t),  MADV_SEQUENTIAL);
            madvise((void*)(num_value + off), count * sizeof(double),  MADV_SEQUENTIAL);
            madvise((void*)(num_adsh  + off), count * sizeof(int32_t), MADV_SEQUENTIAL);
            madvise((void*)(num_tag   + off), count * sizeof(int32_t), MADV_SEQUENTIAL);
        }
    }

    // ===== Allocate single global max_map via malloc+memset =====
    // Capacity = next_pow2(118000 * 2) = 262144
    // malloc+memset avoids mmap(MAP_ANONYMOUS) page-fault overhead
    static const uint32_t MAX_CAP  = 262144;
    static const uint32_t MAX_MASK = MAX_CAP - 1;

    MaxSlot* global_map = (MaxSlot*)malloc(MAX_CAP * sizeof(MaxSlot));
    if (!global_map) {
        std::cerr << "ERROR: malloc failed for global_map\n";
        return;
    }
    // memset 0xFF: sets key fields to UINT64_MAX (correct empty sentinel)
    memset(global_map, 0xFF, MAX_CAP * sizeof(MaxSlot));
    // Fix value fields to -infinity for correct MAX semantics
    for (uint32_t i = 0; i < MAX_CAP; i++) {
        global_map[i].value = -std::numeric_limits<double>::infinity();
    }

    // ===== PHASE 1: Single-threaded build of global max_map =====
    // With only ~2 qualifying blocks (~118k pure rows), single-thread is optimal.
    // OMP overhead (thread wakeup + merge) costs 12ms while saving <2ms in scan time.
    {
        GENDB_PHASE("build_joins");

        for (int b : qualifying_blocks) {
            size_t row_start = (size_t)b * 100000;
            size_t row_end   = std::min(row_start + 100000, num_rows);

            for (size_t i = row_start; i < row_end; i++) {
                if (num_uom[i] != pure_code) continue;
                double val = num_value[i];
                if (std::isnan(val)) continue;
                max_map_update(global_map, MAX_MASK, num_adsh[i], num_tag[i], val);
            }
        }
    }

    // dim_filter phase (fy check is inlined into main_scan below)
    { GENDB_PHASE("dim_filter"); }

    // ===== PHASE 2: Single-threaded probe max_map + sub_fy filter =====
    std::vector<Candidate> candidates;
    candidates.reserve(512);

    {
        GENDB_PHASE("main_scan");

        for (int b : qualifying_blocks) {
            size_t row_start = (size_t)b * 100000;
            size_t row_end   = std::min(row_start + 100000, num_rows);

            for (size_t i = row_start; i < row_end; i++) {
                if (num_uom[i] != pure_code) continue;
                double val = num_value[i];
                if (std::isnan(val)) continue;

                int32_t adsh = num_adsh[i];
                int32_t tag  = num_tag[i];

                // Probe max_map: keep only rows where value == max_value for (adsh, tag)
                double max_val = max_map_lookup(global_map, MAX_MASK, adsh, tag);
                if (val != max_val) continue;

                // Direct array lookup: sub_fy[adsh_code] == 2022 (adsh_code == sub row_id)
                if (sub_fy[adsh] != (int16_t)2022) continue;

                // Row passes all filters
                candidates.push_back({val, adsh, tag, sub_name_arr[adsh]});
            }
        }
    }

    // ===== TOP-100: partial_sort by (value DESC, name ASC, tag ASC) =====
    {
        GENDB_PHASE("output");

        size_t k = std::min((size_t)100, candidates.size());
        std::partial_sort(candidates.begin(),
                          candidates.begin() + k,
                          candidates.end(),
                          [&](const Candidate& a, const Candidate& b) {
                              if (a.value != b.value) return a.value > b.value;
                              std::string_view na = name_dict.get(a.name_code);
                              std::string_view nb = name_dict.get(b.name_code);
                              if (na != nb) return na < nb;
                              std::string_view ta = tag_dict.get(a.tag_code);
                              std::string_view tb = tag_dict.get(b.tag_code);
                              return ta < tb;
                          });
        candidates.resize(k);

        // Write output CSV
        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "name,tag,value\n");

        auto needs_quote = [](std::string_view sv) {
            for (char ch : sv) if (ch == ',' || ch == '"' || ch == '\n') return true;
            return false;
        };

        for (const auto& c : candidates) {
            std::string_view name = name_dict.get(c.name_code);
            std::string_view tag  = tag_dict.get(c.tag_code);

            if (needs_quote(name)) {
                fputc('"', f);
                for (char ch : name) { if (ch == '"') fputc('"', f); fputc(ch, f); }
                fputc('"', f);
            } else {
                fwrite(name.data(), 1, name.size(), f);
            }
            fputc(',', f);
            if (needs_quote(tag)) {
                fputc('"', f);
                for (char ch : tag) { if (ch == '"') fputc('"', f); fputc(ch, f); }
                fputc('"', f);
            } else {
                fwrite(tag.data(), 1, tag.size(), f);
            }
            fprintf(f, ",%.2f\n", c.value);
        }
        fclose(f);
    }

    // Cleanup
    free(global_map);
    if (num_uom)      munmap((void*)num_uom,       num_uom_sz);
    if (num_value)    munmap((void*)num_value,      num_value_sz);
    if (num_adsh)     munmap((void*)num_adsh,       num_adsh_sz);
    if (num_tag)      munmap((void*)num_tag,        num_tag_sz);
    if (sub_fy)       munmap((void*)sub_fy,         sub_fy_sz);
    if (sub_name_arr) munmap((void*)sub_name_arr,   sub_name_sz);
    if (zonemaps_raw) munmap((void*)zonemaps_raw,   zonemaps_sz);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> <results_dir>\n";
        return 1;
    }
    run_q2(argv[1], argv[2]);
    return 0;
}
#endif
