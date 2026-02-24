#include <cstdio>
#include <cstdint>
#include <cmath>
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
#include <omp.h>
#include <climits>
#include <cstring>
#include "timing_utils.h"

// ===== Lazy dict view =====
// Keeps mmap alive; operator[] returns std::string_view into the raw buffer.
// Avoids 200K+ heap string allocations vs std::vector<std::string> approach.
struct DictView {
    const char*           buf  = nullptr;
    size_t                sz   = 0;
    std::vector<const char*> ptrs;
    std::vector<uint32_t>    lens;

    void load(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return; }
        struct stat st; fstat(fd, &st);
        sz  = (size_t)st.st_size;
        if (sz == 0) { close(fd); return; }
        buf = (const char*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (buf == MAP_FAILED) { buf = nullptr; sz = 0; return; }
        const char* p   = buf;
        const char* end = buf + sz;
        while (p < end) {
            const char* nl  = (const char*)memchr(p, '\n', end - p);
            size_t      len = nl ? (size_t)(nl - p) : (size_t)(end - p);
            if (len > 0 && p[len - 1] == '\r') len--;
            ptrs.push_back(p);
            lens.push_back((uint32_t)len);
            if (!nl) break;
            p = nl + 1;
        }
    }

    std::string_view operator[](int32_t code) const {
        return { ptrs[(size_t)code], lens[(size_t)code] };
    }

    size_t size() const { return ptrs.size(); }

    ~DictView() { if (buf) munmap((void*)buf, sz); }
};

// ===== Hash function =====
static inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
}

// ===== Data structures =====
struct SubADSHSlot {
    int32_t adsh_code;  // INT32_MIN = empty slot
    int32_t row_id;
    int32_t _pad0;
    int32_t _pad1;
};  // 16 bytes

// Tuple collected during pass B (gather phase)
struct TupleEntry {
    int32_t adsh_code;
    int32_t tag_code;
    double  value;
};  // 16 bytes

struct Candidate {
    double   value;
    int32_t  adsh_code;
    int32_t  tag_code;
    int32_t  name_code;
};

// ===== Utility =====
static void* mmap_file(const std::string& path, size_t& size_out) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); size_out = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    size_out = (size_t)st.st_size;
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return p;
}

// Fast dict loader for small dicts only (uom_dict: ~15 entries)
static std::vector<std::string> load_dict_fast(const std::string& path) {
    std::vector<std::string> dict;
    size_t sz = 0;
    const char* buf = (const char*)mmap_file(path, sz);
    if (!buf || sz == 0) return dict;
    const char* p   = buf;
    const char* end = buf + sz;
    while (p < end) {
        const char* nl  = (const char*)memchr(p, '\n', end - p);
        size_t      len = nl ? (size_t)(nl - p) : (size_t)(end - p);
        if (len > 0 && p[len - 1] == '\r') len--;
        dict.emplace_back(p, len);
        if (!nl) break;
        p = nl + 1;
    }
    munmap((void*)buf, sz);
    return dict;
}

// ===== Main query =====
void run_q2(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ===== DATA LOADING =====
    // DictView avoids 286K heap string allocations (tag_dict 200K + name_dict 86K).
    // Keeps mmap alive; returns std::string_view on operator[] — zero-copy dict access.
    DictView tag_dict, name_dict;
    std::vector<std::string> uom_dict;   // Only 15 entries — vector<string> is fine
    int16_t pure_code = -1;
    size_t  num_rows  = 0;

    size_t num_uom_sz, num_value_sz, num_adsh_sz, num_tag_sz;
    size_t sub_fy_sz, sub_name_sz, sub_adsh_idx_sz;

    const int16_t* num_uom          = nullptr;
    const double*  num_value        = nullptr;
    const int32_t* num_adsh         = nullptr;
    const int32_t* num_tag          = nullptr;
    const int32_t* sub_fy           = nullptr;
    const int32_t* sub_name         = nullptr;
    const char*    sub_adsh_idx_raw = nullptr;

    {
        GENDB_PHASE("data_loading");

        // C2: resolve dict codes at runtime — never hardcode
        uom_dict = load_dict_fast(gendb_dir + "/num/uom_dict.txt");
        for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
            if (uom_dict[i] == "pure") { pure_code = i; break; }

        // DictView: scan for newlines only (no string allocation for large dicts)
        tag_dict.load(gendb_dir + "/shared/tag_dict.txt");
        name_dict.load(gendb_dir + "/sub/name_dict.txt");

        // mmap binary num columns
        num_uom   = (const int16_t*)mmap_file(gendb_dir + "/num/uom.bin",   num_uom_sz);
        num_value = (const double*) mmap_file(gendb_dir + "/num/value.bin",  num_value_sz);
        num_adsh  = (const int32_t*)mmap_file(gendb_dir + "/num/adsh.bin",   num_adsh_sz);
        num_tag   = (const int32_t*)mmap_file(gendb_dir + "/num/tag.bin",    num_tag_sz);
        // mmap sub columns (small)
        sub_fy    = (const int32_t*)mmap_file(gendb_dir + "/sub/fy.bin",     sub_fy_sz);
        sub_name  = (const int32_t*)mmap_file(gendb_dir + "/sub/name.bin",   sub_name_sz);
        sub_adsh_idx_raw = (const char*)mmap_file(
                               gendb_dir + "/sub/indexes/sub_adsh_hash.bin", sub_adsh_idx_sz);

        num_rows = num_uom_sz / sizeof(int16_t);

        // Two-pass strategy: pass A scans uom sequentially, pass B gathers randomly.
        // Use MADV_SEQUENTIAL only for uom; MADV_RANDOM for value/adsh/tag
        // to suppress kernel readahead on pages we'll touch sparsely.
        madvise((void*)num_uom,   num_uom_sz,   MADV_SEQUENTIAL);
        madvise((void*)num_value, num_value_sz, MADV_RANDOM);
        madvise((void*)num_adsh,  num_adsh_sz,  MADV_RANDOM);
        madvise((void*)num_tag,   num_tag_sz,   MADV_RANDOM);
        // Sub columns: small — eagerly bring into cache
        madvise((void*)sub_fy,           sub_fy_sz,       MADV_WILLNEED);
        madvise((void*)sub_name,         sub_name_sz,     MADV_WILLNEED);
        madvise((void*)sub_adsh_idx_raw, sub_adsh_idx_sz, MADV_WILLNEED);
    }

    // Parse sub_adsh_hash index header at function scope (C32)
    uint32_t sub_cap  = *(const uint32_t*)sub_adsh_idx_raw;
    uint32_t sub_mask = sub_cap - 1;
    const SubADSHSlot* sub_ht = (const SubADSHSlot*)(sub_adsh_idx_raw + 4);

    int nthreads = omp_get_max_threads();

    // Per-thread storage for pass A (row indices) and pass B (qualifying tuples)
    std::vector<std::vector<uint32_t>>   thread_indices(nthreads);
    std::vector<std::vector<TupleEntry>> thread_tuples(nthreads);
    for (int t = 0; t < nthreads; t++) {
        thread_indices[t].reserve(2000);  // ~1478 expected per thread
        thread_tuples[t].reserve(2000);
    }

    // ===== TWO-PASS SCAN =====
    // Pass A: scan ONLY uom column (78MB vs 710MB = 9× bandwidth reduction).
    //         Collect row indices where uom == pure_code.
    // Pass B: parallel gather of value/adsh/tag for ~94K qualifying indices.
    //         SW prefetch hides DRAM latency: PFDIST=16 @ ~20ns/iter > 100ns DRAM.
    //         Total data touched in pass B: ~18MB vs 632MB sequential in current code.
    {
        GENDB_PHASE("build_joins");

        // Pass A: parallel scan of uom only
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_indices[tid];
            #pragma omp for schedule(static, 4096) nowait
            for (size_t i = 0; i < num_rows; i++)
                if (num_uom[i] == pure_code) local.push_back((uint32_t)i);
        }

        // Pass B: parallel gather with software prefetch to hide DRAM latency
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            const auto& indices = thread_indices[tid];
            auto& local = thread_tuples[tid];
            const int    PFDIST = 16;
            const size_t n_idx  = indices.size();
            for (size_t i = 0; i < n_idx; i++) {
                if (i + PFDIST < n_idx) {
                    uint32_t pf = indices[i + PFDIST];
                    __builtin_prefetch(num_value + pf, 0, 0);
                    __builtin_prefetch(num_adsh  + pf, 0, 0);
                    __builtin_prefetch(num_tag   + pf, 0, 0);
                }
                uint32_t idx = indices[i];
                double val = num_value[idx];
                if (std::isnan(val)) continue;
                local.push_back({num_adsh[idx], num_tag[idx], val});
            }
        }
    }

    // ===== AGGREGATION_MERGE: concatenate + sort (replaces hash map) =====
    // Sort-based aggregation on ~94K tuples:
    //   std::sort O(N log N) on small N + O(N) linear scan = ~2ms vs 19ms hash approach.
    //   Eliminates the 4MB global_map entirely — no initialization or hash probing cost.
    std::vector<TupleEntry> all_tuples;
    {
        GENDB_PHASE("aggregation_merge");

        // Concatenate thread-local tuples into flat array
        size_t total = 0;
        for (int t = 0; t < nthreads; t++) total += thread_tuples[t].size();
        all_tuples.reserve(total);
        for (int t = 0; t < nthreads; t++) {
            for (const auto& e : thread_tuples[t]) all_tuples.push_back(e);
            std::vector<TupleEntry>().swap(thread_tuples[t]);
        }
        // Free thread_indices memory
        for (int t = 0; t < nthreads; t++) std::vector<uint32_t>().swap(thread_indices[t]);

        // Sort by (adsh_code, tag_code) using packed uint64 key — single comparison
        // ~94K × 16B = 1.5MB → fits in L3, sort runs in ~1-2ms
        std::sort(all_tuples.begin(), all_tuples.end(),
                  [](const TupleEntry& a, const TupleEntry& b) {
                      uint64_t ka = ((uint64_t)(uint32_t)a.adsh_code << 32) | (uint32_t)a.tag_code;
                      uint64_t kb = ((uint64_t)(uint32_t)b.adsh_code << 32) | (uint32_t)b.tag_code;
                      return ka < kb;
                  });
    }

    // ===== MAIN_SCAN: linear group scan over sorted tuples =====
    // For each (adsh, tag) group: find max value in O(group_size) pass,
    // then probe sub_adsh_hash once per ADSH (cached across tags of same adsh).
    // Total: O(N) linear, no hash table, no random access into large maps.
    std::vector<Candidate> all_candidates;
    {
        GENDB_PHASE("main_scan");

        const size_t n = all_tuples.size();
        size_t i = 0;

        // Cache sub probe result per adsh_code:
        // sorted by (adsh, tag) → consecutive groups share same adsh → cache is hot
        int32_t cached_adsh      = INT32_MIN;
        bool    cached_fy2022    = false;
        int32_t cached_name_code = -1;

        while (i < n) {
            int32_t cur_adsh = all_tuples[i].adsh_code;
            int32_t cur_tag  = all_tuples[i].tag_code;

            // Scan group to find max value
            double mx = all_tuples[i].value;
            size_t j  = i + 1;
            while (j < n &&
                   all_tuples[j].adsh_code == cur_adsh &&
                   all_tuples[j].tag_code  == cur_tag) {
                if (all_tuples[j].value > mx) mx = all_tuples[j].value;
                j++;
            }

            // Probe sub_adsh_hash; cached per adsh_code across different tags (C24: bounded)
            if (cur_adsh != cached_adsh) {
                cached_adsh      = cur_adsh;
                cached_fy2022    = false;
                cached_name_code = -1;
                uint32_t pos = (uint32_t)(hash_int32(cur_adsh) & sub_mask);
                for (uint32_t probe = 0; probe < sub_cap; probe++) {  // C24: bounded
                    uint32_t slot = (pos + probe) & sub_mask;
                    if (sub_ht[slot].adsh_code == INT32_MIN) break;   // not found
                    if (sub_ht[slot].adsh_code == cur_adsh) {
                        int32_t sub_row = sub_ht[slot].row_id;
                        if (sub_fy[sub_row] == 2022) {
                            cached_fy2022    = true;
                            cached_name_code = sub_name[sub_row];
                        }
                        break;
                    }
                }
            }

            // Emit candidates: value == group max AND sub.fy == 2022
            // C29: exact double equality is safe for MAX (not SUM)
            if (cached_fy2022) {
                for (size_t k = i; k < j; k++) {
                    if (all_tuples[k].value == mx) {
                        all_candidates.push_back({mx, cur_adsh, cur_tag, cached_name_code});
                    }
                }
            }

            i = j;
        }
    }

    // ===== TOP-100: partial_sort by (value DESC, name ASC, tag ASC) =====
    // C33: tiebreakers on name + tag ensure deterministic output
    {
        GENDB_PHASE("sort_topk");
        size_t k = std::min((size_t)100, all_candidates.size());
        std::partial_sort(all_candidates.begin(),
                          all_candidates.begin() + k,
                          all_candidates.end(),
                          [&](const Candidate& a, const Candidate& b) {
                              if (a.value != b.value) return a.value > b.value;
                              auto na = name_dict[a.name_code];  // string_view, no alloc
                              auto nb = name_dict[b.name_code];
                              if (na != nb) return na < nb;
                              return tag_dict[a.tag_code] < tag_dict[b.tag_code];
                          });
        all_candidates.resize(k);
    }

    // ===== OUTPUT =====
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q2.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return; }

        fprintf(f, "name,tag,value\n");
        for (const auto& c : all_candidates) {
            auto name_sv = name_dict[c.name_code];  // string_view into mmap'd buffer
            auto tag_sv  = tag_dict[c.tag_code];
            // C31: always double-quote string output columns
            // %.*s: safe output for non-null-terminated string_view data
            fprintf(f, "\"%.*s\",\"%.*s\",%.10g\n",
                    (int)name_sv.size(), name_sv.data(),
                    (int)tag_sv.size(),  tag_sv.data(),
                    c.value);
        }
        fclose(f);
    }
    // Note: DictView destructors munmap dict files at function exit.
    // Binary data files are left for OS reclaim (single-run executor pattern).
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q2(gendb_dir, results_dir);
    return 0;
}
#endif
