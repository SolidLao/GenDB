// Q4 iter_3: FlatAggMap (SoA, no heap allocation per group) + bloom filter eliminated
//
// Root cause of iter_2 regression (84ms main_scan, 42ms merge):
//   unordered_map<uint64_t,AggState> with AggState embedding vector<uint64_t> cik_bits:
//   (1) unordered_map node heap-allocated + linked-list chaining on every group insert
//   (2) cik_bits.assign(cw, 0) = heap alloc ~432B per new group → ~100 allocations/thread
//   -> every subsequent cik_data access is a random cache miss.
//
// Fix: FlatAggMap -- SoA open-addressing hash map.
//   keys[cap] / sums[cap] / cnts[cap] / cik_data[cap*cik_words] -- all pre-allocated, zeroed once.
//   No per-group heap allocation ever. cik_data[slot*cw+word] has no pointer indirection.
//   cap=1024 per worker (466KB/thread, L3-resident); cap=8192 for thread_maps[0] (3.73MB).
//
// Bloom filter eliminated: 42K entries in 65536-slot FlatCountMap -> L2/L3-resident.
//   Direct probe FPR ~ 10^{-7}. Bloom would add 7 hash computations per qualifying row
//   with ~0% FPR benefit -- pure overhead.
//
// Expected: main_scan ~35ms (vs 84ms), merge ~8ms (vs 42ms), total ~65ms.

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include "timing_utils.h"
#include "mmap_utils.h"

using namespace std;
using namespace gendb;

// ---------------------------------------------------------------------------
// FlatCountMap: open-addressing hash map uint64_t->uint32_t count
// cap_hint=25000 -> 65536 slots x 12B = 786KB (L2/L3-resident)
// ---------------------------------------------------------------------------
struct FlatCountMap {
    struct Entry { uint64_t key; uint32_t val; };
    vector<Entry> slots;
    size_t mask_ = 0;
    static constexpr uint64_t EMPTY = UINT64_MAX;

    explicit FlatCountMap(size_t cap_hint) {
        size_t cap = 1;
        while (cap < cap_hint * 2) cap <<= 1;
        slots.assign(cap, {EMPTY, 0});
        mask_ = cap - 1;
    }

    static inline uint64_t mix(uint64_t k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        return k;
    }

    inline void increment(uint64_t key) {
        size_t idx = mix(key) & mask_;
        while (slots[idx].key != EMPTY && slots[idx].key != key)
            idx = (idx + 1) & mask_;
        if (slots[idx].key == EMPTY) slots[idx].key = key;
        slots[idx].val++;
    }

    inline uint32_t get(uint64_t key) const {
        size_t idx = mix(key) & mask_;
        while (slots[idx].key != EMPTY && slots[idx].key != key)
            idx = (idx + 1) & mask_;
        return slots[idx].key == key ? slots[idx].val : 0;
    }
};

// ---------------------------------------------------------------------------
// FlatAggMap: SoA open-addressing hash map for aggregation
//   Keys: uint64_t packed group key = ((uint64_t)(uint16_t)sic << 32) | tagver_code
//   Sentinel: UINT64_MAX (EMPTY)
//   Fields per slot: sum(double) + cnt(int64_t) + cik_data[cw](uint64_t bitset)
//
//   All arrays pre-allocated + zeroed at construction -- no per-group allocation ever.
//   cik_data access: cik_data[slot * cw + word] -- single array, no pointer indirection.
// ---------------------------------------------------------------------------
struct FlatAggMap {
    static constexpr uint64_t EMPTY = UINT64_MAX;

    size_t   cap  = 0;
    size_t   mask = 0;
    size_t   cw   = 0;  // cik_words

    vector<uint64_t> keys;      // cap entries, init to EMPTY
    vector<double>   sums;      // cap entries, init to 0.0
    vector<int64_t>  cnts;      // cap entries, init to 0
    vector<uint64_t> cik_data;  // cap * cw entries, init to 0 (flat bitset)

    FlatAggMap() = default;

    void init(size_t capacity, size_t cik_words) {
        // capacity must be a power of 2; cik_words = ceil(N_ciks/64)
        cap  = capacity;
        mask = cap - 1;
        cw   = cik_words;
        keys.assign(cap, EMPTY);
        sums.assign(cap, 0.0);
        cnts.assign(cap, 0LL);
        cik_data.assign(cap * cw, 0ULL);
    }

    static inline uint64_t mix(uint64_t k) {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        return k;
    }

    // Returns slot index; inserts EMPTY->key if not found.
    inline size_t find_or_insert(uint64_t key) {
        size_t idx = mix(key) & mask;
        while (keys[idx] != EMPTY && keys[idx] != key)
            idx = (idx + 1) & mask;
        if (keys[idx] == EMPTY) keys[idx] = key;
        return idx;
    }

    // Accumulate one num row into this map.
    inline void accumulate(uint64_t key, double val_times_cnt, int64_t eq_cnt, uint16_t ci) {
        size_t slot = find_or_insert(key);
        sums[slot] += val_times_cnt;
        cnts[slot] += eq_cnt;
        cik_data[slot * cw + (ci >> 6)] |= 1ULL << (ci & 63);
    }

    // Merge src into this map (this is the dst).
    // For each occupied src slot: find/insert matching dst slot; add sums/cnts; OR cik_data.
    void merge_from(const FlatAggMap& src) {
        const size_t src_cap = src.cap;
        const size_t src_cw  = src.cw;  // must equal this->cw
        const uint64_t* s_keys  = src.keys.data();
        const double*   s_sums  = src.sums.data();
        const int64_t*  s_cnts  = src.cnts.data();
        const uint64_t* s_cdata = src.cik_data.data();
        uint64_t* d_cdata = cik_data.data();
        for (size_t s = 0; s < src_cap; s++) {
            if (s_keys[s] == EMPTY) continue;
            size_t d = find_or_insert(s_keys[s]);
            sums[d] += s_sums[s];
            cnts[d] += s_cnts[s];
            const uint64_t* sc = s_cdata + s * src_cw;
            uint64_t*       dc = d_cdata + d * cw;
            for (size_t j = 0; j < cw; j++)
                dc[j] |= sc[j];
        }
    }
};

// ---------------------------------------------------------------------------
// Load code dict: uint8_t N; N x { int8_t code, uint8_t slen, char[slen] }
// ---------------------------------------------------------------------------
static int8_t load_dict_code(const string& path, const char* key_str) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    uint8_t N;
    fread(&N, 1, 1, f);
    for (int i = 0; i < (int)N; i++) {
        int8_t code; uint8_t slen;
        fread(&code, 1, 1, f);
        fread(&slen, 1, 1, f);
        char buf[256] = {};
        fread(buf, 1, slen, f);
        buf[slen] = '\0';
        if (strcmp(buf, key_str) == 0) { fclose(f); return code; }
    }
    fclose(f);
    fprintf(stderr, "Key '%s' not found in %s\n", key_str, path.c_str());
    exit(1);
}

// ---------------------------------------------------------------------------
// Zone map: {int8 min_uom, int8 max_uom, [2-byte pad], int32 min_ddate, int32 max_ddate}
// ---------------------------------------------------------------------------
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap must be 12 bytes");

// ---------------------------------------------------------------------------
// Write CSV field (quote if needed)
// ---------------------------------------------------------------------------
static void write_csv_field(FILE* f, const string& s) {
    bool need_quote = s.find(',') != string::npos || s.find('"') != string::npos
                   || s.find('\n') != string::npos;
    if (!need_quote) { fwrite(s.data(), 1, s.size(), f); return; }
    fputc('"', f);
    for (char c : s) { if (c == '"') fputc('"', f); fputc(c, f); }
    fputc('"', f);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    GENDB_PHASE("total");

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const string gendb_dir   = argv[1];
    const string results_dir = argv[2];

    int num_threads = (int)thread::hardware_concurrency();
    if (num_threads < 1) num_threads = 1;
    if (num_threads > 64) num_threads = 64;

    // -----------------------------------------------------------------------
    // Phase: data_loading -- load code dicts
    // -----------------------------------------------------------------------
    int8_t usd_code, eq_code;
    {
        GENDB_PHASE("data_loading");
        usd_code = load_dict_code(gendb_dir + "/indexes/uom_codes.bin",  "USD");
        eq_code  = load_dict_code(gendb_dir + "/indexes/stmt_codes.bin", "EQ");
    }

    static const size_t SUB_N = 86135;
    static const size_t TAG_N = 1070662;

    // -----------------------------------------------------------------------
    // Phase: dim_filter -- load sub/tag arrays, build CIK qualifying structures
    //
    // adsh_qualifying_bits (~10.8KB, L1-resident): bit i set iff sic[i] BETWEEN 4000-4999
    // idx_to_cik / adsh_to_cik_idx: compact CIK remap (N_ciks <= 3445)
    // cik_words = ceil(N_ciks / 64) ~= 54
    // -----------------------------------------------------------------------
    MmapColumn<int16_t>  sub_sic       (gendb_dir + "/sub/sic.bin");
    MmapColumn<int32_t>  sub_cik       (gendb_dir + "/sub/cik.bin");
    MmapColumn<int8_t>   tag_abstract  (gendb_dir + "/tag/abstract.bin");
    MmapColumn<uint32_t> tlabel_offsets(gendb_dir + "/tag/tlabel_offsets.bin");

    // mmap tlabel_data
    int tlabel_fd = open((gendb_dir + "/tag/tlabel_data.bin").c_str(), O_RDONLY);
    struct stat tlabel_st;
    fstat(tlabel_fd, &tlabel_st);
    const char* tlabel_data = (const char*)mmap(nullptr, tlabel_st.st_size,
                                                  PROT_READ, MAP_PRIVATE, tlabel_fd, 0);
    ::close(tlabel_fd);

    static const size_t BITSET_WORDS = (SUB_N + 63) / 64; // 1346 words
    vector<uint64_t> adsh_qualifying_bits(BITSET_WORDS, 0ULL);
    vector<uint16_t> adsh_to_cik_idx(SUB_N, UINT16_MAX);
    vector<int32_t>  idx_to_cik;
    idx_to_cik.reserve(4096);
    size_t cik_words = 0;

    {
        GENDB_PHASE("dim_filter");

        unordered_map<int32_t, uint16_t> cik_to_idx;
        cik_to_idx.reserve(4096);

        for (size_t i = 0; i < SUB_N; i++) {
            int16_t sic = sub_sic.data[i];
            if (sic >= 4000 && sic <= 4999) {
                adsh_qualifying_bits[i >> 6] |= 1ULL << (i & 63);

                int32_t cik = sub_cik.data[i];
                auto [it, inserted] = cik_to_idx.emplace(cik, (uint16_t)idx_to_cik.size());
                if (inserted) idx_to_cik.push_back(cik);
                adsh_to_cik_idx[i] = it->second;
            }
        }

        size_t N_ciks = idx_to_cik.size();
        cik_words = (N_ciks + 63) / 64;
    }

    // -----------------------------------------------------------------------
    // Load zone maps and mmap num columns (before build_joins to overlap I/O)
    // -----------------------------------------------------------------------
    vector<ZoneMap> zone_maps;
    uint32_t n_blocks = 0;
    {
        int fd = open((gendb_dir + "/indexes/num_zone_maps.bin").c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open num_zone_maps.bin\n"); return 1; }
        read(fd, &n_blocks, sizeof(uint32_t));
        zone_maps.resize(n_blocks);
        read(fd, zone_maps.data(), n_blocks * sizeof(ZoneMap));
        ::close(fd);
    }

    MmapColumn<int8_t>  num_uom   (gendb_dir + "/num/uom_code.bin");
    MmapColumn<int32_t> num_adsh  (gendb_dir + "/num/adsh_code.bin");
    MmapColumn<int32_t> num_tagver(gendb_dir + "/num/tagver_code.bin");
    MmapColumn<double>  num_value (gendb_dir + "/num/value.bin");
    const size_t NUM_N    = num_uom.count;
    const size_t BLOCK_SZ = 100000;

    mmap_prefetch_all(num_uom, num_adsh, num_tagver, num_value);

    // -----------------------------------------------------------------------
    // Phase: build_joins
    //
    // Parallel pre scan with SIC pre-filter: only emit keys where
    //   adsh_qualifying_bits[adsh] is set AND stmt==eq_code.
    //   ~42K keys -> single-threaded insert into 786KB FlatCountMap.
    //
    // NO bloom filter: 42K entries / 65536 slots -> direct probe is L2/L3-resident.
    //   Bloom adds 7 hash computations per qualifying row with ~0% FPR benefit.
    // -----------------------------------------------------------------------
    FlatCountMap eq_count_map(25000); // -> 65536 slots x 12B = 786KB

    {
        GENDB_PHASE("build_joins");

        MmapColumn<int32_t> pre_adsh  (gendb_dir + "/pre/adsh_code.bin");
        MmapColumn<int32_t> pre_tagver(gendb_dir + "/pre/tagver_code.bin");
        MmapColumn<int8_t>  pre_stmt  (gendb_dir + "/pre/stmt_code.bin");
        const size_t PRE_N = pre_adsh.count;

        const uint64_t* qual_bits_pre = adsh_qualifying_bits.data();

        // Parallel scan: collect SIC-qualified EQ-stmt keys per thread
        vector<vector<uint64_t>> thread_keys(num_threads);
        {
            vector<thread> pre_thrs;
            pre_thrs.reserve(num_threads);
            for (int tid = 0; tid < num_threads; tid++) {
                pre_thrs.emplace_back([&, tid]() {
                    size_t start = (size_t)tid * PRE_N / (size_t)num_threads;
                    size_t end   = (size_t)(tid + 1) * PRE_N / (size_t)num_threads;
                    auto& keys = thread_keys[tid];
                    keys.reserve((end - start) / 20 + 16);
                    for (size_t i = start; i < end; i++) {
                        if (pre_stmt.data[i] != eq_code) continue;
                        uint32_t ac = (uint32_t)pre_adsh.data[i];
                        if (ac >= SUB_N) continue;
                        if (!((qual_bits_pre[ac >> 6] >> (ac & 63)) & 1)) continue;
                        uint64_t key = ((uint64_t)ac << 32) | (uint32_t)pre_tagver.data[i];
                        if (key != FlatCountMap::EMPTY)
                            keys.push_back(key);
                    }
                });
            }
            for (auto& t : pre_thrs) t.join();
        }

        // Single-threaded insert into 786KB FlatCountMap (~42K insertions)
        for (auto& keys : thread_keys)
            for (uint64_t k : keys)
                eq_count_map.increment(k);
    }

    // -----------------------------------------------------------------------
    // Init FlatAggMaps (after cik_words is known from dim_filter phase)
    //   All thread maps: cap=8192 (3.73MB each).
    //   NOTE: plan's cap=1024 for workers is unsafe -- each worker thread sees ALL
    //   unique (sic,tagver) groups (multiple blocks contain the same groups), so with
    //   G~500+ unique groups, cap=1024 (512 safe slots at 50% LF) causes find_or_insert
    //   to loop infinitely. Using cap=8192 (4096 safe slots) covers all expected groups.
    //   Memory: 64 x 3.73MB = 238MB total, fits in 376GB RAM.
    // -----------------------------------------------------------------------
    static const size_t FAGG_CAP = 8192; // power of 2; holds up to ~4096 unique groups safely
    vector<FlatAggMap> thread_maps(num_threads);
    for (int i = 0; i < num_threads; i++)
        thread_maps[i].init(FAGG_CAP, cik_words);

    // -----------------------------------------------------------------------
    // Phase: main_scan -- parallel morsel-driven with zone-map block skipping
    //
    // Predicate order per num row (innermost loop):
    //   1. uom_code == usd_code  (zone map at block level, then row check)
    //   2. tagver_code != -1
    //   3. sic BETWEEN 4000-4999 via adsh_qualifying_bits (~10KB, L1-resident)
    //   4. tag_abstract[tagver_code] == 0
    //   5. eq_count_map.get(key) != 0 (786KB, L2/L3-resident, NO bloom pre-check)
    //
    // Group key: ((uint64_t)(uint16_t)sic << 32) | tagver_code
    // Accumulate in thread-local FlatAggMap (SoA, no heap allocation per group)
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("main_scan");

        atomic<uint32_t> next_block{0};
        const uint64_t* qual_bits    = adsh_qualifying_bits.data();
        const uint16_t* cik_idx_arr  = adsh_to_cik_idx.data();
        const size_t    cw           = cik_words;

        vector<thread> thrs;
        thrs.reserve(num_threads);
        for (int tid = 0; tid < num_threads; tid++) {
            thrs.emplace_back([&, tid, cw]() {
                FlatAggMap& agg = thread_maps[tid];

                while (true) {
                    uint32_t b = next_block.fetch_add(1, memory_order_relaxed);
                    if (b >= n_blocks) break;

                    const ZoneMap& zm = zone_maps[b];
                    if (zm.min_uom > usd_code || zm.max_uom < usd_code) continue;

                    size_t row_start = (size_t)b * BLOCK_SZ;
                    size_t row_end   = min(row_start + BLOCK_SZ, NUM_N);

                    const int8_t*  uom    = num_uom.data    + row_start;
                    const int32_t* adsh   = num_adsh.data   + row_start;
                    const int32_t* tagver = num_tagver.data + row_start;
                    const double*  val    = num_value.data  + row_start;
                    const size_t   n      = row_end - row_start;

                    for (size_t i = 0; i < n; i++) {
                        // 1. uom == USD
                        if (uom[i] != usd_code) continue;

                        // 2. tagver_code != -1
                        int32_t tv = tagver[i];
                        if (tv < 0) continue;

                        // 3. sic BETWEEN 4000-4999 (~4% pass, bitset L1-resident)
                        uint32_t ac = (uint32_t)adsh[i];
                        if (ac >= SUB_N) continue;
                        if (!((qual_bits[ac >> 6] >> (ac & 63)) & 1)) continue;

                        // 4. abstract == 0 (~95% pass)
                        if ((uint32_t)tv >= TAG_N) continue;
                        if (tag_abstract.data[tv] != 0) continue;

                        // 5. eq_count_map probe (786KB, L2/L3-resident, NO bloom pre-check)
                        uint64_t eq_key = ((uint64_t)ac << 32) | (uint32_t)tv;
                        uint32_t eq_cnt = eq_count_map.get(eq_key);
                        if (eq_cnt == 0) continue;

                        // Accumulate into thread-local FlatAggMap (no heap allocation)
                        int16_t  sic = sub_sic.data[ac];
                        uint64_t gk  = ((uint64_t)(uint16_t)sic << 32) | (uint32_t)tv;
                        uint16_t ci  = cik_idx_arr[ac];
                        agg.accumulate(gk, val[i] * (double)eq_cnt, (int64_t)eq_cnt, ci);
                    }
                }
            });
        }
        for (auto& t : thrs) t.join();
    }

    // -----------------------------------------------------------------------
    // Phase: merge_aggregates -- parallel binary-tree reduction
    //
    // Round r (stride = 2^r): thread i merges FROM thread i+stride (parallel).
    //   thread_maps[0] (cap=8192) accumulates all groups.
    //   Intermediate threads (cap=1024) accumulate only their subtree's groups.
    //
    // CIK merge: inline bitwise OR over cik_data[slot*cw .. slot*cw+cw].
    //   O(cik_words) = O(54) ops per group, no heap allocation, no pointer following.
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("merge_aggregates");

        for (int stride = 1; stride < num_threads; stride <<= 1) {
            vector<thread> merge_thrs;
            for (int i = 0; i + stride < num_threads; i += stride * 2) {
                merge_thrs.emplace_back([&, i, stride]() {
                    thread_maps[i].merge_from(thread_maps[i + stride]);
                    // Release source map memory
                    FlatAggMap empty;
                    thread_maps[i + stride] = move(empty);
                });
            }
            for (auto& t : merge_thrs) t.join();
        }
    }

    FlatAggMap& global_agg = thread_maps[0];

    // -----------------------------------------------------------------------
    // Phase: output
    //
    // 1. Iterate global FlatAggMap (cap=8192, all non-empty slots)
    // 2. Decode tlabel from tlabel_offsets + tlabel_data[tagver_code]
    // 3. Re-aggregate by (sic, tlabel) string key with bitwise-OR CIK merge
    // 4. Popcount for final cik_count per group
    // 5. HAVING cik_count >= 2
    // 6. Sort by total_value DESC, LIMIT 500
    // 7. Write CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        const size_t cw = cik_words;

        // Secondary aggregation key: (sic, tlabel) string
        struct SicTlabel {
            int16_t sic;
            string  tlabel;
            bool operator==(const SicTlabel& o) const {
                return sic == o.sic && tlabel == o.tlabel;
            }
        };
        struct SicTlabelHash {
            size_t operator()(const SicTlabel& k) const {
                size_t h = hash<int16_t>{}(k.sic);
                h ^= hash<string>{}(k.tlabel) + 0x9e3779b9 + (h << 6) + (h >> 2);
                return h;
            }
        };

        struct AggState2 {
            double           sum = 0.0;
            int64_t          cnt = 0;
            vector<uint64_t> cik_bits;
        };

        unordered_map<SicTlabel, AggState2, SicTlabelHash> final_agg;
        final_agg.reserve(global_agg.cap);

        for (size_t s = 0; s < global_agg.cap; s++) {
            if (global_agg.keys[s] == FlatAggMap::EMPTY) continue;

            uint64_t gk  = global_agg.keys[s];
            int16_t  sic = (int16_t)(gk >> 32);
            int32_t  tv  = (int32_t)(gk & 0xFFFFFFFF);

            // Decode tlabel from varlen column
            uint32_t off0 = tlabel_offsets.data[tv];
            uint32_t off1 = tlabel_offsets.data[tv + 1];
            string tlabel_str(tlabel_data + off0, off1 - off0);

            SicTlabel key{sic, move(tlabel_str)};
            AggState2& dst = final_agg[key];
            if (dst.cik_bits.empty()) dst.cik_bits.assign(cw, 0ULL);
            dst.sum += global_agg.sums[s];
            dst.cnt += global_agg.cnts[s];

            const uint64_t* s_cik = global_agg.cik_data.data() + s * cw;
            for (size_t j = 0; j < cw; j++)
                dst.cik_bits[j] |= s_cik[j];
        }

        struct OutputRow {
            int16_t sic;
            string  tlabel;
            int64_t num_companies;
            double  total_value;
            double  avg_value;
        };
        vector<OutputRow> results;
        results.reserve(final_agg.size());

        for (auto& [k, g] : final_agg) {
            int64_t cik_count = 0;
            for (uint64_t w : g.cik_bits)
                cik_count += (int64_t)__builtin_popcountll(w);
            if (cik_count < 2) continue;

            double avg = g.cnt > 0 ? g.sum / (double)g.cnt : 0.0;
            results.push_back({k.sic, k.tlabel, cik_count, g.sum, avg});
        }

        sort(results.begin(), results.end(), [](const OutputRow& a, const OutputRow& b) {
            return a.total_value > b.total_value;
        });
        if (results.size() > 500) results.resize(500);

        string out_path = results_dir + "/Q4.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { fprintf(stderr, "Cannot open output %s\n", out_path.c_str()); return 1; }
        fprintf(f, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
        for (const auto& r : results) {
            fprintf(f, "%d,", (int)r.sic);
            write_csv_field(f, r.tlabel);
            fprintf(f, ",EQ,%ld,%.2f,%.2f\n", r.num_companies, r.total_value, r.avg_value);
        }
        fclose(f);
    }

    if (tlabel_data && tlabel_st.st_size > 0)
        munmap((void*)tlabel_data, tlabel_st.st_size);

    return 0;
}
