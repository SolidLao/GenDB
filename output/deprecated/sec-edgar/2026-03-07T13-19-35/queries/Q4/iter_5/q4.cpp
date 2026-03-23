// Q4 iter_5: Direct-array aggregation with counting-sort CIK counting
//
// Key changes from iter_4:
// (1) NO bloom filter — removed entirely. FlatCountMap 786KB is L3-resident;
//     adding 7 hash ops for bloom was overhead with ~0% FPR benefit.
// (2) Pre-enumerate N_groups unique (sic,tagver) groups from eq_count_map slots
//     → slot_to_group_idx[65536]: O(1) lookup by slot index during main_scan.
// (3) Direct-array local_agg[N_groups] per thread (calloc-zeroed) — no hash map,
//     no heap allocation per group, no collision handling in hot loop.
// (4) cik_buf per thread: sequential push_back of packed (group_idx<<16 | cik_idx).
// (5) Post-scan: parallel element-wise sum/cnt reduction (partition groups by thread).
// (6) Post-scan: counting-sort on cik_buf → per-group L1-resident bitset (<=432B).
//     Merge cik bitsets directly into final_agg[(sic,tlabel)] during this pass.

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
#include <cstdlib>
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
// FlatCountMap: open-addressing uint64_t->uint32_t count map
// cap_hint=25000 -> 65536 slots x 12B = 786KB (L3-resident)
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

    // Probe: returns true iff key found.
    // Sets *slot_out = slot index (for slot_to_group_idx lookup),
    //      *cnt_out  = occurrence count.
    inline bool probe(uint64_t key, size_t* slot_out, uint32_t* cnt_out) const {
        size_t idx = mix(key) & mask_;
        while (slots[idx].key != EMPTY && slots[idx].key != key)
            idx = (idx + 1) & mask_;
        if (slots[idx].key != key) return false;
        *slot_out = idx;
        *cnt_out  = slots[idx].val;
        return true;
    }
};

// ---------------------------------------------------------------------------
// Zone map layout: int8 min_uom, int8 max_uom, [2-byte pad], int32 min_ddate, int32 max_ddate
// Total 12 bytes; natural 2-byte padding before int32.
// ---------------------------------------------------------------------------
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap must be 12 bytes");

// ---------------------------------------------------------------------------
// Per-thread aggregation state (direct-array indexed by group_idx)
// ---------------------------------------------------------------------------
struct SumCnt {
    double  sum;
    int64_t cnt;
};

// ---------------------------------------------------------------------------
// Load code dict: uint8_t N; N x { int8_t code, uint8_t slen, char[slen] }
// ---------------------------------------------------------------------------
static int8_t load_dict_code(const string& path, const char* key_str) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    uint8_t N; fread(&N, 1, 1, f);
    for (int i = 0; i < (int)N; i++) {
        int8_t code; uint8_t slen;
        fread(&code, 1, 1, f);
        fread(&slen,  1, 1, f);
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
// Write CSV field (quote if contains comma, double-quote, or newline)
// ---------------------------------------------------------------------------
static void write_csv_field(FILE* f, const string& s) {
    bool need = s.find(',')  != string::npos
             || s.find('"')  != string::npos
             || s.find('\n') != string::npos;
    if (!need) { fwrite(s.data(), 1, s.size(), f); return; }
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
    if (num_threads < 1)  num_threads = 1;
    if (num_threads > 64) num_threads = 64;

    // -----------------------------------------------------------------------
    // Phase: data_loading -- load code dictionaries
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
    // Phase: dim_filter -- load sub arrays, build qualifying structures
    //
    //   adsh_qualifying_bits[1346 uint64_ts = ~10.8KB, L1-resident]:
    //     bit i set iff sub_sic[i] BETWEEN 4000-4999
    //   adsh_to_cik_idx[SUB_N uint16_t = 172KB]:
    //     UINT16_MAX for non-qualifying adsh
    //   idx_to_cik: remap index -> original CIK (N_ciks <= 3445)
    //   cik_words = ceil(N_ciks / 64) <= 54 uint64_ts = 432 bytes (L1-resident)
    // -----------------------------------------------------------------------
    MmapColumn<int16_t>  sub_sic       (gendb_dir + "/sub/sic.bin");
    MmapColumn<int32_t>  sub_cik_col   (gendb_dir + "/sub/cik.bin");
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
    size_t N_ciks    = 0;
    size_t cik_words = 0;

    {
        GENDB_PHASE("dim_filter");
        unordered_map<int32_t, uint16_t> cik_to_idx;
        cik_to_idx.reserve(4096);

        for (size_t i = 0; i < SUB_N; i++) {
            int16_t sic = sub_sic.data[i];
            if (sic < 4000 || sic > 4999) continue;
            adsh_qualifying_bits[i >> 6] |= 1ULL << (i & 63);
            int32_t cik = sub_cik_col.data[i];
            auto [it, ins] = cik_to_idx.emplace(cik, (uint16_t)idx_to_cik.size());
            if (ins) idx_to_cik.push_back(cik);
            adsh_to_cik_idx[i] = it->second;
        }
        N_ciks    = idx_to_cik.size();
        cik_words = (N_ciks + 63) / 64;
    }

    // -----------------------------------------------------------------------
    // Load zone maps + num columns; prefetch num columns to overlap I/O
    // -----------------------------------------------------------------------
    vector<ZoneMap> zone_maps;
    uint32_t n_blocks = 0;
    {
        int fd = open((gendb_dir + "/indexes/num_zone_maps.bin").c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open num_zone_maps.bin\n"); return 1; }
        ::read(fd, &n_blocks, sizeof(uint32_t));
        zone_maps.resize(n_blocks);
        ::read(fd, zone_maps.data(), n_blocks * sizeof(ZoneMap));
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
    // Step 1: parallel pre scan with SIC pre-filter -> thread-local key vectors
    //   Only emit key if adsh_qualifying_bits[adsh] AND stmt==eq_code.
    //   ~42K qualifying keys (vs 1.06M without filter).
    //
    // Step 2: single-threaded insert into FlatCountMap eq_count_map(25000)
    //   -> 65536 slots x 12B = 786KB
    //
    // Step 3: enumerate N_groups unique (sic, tagver) groups from occupied slots
    //   Build slot_to_group_idx[65536] (256KB), group_sic[], group_tagver[]
    //   N_groups <= 42K (group_idx fits in uint16_t)
    // -----------------------------------------------------------------------
    FlatCountMap eq_count_map(25000); // 65536 slots

    // slot_to_group_idx[65536]: UINT32_MAX for empty slots
    static const size_t SLOT_CAP = 65536;
    vector<uint32_t> slot_to_group_idx(SLOT_CAP, UINT32_MAX);

    vector<int16_t> group_sic;
    vector<int32_t> group_tagver;
    uint32_t N_groups = 0;

    {
        GENDB_PHASE("build_joins");

        MmapColumn<int32_t> pre_adsh  (gendb_dir + "/pre/adsh_code.bin");
        MmapColumn<int32_t> pre_tagver(gendb_dir + "/pre/tagver_code.bin");
        MmapColumn<int8_t>  pre_stmt  (gendb_dir + "/pre/stmt_code.bin");
        const size_t PRE_N = pre_adsh.count;

        const uint64_t* qbits = adsh_qualifying_bits.data();

        // Step 1: parallel pre scan with SIC pre-filter
        vector<vector<uint64_t>> thr_keys(num_threads);
        {
            auto pre_worker = [&](int tid) {
                size_t start = (size_t)tid * PRE_N / (size_t)num_threads;
                size_t end   = (size_t)(tid + 1) * PRE_N / (size_t)num_threads;
                auto& keys = thr_keys[tid];
                keys.reserve((end - start) / 20 + 16);
                for (size_t i = start; i < end; i++) {
                    if (pre_stmt.data[i] != eq_code) continue;
                    uint32_t ac = (uint32_t)pre_adsh.data[i];
                    if (ac >= SUB_N) continue;
                    if (!((qbits[ac >> 6] >> (ac & 63)) & 1)) continue;
                    uint64_t key = ((uint64_t)ac << 32) | (uint32_t)pre_tagver.data[i];
                    if (key != FlatCountMap::EMPTY)
                        keys.push_back(key);
                }
            };
            vector<thread> thrs;
            thrs.reserve(num_threads);
            for (int i = 0; i < num_threads; i++) thrs.emplace_back(pre_worker, i);
            for (auto& t : thrs) t.join();
        }

        // Step 2: single-threaded insert into 786KB eq_count_map (~42K keys)
        for (auto& keys : thr_keys)
            for (uint64_t k : keys)
                eq_count_map.increment(k);

        // Step 3: enumerate N_groups by iterating all 65536 slots
        //   For each occupied slot s:
        //     adsh_code  = key >> 32
        //     tagver_code = key & 0xFFFFFFFF
        //     sic        = sub_sic[adsh_code]
        //     group_key  = ((uint64_t)(uint16_t)sic << 32) | tagver_code
        //     assign group_idx via group_key_to_idx
        //     slot_to_group_idx[s] = group_idx
        group_sic.reserve(50000);
        group_tagver.reserve(50000);
        unordered_map<uint64_t, uint32_t> group_key_to_idx;
        group_key_to_idx.reserve(50000);

        for (size_t s = 0; s < SLOT_CAP; s++) {
            const auto& slot = eq_count_map.slots[s];
            if (slot.key == FlatCountMap::EMPTY) continue;

            uint32_t adsh_code   = (uint32_t)(slot.key >> 32);
            uint32_t tagver_code = (uint32_t)(slot.key & 0xFFFFFFFF);
            int16_t  sic         = sub_sic.data[adsh_code];
            uint64_t gkey        = ((uint64_t)(uint16_t)sic << 32) | tagver_code;

            auto [it, inserted] = group_key_to_idx.emplace(gkey, N_groups);
            if (inserted) {
                group_sic.push_back(sic);
                group_tagver.push_back((int32_t)tagver_code);
                N_groups++;
            }
            slot_to_group_idx[s] = it->second;
        }
    }

    // -----------------------------------------------------------------------
    // Allocate global reduction targets (zero-initialized)
    // -----------------------------------------------------------------------
    vector<double>  global_sum(N_groups, 0.0);
    vector<int64_t> global_cnt(N_groups, 0LL);

    // -----------------------------------------------------------------------
    // Phase: main_scan -- parallel morsel-driven with zone-map block skipping
    //
    // Predicate order per row:
    //   [block] zone-map: skip if zm[b].min_uom > usd_code || zm[b].max_uom < usd_code
    //   1. uom_code == usd_code
    //   2. tagver_code != -1
    //   3. adsh_qualifying_bits[ac>>6] bit test -- 4% pass (L1-resident ~10KB)
    //   4. tag_abstract[tv] == 0 -- 95% pass (~1MB L3-resident)
    //   5. eq_count_map.probe(key, &slot, &cnt) -- 786KB L3-resident, NO BLOOM FILTER
    //   6. slot_to_group_idx[slot] -- O(1) group lookup (256KB L3-resident)
    //   7. local_agg[group_idx].sum += val*cnt; .cnt += cnt
    //   8. cik_buf.push_back((group_idx<<16) | cik_idx)
    // -----------------------------------------------------------------------
    vector<SumCnt*>          thr_local_agg(num_threads, nullptr);
    vector<vector<uint32_t>> thr_cik_buf(num_threads);

    {
        GENDB_PHASE("main_scan");

        atomic<uint32_t> next_block{0};
        const uint64_t* qual_bits   = adsh_qualifying_bits.data();
        const uint16_t* cik_idx_arr = adsh_to_cik_idx.data();
        const uint32_t* s2g         = slot_to_group_idx.data();
        const int8_t*   tag_abs     = tag_abstract.data;
        uint32_t        ng          = N_groups;

        // Allocate per-thread local_agg (calloc-zeroed, N_groups entries)
        for (int t = 0; t < num_threads; t++) {
            if (ng > 0) {
                thr_local_agg[t] = (SumCnt*)calloc(ng, sizeof(SumCnt));
                if (!thr_local_agg[t]) {
                    fprintf(stderr, "OOM: calloc local_agg thread %d\n", t);
                    return 1;
                }
            }
        }

        auto scan_worker = [&](int tid) {
            SumCnt*           la = thr_local_agg[tid];
            vector<uint32_t>& cb = thr_cik_buf[tid];
            cb.reserve(16384);

            while (true) {
                uint32_t b = next_block.fetch_add(1, memory_order_relaxed);
                if (b >= n_blocks) break;

                const ZoneMap& zm = zone_maps[b];
                if (zm.min_uom > usd_code || zm.max_uom < usd_code) continue;

                size_t row_start = (size_t)b * BLOCK_SZ;
                size_t row_end   = min(row_start + BLOCK_SZ, NUM_N);
                size_t n         = row_end - row_start;

                const int8_t*  uom    = num_uom.data    + row_start;
                const int32_t* adsh   = num_adsh.data   + row_start;
                const int32_t* tagver = num_tagver.data + row_start;
                const double*  val    = num_value.data  + row_start;

                for (size_t i = 0; i < n; i++) {
                    // 1. uom == USD
                    if (uom[i] != usd_code) continue;

                    // 2. tagver_code != -1
                    int32_t tv = tagver[i];
                    if (tv < 0) continue;

                    // 3. sic BETWEEN 4000-4999 via bitset (L1-resident)
                    uint32_t ac = (uint32_t)adsh[i];
                    if (ac >= SUB_N) continue;
                    if (!((qual_bits[ac >> 6] >> (ac & 63)) & 1)) continue;

                    // 4. abstract == 0 (1MB, L3-resident)
                    if ((uint32_t)tv >= TAG_N) continue;
                    if (tag_abs[tv] != 0) continue;

                    // 5. eq_count_map probe -- 786KB L3-resident, NO BLOOM FILTER
                    uint64_t eq_key = ((uint64_t)ac << 32) | (uint32_t)tv;
                    size_t   slot   = 0;
                    uint32_t eq_cnt = 0;
                    if (!eq_count_map.probe(eq_key, &slot, &eq_cnt)) continue;
                    // eq_cnt > 0 guaranteed for occupied slots

                    // 6. O(1) group lookup
                    uint32_t gidx = s2g[slot];

                    // 7. Direct-array aggregation (no hashing, no allocation)
                    if (la) {
                        la[gidx].sum += val[i] * (double)eq_cnt;
                        la[gidx].cnt += (int64_t)eq_cnt;
                    }

                    // 8. Record (group_idx, cik_idx) for CIK counting
                    uint16_t ci = cik_idx_arr[ac];
                    cb.push_back(((uint32_t)gidx << 16) | (uint32_t)ci);
                }
            }
        };

        vector<thread> thrs;
        thrs.reserve(num_threads);
        for (int i = 0; i < num_threads; i++) thrs.emplace_back(scan_worker, i);
        for (auto& t : thrs) t.join();
    }

    // -----------------------------------------------------------------------
    // Phase: sum_cnt_reduction -- parallel element-wise reduction
    //
    // Each thread owns partition of group indices [g_start, g_end).
    // Reads ALL thread-local arrays for its partition -> sequential access.
    // No hash operations, no merging of maps.
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("sum_cnt_reduction");

        uint32_t ng = N_groups;

        auto reduce_worker = [&](int tid) {
            size_t g_start = (size_t)tid       * ng / (size_t)num_threads;
            size_t g_end   = (size_t)(tid + 1) * ng / (size_t)num_threads;
            for (int t = 0; t < num_threads; t++) {
                const SumCnt* la = thr_local_agg[t];
                if (!la) continue;
                for (size_t g = g_start; g < g_end; g++) {
                    global_sum[g] += la[g].sum;
                    global_cnt[g] += la[g].cnt;
                }
            }
        };

        vector<thread> thrs;
        thrs.reserve(num_threads);
        for (int i = 0; i < num_threads; i++) thrs.emplace_back(reduce_worker, i);
        for (auto& t : thrs) t.join();

        // Free per-thread local_agg arrays
        for (int t = 0; t < num_threads; t++) {
            free(thr_local_agg[t]);
            thr_local_agg[t] = nullptr;
        }
    }

    // -----------------------------------------------------------------------
    // Phase: cik_counting -- counting-sort + per-group L1-resident bitset
    //                        + final (sic,tlabel) aggregation
    //
    // Step 1: gather all per-thread cik_buf entries into flat array
    // Step 2: counting-sort by group_idx (high 16 bits) -> grouped sorted_pairs
    // Step 3: for each group g:
    //   (a) memset local_bits (<=432B, L1-resident)
    //   (b) set bits for each cik_idx in group's slice
    //   (c) OR into final_agg[(sic,tlabel)].cik_bits
    //   (d) accumulate sum/cnt into final_agg
    // -----------------------------------------------------------------------

    // final_agg key: (sic, tlabel string)
    struct SicTlabelKey {
        int16_t sic;
        string  tlabel;
        bool operator==(const SicTlabelKey& o) const {
            return sic == o.sic && tlabel == o.tlabel;
        }
    };
    struct SicTlabelHash {
        size_t operator()(const SicTlabelKey& k) const {
            size_t h = hash<int16_t>{}(k.sic);
            h ^= hash<string>{}(k.tlabel) + 0x9e3779b9u + (h << 6) + (h >> 2);
            return h;
        }
    };
    struct AggFinal {
        double           sum = 0.0;
        int64_t          cnt = 0;
        vector<uint64_t> cik_bits; // size = cik_words
    };

    unordered_map<SicTlabelKey, AggFinal, SicTlabelHash> final_agg;

    {
        GENDB_PHASE("cik_counting");

        uint32_t ng = N_groups;
        size_t   cw = cik_words;

        // Step 1: gather all cik_buf entries
        size_t total_cik = 0;
        for (int t = 0; t < num_threads; t++) total_cik += thr_cik_buf[t].size();

        if (ng > 0 && total_cik > 0) {
            vector<uint32_t> all_pairs;
            all_pairs.reserve(total_cik);
            for (int t = 0; t < num_threads; t++) {
                auto& cb = thr_cik_buf[t];
                all_pairs.insert(all_pairs.end(), cb.begin(), cb.end());
                vector<uint32_t>().swap(cb); // free
            }

            // Step 2: counting-sort by group_idx (high 16 bits)
            vector<uint32_t> grp_count(ng, 0);
            for (uint32_t e : all_pairs) grp_count[e >> 16]++;

            vector<uint32_t> grp_offsets(ng + 1, 0);
            for (uint32_t g = 0; g < ng; g++)
                grp_offsets[g + 1] = grp_offsets[g] + grp_count[g];

            vector<uint32_t> sorted_pairs(total_cik);
            {
                vector<uint32_t> scatter_pos(grp_offsets.begin(),
                                             grp_offsets.begin() + ng);
                for (uint32_t e : all_pairs) {
                    uint32_t g = e >> 16;
                    sorted_pairs[scatter_pos[g]++] = e;
                }
            }
            all_pairs.clear();
            all_pairs.shrink_to_fit();

            // Step 3: per-group L1-resident bitset -> merge into final_agg
            vector<uint64_t> local_bits(max(cw, (size_t)1), 0ULL);
            final_agg.reserve(ng);

            for (uint32_t g = 0; g < ng; g++) {
                uint32_t s = grp_offsets[g];
                uint32_t e = grp_offsets[g + 1];
                if (s == e) continue; // no entries for this group

                // Compute cik bitset
                if (cw > 0) {
                    memset(local_bits.data(), 0, cw * sizeof(uint64_t));
                    for (uint32_t i = s; i < e; i++) {
                        uint16_t ci = (uint16_t)(sorted_pairs[i] & 0xFFFF);
                        local_bits[ci >> 6] |= 1ULL << (ci & 63);
                    }
                }

                // Decode tlabel for this group's tagver
                int32_t  tv   = group_tagver[g];
                uint32_t off0 = tlabel_offsets.data[tv];
                uint32_t off1 = tlabel_offsets.data[tv + 1];
                string   tlbl(tlabel_data + off0, off1 - off0);

                SicTlabelKey key{group_sic[g], move(tlbl)};
                AggFinal& fa = final_agg[key];
                if (fa.cik_bits.empty()) fa.cik_bits.assign(cw, 0ULL);
                fa.sum += global_sum[g];
                fa.cnt += global_cnt[g];
                for (size_t j = 0; j < cw; j++)
                    fa.cik_bits[j] |= local_bits[j];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: output -- HAVING, sort by total_value DESC, limit 500, write CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct OutputRow {
            int16_t sic;
            string  tlabel;
            int64_t num_companies;
            double  total_value;
            double  avg_value;
        };
        vector<OutputRow> results;
        results.reserve(final_agg.size());

        size_t cw = cik_words;
        for (auto& [k, fa] : final_agg) {
            // COUNT(DISTINCT cik) via popcount over merged cik bitset
            int64_t cik_count = 0;
            for (uint64_t w : fa.cik_bits)
                cik_count += (int64_t)__builtin_popcountll(w);

            // HAVING COUNT(DISTINCT cik) >= 2
            if (cik_count < 2) continue;

            double avg = fa.cnt > 0 ? fa.sum / (double)fa.cnt : 0.0;
            results.push_back({k.sic, k.tlabel, cik_count, fa.sum, avg});
        }

        // ORDER BY total_value DESC, LIMIT 500
        sort(results.begin(), results.end(), [](const OutputRow& a, const OutputRow& b) {
            return a.total_value > b.total_value;
        });
        if (results.size() > 500) results.resize(500);

        // Write CSV
        string out_path = results_dir + "/Q4.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { fprintf(stderr, "Cannot open output %s\n", out_path.c_str()); return 1; }

        fprintf(f, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
        for (const auto& r : results) {
            fprintf(f, "%d,", (int)r.sic);
            write_csv_field(f, r.tlabel);
            fprintf(f, ",EQ,%ld,%.2f,%.2f\n",
                    r.num_companies, r.total_value, r.avg_value);
        }
        fclose(f);
    }

    // Cleanup
    if (tlabel_data && tlabel_st.st_size > 0)
        munmap((void*)tlabel_data, tlabel_st.st_size);

    return 0;
}
