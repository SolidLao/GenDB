// Q4 iter_1: Optimized with LLC-resident FlatCountMap + bloom filter + sorted-vector CIKs
//            + parallel binary-tree merge reduction
//
// Three root-cause fixes vs iter_0:
// (1) FlatCountMap(700000) → 2,097,152 slots × 12B = 25.2MB (fits in 44MB LLC)
//     Previously FlatCountMap(2200000) → 8,388,608 slots × 12B = 100.6MB (2.3× LLC)
// (2) 2MB bloom filter (16M bits, 7 hashes, ~0.78% FPR) rejects ~99.2% of non-qualifying
//     probes before touching eq_count_map — eliminates wasted LLC-miss traffic
// (3) Parallel binary-tree reduction (6 rounds) replaces single-threaded merge (~150ms)
//     CIKs use sorted vector<int32_t> instead of unordered_set for cache-friendly merge

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
// Open-addressing hash map: uint64_t key → uint32_t value (count)
// Sentinel = UINT64_MAX (caller must not use this key)
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
// Bloom filter: 16M bits = 2MB, 7 independent hash functions via double-hashing
// ---------------------------------------------------------------------------
static constexpr uint32_t BLOOM_BITS = 16777216; // 2^24

inline void bloom_set(uint64_t* __restrict__ bloom, uint64_t key) {
    uint64_t h1 = key ^ (key >> 33);
    h1 *= 0xff51afd7ed558ccdULL;
    h1 ^= h1 >> 33;
    uint64_t h2 = key ^ (key >> 30);
    h2 *= 0xbf58476d1ce4e5b9ULL;
    h2 ^= h2 >> 27;
    for (int i = 0; i < 7; i++) {
        uint32_t bit = (uint32_t)((h1 + (uint64_t)i * h2) & (BLOOM_BITS - 1));
        bloom[bit >> 6] |= 1ULL << (bit & 63);
    }
}

inline bool bloom_check(const uint64_t* __restrict__ bloom, uint64_t key) {
    uint64_t h1 = key ^ (key >> 33);
    h1 *= 0xff51afd7ed558ccdULL;
    h1 ^= h1 >> 33;
    uint64_t h2 = key ^ (key >> 30);
    h2 *= 0xbf58476d1ce4e5b9ULL;
    h2 ^= h2 >> 27;
    for (int i = 0; i < 7; i++) {
        uint32_t bit = (uint32_t)((h1 + (uint64_t)i * h2) & (BLOOM_BITS - 1));
        if (!((bloom[bit >> 6] >> (bit & 63)) & 1)) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Load code dict: uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }
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
// Aggregation state: sorted CIK vector for cache-friendly dedup + merge
// ---------------------------------------------------------------------------
struct AggState {
    double          sum  = 0.0;
    int64_t         cnt  = 0;
    vector<int32_t> ciks_sorted; // kept sorted; insert via lower_bound dedup
};

// ---------------------------------------------------------------------------
// Insert CIK into sorted vector (dedup)
// ---------------------------------------------------------------------------
static inline void ciks_insert(vector<int32_t>& v, int32_t cik) {
    auto it = lower_bound(v.begin(), v.end(), cik);
    if (it == v.end() || *it != cik)
        v.insert(it, cik);
}

// ---------------------------------------------------------------------------
// Merge two sorted CIK vectors (dedup) into dst
// ---------------------------------------------------------------------------
static inline void ciks_merge_into(vector<int32_t>& dst, const vector<int32_t>& src) {
    if (src.empty()) return;
    if (dst.empty()) { dst = src; return; }
    vector<int32_t> merged;
    merged.resize(dst.size() + src.size());
    auto end_it = std::merge(dst.begin(), dst.end(), src.begin(), src.end(), merged.begin());
    end_it = std::unique(merged.begin(), end_it);
    merged.resize((size_t)(end_it - merged.begin()));
    dst = move(merged);
}

// ---------------------------------------------------------------------------
// Output row
// ---------------------------------------------------------------------------
struct OutputRow {
    int16_t sic;
    string  tlabel;
    int64_t num_companies;
    double  total_value;
    double  avg_value;
};

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
    // Phase: data_loading — dicts, sub arrays, tag arrays, zone maps
    // -----------------------------------------------------------------------
    int8_t usd_code, eq_code;
    {
        GENDB_PHASE("data_loading");
        usd_code = load_dict_code(gendb_dir + "/indexes/uom_codes.bin",  "USD");
        eq_code  = load_dict_code(gendb_dir + "/indexes/stmt_codes.bin", "EQ");
    }

    static const size_t SUB_N = 86135;
    static const size_t TAG_N = 1070662;

    // sub arrays (172KB + 344KB — fully resident)
    MmapColumn<int16_t> sub_sic(gendb_dir + "/sub/sic.bin");
    MmapColumn<int32_t> sub_cik(gendb_dir + "/sub/cik.bin");

    // tag arrays (~1MB abstract + offsets + data)
    MmapColumn<int8_t>   tag_abstract  (gendb_dir + "/tag/abstract.bin");
    MmapColumn<uint32_t> tlabel_offsets(gendb_dir + "/tag/tlabel_offsets.bin");

    // tlabel_data raw bytes
    int tlabel_fd = open((gendb_dir + "/tag/tlabel_data.bin").c_str(), O_RDONLY);
    struct stat tlabel_st;
    fstat(tlabel_fd, &tlabel_st);
    const char* tlabel_data = (const char*)mmap(nullptr, tlabel_st.st_size,
                                                  PROT_READ, MAP_PRIVATE, tlabel_fd, 0);
    ::close(tlabel_fd);

    // zone maps (395 blocks)
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

    // num columns
    MmapColumn<int8_t>  num_uom   (gendb_dir + "/num/uom_code.bin");
    MmapColumn<int32_t> num_adsh  (gendb_dir + "/num/adsh_code.bin");
    MmapColumn<int32_t> num_tagver(gendb_dir + "/num/tagver_code.bin");
    MmapColumn<double>  num_value (gendb_dir + "/num/value.bin");

    const size_t NUM_N    = num_uom.count;
    const size_t BLOCK_SZ = 100000;

    mmap_prefetch_all(num_uom, num_adsh, num_tagver, num_value);

    // -----------------------------------------------------------------------
    // Phase: build_joins — build eq_count_map (25MB LLC-resident) + bloom filter (2MB)
    //
    // FlatCountMap(700000) → rounds to 2,097,152 slots × 12B = 25.2MB (fits in 44MB LLC)
    // Previously 2,200,000 → 8,388,608 slots = 100.6MB (2.3× LLC → every probe a miss)
    // -----------------------------------------------------------------------
    FlatCountMap eq_count_map(700000);
    vector<uint64_t> bloom(262144, 0ULL); // 262144 × 64 bits = 16M bits = 2MB

    {
        GENDB_PHASE("build_joins");

        MmapColumn<int32_t> pre_adsh  (gendb_dir + "/pre/adsh_code.bin");
        MmapColumn<int32_t> pre_tagver(gendb_dir + "/pre/tagver_code.bin");
        MmapColumn<int8_t>  pre_stmt  (gendb_dir + "/pre/stmt_code.bin");
        const size_t PRE_N = pre_adsh.count;

        // Parallel scan: collect EQ-stmt keys per thread
        vector<vector<uint64_t>> thread_keys(num_threads);
        {
            auto pre_worker = [&](int tid) {
                size_t start = (size_t)tid * PRE_N / (size_t)num_threads;
                size_t end   = (size_t)(tid + 1) * PRE_N / (size_t)num_threads;
                auto& keys = thread_keys[tid];
                keys.reserve((end - start) / 8 + 16);
                for (size_t i = start; i < end; i++) {
                    if (pre_stmt.data[i] == eq_code) {
                        uint64_t key = ((uint64_t)(uint32_t)pre_adsh.data[i] << 32)
                                     | (uint32_t)pre_tagver.data[i];
                        if (key != FlatCountMap::EMPTY)
                            keys.push_back(key);
                    }
                }
            };

            vector<thread> pre_thrs;
            pre_thrs.reserve(num_threads);
            for (int i = 0; i < num_threads; i++) pre_thrs.emplace_back(pre_worker, i);
            for (auto& t : pre_thrs) t.join();
        }

        // Single-threaded insert into FlatCountMap (avoids lock overhead)
        for (auto& keys : thread_keys)
            for (uint64_t k : keys)
                eq_count_map.increment(k);

        // Build bloom filter from all populated slots in eq_count_map
        uint64_t* bloom_ptr = bloom.data();
        for (const auto& slot : eq_count_map.slots) {
            if (slot.key != FlatCountMap::EMPTY)
                bloom_set(bloom_ptr, slot.key);
        }
    }

    // -----------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven with zone-map block skipping
    //
    // Predicate order (most selective first, cheapest ops first):
    //   1. uom_code == usd_code  (zone map at block level, then row check)
    //   2. tagver_code != -1
    //   3. sub_sic[adsh_code] BETWEEN 4000-4999  (~4% pass, array O(1))
    //   4. tag_abstract[tagver_code] == 0          (~95% pass)
    //   5. bloom_check(key)                        (rejects ~99.2% of non-qualifiers)
    //   6. eq_count_map.get(key) != 0              (LLC-resident 25MB table)
    //
    // AggState uses sorted vector<int32_t> for CIKs (O(log n) insert, cache-friendly merge)
    // -----------------------------------------------------------------------
    using AggMap = unordered_map<uint64_t, AggState>;
    vector<AggMap> thread_maps(num_threads);

    {
        GENDB_PHASE("main_scan");

        atomic<uint32_t> next_block{0};
        const uint64_t* bloom_ptr = bloom.data();

        auto scan_worker = [&](int tid) {
            AggMap& agg = thread_maps[tid];
            agg.reserve(2048);

            while (true) {
                uint32_t b = next_block.fetch_add(1, memory_order_relaxed);
                if (b >= n_blocks) break;

                // Zone map: skip block if uom definitely != usd_code
                const ZoneMap& zm = zone_maps[b];
                if (zm.min_uom > usd_code || zm.max_uom < usd_code) continue;

                size_t row_start = (size_t)b * BLOCK_SZ;
                size_t row_end   = min(row_start + BLOCK_SZ, NUM_N);

                const int8_t*  uom    = num_uom.data    + row_start;
                const int32_t* adsh   = num_adsh.data   + row_start;
                const int32_t* tagver = num_tagver.data + row_start;
                const double*  val    = num_value.data  + row_start;
                size_t n = row_end - row_start;

                for (size_t i = 0; i < n; i++) {
                    // 1. uom == USD
                    if (uom[i] != usd_code) continue;

                    // 2. tagver_code != -1
                    int32_t tv = tagver[i];
                    if (tv < 0) continue;

                    // 3. sic BETWEEN 4000 AND 4999 (most selective: ~4%)
                    int32_t ac = adsh[i];
                    if ((uint32_t)ac >= SUB_N) continue;
                    int16_t sic = sub_sic.data[ac];
                    if (sic < 4000 || sic > 4999) continue;

                    // 4. abstract == 0
                    if ((uint32_t)tv >= TAG_N) continue;
                    if (tag_abstract.data[tv] != 0) continue;

                    // 5+6. Bloom filter pre-check → then eq_count_map probe
                    uint64_t eq_key = ((uint64_t)(uint32_t)ac << 32) | (uint32_t)tv;
                    if (!bloom_check(bloom_ptr, eq_key)) continue;

                    uint32_t eq_cnt = eq_count_map.get(eq_key);
                    if (eq_cnt == 0) continue;

                    // Accumulate: multiply by join multiplicity (INNER JOIN semantics)
                    int32_t cik = sub_cik.data[ac];
                    uint64_t gk  = ((uint64_t)(uint16_t)sic << 32) | (uint32_t)tv;
                    AggState& g  = agg[gk];
                    g.sum += val[i] * (double)eq_cnt;
                    g.cnt += (int64_t)eq_cnt;
                    ciks_insert(g.ciks_sorted, cik);
                }
            }
        };

        vector<thread> thrs;
        thrs.reserve(num_threads);
        for (int i = 0; i < num_threads; i++) thrs.emplace_back(scan_worker, i);
        for (auto& t : thrs) t.join();
    }

    // -----------------------------------------------------------------------
    // Phase: merge_aggregates — parallel binary-tree reduction
    //
    // 6 rounds for 64 threads; each round fully parallelizes merges at current stride.
    // CIK merge: std::merge + std::unique on sorted vectors (branchless, cache-friendly).
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("merge_aggregates");

        for (int stride = 1; stride < num_threads; stride <<= 1) {
            vector<thread> merge_thrs;
            for (int i = 0; i + stride < num_threads; i += stride * 2) {
                merge_thrs.emplace_back([&, i, stride]() {
                    int dst_idx = i;
                    int src_idx = i + stride;
                    AggMap& dst_map = thread_maps[dst_idx];
                    AggMap& src_map = thread_maps[src_idx];

                    for (auto& [k, src] : src_map) {
                        AggState& dst = dst_map[k];
                        dst.sum += src.sum;
                        dst.cnt += src.cnt;
                        ciks_merge_into(dst.ciks_sorted, src.ciks_sorted);
                    }
                    // Free source map memory
                    AggMap empty;
                    src_map.swap(empty);
                });
            }
            for (auto& t : merge_thrs) t.join();
        }
    }

    AggMap& global_agg = thread_maps[0];

    // -----------------------------------------------------------------------
    // Phase: output — decode tlabel, re-aggregate by (sic, tlabel), HAVING, sort, limit
    //
    // Multiple tagver_codes may share the same tlabel string. After primary aggregation
    // merge, decode tlabel per group and re-aggregate by (sic, tlabel) string key.
    // -----------------------------------------------------------------------
    vector<OutputRow> results;
    {
        GENDB_PHASE("output");

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
            double          sum = 0.0;
            int64_t         cnt = 0;
            vector<int32_t> ciks_sorted;
        };

        unordered_map<SicTlabel, AggState2, SicTlabelHash> final_agg;
        final_agg.reserve(global_agg.size());

        for (auto& [gk, g] : global_agg) {
            int16_t sic     = (int16_t)(gk >> 32);
            int32_t tv_code = (int32_t)(gk & 0xFFFFFFFF);

            uint32_t off0 = tlabel_offsets.data[tv_code];
            uint32_t off1 = tlabel_offsets.data[tv_code + 1];
            string tlabel(tlabel_data + off0, off1 - off0);

            SicTlabel key{sic, move(tlabel)};
            AggState2& dst = final_agg[key];
            dst.sum += g.sum;
            dst.cnt += g.cnt;
            ciks_merge_into(dst.ciks_sorted, g.ciks_sorted);
        }

        // HAVING: cik_count >= 2
        results.reserve(final_agg.size());
        for (auto& [k, g] : final_agg) {
            if ((int64_t)g.ciks_sorted.size() < 2) continue;
            double avg = g.cnt > 0 ? g.sum / (double)g.cnt : 0.0;
            results.push_back({k.sic, k.tlabel, (int64_t)g.ciks_sorted.size(), g.sum, avg});
        }

        // Sort by total_value DESC, limit 500
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
            fprintf(f, ",EQ,%ld,%.2f,%.2f\n", r.num_companies, r.total_value, r.avg_value);
        }
        fclose(f);
    }

    // Cleanup
    if (tlabel_data && tlabel_st.st_size > 0)
        munmap((void*)tlabel_data, tlabel_st.st_size);

    return 0;
}
