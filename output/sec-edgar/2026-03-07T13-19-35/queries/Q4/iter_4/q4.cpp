// Q4 iter_2: Three optimizations over iter_1
// (1) SIC-prefiltered eq_count_map: adsh_qualifying_bitset (~10KB) built from sub scan.
//     During pre scan, only emit key if bitset[adsh] AND stmt==EQ.
//     ~42K entries → FlatCountMap(25000) = 65536 slots × 12B = 786KB (vs 25MB iter_1).
// (2) Smaller bloom filter: 256KB (2M bits, L2-resident) vs iter_1's 2MB (L3-resident).
//     With 42K entries, FPR ≈ 0% — every bloom pass is a true positive.
//     7 L2 hits (~5ns each = 35ns) vs 7 L3 hits (~30ns = 210ns). Faster per check.
// (3) Compact bitset CIK tracking: remap qualifying CIKs → [0, N_ciks) indices.
//     Per-group: vector<uint64_t>(cik_words) bitset. Insert = 1 bit-set (O(1), no sort).
//     Merge = cik_words bitwise ORs. Count = popcountll. No sorted-vector overhead.

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
// cap_hint=25000 → 65536 slots × 12B = 786KB (L2/L3-resident)
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
// Bloom filter: 2M bits = 256KB (L2-resident), 7 hashes via double-hashing
// With 42K entries: FPR ≈ (1-e^(-7*42K/2M))^7 ≈ 10^{-7} ≈ essentially zero
// ---------------------------------------------------------------------------
static constexpr uint32_t BLOOM_BITS = 1u << 21; // 2M bits = 256KB

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
// Aggregation state: compact bitset for CIKs (no sorted-vector overhead)
// cik_bits: vector<uint64_t>(cik_words); lazily initialized on first insert
// ---------------------------------------------------------------------------
struct AggState {
    double           sum  = 0.0;
    int64_t          cnt  = 0;
    vector<uint64_t> cik_bits; // cik_words uint64_ts; empty until first use
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
    // Phase: data_loading — load code dicts
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
    // Phase: dim_filter — load sub/tag arrays, build CIK qualifying structures
    //
    // Builds:
    //   adsh_qualifying_bits (~10.8KB, L1-resident): bit i set iff sub_sic[i] BETWEEN 4000-4999
    //   idx_to_cik: remap index → original CIK value (N_ciks ≤ 3445)
    //   adsh_to_cik_idx[SUB_N]: uint16_t, UINT16_MAX for non-qualifying adsh
    //   cik_words = ceil(N_ciks / 64)
    // -----------------------------------------------------------------------
    MmapColumn<int16_t>  sub_sic       (gendb_dir + "/sub/sic.bin");
    MmapColumn<int32_t>  sub_cik       (gendb_dir + "/sub/cik.bin");
    MmapColumn<int8_t>   tag_abstract  (gendb_dir + "/tag/abstract.bin");
    MmapColumn<uint32_t> tlabel_offsets(gendb_dir + "/tag/tlabel_offsets.bin");

    int tlabel_fd = open((gendb_dir + "/tag/tlabel_data.bin").c_str(), O_RDONLY);
    struct stat tlabel_st;
    fstat(tlabel_fd, &tlabel_st);
    const char* tlabel_data = (const char*)mmap(nullptr, tlabel_st.st_size,
                                                  PROT_READ, MAP_PRIVATE, tlabel_fd, 0);
    ::close(tlabel_fd);

    // adsh_qualifying_bitset: 86135 bits as uint64_t words (~10.8KB, L1-resident)
    static const size_t BITSET_WORDS = (SUB_N + 63) / 64; // 1346 words
    vector<uint64_t> adsh_qualifying_bits(BITSET_WORDS, 0ULL);

    // adsh_to_cik_idx: uint16_t[SUB_N], init to UINT16_MAX
    vector<uint16_t> adsh_to_cik_idx(SUB_N, UINT16_MAX);

    vector<int32_t> idx_to_cik;  // remap index → CIK value
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
        // Typically N_ciks ≤ 3445, cik_words ≤ 54 uint64_ts
    }

    // -----------------------------------------------------------------------
    // Load zone maps and num columns (before build_joins so prefetch runs in parallel)
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
    // (A) Parallel pre scan with SIC pre-filter: collect only keys where
    //     adsh_qualifying_bits[adsh] is set AND stmt==eq_code.
    //     ~42K keys (vs 1.06M before). Single-threaded insert into 786KB map.
    //
    // (B) Build 256KB bloom filter from the 42K eq_count_map entries.
    //     With 42K entries / 2M bits: FPR ≈ 10^{-7} ≈ 0.
    //     7 L2 hits at ~5ns = 35ns per check (vs 210ns for 2MB bloom in iter_1).
    // -----------------------------------------------------------------------
    FlatCountMap eq_count_map(25000); // → 65536 slots × 12B = 786KB
    vector<uint64_t> bloom(BLOOM_BITS / 64, 0ULL); // 32768 × 8B = 256KB

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
            auto pre_worker = [&](int tid) {
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
            };

            vector<thread> pre_thrs;
            pre_thrs.reserve(num_threads);
            for (int i = 0; i < num_threads; i++) pre_thrs.emplace_back(pre_worker, i);
            for (auto& t : pre_thrs) t.join();
        }

        // Single-threaded insert into 786KB FlatCountMap (~42K insertions)
        for (auto& keys : thread_keys)
            for (uint64_t k : keys)
                eq_count_map.increment(k);

        // Build 256KB bloom from the 42K populated slots
        uint64_t* bloom_ptr = bloom.data();
        for (const auto& slot : eq_count_map.slots) {
            if (slot.key != FlatCountMap::EMPTY)
                bloom_set(bloom_ptr, slot.key);
        }
    }

    // -----------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven with zone-map block skipping
    //
    // Predicate order:
    //   1. uom_code == usd_code  (zone map at block level, then row check)
    //   2. tagver_code != -1
    //   3. sic BETWEEN 4000-4999 via adsh_qualifying_bits (~10KB, L1-resident)
    //   4. tag_abstract[tagver_code] == 0
    //   5. bloom_check (256KB, L2-resident, ~0% FPR) — rejects ~100% of misses
    //   6. eq_count_map.get (786KB, L2/L3-resident) — only true positives reach here
    //
    // CIK tracking: bitset (O(1) insert, O(cik_words) merge via bitwise OR)
    // -----------------------------------------------------------------------
    using AggMap = unordered_map<uint64_t, AggState>;
    vector<AggMap> thread_maps(num_threads);

    {
        GENDB_PHASE("main_scan");

        atomic<uint32_t> next_block{0};
        const uint64_t* bloom_ptr    = bloom.data();
        const uint64_t* qual_bits    = adsh_qualifying_bits.data();
        const uint16_t* cik_idx_arr  = adsh_to_cik_idx.data();
        size_t cw = cik_words;

        auto scan_worker = [&](int tid) {
            AggMap& agg = thread_maps[tid];
            agg.reserve(4096);

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
                size_t n = row_end - row_start;

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

                    // 5. Bloom filter (256KB, L2-resident, ~0% FPR)
                    uint64_t eq_key = ((uint64_t)ac << 32) | (uint32_t)tv;
                    if (!bloom_check(bloom_ptr, eq_key)) continue;

                    // 6. eq_count_map probe (786KB, only true positives reach here)
                    uint32_t eq_cnt = eq_count_map.get(eq_key);
                    if (eq_cnt == 0) continue;

                    // Accumulate (multiply by JOIN multiplicity)
                    int16_t sic = sub_sic.data[ac];
                    uint64_t gk  = ((uint64_t)(uint16_t)sic << 32) | (uint32_t)tv;
                    AggState& g  = agg[gk];
                    if (g.cik_bits.empty()) g.cik_bits.assign(cw, 0ULL);
                    g.sum += val[i] * (double)eq_cnt;
                    g.cnt += (int64_t)eq_cnt;
                    // CIK bit-set: O(1), no allocation, no sorting
                    uint16_t ci = cik_idx_arr[ac];
                    g.cik_bits[ci >> 6] |= 1ULL << (ci & 63);
                }
            }
        };

        vector<thread> thrs;
        thrs.reserve(num_threads);
        for (int i = 0; i < num_threads; i++) thrs.emplace_back(scan_worker, i);
        for (auto& t : thrs) t.join();
    }

    // -----------------------------------------------------------------------
    // Phase: merge_aggregates — parallel binary-tree reduction (6 rounds for 64 threads)
    //
    // CIK merge: cik_words bitwise ORs per group (O(54) = O(54×8B))
    // vs iter_1's std::merge+unique+heap allocation per group
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("merge_aggregates");

        size_t cw = cik_words;
        for (int stride = 1; stride < num_threads; stride <<= 1) {
            vector<thread> merge_thrs;
            for (int i = 0; i + stride < num_threads; i += stride * 2) {
                merge_thrs.emplace_back([&, i, stride, cw]() {
                    AggMap& dst_map = thread_maps[i];
                    AggMap& src_map = thread_maps[i + stride];

                    for (auto& [k, src] : src_map) {
                        AggState& dst = dst_map[k];
                        if (dst.cik_bits.empty()) dst.cik_bits.assign(cw, 0ULL);
                        dst.sum += src.sum;
                        dst.cnt += src.cnt;
                        // Bitwise OR merge: O(cik_words) = O(54) ops, no allocation
                        for (size_t j = 0; j < cw; j++)
                            dst.cik_bits[j] |= src.cik_bits[j];
                    }
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
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        size_t cw = cik_words;

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
        final_agg.reserve(global_agg.size());

        for (auto& [gk, g] : global_agg) {
            if (g.cik_bits.empty()) continue;

            int16_t sic     = (int16_t)(gk >> 32);
            int32_t tv_code = (int32_t)(gk & 0xFFFFFFFF);

            uint32_t off0 = tlabel_offsets.data[tv_code];
            uint32_t off1 = tlabel_offsets.data[tv_code + 1];
            string tlabel_str(tlabel_data + off0, off1 - off0);

            SicTlabel key{sic, move(tlabel_str)};
            AggState2& dst = final_agg[key];
            if (dst.cik_bits.empty()) dst.cik_bits.assign(cw, 0ULL);
            dst.sum += g.sum;
            dst.cnt += g.cnt;
            for (size_t j = 0; j < cw; j++)
                dst.cik_bits[j] |= g.cik_bits[j];
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
            for (uint64_t w : g.cik_bits) cik_count += (int64_t)__builtin_popcountll(w);
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
