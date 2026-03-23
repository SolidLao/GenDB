// Q4 iter_0: Columnar format, parallel morsel-driven scan with zone maps
// KEY FIX: The SQL uses INNER JOIN pre (not semi-join), so each num row is
// multiplied by the count of matching pre rows with stmt='EQ'.
// We build eq_count_map: (adsh_code,tagver_code) → count of EQ pre rows.
// In the scan: sum += value * count; cnt += count; ciks.insert(cik) once.

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
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

    // Increment count for key (insert with count=1 if new)
    inline void increment(uint64_t key) {
        size_t idx = mix(key) & mask_;
        while (slots[idx].key != EMPTY && slots[idx].key != key)
            idx = (idx + 1) & mask_;
        if (slots[idx].key == EMPTY) slots[idx].key = key;
        slots[idx].val++;
    }

    // Returns 0 if key not present, else the count
    inline uint32_t get(uint64_t key) const {
        size_t idx = mix(key) & mask_;
        while (slots[idx].key != EMPTY && slots[idx].key != key)
            idx = (idx + 1) & mask_;
        return slots[idx].key == key ? slots[idx].val : 0;
    }
};

// ---------------------------------------------------------------------------
// Load code dict: uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }
// ---------------------------------------------------------------------------
static int8_t load_dict_code(const string& path, const char* key_str) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    uint8_t N;
    fread(&N, 1, 1, f);
    for (int i = 0; i < (int)N; i++) {
        int8_t code;  uint8_t slen;
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
// Natural alignment gives sizeof = 12 bytes.
// ---------------------------------------------------------------------------
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    // 2 bytes padding (natural alignment)
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap must be 12 bytes");

// ---------------------------------------------------------------------------
// Aggregation state
// ---------------------------------------------------------------------------
struct AggState {
    double   sum   = 0.0;
    int64_t  cnt   = 0;
    unordered_set<int32_t> ciks;
};

// ---------------------------------------------------------------------------
// Output row (post-HAVING)
// ---------------------------------------------------------------------------
struct OutputRow {
    int16_t  sic;
    string   tlabel;
    int64_t  num_companies;
    double   total_value;
    double   avg_value;
};

// ---------------------------------------------------------------------------
// Write CSV field (quote if contains comma/double-quote/newline)
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
    const string gendb_dir  = argv[1];
    const string results_dir = argv[2];

    int num_threads = (int)thread::hardware_concurrency();
    if (num_threads < 1) num_threads = 1;
    if (num_threads > 64) num_threads = 64;

    // -----------------------------------------------------------------------
    // Phase: data_loading
    // -----------------------------------------------------------------------
    int8_t usd_code, eq_code;
    {
        GENDB_PHASE("data_loading");
        usd_code = load_dict_code(gendb_dir + "/indexes/uom_codes.bin",  "USD");
        eq_code  = load_dict_code(gendb_dir + "/indexes/stmt_codes.bin", "EQ");
    }

    static const size_t SUB_N = 86135;
    static const size_t TAG_N = 1070662;

    // Load sub arrays (sic, cik) — O(1) lookup by adsh_code
    MmapColumn<int16_t> sub_sic(gendb_dir + "/sub/sic.bin");
    MmapColumn<int32_t> sub_cik(gendb_dir + "/sub/cik.bin");

    // Load tag arrays — O(1) lookup by tagver_code
    MmapColumn<int8_t>   tag_abstract  (gendb_dir + "/tag/abstract.bin");
    MmapColumn<uint32_t> tlabel_offsets(gendb_dir + "/tag/tlabel_offsets.bin");

    // mmap tlabel_data
    int tlabel_fd = open((gendb_dir + "/tag/tlabel_data.bin").c_str(), O_RDONLY);
    struct stat tlabel_st;
    fstat(tlabel_fd, &tlabel_st);
    const char* tlabel_data = (const char*)mmap(nullptr, tlabel_st.st_size,
                                                  PROT_READ, MAP_PRIVATE, tlabel_fd, 0);
    ::close(tlabel_fd);

    // Load zone maps
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

    // mmap num columns
    MmapColumn<int8_t>  num_uom  (gendb_dir + "/num/uom_code.bin");
    MmapColumn<int32_t> num_adsh (gendb_dir + "/num/adsh_code.bin");
    MmapColumn<int32_t> num_tagver(gendb_dir + "/num/tagver_code.bin");
    MmapColumn<double>  num_value(gendb_dir + "/num/value.bin");

    const size_t NUM_N    = num_uom.count;
    const size_t BLOCK_SZ = 100000;

    mmap_prefetch_all(num_uom, num_adsh, num_tagver, num_value);

    // -----------------------------------------------------------------------
    // Phase: build_joins — build eq_count_map from pre columns (parallel)
    // eq_count_map: (adsh_code, tagver_code) → count of pre rows with stmt='EQ'
    // This implements the INNER JOIN semantics (not semi-join).
    // -----------------------------------------------------------------------
    FlatCountMap eq_count_map(2200000);
    {
        GENDB_PHASE("build_joins");

        MmapColumn<int32_t> pre_adsh  (gendb_dir + "/pre/adsh_code.bin");
        MmapColumn<int32_t> pre_tagver(gendb_dir + "/pre/tagver_code.bin");
        MmapColumn<int8_t>  pre_stmt  (gendb_dir + "/pre/stmt_code.bin");
        const size_t PRE_N = pre_adsh.count;

        // Parallel scan: each thread collects (key, count) increments
        // Use thread-local vectors of keys, then merge
        int pre_threads = num_threads;
        vector<vector<uint64_t>> thread_keys(pre_threads);

        auto pre_worker = [&](int tid) {
            size_t start = (size_t)tid * PRE_N / pre_threads;
            size_t end   = (size_t)(tid + 1) * PRE_N / pre_threads;
            auto& keys = thread_keys[tid];
            keys.reserve((end - start) / 8);
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
        pre_thrs.reserve(pre_threads);
        for (int i = 0; i < pre_threads; i++) pre_thrs.emplace_back(pre_worker, i);
        for (auto& t : pre_thrs) t.join();

        // Single-threaded merge: increment count for each key occurrence
        for (auto& keys : thread_keys)
            for (uint64_t k : keys)
                eq_count_map.increment(k);
    }

    // -----------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven scan with zone-map filtering
    // For each passing num row: sum += value * pre_count; cnt += pre_count
    // Group key: ((uint64_t)(uint16_t)sic << 32) | (uint32_t)tagver_code
    // -----------------------------------------------------------------------
    using AggMap = unordered_map<uint64_t, AggState>;
    vector<AggMap> thread_maps(num_threads);

    {
        GENDB_PHASE("main_scan");

        atomic<uint32_t> next_block{0};

        auto scan_worker = [&](int tid) {
            AggMap& agg = thread_maps[tid];
            agg.reserve(4096);

            while (true) {
                uint32_t b = next_block.fetch_add(1, memory_order_relaxed);
                if (b >= n_blocks) break;

                // Zone map: skip block if uom_code definitely != usd_code
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

                    // 3. sic BETWEEN 4000 AND 4999
                    int32_t ac = adsh[i];
                    if ((uint32_t)ac >= SUB_N) continue;
                    int16_t sic = sub_sic.data[ac];
                    if (sic < 4000 || sic > 4999) continue;

                    // 4. abstract == 0
                    if ((uint32_t)tv >= TAG_N) continue;
                    if (tag_abstract.data[tv] != 0) continue;

                    // 5. eq_count probe (INNER JOIN: count matching pre rows)
                    uint64_t eq_key = ((uint64_t)(uint32_t)ac << 32) | (uint32_t)tv;
                    uint32_t eq_cnt = eq_count_map.get(eq_key);
                    if (eq_cnt == 0) continue;

                    // Accumulate: each num row appears eq_cnt times in the result
                    int32_t cik = sub_cik.data[ac];
                    uint64_t gk = ((uint64_t)(uint16_t)sic << 32) | (uint32_t)tv;
                    AggState& g = agg[gk];
                    g.sum += val[i] * (double)eq_cnt;  // multiply by join multiplicity
                    g.cnt += (int64_t)eq_cnt;           // count each (num, pre) pair
                    g.ciks.insert(cik);                 // cik distinct: insert once
                }
            }
        };

        vector<thread> thrs;
        thrs.reserve(num_threads);
        for (int i = 0; i < num_threads; i++) thrs.emplace_back(scan_worker, i);
        for (auto& t : thrs) t.join();
    }

    // -----------------------------------------------------------------------
    // Phase: merge_aggregates — merge thread-local maps into global
    // -----------------------------------------------------------------------
    AggMap global_agg;
    {
        // Move the largest map to global_agg
        int largest = 0;
        for (int i = 1; i < num_threads; i++)
            if (thread_maps[i].size() > thread_maps[largest].size()) largest = i;
        global_agg = move(thread_maps[largest]);

        for (int i = 0; i < num_threads; i++) {
            if (i == largest) continue;
            for (auto& [k, src] : thread_maps[i]) {
                AggState& dst = global_agg[k];
                dst.sum += src.sum;
                dst.cnt += src.cnt;
                for (int32_t c : src.ciks) dst.ciks.insert(c);
            }
            thread_maps[i].clear();
        }
    }

    // -----------------------------------------------------------------------
    // Phase: decode_tlabel + re-aggregate by (sic, tlabel) + HAVING + sort
    // Different tagver_codes can share the same tlabel string; merge them.
    // -----------------------------------------------------------------------
    vector<OutputRow> results;
    {
        GENDB_PHASE("output");

        // Second-pass aggregation by (sic, tlabel_string)
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
            double  sum = 0.0;
            int64_t cnt = 0;
            unordered_set<int32_t> ciks;
        };

        unordered_map<SicTlabel, AggState2, SicTlabelHash> final_agg;
        final_agg.reserve(global_agg.size());

        for (auto& [gk, g] : global_agg) {
            int16_t sic     = (int16_t)(gk >> 32);
            int32_t tv_code = (int32_t)(gk & 0xFFFFFFFF);

            // Decode tlabel from offsets + data
            uint32_t off0 = tlabel_offsets.data[tv_code];
            uint32_t off1 = tlabel_offsets.data[tv_code + 1];
            string   tlabel(tlabel_data + off0, off1 - off0);

            SicTlabel key{sic, move(tlabel)};
            AggState2& dst = final_agg[key];
            dst.sum += g.sum;
            dst.cnt += g.cnt;
            for (int32_t c : g.ciks) dst.ciks.insert(c);
        }

        // HAVING, sort, limit
        results.reserve(final_agg.size());
        for (auto& [k, g] : final_agg) {
            if ((int64_t)g.ciks.size() < 2) continue;
            double avg = g.cnt > 0 ? g.sum / (double)g.cnt : 0.0;
            results.push_back({k.sic, k.tlabel, (int64_t)g.ciks.size(), g.sum, avg});
        }

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
