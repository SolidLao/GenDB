/*
 * Q4 - Four-way join with GROUP BY, HAVING COUNT(DISTINCT), SUM, AVG
 * Strategy:
 *   - Pre-built mmap indexes: sub_adsh_hash, tag_tv_hash
 *   - Runtime pre_eq_hash (stmt='EQ', zone-map-guided build)
 *   - Parallel morsel-driven scan of num with uom zone map
 *   - Thread-local hash aggregation, post-barrier merge
 *   - C29: int64_t cents accumulation for SUM/AVG precision
 */

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <omp.h>

#include "timing_utils.h"

// ── Utilities ────────────────────────────────────────────────────────────────

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    if (!f.is_open()) { std::cerr << "Cannot open dict: " << path << "\n"; exit(1); }
    std::string line;
    while (std::getline(f, line)) dict.push_back(line);
    return dict;
}

static int16_t find_code_i16(const std::vector<std::string>& dict, const std::string& val) {
    for (int i = 0; i < (int)dict.size(); i++)
        if (dict[i] == val) return (int16_t)i;
    return -1;
}

static void* mmap_file(const std::string& path, size_t& sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    sz = st.st_size;
    if (sz == 0) { close(fd); return nullptr; }
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, (off_t)sz, POSIX_FADV_SEQUENTIAL);
    close(fd);
    return p;
}

// ── Zone map: [uint32_t num_blocks][ZoneMapBlock × num_blocks] ───────────────
// ZoneMapBlock = {int16_t min_val, int16_t max_val, uint32_t row_count}
struct ZoneMapBlock {
    int16_t  min_val;
    int16_t  max_val;
    uint32_t row_count;
};

// ── Pre-built index structs ───────────────────────────────────────────────────

// sub_adsh_hash: [uint64_t cap][SubAdhSlot × cap]
// Sentinel: adsh_code == INT32_MIN
struct SubAdhSlot {
    int32_t adsh_code;
    int32_t row_id;
};

// tag_tv_hash: [uint64_t cap][TagTVSlot × cap]
// Sentinel: tag_code == INT32_MIN
struct TagTVSlot {
    int32_t tag_code;
    int32_t version_code;
    int32_t row_id;
    int32_t _pad;
};

// ── Runtime pre EQ hash MAP (count per key) ──────────────────────────────────
// Key: (adsh_code, tag_code, version_code), sentinel adsh_code == INT32_MIN
// Value: count of pre rows with stmt='EQ' for this key (for correct inner join)
// Unique keys ~1,069,672 → cap = next_pow2(1069672 * 2) = 4194304 (≤50% load)
static constexpr uint32_t PRE_EQ_CAP  = 4194304u;
static constexpr uint32_t PRE_EQ_MASK = PRE_EQ_CAP - 1;

struct PreEQSlot {
    int32_t adsh_code;   // INT32_MIN = empty
    int32_t tag_code;
    int32_t version_code;
    int32_t count;       // number of matching pre rows (for inner join multiplicity)
};

static inline uint64_t pre_hash3(int32_t a, int32_t t, int32_t v) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL;
    h ^= (uint64_t)(uint32_t)t * 40503ULL;
    h ^= (uint64_t)(uint32_t)v * 104395301ULL;
    return h;
}

// ── sub_adsh_hash probe: h = adsh_code * 2654435761 ─────────────────────────
static inline uint64_t sub_hash1(int32_t adsh_code) {
    return (uint64_t)(uint32_t)adsh_code * 2654435761ULL;
}

// ── tag_tv_hash probe: h = tag_code*2654435761 ^ version_code*40503 ──────────
static inline uint64_t tag_tv_hash2(int32_t tag_code, int32_t version_code) {
    return (uint64_t)(uint32_t)tag_code * 2654435761ULL
         ^ (uint64_t)(uint32_t)version_code * 40503ULL;
}

// ── Filtered sub hash (adsh_code → {sic, cik}) ──────────────────────────────
// Cap=16384 = next_pow2(5300*2); slot=16B; total=256KB → fits L2 cache (C9)
static constexpr uint32_t FSUB_CAP  = 16384u;
static constexpr uint32_t FSUB_MASK = FSUB_CAP - 1u;

struct FSSubSlot {
    int32_t adsh_code; // INT32_MIN = empty
    int32_t sic;
    int32_t cik;
    int32_t _pad;
};

// ── Aggregation ───────────────────────────────────────────────────────────────
// Key: packed uint64 = (uint32_t(sic) << 32) | uint32_t(tlabel_code)
// Value: sum_cents (C29), count, ciks vector (allow duplicates; sort+unique at output)

struct AggValue {
    int64_t sum_cents = 0;
    int64_t count     = 0;
    std::vector<int32_t> ciks; // duplicates OK; deduped at output
};

static inline uint64_t make_agg_key(int32_t sic, int32_t tlabel_code) {
    return ((uint64_t)(uint32_t)sic << 32) | (uint32_t)tlabel_code;
}

// ── CSV quoting helper ────────────────────────────────────────────────────────
static std::string csv_quote(const std::string& s) {
    bool need_quote = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { need_quote = true; break; }
    }
    if (!need_quote) return s;
    std::string out = "\"";
    for (char c : s) { if (c == '"') out += '"'; out += c; }
    out += '"';
    return out;
}

// ── Main query ────────────────────────────────────────────────────────────────
void run_q4(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Load dictionaries ────────────────────────────────────────────────────
    auto uom_dict    = load_dict(gendb_dir + "/num/uom_dict.txt");
    auto stmt_dict   = load_dict(gendb_dir + "/pre/stmt_dict.txt");
    auto tlabel_dict = load_dict(gendb_dir + "/tag/tlabel_dict.txt");

    const int16_t usd_code = find_code_i16(uom_dict, "USD");
    const int16_t eq_code  = find_code_i16(stmt_dict, "EQ");
    if (usd_code < 0) { std::cerr << "USD not found in uom_dict\n"; exit(1); }
    if (eq_code  < 0) { std::cerr << "EQ not found in stmt_dict\n"; exit(1); }

    // ── Data loading ──────────────────────────────────────────────────────────
    const int16_t* num_uom     = nullptr;
    const double*  num_value   = nullptr;
    const int32_t* num_adsh    = nullptr;
    const int32_t* num_tag     = nullptr;
    const int32_t* num_version = nullptr;
    int64_t        num_rows    = 0;

    const int16_t* pre_stmt    = nullptr;
    const int32_t* pre_adsh    = nullptr;
    const int32_t* pre_tag     = nullptr;
    const int32_t* pre_version = nullptr;
    int64_t        pre_rows    = 0;

    const int32_t* sub_sic     = nullptr;
    const int32_t* sub_cik     = nullptr;
    const int32_t* sub_adsh_col = nullptr;
    int64_t        sub_rows     = 0;

    const int32_t* tag_abstract = nullptr;
    const int32_t* tag_tlabel   = nullptr;

    const SubAdhSlot* sub_adsh_ht  = nullptr;
    uint64_t          sub_adsh_cap = 0;

    const TagTVSlot*  tag_tv_ht  = nullptr;
    uint64_t          tag_tv_cap = 0;

    const ZoneMapBlock* num_uom_zm     = nullptr;
    uint32_t            num_uom_nblocks = 0;
    const ZoneMapBlock* pre_stmt_zm    = nullptr;
    uint32_t            pre_stmt_nblocks = 0;

    {
        GENDB_PHASE("data_loading");
        size_t sz;

        // num columns
        num_uom     = (const int16_t*)mmap_file(gendb_dir + "/num/uom.bin",     sz);
        num_rows    = (int64_t)(sz / sizeof(int16_t));
        num_value   = (const double*)  mmap_file(gendb_dir + "/num/value.bin",   sz);
        num_adsh    = (const int32_t*) mmap_file(gendb_dir + "/num/adsh.bin",    sz);
        num_tag     = (const int32_t*) mmap_file(gendb_dir + "/num/tag.bin",     sz);
        num_version = (const int32_t*) mmap_file(gendb_dir + "/num/version.bin", sz);

        // pre columns
        pre_stmt    = (const int16_t*) mmap_file(gendb_dir + "/pre/stmt.bin",    sz);
        pre_rows    = (int64_t)(sz / sizeof(int16_t));
        pre_adsh    = (const int32_t*) mmap_file(gendb_dir + "/pre/adsh.bin",    sz);
        pre_tag     = (const int32_t*) mmap_file(gendb_dir + "/pre/tag.bin",     sz);
        pre_version = (const int32_t*) mmap_file(gendb_dir + "/pre/version.bin", sz);

        // sub columns
        sub_sic     = (const int32_t*) mmap_file(gendb_dir + "/sub/sic.bin",  sz);
        sub_rows    = (int64_t)(sz / sizeof(int32_t));
        sub_cik     = (const int32_t*) mmap_file(gendb_dir + "/sub/cik.bin",  sz);
        sub_adsh_col = (const int32_t*) mmap_file(gendb_dir + "/sub/adsh.bin", sz);

        // tag columns
        tag_abstract = (const int32_t*) mmap_file(gendb_dir + "/tag/abstract.bin", sz);
        tag_tlabel   = (const int32_t*) mmap_file(gendb_dir + "/tag/tlabel.bin",   sz);

        // Pre-built indexes
        {
            void* raw = mmap_file(gendb_dir + "/indexes/sub_adsh_hash.bin", sz);
            sub_adsh_cap = *reinterpret_cast<const uint64_t*>(raw);
            sub_adsh_ht  = reinterpret_cast<const SubAdhSlot*>((const char*)raw + 8);
        }
        {
            void* raw = mmap_file(gendb_dir + "/indexes/tag_tv_hash.bin", sz);
            tag_tv_cap = *reinterpret_cast<const uint64_t*>(raw);
            tag_tv_ht  = reinterpret_cast<const TagTVSlot*>((const char*)raw + 8);
        }

        // Zone maps: [uint32_t num_blocks][ZoneMapBlock × num_blocks]
        {
            void* raw = mmap_file(gendb_dir + "/indexes/num_uom_zone_map.bin", sz);
            num_uom_nblocks = *reinterpret_cast<const uint32_t*>(raw);
            num_uom_zm      = reinterpret_cast<const ZoneMapBlock*>((const char*)raw + 4);
        }
        {
            void* raw = mmap_file(gendb_dir + "/indexes/pre_stmt_zone_map.bin", sz);
            pre_stmt_nblocks = *reinterpret_cast<const uint32_t*>(raw);
            pre_stmt_zm      = reinterpret_cast<const ZoneMapBlock*>((const char*)raw + 4);
        }
    }

    // ── nthreads available for both build and scan phases ────────────────────
    const int nthreads = omp_get_max_threads();

    // ── Build filtered sub hash (sic BETWEEN 4000 AND 4999) ─────────────────
    // 16384 slots × 16B = 256KB → L2 resident; eliminates extra sic/cik lookups
    std::vector<FSSubSlot> fsub_vec(FSUB_CAP);
    FSSubSlot* fsub_ht = fsub_vec.data();
    {
        GENDB_PHASE("build_filtered_sub");
        FSSubSlot empty_fsub{INT32_MIN, 0, 0, 0};
        std::fill(fsub_ht, fsub_ht + FSUB_CAP, empty_fsub);

        for (int64_t i = 0; i < sub_rows; i++) {
            const int32_t sic = sub_sic[i];
            if (sic < 4000 || sic > 4999) continue;
            const int32_t adsh_c = sub_adsh_col[i];
            const int32_t cik    = sub_cik[i];
            uint32_t h = (uint32_t)(sub_hash1(adsh_c) & FSUB_MASK);
            // C24: bounded probing
            for (uint32_t p = 0; p < FSUB_CAP; p++) {
                uint32_t idx = (h + p) & FSUB_MASK;
                if (fsub_ht[idx].adsh_code == INT32_MIN) {
                    fsub_ht[idx] = {adsh_c, sic, cik, 0};
                    break;
                }
            }
        }
    }

    // ── Build pre EQ hash (stmt='EQ', zone-map guided) ───────────────────────
    // P22: raw new (no zero-init for POD) → page faults deferred to parallel fill
    // PRE_EQ_CAP=4194304 * 16B = 64MB
    PreEQSlot* pre_eq_table = new PreEQSlot[PRE_EQ_CAP];
    {
        GENDB_PHASE("build_joins");
        // P22: parallel fill distributes 16K page faults across all cores (~1ms vs ~80ms serial)
        // C20: explicit sentinel via assignment (NOT memset)
        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (uint32_t i = 0; i < PRE_EQ_CAP; i++)
            pre_eq_table[i] = {INT32_MIN, 0, 0, 0};

        static constexpr int64_t BLOCK_SZ = 100000;

        for (uint32_t b = 0; b < pre_stmt_nblocks; b++) {
            // Zone map skip: skip blocks where EQ cannot appear
            if (pre_stmt_zm[b].max_val < eq_code || pre_stmt_zm[b].min_val > eq_code)
                continue;

            int64_t rs = (int64_t)b * BLOCK_SZ;
            int64_t re = rs + (int64_t)pre_stmt_zm[b].row_count;
            if (re > pre_rows) re = pre_rows;

            for (int64_t i = rs; i < re; i++) {
                if (pre_stmt[i] != eq_code) continue;

                int32_t a = pre_adsh[i];
                int32_t t = pre_tag[i];
                int32_t v = pre_version[i];

                uint32_t h = (uint32_t)(pre_hash3(a, t, v) & PRE_EQ_MASK);
                // C24: bounded probing
                for (uint32_t p = 0; p < PRE_EQ_CAP; p++) {
                    uint32_t idx = (h + p) & PRE_EQ_MASK;
                    int32_t  k   = pre_eq_table[idx].adsh_code;
                    if (k == INT32_MIN) {
                        // empty slot — insert new key with count=1
                        pre_eq_table[idx].adsh_code    = a;
                        pre_eq_table[idx].tag_code     = t;
                        pre_eq_table[idx].version_code = v;
                        pre_eq_table[idx].count        = 1;
                        break;
                    }
                    // found existing key — increment count (inner join multiplicity)
                    if (k == a &&
                        pre_eq_table[idx].tag_code     == t &&
                        pre_eq_table[idx].version_code == v) {
                        pre_eq_table[idx].count++;
                        break;
                    }
                }
            }
        }
    }

    // ── Parallel scan of num with thread-local aggregation ───────────────────
    // pre_eq_table no longer needed after build_joins; delete to free 64MB
    // (keep pointer valid — it's used in main_scan below, delete after)
    std::vector<std::unordered_map<uint64_t, AggValue>> thread_maps(nthreads);

    {
        GENDB_PHASE("main_scan");

        static constexpr int64_t BLOCK_SZ = 100000;
        const uint64_t tag_mask = tag_tv_cap - 1;

        #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
        for (uint32_t b = 0; b < num_uom_nblocks; b++) {
            // Zone map skip for uom
            if (num_uom_zm[b].max_val < usd_code || num_uom_zm[b].min_val > usd_code)
                continue;

            const int tid = omp_get_thread_num();
            auto& my_map  = thread_maps[tid];

            int64_t rs = (int64_t)b * BLOCK_SZ;
            int64_t re = rs + (int64_t)num_uom_zm[b].row_count;
            if (re > num_rows) re = num_rows;

            for (int64_t i = rs; i < re; i++) {
                // Filter 1: uom = 'USD'
                if (num_uom[i] != usd_code) continue;

                // Filter 2: value IS NOT NULL
                const double val = num_value[i];
                if (std::isnan(val)) continue;

                const int32_t adsh    = num_adsh[i];
                const int32_t tag_c   = num_tag[i];
                const int32_t ver_c   = num_version[i];

                // ── Join 1: probe filtered sub hash → {sic, cik} ───────────
                // fsub_ht is 256KB (L2-resident); only sic-filtered rows present
                int32_t sic = -1, cik = -1;
                {
                    uint32_t h = (uint32_t)(sub_hash1(adsh) & FSUB_MASK);
                    // C24: bounded probing
                    for (uint32_t p = 0; p < FSUB_CAP; p++) {
                        uint32_t idx = (h + p) & FSUB_MASK;
                        const int32_t k = fsub_ht[idx].adsh_code;
                        if (k == INT32_MIN) break;   // empty → not found
                        if (k == adsh) {
                            sic = fsub_ht[idx].sic;
                            cik = fsub_ht[idx].cik;
                            break;
                        }
                    }
                }
                if (sic < 0) continue;  // not in filtered sub (sic BETWEEN 4000-4999)

                // ── Join 2: probe tag_tv_hash → get tag_row_id ──────────────
                int32_t tag_row = -1;
                {
                    uint64_t h = tag_tv_hash2(tag_c, ver_c) & tag_mask;
                    // C24: bounded probing
                    for (uint64_t p = 0; p < tag_tv_cap; p++) {
                        uint64_t idx = (h + p) & tag_mask;
                        const int32_t k = tag_tv_ht[idx].tag_code;
                        if (k == INT32_MIN) break;
                        if (k == tag_c && tag_tv_ht[idx].version_code == ver_c) {
                            tag_row = tag_tv_ht[idx].row_id;
                            break;
                        }
                    }
                }
                if (tag_row < 0) continue;

                // Filter 4: abstract = 0
                if (tag_abstract[tag_row] != 0) continue;
                const int32_t tlabel_code = tag_tlabel[tag_row];

                // ── Join 3: probe pre_eq_hash (stmt='EQ') ────────────────────
                // pre_count: number of pre rows with this (adsh,tag,version,stmt='EQ')
                // SQL inner join produces pre_count output tuples per num row → multiply
                int32_t pre_count = 0;
                {
                    uint32_t h = (uint32_t)(pre_hash3(adsh, tag_c, ver_c) & PRE_EQ_MASK);
                    // C24: bounded probing
                    for (uint32_t p = 0; p < PRE_EQ_CAP; p++) {
                        uint32_t idx = (h + p) & PRE_EQ_MASK;
                        const int32_t k = pre_eq_table[idx].adsh_code;
                        if (k == INT32_MIN) break;
                        if (k == adsh &&
                            pre_eq_table[idx].tag_code     == tag_c &&
                            pre_eq_table[idx].version_code == ver_c) {
                            pre_count = pre_eq_table[idx].count;
                            break;
                        }
                    }
                }
                if (pre_count == 0) continue;

                // ── Aggregate ─────────────────────────────────────────────────
                // C29: accumulate as int64 cents, multiply by pre_count for inner join
                const uint64_t key = make_agg_key(sic, tlabel_code);
                AggValue& agg = my_map[key];
                agg.sum_cents += llround(val * 100.0) * pre_count;
                agg.count     += pre_count;
                agg.ciks.push_back(cik);  // DISTINCT: deduped via sort+unique at output
            }
        } // parallel for
    }

    // pre_eq_table no longer needed; free 64MB
    delete[] pre_eq_table;

    // ── Merge thread-local aggregation maps ──────────────────────────────────
    std::unordered_map<uint64_t, AggValue> global_map;
    global_map.reserve(8192);
    {
        GENDB_PHASE("aggregation_merge");
        for (auto& tm : thread_maps) {
            for (auto& [k, v] : tm) {
                AggValue& g = global_map[k];
                g.sum_cents += v.sum_cents;
                g.count     += v.count;
                // vector append (memcpy-speed) vs unordered_set insert per element
                g.ciks.insert(g.ciks.end(), v.ciks.begin(), v.ciks.end());
            }
        }
    }

    // ── Apply HAVING COUNT(DISTINCT cik) >= 2 and collect results ────────────
    struct ResultRow {
        int32_t sic;
        int32_t tlabel_code;
        int64_t num_companies;
        int64_t sum_cents;
        int64_t count;
    };

    std::vector<ResultRow> results;
    results.reserve(global_map.size());
    for (auto& [k, v] : global_map) {
        // Sort+unique for COUNT(DISTINCT cik)
        std::sort(v.ciks.begin(), v.ciks.end());
        auto ue = std::unique(v.ciks.begin(), v.ciks.end());
        int64_t num_distinct = (int64_t)std::distance(v.ciks.begin(), ue);
        if (num_distinct < 2) continue;
        int32_t sic         = (int32_t)((k >> 32) & 0xFFFFFFFFu);
        int32_t tlabel_code = (int32_t)(k & 0xFFFFFFFFu);
        results.push_back({sic, tlabel_code, num_distinct, v.sum_cents, v.count});
    }

    // ── Sort by total_value DESC, LIMIT 500 ──────────────────────────────────
    {
        GENDB_PHASE("sort_topk");
        int keep = std::min((int)results.size(), 500);
        std::partial_sort(results.begin(), results.begin() + keep, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                return a.sum_cents > b.sum_cents;
            });
        results.resize(keep);
    }

    // ── Output CSV ────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        std::ofstream out(results_dir + "/Q4.csv");
        if (!out.is_open()) { std::cerr << "Cannot open output file\n"; exit(1); }

        out << "sic,tlabel,stmt,num_companies,total_value,avg_value\n";

        for (const auto& r : results) {
            // Decode tlabel (C18)
            std::string tlabel_str;
            if (r.tlabel_code >= 0 && (size_t)r.tlabel_code < tlabel_dict.size())
                tlabel_str = csv_quote(tlabel_dict[r.tlabel_code]);
            else
                tlabel_str = "";

            // C29: total_value as exact int64/100 with sign handling
            int64_t abs_cents = (r.sum_cents >= 0) ? r.sum_cents : -r.sum_cents;
            long long whole = (long long)(abs_cents / 100);
            long long frac  = (long long)(abs_cents % 100);

            // avg_value: (double)sum_cents / count / 100.0
            double avg_val = (double)r.sum_cents / (double)r.count / 100.0;

            out << r.sic << ","
                << tlabel_str << ","
                << "EQ" << ","
                << r.num_companies << ",";

            // total_value with sign
            if (r.sum_cents < 0) out << "-";
            out << whole << "." << (frac < 10 ? "0" : "")
                << frac << ",";

            out << std::fixed << std::setprecision(2) << avg_val << "\n";
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q4(gendb_dir, results_dir);
    return 0;
}
#endif
