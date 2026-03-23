// Q4 — SEC EDGAR
// SELECT s.sic, t.tlabel, p.stmt,
//        COUNT(DISTINCT s.cik) AS num_companies,
//        SUM(n.value) AS total_value,
//        AVG(n.value) AS avg_value
// FROM num n
// JOIN sub s ON n.adsh = s.adsh
// JOIN tag t ON n.tag = t.tag AND n.version = t.version
// JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
// WHERE n.uom = 'USD' AND p.stmt = 'EQ'
//       AND s.sic BETWEEN 4000 AND 4999
//       AND n.value IS NOT NULL AND t.abstract = 0
// GROUP BY s.sic, t.tlabel, p.stmt
// HAVING COUNT(DISTINCT s.cik) >= 2
// ORDER BY total_value DESC LIMIT 500;

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <climits>
#include <cassert>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <filesystem>

#include "timing_utils.h"

// ============================================================
// mmap helper
// ============================================================
static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return ptr;
}

// ============================================================
// Binary dict loader: [n:uint32][len:uint16, bytes...]*n
// ============================================================
static std::vector<std::string> load_binary_dict(const std::string& path) {
    size_t sz;
    const uint8_t* d = (const uint8_t*)mmap_file(path, sz);
    uint32_t n = *(const uint32_t*)d;
    std::vector<std::string> dict;
    dict.reserve(n);
    size_t off = 4;
    for (uint32_t i = 0; i < n; i++) {
        uint16_t len = *(const uint16_t*)(d + off); off += 2;
        dict.emplace_back((const char*)(d + off), len); off += len;
    }
    munmap((void*)d, sz);
    return dict;
}

// ============================================================
// CSV field writing with quoting
// ============================================================
static void write_csv_field(FILE* fp, const char* data, size_t len) {
    bool needs_quote = false;
    for (size_t i = 0; i < len; i++) {
        char c = data[i];
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            needs_quote = true;
            break;
        }
    }
    if (needs_quote) {
        fputc('"', fp);
        for (size_t i = 0; i < len; i++) {
            if (data[i] == '"') fputc('"', fp);
            fputc(data[i], fp);
        }
        fputc('"', fp);
    } else {
        fwrite(data, 1, len, fp);
    }
}

// ============================================================
// Pre key entry for binary search
// ============================================================
#pragma pack(push, 1)
struct PreKeyEntry {
    int32_t adsh, tag, ver, row_id;
};
#pragma pack(pop)
static_assert(sizeof(PreKeyEntry) == 16, "PreKeyEntry must be 16 bytes");

struct PreKeyCmp {
    bool operator()(const PreKeyEntry& a, const PreKeyEntry& b) const {
        if (a.adsh != b.adsh) return a.adsh < b.adsh;
        if (a.tag  != b.tag)  return a.tag  < b.tag;
        return a.ver < b.ver;
    }
};

// ============================================================
// Tag join map — open addressing, 2^21 slots, 16 bytes/slot
// Key = (uint64_t)(uint32_t)tag_code << 32 | (uint32_t)ver_code
// Empty sentinel = UINT64_MAX
// ============================================================
static constexpr uint64_t TJM_EMPTY = UINT64_MAX;
static constexpr uint32_t TJM_CAP   = 1u << 21;  // 2,097,152
static constexpr uint32_t TJM_MASK  = TJM_CAP - 1;

struct TJMSlot {
    uint64_t key;           // TJM_EMPTY = empty
    int32_t  tlabel_code;   // code into tlabel_strings[] for grouping
    int8_t   abstract_val;
    uint8_t  _pad[3];
};  // 16 bytes

static inline uint32_t tjm_hash(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33;
    return (uint32_t)(key & TJM_MASK);
}

// ============================================================
// CIK entry for distinct counting (sort-based)
// group_key = (int64_t)sic << 32 | (uint32_t)tag_row_id
// ============================================================
struct CIKEntry {
    int64_t group_key;
    int32_t cik;
    int32_t _pad;
};  // 16 bytes

// ============================================================
// Zonemap entry for num blocks (packed, 10 bytes)
// Layout: [int8 uom_min][int8 uom_max][int32 ddate_min][int32 ddate_max]
// ============================================================
#pragma pack(push, 1)
struct NumZoneEntry {
    int8_t  uom_min;
    int8_t  uom_max;
    int32_t ddate_min;
    int32_t ddate_max;
};  // 10 bytes packed
#pragma pack(pop)
static_assert(sizeof(NumZoneEntry) == 10, "NumZoneEntry must be 10 bytes");

// ============================================================
// Main
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir  = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // --------------------------------------------------------
    // Phase 1: Data loading
    // --------------------------------------------------------
    // Load shared dicts
    std::vector<std::string> uom_dict  = load_binary_dict(gendb_dir + "/shared/uom.dict");
    std::vector<std::string> stmt_dict = load_binary_dict(gendb_dir + "/shared/stmt.dict");
    std::vector<std::string> tag_numpre_dict = load_binary_dict(gendb_dir + "/shared/tag_numpre.dict");
    std::vector<std::string> ver_numpre_dict = load_binary_dict(gendb_dir + "/shared/version_numpre.dict");

    // Find USD and EQ codes
    int8_t usd_code = -1, eq_code = -1;
    for (size_t i = 0; i < uom_dict.size(); i++)
        if (uom_dict[i] == "USD") { usd_code = (int8_t)i; break; }
    for (size_t i = 0; i < stmt_dict.size(); i++)
        if (stmt_dict[i] == "EQ") { eq_code = (int8_t)i; break; }
    if (usd_code < 0 || eq_code < 0) {
        fprintf(stderr, "Failed to find USD or EQ in dicts\n"); return 1;
    }

    // mmap num columns
    const int8_t*  num_uom;
    const double*  num_val;
    const int32_t* num_adsh;
    const int32_t* num_tag;
    const int32_t* num_ver;
    int64_t num_N;

    {
        GENDB_PHASE("data_loading");

        size_t s1, s2, s3, s4, s5;
        num_uom  = (const int8_t* )mmap_file(gendb_dir + "/num/uom.bin",     s1);
        num_val  = (const double*  )mmap_file(gendb_dir + "/num/value.bin",   s2);
        num_adsh = (const int32_t* )mmap_file(gendb_dir + "/num/adsh.bin",    s3);
        num_tag  = (const int32_t* )mmap_file(gendb_dir + "/num/tag.bin",     s4);
        num_ver  = (const int32_t* )mmap_file(gendb_dir + "/num/version.bin", s5);
        num_N = (int64_t)(s1 / sizeof(int8_t));

        // Prefetch num columns
        madvise((void*)num_uom,  s1, MADV_SEQUENTIAL);
        madvise((void*)num_val,  s2, MADV_SEQUENTIAL);
        madvise((void*)num_adsh, s3, MADV_SEQUENTIAL);
        madvise((void*)num_tag,  s4, MADV_SEQUENTIAL);
        madvise((void*)num_ver,  s5, MADV_SEQUENTIAL);
    }

    // --------------------------------------------------------
    // Phase 2: Build sub arrays
    // --------------------------------------------------------
    static constexpr int32_t SUB_N = 86135;
    // Bitset for valid adsh codes (sic in [4000,4999])
    static constexpr int32_t BITSET_WORDS = (SUB_N + 63) / 64;
    uint64_t sub_valid[BITSET_WORDS] = {};

    const int16_t* sub_sic_col;
    const int32_t* sub_cik_col;
    int16_t sub_sic_arr[SUB_N];
    int32_t sub_cik_arr[SUB_N];

    {
        GENDB_PHASE("build_sub_arrays");

        size_t sic_sz, cik_sz;
        sub_sic_col = (const int16_t*)mmap_file(gendb_dir + "/sub/sic.bin", sic_sz);
        sub_cik_col = (const int32_t*)mmap_file(gendb_dir + "/sub/cik.bin", cik_sz);

        int32_t sub_rows = (int32_t)(sic_sz / sizeof(int16_t));
        for (int32_t i = 0; i < sub_rows && i < SUB_N; i++) {
            int16_t sic = sub_sic_col[i];
            int32_t cik = sub_cik_col[i];
            sub_sic_arr[i] = sic;
            sub_cik_arr[i] = cik;
            if (sic >= 4000 && sic <= 4999) {
                sub_valid[i >> 6] |= (1ULL << (i & 63));
            }
        }
    }

    // --------------------------------------------------------
    // Phase 3: Build tag join map
    // --------------------------------------------------------
    // Allocate tag_join_map: TJM_CAP slots × 16 bytes = 32 MB
    std::vector<TJMSlot> tag_join_map(TJM_CAP);
    for (auto& s : tag_join_map) { s.key = TJM_EMPTY; }

    // mmap tag varlen columns + abstract
    const int64_t* tag_tag_offsets;
    const char*    tag_tag_data;
    const int64_t* tag_ver_offsets;
    const char*    tag_ver_data;
    const int8_t*  tag_abstract;
    const int64_t* tag_tlabel_offsets;
    const char*    tag_tlabel_data;
    size_t tag_tag_off_sz, tag_tag_dat_sz;
    size_t tag_ver_off_sz, tag_ver_dat_sz;
    size_t tag_abs_sz;
    size_t tag_tlabel_off_sz, tag_tlabel_dat_sz;

    // tlabel strings indexed by tlabel_code (for output decoding)
    std::vector<std::string> tlabel_strings;

    {
        GENDB_PHASE("build_tag_join_map");

        tag_tag_offsets  = (const int64_t*)mmap_file(gendb_dir + "/tag/tag.offsets",     tag_tag_off_sz);
        tag_tag_data     = (const char*   )mmap_file(gendb_dir + "/tag/tag.data",         tag_tag_dat_sz);
        tag_ver_offsets  = (const int64_t*)mmap_file(gendb_dir + "/tag/version.offsets",  tag_ver_off_sz);
        tag_ver_data     = (const char*   )mmap_file(gendb_dir + "/tag/version.data",     tag_ver_dat_sz);
        tag_abstract     = (const int8_t* )mmap_file(gendb_dir + "/tag/abstract.bin",     tag_abs_sz);
        tag_tlabel_offsets = (const int64_t*)mmap_file(gendb_dir + "/tag/tlabel.offsets", tag_tlabel_off_sz);
        tag_tlabel_data    = (const char*   )mmap_file(gendb_dir + "/tag/tlabel.data",    tag_tlabel_dat_sz);

        int32_t tag_rows = (int32_t)(tag_abs_sz / sizeof(int8_t));

        // Build inverse maps: string → shared dict code
        std::unordered_map<std::string, int32_t> inv_tag, inv_ver;
        inv_tag.reserve(tag_numpre_dict.size() * 2);
        inv_ver.reserve(ver_numpre_dict.size() * 2);
        for (int32_t i = 0; i < (int32_t)tag_numpre_dict.size(); i++)
            inv_tag.emplace(tag_numpre_dict[i], i);
        for (int32_t j = 0; j < (int32_t)ver_numpre_dict.size(); j++)
            inv_ver.emplace(ver_numpre_dict[j], j);

        // Build tlabel string → tlabel_code dict
        // GROUP BY s.sic, t.tlabel: must group by tlabel content, not tag_row_id
        std::unordered_map<std::string, int32_t> tlabel_to_code;
        tlabel_to_code.reserve(200000);
        tlabel_strings.reserve(200000);

        // Iterate tag table rows, populate tag_join_map
        for (int32_t row = 0; row < tag_rows; row++) {
            int8_t abs_val = tag_abstract[row];

            // Get tag string
            int64_t t0 = tag_tag_offsets[row], t1 = tag_tag_offsets[row + 1];
            std::string tag_str(tag_tag_data + t0, (size_t)(t1 - t0));

            // Get version string
            int64_t v0 = tag_ver_offsets[row], v1 = tag_ver_offsets[row + 1];
            std::string ver_str(tag_ver_data + v0, (size_t)(v1 - v0));

            // Look up in inverse maps
            auto ti = inv_tag.find(tag_str);
            if (ti == inv_tag.end()) continue;
            auto vi = inv_ver.find(ver_str);
            if (vi == inv_ver.end()) continue;

            int32_t tag_code = ti->second;
            int32_t ver_code = vi->second;

            // Get tlabel string and assign code
            int64_t tl0 = tag_tlabel_offsets[row], tl1 = tag_tlabel_offsets[row + 1];
            std::string tlabel_str(tag_tlabel_data + tl0, (size_t)(tl1 - tl0));

            auto [tlabel_it, inserted] = tlabel_to_code.emplace(tlabel_str, (int32_t)tlabel_strings.size());
            int32_t tlabel_code = tlabel_it->second;
            if (inserted) tlabel_strings.push_back(std::move(tlabel_str));

            uint64_t key = ((uint64_t)(uint32_t)tag_code << 32) | (uint32_t)ver_code;
            uint32_t pos = tjm_hash(key);

            for (uint32_t probe = 0; probe < TJM_CAP; probe++) {
                uint32_t slot = (pos + probe) & TJM_MASK;
                if (tag_join_map[slot].key == TJM_EMPTY) {
                    tag_join_map[slot].key          = key;
                    tag_join_map[slot].tlabel_code  = tlabel_code;
                    tag_join_map[slot].abstract_val = abs_val;
                    break;
                }
                if (tag_join_map[slot].key == key) {
                    // Duplicate (tag, version) pair — shouldn't happen
                    break;
                }
            }
        }
    }

    // --------------------------------------------------------
    // Load pre_key_sorted index and pre/stmt.bin
    // --------------------------------------------------------
    size_t pre_sorted_sz, pre_stmt_sz;
    const uint8_t*      pre_sorted_raw = (const uint8_t*)mmap_file(gendb_dir + "/indexes/pre_key_sorted.bin", pre_sorted_sz);
    const int8_t*       pre_stmt       = (const int8_t* )mmap_file(gendb_dir + "/pre/stmt.bin", pre_stmt_sz);

    uint32_t n_pre_sorted = *(const uint32_t*)pre_sorted_raw;
    const PreKeyEntry* pre_sorted = (const PreKeyEntry*)(pre_sorted_raw + 4);

    madvise((void*)pre_sorted_raw, pre_sorted_sz, MADV_RANDOM);
    madvise((void*)pre_stmt, pre_stmt_sz, MADV_RANDOM);

    // --------------------------------------------------------
    // Load num_zonemaps for block-level USD filtering
    // --------------------------------------------------------
    size_t zm_sz;
    const uint8_t* zm_raw = (const uint8_t*)mmap_file(gendb_dir + "/indexes/num_zonemaps.bin", zm_sz);
    int32_t n_blocks = *(const int32_t*)zm_raw;
    const NumZoneEntry* zonemaps = (const NumZoneEntry*)(zm_raw + 4);

    // Pre-compute valid blocks (those containing USD rows, i.e. uom_max >= 0)
    // num sorted by (uom_code as int8_t): negatives first, then 0 (USD), then positives
    // Skip blocks where uom_min > 0 (no USD)
    std::vector<int32_t> valid_blocks;
    valid_blocks.reserve(n_blocks);
    for (int32_t b = 0; b < n_blocks; b++) {
        if (zonemaps[b].uom_max >= 0) {  // could contain USD (code=0)
            // also check: if uom_min > 0, skip; if uom_max < 0, skip
            // uom_max >= 0 already handles both
            valid_blocks.push_back(b);
        }
    }

    // --------------------------------------------------------
    // Phase 4: Parallel scan
    // --------------------------------------------------------
    int nthreads = omp_get_max_threads();
    std::vector<std::unordered_map<int64_t, std::pair<double,int64_t>>> tl_agg(nthreads);
    std::vector<std::vector<CIKEntry>> tl_cik(nthreads);
    for (int t = 0; t < nthreads; t++) {
        tl_agg[t].reserve(4096);
        tl_cik[t].reserve(65536);
    }

    std::atomic<int32_t> block_counter{0};
    int32_t n_valid_blocks = (int32_t)valid_blocks.size();

    // Also need adsh_code upper bound check
    const TJMSlot* tjm = tag_join_map.data();

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local_agg = tl_agg[tid];
            auto& local_cik = tl_cik[tid];

            while (true) {
                int32_t bidx = block_counter.fetch_add(1, std::memory_order_relaxed);
                if (bidx >= n_valid_blocks) break;

                int32_t b = valid_blocks[bidx];
                int64_t row_start = (int64_t)b * 100000;
                int64_t row_end   = std::min(row_start + (int64_t)100000, num_N);

                const NumZoneEntry& zm = zonemaps[b];
                bool all_usd = (zm.uom_min == 0 && zm.uom_max == 0);

                for (int64_t i = row_start; i < row_end; i++) {
                    // Filter: uom == USD
                    if (!all_usd && num_uom[i] != usd_code) continue;

                    // Filter: value IS NOT NULL
                    double v = num_val[i];
                    if (std::isnan(v)) continue;

                    // Sub filter via bitset (adsh_code == sub row_id)
                    int32_t adsh_c = num_adsh[i];
                    if (adsh_c < 0 || adsh_c >= SUB_N) continue;
                    if (!(sub_valid[adsh_c >> 6] & (1ULL << (adsh_c & 63)))) continue;

                    int32_t tag_c = num_tag[i];
                    int32_t ver_c = num_ver[i];

                    // Tag join map probe
                    uint64_t tkey = ((uint64_t)(uint32_t)tag_c << 32) | (uint32_t)ver_c;
                    uint32_t tpos = tjm_hash(tkey);

                    int32_t tlabel_c = -1;
                    for (uint32_t probe = 0; probe < TJM_CAP; probe++) {
                        uint32_t slot = (tpos + probe) & TJM_MASK;
                        uint64_t skey = tjm[slot].key;
                        if (skey == TJM_EMPTY) break;
                        if (skey == tkey) {
                            if (tjm[slot].abstract_val == 0) {
                                tlabel_c = tjm[slot].tlabel_code;
                            }
                            break;
                        }
                    }
                    if (tlabel_c < 0) continue;

                    // Pre join via binary search on pre_key_sorted
                    PreKeyEntry search_key = {adsh_c, tag_c, ver_c, 0};
                    const PreKeyEntry* lb = std::lower_bound(
                        pre_sorted, pre_sorted + n_pre_sorted, search_key, PreKeyCmp{});

                    if (lb == pre_sorted + n_pre_sorted) continue;
                    if (lb->adsh != adsh_c || lb->tag != tag_c || lb->ver != ver_c) continue;

                    // Get sub fields
                    int16_t sic = sub_sic_arr[adsh_c];
                    int32_t cik = sub_cik_arr[adsh_c];
                    int64_t group_key = ((int64_t)sic << 32) | (uint32_t)tlabel_c;

                    // Scan matching pre rows
                    for (const PreKeyEntry* it = lb;
                         it != pre_sorted + n_pre_sorted &&
                         it->adsh == adsh_c && it->tag == tag_c && it->ver == ver_c;
                         ++it) {
                        int32_t pre_row = it->row_id;
                        if (pre_stmt[pre_row] != eq_code) continue;

                        // Aggregate
                        auto& slot_agg = local_agg[group_key];
                        slot_agg.first  += v;
                        slot_agg.second += 1;

                        // Record CIK for distinct counting
                        local_cik.push_back({group_key, cik, 0});
                    }
                }
            }
        }
    }

    // --------------------------------------------------------
    // Phase 5: Merge thread-local aggregates
    // --------------------------------------------------------
    std::unordered_map<int64_t, std::pair<double,int64_t>> global_agg;
    global_agg.reserve(8192);

    std::vector<CIKEntry> all_cik_entries;

    {
        GENDB_PHASE("merge_thread_local_aggregates");

        // Merge aggregates
        for (int t = 0; t < nthreads; t++) {
            for (auto& [k, v] : tl_agg[t]) {
                auto& g = global_agg[k];
                g.first  += v.first;
                g.second += v.second;
            }
        }

        // Collect all CIK entries
        size_t total_cik = 0;
        for (int t = 0; t < nthreads; t++) total_cik += tl_cik[t].size();
        all_cik_entries.reserve(total_cik);
        for (int t = 0; t < nthreads; t++) {
            all_cik_entries.insert(all_cik_entries.end(), tl_cik[t].begin(), tl_cik[t].end());
        }

        // Sort CIK entries by (group_key, cik)
        std::sort(all_cik_entries.begin(), all_cik_entries.end(),
            [](const CIKEntry& a, const CIKEntry& b) {
                if (a.group_key != b.group_key) return a.group_key < b.group_key;
                return a.cik < b.cik;
            });
    }

    // Count distinct CIKs per group
    std::unordered_map<int64_t, int64_t> cik_count;
    cik_count.reserve(8192);
    if (!all_cik_entries.empty()) {
        int64_t cur_gk   = all_cik_entries[0].group_key;
        int32_t prev_cik = INT32_MIN;
        int64_t cnt      = 0;
        for (const CIKEntry& e : all_cik_entries) {
            if (e.group_key != cur_gk) {
                cik_count[cur_gk] = cnt;
                cur_gk   = e.group_key;
                prev_cik = INT32_MIN;
                cnt      = 0;
            }
            if (e.cik != prev_cik) {
                cnt++;
                prev_cik = e.cik;
            }
        }
        cik_count[cur_gk] = cnt;
    }

    // --------------------------------------------------------
    // Phase 6: HAVING filter + top-K sort
    // --------------------------------------------------------
    struct ResultRow {
        int16_t sic;
        int32_t tlabel_code;
        int64_t num_companies;
        double  total_value;
        double  avg_value;
    };

    std::vector<ResultRow> results;
    results.reserve(2048);

    {
        GENDB_PHASE("having_filter");

        for (auto& [gk, sv] : global_agg) {
            auto it = cik_count.find(gk);
            int64_t nc = (it != cik_count.end()) ? it->second : 0;
            if (nc < 2) continue;

            int16_t sic         = (int16_t)(gk >> 32);
            int32_t tlabel_code = (int32_t)(gk & 0xFFFFFFFFLL);
            double  sum_val     = sv.first;
            int64_t cnt_val     = sv.second;

            ResultRow r;
            r.sic         = sic;
            r.tlabel_code = tlabel_code;
            r.num_companies = nc;
            r.total_value  = sum_val;
            r.avg_value    = (cnt_val > 0) ? sum_val / (double)cnt_val : 0.0;
            results.push_back(r);
        }
    }

    {
        GENDB_PHASE("topk_sort");

        size_t k = std::min((size_t)500, results.size());
        std::partial_sort(results.begin(), results.begin() + k, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                return a.total_value > b.total_value;
            });
        results.resize(k);
    }

    // --------------------------------------------------------
    // Phase 7: Decode and output
    // --------------------------------------------------------
    {
        GENDB_PHASE("decode_output");

        std::filesystem::create_directories(results_dir);
        std::string out_path = results_dir + "/Q4.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); return 1; }

        fprintf(fp, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");

        for (const ResultRow& r : results) {
            // Decode tlabel from tlabel_strings dict
            const std::string& tlabel = tlabel_strings[r.tlabel_code];

            fprintf(fp, "%d,", (int)r.sic);
            write_csv_field(fp, tlabel.data(), tlabel.size());
            fprintf(fp, ",EQ,%lld,%.2f,%.2f\n",
                    (long long)r.num_companies,
                    r.total_value,
                    r.avg_value);
        }

        fclose(fp);
    }

    return 0;
}
