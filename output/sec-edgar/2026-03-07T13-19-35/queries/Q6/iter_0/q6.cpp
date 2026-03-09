/**
 * Q6 — SEC EDGAR: GROUP BY (name, stmt, tag, plabel), SUM(value), COUNT(*)
 * WHERE uom='USD' AND stmt='IS' AND fy=2023 AND value IS NOT NULL
 * ORDER BY total_value DESC LIMIT 200
 *
 * Strategy:
 *   1. Load usd_code + is_code from dict files
 *   2. mmap sub/fy.bin flat array (172KB, fits L3)
 *   3. Scan pre → build is_map: (adsh_code<<32|tagver_code) → pre_row_index
 *   4. Parallel morsel-driven scan of num with zone-map block skipping
 *      Per row: uom==usd, tagver!=-1, !isnan(value), sub_fy[adsh]==2023, is_map probe
 *      Accumulate into thread-local hash maps
 *   5. Merge thread-local maps
 *   6. Decode strings, sort top-200 by total_value DESC, write CSV
 */

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <atomic>
#include <unordered_map>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "mmap_utils.h"
#include "hash_utils.h"
#include "timing_utils.h"

using namespace gendb;

// ─── Zone-map struct (matches index layout: 12 bytes with 2-byte implicit padding) ───
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    // 2 bytes padding (compiler-generated, int32_t alignment)
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap size mismatch");

// ─── Aggregation accumulator ───
struct AggEntry {
    double  sum_value = 0.0;
    int64_t cnt       = 0;
};

// ─── Result row for output ───
struct ResultRow {
    std::string name;
    std::string tag;
    std::string plabel;
    double      total_value;
    int64_t     cnt;
};

// ─── Load a compact dict file, return code for target string ───
// Layout: uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }
static int8_t load_dict_code(const std::string& path, const std::string& target) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open dict: " + path);
    uint8_t N = 0;
    ::read(fd, &N, 1);
    for (int i = 0; i < (int)N; i++) {
        int8_t  code = 0;
        uint8_t slen = 0;
        ::read(fd, &code, 1);
        ::read(fd, &slen, 1);
        char buf[256] = {};
        ::read(fd, buf, slen);
        if (std::string(buf, slen) == target) {
            ::close(fd);
            return code;
        }
    }
    ::close(fd);
    throw std::runtime_error("Dict key not found: " + target + " in " + path);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto P = [&](const std::string& rel) { return gendb_dir + "/" + rel; };

    GENDB_PHASE_MS("total", t_total);

    // ── Phase 1: load_dicts ────────────────────────────────────────────────
    int8_t usd_code, is_code;
    {
        GENDB_PHASE("data_loading");
        usd_code = load_dict_code(P("indexes/uom_codes.bin"),  "USD");
        is_code  = load_dict_code(P("indexes/stmt_codes.bin"), "IS");
    }

    // ── Phase 2: load_sub_fy_array ─────────────────────────────────────────
    MmapColumn<int16_t> sub_fy(P("sub/fy.bin"));
    // sub_fy[adsh_code] gives the fy for that adsh
    // sub has 86135 rows — adsh_code is 0-based index

    // ── Phase 3: build is_map from pre ────────────────────────────────────
    // is_map: (adsh_code<<32 | tagver_code) → head of linked-list of pre_row indices
    // Multiple pre rows can share the same (adsh_code, tagver_code) with different plabels.
    // next_is[pre_row] = next pre_row in chain, or UINT32_MAX if tail.
    CompactHashMap<uint64_t, uint32_t> is_map(1730000);
    MmapColumn<int32_t> pre_adsh    (P("pre/adsh_code.bin"));
    MmapColumn<int32_t> pre_tagver  (P("pre/tagver_code.bin"));
    MmapColumn<int8_t>  pre_stmt    (P("pre/stmt_code.bin"));

    const size_t pre_N = pre_adsh.count;
    std::vector<uint32_t> next_is(pre_N, UINT32_MAX);  // linked-list next pointers

    {
        GENDB_PHASE("build_joins");
        for (size_t i = 0; i < pre_N; i++) {
            if (pre_stmt.data[i] != is_code) continue;
            uint64_t key = ((uint64_t)(uint32_t)pre_adsh.data[i] << 32)
                         | (uint32_t)pre_tagver.data[i];
            // Prepend to chain: new head points to old head
            uint32_t* head_ptr = is_map.find(key);
            if (head_ptr) {
                next_is[i] = *head_ptr;
                *head_ptr = (uint32_t)i;
            } else {
                is_map.insert(key, (uint32_t)i);
            }
        }
    }

    // ── Load num columns ──────────────────────────────────────────────────
    MmapColumn<int8_t>  num_uom   (P("num/uom_code.bin"));
    MmapColumn<int32_t> num_adsh  (P("num/adsh_code.bin"));
    MmapColumn<int32_t> num_tagver(P("num/tagver_code.bin"));
    MmapColumn<double>  num_value (P("num/value.bin"));

    // Prefetch large num columns (HDD — overlap I/O with any remaining setup)
    mmap_prefetch_all(num_uom, num_adsh, num_tagver, num_value);

    // ── Load num zone maps ────────────────────────────────────────────────
    // Layout: uint32_t n_blocks, then n_blocks × ZoneMap (12 bytes each)
    uint32_t zm_n_blocks = 0;
    const ZoneMap* zm_data = nullptr;
    size_t zm_file_size = 0;
    int zm_fd = -1;
    {
        std::string zm_path = P("indexes/num_zone_maps.bin");
        zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone maps: " + zm_path);
        struct stat st;
        fstat(zm_fd, &st);
        zm_file_size = st.st_size;
        void* zm_ptr = mmap(nullptr, zm_file_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_ptr == MAP_FAILED) throw std::runtime_error("mmap failed for zone maps");
        const uint8_t* raw = (const uint8_t*)zm_ptr;
        memcpy(&zm_n_blocks, raw, sizeof(uint32_t));
        zm_data = reinterpret_cast<const ZoneMap*>(raw + sizeof(uint32_t));
    }

    // ── Phase 4: parallel_scan_num_with_zone_maps ─────────────────────────
    static const size_t BLOCK_SIZE = 100000;
    const size_t num_N = num_uom.count;
    // n_blocks should match zm_n_blocks
    const uint32_t n_blocks = zm_n_blocks;

    int n_threads = omp_get_max_threads();

    // Per-thread aggregation maps
    std::vector<CompactHashMap<uint64_t, AggEntry>> thread_aggs;
    thread_aggs.reserve(n_threads);
    for (int t = 0; t < n_threads; t++) {
        thread_aggs.emplace_back(16384);
    }

    // Raw pointers for inner loop performance
    const int8_t*   uom_ptr     = num_uom.data;
    const int32_t*  adsh_ptr    = num_adsh.data;
    const int32_t*  tagver_ptr  = num_tagver.data;
    const double*   value_ptr   = num_value.data;
    const int16_t*  fy_ptr      = sub_fy.data;
    const uint32_t* next_is_ptr = next_is.data();

    std::atomic<uint32_t> next_block{0};

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local_agg = thread_aggs[tid];

            uint32_t b;
            while ((b = next_block.fetch_add(1, std::memory_order_relaxed)) < n_blocks) {
                // Zone-map block skip for uom
                if (zm_data[b].min_uom > usd_code || zm_data[b].max_uom < usd_code)
                    continue;

                size_t row_start = (size_t)b * BLOCK_SIZE;
                size_t row_end   = row_start + BLOCK_SIZE;
                if (row_end > num_N) row_end = num_N;

                for (size_t row = row_start; row < row_end; row++) {
                    // Filter 1: uom == USD
                    if (uom_ptr[row] != usd_code) continue;

                    // Filter 2: tagver != -1
                    int32_t tagver = tagver_ptr[row];
                    if (tagver == -1) continue;

                    // Filter 3: value IS NOT NULL
                    double val = value_ptr[row];
                    if (std::isnan(val)) continue;

                    // Filter 4: sub.fy == 2023
                    int32_t adsh = adsh_ptr[row];
                    if (fy_ptr[adsh] != 2023) continue;

                    // Filter 5 + decode pre_row: is_map probe
                    uint64_t is_key = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tagver;
                    const uint32_t* pre_row_ptr = is_map.find(is_key);
                    if (!pre_row_ptr) continue;

                    // Follow linked-list chain: one pre_row per (adsh,tagver,plabel) group
                    uint32_t pr = *pre_row_ptr;
                    while (pr != UINT32_MAX) {
                        // Aggregation key: (adsh_code, pre_row)
                        uint64_t group_key = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)pr;
                        // Use find()+insert() instead of operator[] to avoid Robin Hood displacement bug
                        AggEntry* ep = local_agg.find(group_key);
                        if (ep) {
                            ep->sum_value += val;
                            ep->cnt++;
                        } else {
                            local_agg.insert(group_key, AggEntry{val, 1});
                        }
                        pr = next_is_ptr[pr];
                    }
                }
            }
        }
    } // main_scan

    // ── Phase 5: merge_thread_local_agg ──────────────────────────────────
    CompactHashMap<uint64_t, AggEntry> merged_agg(131072);
    {
        GENDB_PHASE("merge_thread_local_agg");
        for (auto& local : thread_aggs) {
            for (auto [key, val] : local) {
                // Use find()+insert() instead of operator[] to avoid Robin Hood displacement bug
                AggEntry* ep = merged_agg.find(key);
                if (ep) {
                    ep->sum_value += val.sum_value;
                    ep->cnt       += val.cnt;
                } else {
                    merged_agg.insert(key, val);
                }
            }
        }
    }

    // ── Phase 6: decode_strings_topk_sort ────────────────────────────────
    // The GROUP BY is on (name, stmt, tag, plabel) STRINGS.
    // Multiple adsh_codes can map to the same company name (e.g., annual + quarterly filings),
    // so we must re-aggregate by decoded strings after the integer-keyed first pass.
    MmapColumn<uint32_t> sub_name_offsets(P("sub/name_offsets.bin"));
    MmapColumn<char>     sub_name_data   (P("sub/name_data.bin"));
    MmapColumn<uint32_t> pre_plabel_offsets(P("pre/plabel_offsets.bin"));
    MmapColumn<char>     pre_plabel_data   (P("pre/plabel_data.bin"));
    MmapColumn<uint32_t> tag_offsets     (P("tag/tag_offsets.bin"));
    MmapColumn<char>     tag_data        (P("tag/tag_data.bin"));
    // pre/tagver_code already loaded

    // Final aggregation value: tagver_code stored for later tag decode
    struct FinalEntry {
        int32_t tagver_code;
        std::string plabel;
        double  sum_value = 0.0;
        int64_t cnt       = 0;
    };

    // Key: name + '\0' + plabel  (tagver_code embedded as 4 raw bytes between the two)
    // using '\x01' (SOH control char) as separator — safe since SEC names/labels use ASCII
    std::unordered_map<std::string, FinalEntry> final_agg;
    final_agg.reserve(merged_agg.size() * 2);

    std::vector<ResultRow> results;

    {
        GENDB_PHASE("decode_strings_topk_sort");
        const uint32_t* name_off = sub_name_offsets.data;
        const char*     name_dat = sub_name_data.data;
        const uint32_t* plbl_off = pre_plabel_offsets.data;
        const char*     plbl_dat = pre_plabel_data.data;
        const uint32_t* tag_off  = tag_offsets.data;
        const char*     tag_dat  = tag_data.data;
        const int32_t*  ptv_ptr  = pre_tagver.data;  // pre/tagver_code

        // Step A: decode strings and re-aggregate by (name, tagver_code, plabel)
        for (auto [key, agg] : merged_agg) {
            uint32_t adsh_code = (uint32_t)(key >> 32);
            uint32_t pre_row   = (uint32_t)(key & 0xFFFFFFFFULL);

            // Decode name
            uint32_t ns = name_off[adsh_code];
            uint32_t ne = name_off[adsh_code + 1];
            std::string_view name_sv(name_dat + ns, ne - ns);

            // tagver_code (identifies the tag string globally)
            int32_t tagver_code = ptv_ptr[pre_row];

            // Decode plabel
            uint32_t ps = plbl_off[pre_row];
            uint32_t pe = plbl_off[pre_row + 1];
            std::string_view plabel_sv(plbl_dat + ps, pe - ps);

            // Build composite key: name + \x01 + 4-byte tagver_code + \x01 + plabel
            std::string gk;
            gk.reserve(name_sv.size() + 1 + sizeof(int32_t) + 1 + plabel_sv.size());
            gk.append(name_sv);
            gk += '\x01';
            gk.append(reinterpret_cast<const char*>(&tagver_code), sizeof(tagver_code));
            gk += '\x01';
            gk.append(plabel_sv);

            auto [it, inserted] = final_agg.emplace(std::piecewise_construct,
                std::forward_as_tuple(std::move(gk)),
                std::forward_as_tuple());
            FinalEntry& fe = it->second;
            if (inserted) {
                fe.tagver_code = tagver_code;
                fe.plabel      = std::string(plabel_sv);
            }
            fe.sum_value += agg.sum_value;
            fe.cnt       += agg.cnt;
        }

        // Step B: collect results and decode tag string
        results.reserve(final_agg.size());
        for (auto& [gk, fe] : final_agg) {
            // Decode name from key (before first \x01)
            size_t sep1 = gk.find('\x01');
            std::string name = gk.substr(0, sep1);

            // Decode tag from tagver_code
            uint32_t ts = tag_off[fe.tagver_code];
            uint32_t te = tag_off[fe.tagver_code + 1];
            std::string tag(tag_dat + ts, te - ts);

            results.push_back({std::move(name), std::move(tag),
                               fe.plabel, fe.sum_value, fe.cnt});
        }

        // Step C: partial sort top-200 by total_value DESC
        constexpr size_t LIMIT = 200;
        if (results.size() > LIMIT) {
            std::partial_sort(results.begin(), results.begin() + LIMIT, results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    return a.total_value > b.total_value;
                });
            results.resize(LIMIT);
        } else {
            std::sort(results.begin(), results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    return a.total_value > b.total_value;
                });
        }
    }

    // ── Output ────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) {
            perror("fopen");
            return 1;
        }

        // CSV quoting helper: quote fields that contain comma, double-quote, or newline
        auto csv_write = [&](const std::string& field) {
            bool need_quote = false;
            for (char c : field) {
                if (c == ',' || c == '"' || c == '\n' || c == '\r') {
                    need_quote = true; break;
                }
            }
            if (need_quote) {
                fputc('"', fp);
                for (char c : field) {
                    if (c == '"') fputc('"', fp);  // escape embedded quotes
                    fputc(c, fp);
                }
                fputc('"', fp);
            } else {
                fwrite(field.data(), 1, field.size(), fp);
            }
        };

        fprintf(fp, "name,stmt,tag,plabel,total_value,cnt\n");
        for (const auto& r : results) {
            csv_write(r.name);  fputs(",IS,", fp);
            csv_write(r.tag);   fputc(',', fp);
            csv_write(r.plabel);
            fprintf(fp, ",%.2f,%lld\n", r.total_value, (long long)r.cnt);
        }
        fclose(fp);
    }

    // Cleanup zone maps
    if (zm_data) munmap((void*)((const uint8_t*)zm_data - sizeof(uint32_t)), zm_file_size);
    if (zm_fd >= 0) ::close(zm_fd);

    return 0;
}
