/**
 * Q6 — SEC EDGAR iter_1: GROUP BY (name, stmt, tag, plabel), SUM(value), COUNT(*)
 * WHERE uom='USD' AND stmt='IS' AND fy=2023 AND value IS NOT NULL
 * ORDER BY total_value DESC LIMIT 200
 *
 * Iter_1 improvements over iter_0:
 *   PERFORMANCE: Pre-filter is_map to IS+fy2023 only (550K entries, 16MB → L3-resident)
 *                Parallel pre scan → per-thread pair vectors → serial merge build
 *                Eliminates 38.4MB next_is linked-list array
 *   CORRECTNESS: Use decoded tag STRING in phase-2 group key, not raw tagver_code bytes
 *                Different tagver_codes for same tag string now correctly merged
 *
 * Pipeline:
 *   1. load_dicts (usd_code, is_code)
 *   2. load_sub_fy_array + build fy2023_bitmap (DenseBitmap 86135 bits = 10.8KB, L1)
 *   3. parallel_scan_pre: collect IS+fy2023 (key, pre_row) pairs per thread
 *   4. serial_merge: build is_map<uint64_t, uint32_t>(capacity=1M, ~16MB, L3)
 *   5. parallel_scan_num with zone-map skip → thread-local agg(adsh<<32|pre_row → {sum,cnt})
 *   6. merge_thread_local_agg
 *   7. decode_strings: tag STRING key for phase-2 re-agg, partial_sort top-200, output CSV
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

// ─── Zone-map struct (12 bytes: 2 int8, 2 pad, 2 int32) ─────────────────────
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    // 2 bytes implicit padding (int32_t alignment)
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap size mismatch");

// ─── Phase-1 aggregation accumulator ────────────────────────────────────────
struct AggEntry {
    double  sum_value = 0.0;
    int64_t cnt       = 0;
};

// ─── Result row for final output ────────────────────────────────────────────
struct ResultRow {
    std::string name;
    std::string tag;
    std::string plabel;
    double      total_value;
    int64_t     cnt;
};

// ─── Load compact dict file → return code for target string ─────────────────
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

    // ── Phase 2: load_sub_fy_array + build fy2023_bitmap ──────────────────
    // sub/fy.bin: int16_t[86135]. Build DenseBitmap(86135) for fy==2023.
    MmapColumn<int16_t> sub_fy(P("sub/fy.bin"));
    const size_t sub_N = sub_fy.count;  // 86135

    DenseBitmap fy2023_bits(sub_N);
    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < sub_N; i++) {
            if (sub_fy.data[i] == 2023) fy2023_bits.set(i);
        }
    }

    // ── Load pre columns (needed for build phase) ──────────────────────────
    MmapColumn<int8_t>  pre_stmt  (P("pre/stmt_code.bin"));
    MmapColumn<int32_t> pre_adsh  (P("pre/adsh_code.bin"));
    MmapColumn<int32_t> pre_tagver(P("pre/tagver_code.bin"));
    const size_t pre_N = pre_stmt.count;

    // Prefetch pre columns and num columns early to overlap I/O with CPU work
    mmap_prefetch_all(pre_stmt, pre_adsh, pre_tagver);

    MmapColumn<int8_t>  num_uom   (P("num/uom_code.bin"));
    MmapColumn<int32_t> num_adsh  (P("num/adsh_code.bin"));
    MmapColumn<int32_t> num_tagver(P("num/tagver_code.bin"));
    MmapColumn<double>  num_value (P("num/value.bin"));
    mmap_prefetch_all(num_uom, num_adsh, num_tagver, num_value);

    int n_threads = omp_get_max_threads();

    // ── Phase 3: parallel_scan_pre_collect_IS_fy2023_pairs ────────────────
    // OMP parallel: each thread scans its static slice of pre rows.
    // Passes: pre_stmt==is_code AND fy2023_bits.test(pre_adsh_code)
    // Emits (key=(adsh_code<<32|tagver_code), pre_row) to thread-local vector.
    struct KeyRow { uint64_t key; uint32_t pre_row; };
    std::vector<std::vector<KeyRow>> thread_pairs(n_threads);

    {
        GENDB_PHASE("build_joins");

        const int8_t*  ps_ptr = pre_stmt.data;
        const int32_t* pa_ptr = pre_adsh.data;
        const int32_t* pt_ptr = pre_tagver.data;

        #pragma omp parallel num_threads(n_threads)
        {
            int tid = omp_get_thread_num();
            auto& local_vec = thread_pairs[tid];
            local_vec.reserve(16384);

            #pragma omp for schedule(static)
            for (size_t i = 0; i < pre_N; i++) {
                if (ps_ptr[i] != is_code) continue;
                uint32_t adsh_c = (uint32_t)pa_ptr[i];
                if (!fy2023_bits.test(adsh_c)) continue;
                uint64_t key = ((uint64_t)adsh_c << 32) | (uint32_t)pt_ptr[i];
                local_vec.push_back({key, (uint32_t)i});
            }
        }
    } // build_joins phase

    // ── Phase 4: serial_merge_build_is_map ────────────────────────────────
    // Iterate threads in index order (thread 0 first → row-ascending order within each thread).
    // Insert only if key not yet present → keeps first (smallest) pre_row per key.
    // Expected: 550374 entries. Capacity 1M x 16B = 16MB (L3-resident).
    CompactHashMap<uint64_t, uint32_t> is_map(1048576);
    {
        GENDB_PHASE("serial_merge_build_is_map");
        for (int t = 0; t < n_threads; t++) {
            for (const auto& kr : thread_pairs[t]) {
                if (!is_map.find(kr.key)) {
                    is_map.insert(kr.key, kr.pre_row);
                }
            }
        }
        thread_pairs.clear();
        thread_pairs.shrink_to_fit();
    }

    // ── Load num zone maps ────────────────────────────────────────────────
    uint32_t zm_n_blocks = 0;
    const ZoneMap* zm_data = nullptr;
    size_t zm_file_size = 0;
    int zm_fd = -1;
    {
        std::string zm_path = P("indexes/num_zone_maps.bin");
        zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        if (zm_fd < 0) throw std::runtime_error("Cannot open zone maps");
        struct stat st;
        fstat(zm_fd, &st);
        zm_file_size = st.st_size;
        void* zm_ptr = mmap(nullptr, zm_file_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_ptr == MAP_FAILED) throw std::runtime_error("mmap zone maps failed");
        const uint8_t* raw = (const uint8_t*)zm_ptr;
        memcpy(&zm_n_blocks, raw, sizeof(uint32_t));
        zm_data = reinterpret_cast<const ZoneMap*>(raw + sizeof(uint32_t));
    }

    // ── Phase 5: parallel_scan_num_with_zone_maps ─────────────────────────
    static constexpr size_t BLOCK_SIZE = 100000;
    const size_t   num_N    = num_uom.count;
    const uint32_t n_blocks = zm_n_blocks;

    // Per-thread aggregation: key=(adsh_code<<32|pre_row), value={sum_value, cnt}
    std::vector<CompactHashMap<uint64_t, AggEntry>> thread_aggs(n_threads,
        CompactHashMap<uint64_t, AggEntry>(32768));

    // Raw pointers for inner loop performance
    const int8_t*  uom_ptr    = num_uom.data;
    const int32_t* adsh_ptr   = num_adsh.data;
    const int32_t* tagver_ptr = num_tagver.data;
    const double*  value_ptr  = num_value.data;
    const int16_t* fy_ptr     = sub_fy.data;
    // Pre columns for duplicate forward scan (0.7% of keys)
    const int8_t*  ps_ptr2    = pre_stmt.data;
    const int32_t* pa_ptr2    = pre_adsh.data;
    const int32_t* pt_ptr2    = pre_tagver.data;

    std::atomic<uint32_t> next_block{0};

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel num_threads(n_threads)
        {
            int tid = omp_get_thread_num();
            auto& local_agg = thread_aggs[tid];
            const uint32_t pre_N32 = (uint32_t)pre_N;

            uint32_t b;
            while ((b = next_block.fetch_add(1, std::memory_order_relaxed)) < n_blocks) {
                // Zone-map block skip: skip block if no USD rows present
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

                    // Filter 4: sub.fy == 2023 (direct array, L1-resident 172KB)
                    int32_t adsh = adsh_ptr[row];
                    if (fy_ptr[adsh] != 2023) continue;

                    // Filter 5: probe is_map for IS+fy2023 first pre_row
                    uint64_t is_key = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tagver;
                    const uint32_t* pre_row_ptr = is_map.find(is_key);
                    if (!pre_row_ptr) continue;

                    uint32_t first_pr = *pre_row_ptr;

                    // Accumulate for first pre_row
                    {
                        uint64_t gk = ((uint64_t)(uint32_t)adsh << 32) | (uint64_t)first_pr;
                        AggEntry* ep = local_agg.find(gk);
                        if (ep) { ep->sum_value += val; ep->cnt++; }
                        else     local_agg.insert(gk, AggEntry{val, 1});
                    }

                    // Handle duplicate (adsh, tagver) pre_rows (0.7% of keys).
                    // pre is sorted by (adsh_code, tagver_code), so duplicates are contiguous.
                    // Forward scan until (adsh, tagver) changes.
                    for (uint32_t npr = first_pr + 1; npr < pre_N32; npr++) {
                        if (pa_ptr2[npr] != adsh || pt_ptr2[npr] != tagver) break;
                        if (ps_ptr2[npr] != is_code) continue;  // must be IS stmt
                        uint64_t gk2 = ((uint64_t)(uint32_t)adsh << 32) | (uint64_t)npr;
                        AggEntry* ep2 = local_agg.find(gk2);
                        if (ep2) { ep2->sum_value += val; ep2->cnt++; }
                        else      local_agg.insert(gk2, AggEntry{val, 1});
                    }
                }
            }
        }
    } // main_scan

    // ── Phase 6: merge_thread_local_agg ──────────────────────────────────
    CompactHashMap<uint64_t, AggEntry> merged_agg(786432);
    {
        GENDB_PHASE("merge_thread_local_agg");
        for (auto& local : thread_aggs) {
            for (auto [key, val] : local) {
                AggEntry* ep = merged_agg.find(key);
                if (ep) {
                    ep->sum_value += val.sum_value;
                    ep->cnt       += val.cnt;
                } else {
                    merged_agg.insert(key, val);
                }
            }
        }
        thread_aggs.clear();
        thread_aggs.shrink_to_fit();
    }

    // ── Phase 7: decode_strings_reaggregate_topk_sort ─────────────────────
    // CORRECTNESS FIX: Phase-2 composite key embeds decoded TAG STRING (not tagver_code bytes).
    // Key = name_sv + '\x01' + tag_string + '\x01' + plabel_sv
    // This merges groups with same (name, tag, plabel) but different tagver_codes.
    MmapColumn<uint32_t> sub_name_offsets (P("sub/name_offsets.bin"));
    MmapColumn<char>     sub_name_data    (P("sub/name_data.bin"));
    MmapColumn<uint32_t> pre_plabel_offsets(P("pre/plabel_offsets.bin"));
    MmapColumn<char>     pre_plabel_data  (P("pre/plabel_data.bin"));
    MmapColumn<uint32_t> tag_offsets      (P("tag/tag_offsets.bin"));
    MmapColumn<char>     tag_data         (P("tag/tag_data.bin"));

    struct FinalEntry {
        double  sum_value = 0.0;
        int64_t cnt       = 0;
        std::string plabel;  // stored for output convenience
    };

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
        const int32_t*  ptv_ptr  = pre_tagver.data;

        // Step A: decode strings → build phase-2 string-keyed aggregation
        for (auto [key, agg] : merged_agg) {
            uint32_t adsh_code = (uint32_t)(key >> 32);
            uint32_t pre_row   = (uint32_t)(key & 0xFFFFFFFFULL);

            // Decode name
            uint32_t ns = name_off[adsh_code];
            uint32_t ne = name_off[adsh_code + 1];
            std::string_view name_sv(name_dat + ns, ne - ns);

            // Decode tag STRING from tagver_code (CORRECTNESS FIX)
            int32_t tagver_code = ptv_ptr[pre_row];
            uint32_t ts = tag_off[tagver_code];
            uint32_t te = tag_off[tagver_code + 1];
            std::string_view tag_sv(tag_dat + ts, te - ts);

            // Decode plabel
            uint32_t ps = plbl_off[pre_row];
            uint32_t pe = plbl_off[pre_row + 1];
            std::string_view plabel_sv(plbl_dat + ps, pe - ps);

            // Composite key: name + '\x01' + tag_string + '\x01' + plabel
            std::string gk;
            gk.reserve(name_sv.size() + 1 + tag_sv.size() + 1 + plabel_sv.size());
            gk.append(name_sv);
            gk += '\x01';
            gk.append(tag_sv);
            gk += '\x01';
            gk.append(plabel_sv);

            auto [it, inserted] = final_agg.emplace(std::piecewise_construct,
                std::forward_as_tuple(std::move(gk)),
                std::forward_as_tuple());
            FinalEntry& fe = it->second;
            if (inserted) {
                fe.plabel = std::string(plabel_sv);
            }
            fe.sum_value += agg.sum_value;
            fe.cnt       += agg.cnt;
        }

        // Step B: collect results, parse name and tag from composite key
        results.reserve(final_agg.size());
        for (auto& [gk, fe] : final_agg) {
            size_t sep1 = gk.find('\x01');
            size_t sep2 = gk.find('\x01', sep1 + 1);
            std::string name = gk.substr(0, sep1);
            std::string tag  = gk.substr(sep1 + 1, sep2 - sep1 - 1);
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
    } // decode_strings_topk_sort

    // ── Output ────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror("fopen"); return 1; }

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
                    if (c == '"') fputc('"', fp);
                    fputc(c, fp);
                }
                fputc('"', fp);
            } else {
                fwrite(field.data(), 1, field.size(), fp);
            }
        };

        fprintf(fp, "name,stmt,tag,plabel,total_value,cnt\n");
        for (const auto& r : results) {
            csv_write(r.name); fputs(",IS,", fp);
            csv_write(r.tag);  fputc(',', fp);
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
