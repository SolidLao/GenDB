/**
 * Q6 — SEC EDGAR iter_4: GROUP BY (name, stmt, tag, plabel), SUM(value), COUNT(*)
 * WHERE uom='USD' AND stmt='IS' AND fy=2023 AND value IS NOT NULL
 * ORDER BY total_value DESC LIMIT 200
 *
 * Iter_4 optimizations over iter_3 (baseline 5240ms):
 *
 * FIX 1 — is_map sizing (ROOT CAUSE of 3654ms serial_merge):
 *   OLD: is_map(1048576) → cap = next_pow2(1048576*4/3=1398101) = 2097152 → 32MB
 *        After 86MB parallel pre scan, 32MB is_map is L3-cold → DRAM-speed serial merge.
 *   NEW: is_map(600000) → cap = next_pow2(600000*4/3=800000) = 1048576 → 16MB
 *        16MB fits in 44MB L3; more likely to stay warm during/after pre scan.
 *        Serial merge into L3-resident 16MB table is faster.
 *
 * FIX 2 — thread_local_agg oversizing:
 *   OLD: CompactHashMap(32768) → cap=65536 × 28B = 1.8MB/thread × 64 = 115MB >> L3
 *        Every agg probe causes DRAM-speed cache miss.
 *   NEW: CompactHashMap(8192) → cap=16384 × 28B = 458KB/thread (L2-resident)
 *        64 × 458KB = 29MB fits comfortably in 44MB L3.
 *
 * FIX 3 — String-keyed phase-2 reaggregation:
 *   OLD: unordered_map<string,FinalEntry> — 600K composite string heap allocs,
 *        slow string hashing, heap fragmentation.
 *   NEW: CompactHashMap<uint64_t,ReAggEntry>(250000) keyed on FNV-64 hash of
 *        composite string (name||0x01||tag||0x01||plabel). Zero heap allocs during
 *        reagg. Decode strings only for the final top-200 output rows.
 *
 * Pipeline:
 *   1. load_dicts (usd_code, is_code)
 *   2. load_sub_fy_array + build fy2023_bitmap (10.8KB DenseBitmap, L1-resident)
 *   3. build_joins: parallel pre scan (64 threads) → per-thread pair vectors
 *                   → serial merge into is_map(600000, 16MB, L3-resident)
 *   4. main_scan: parallel morsel-driven num scan + zone-map skip
 *                 → thread-local agg(8192, 458KB/thread, L2-resident)
 *   5. merge_thread_local_agg → merged_agg
 *   6. hash_reaggregate: FNV-64-keyed CompactHashMap, partial_sort top-200
 *   7. output: decode strings for top-200, write CSV
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

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "mmap_utils.h"
#include "hash_utils.h"
#include "timing_utils.h"

using namespace gendb;

// ─── Zone-map struct (12 bytes: 2 int8 + 2 pad + 2 int32) ───────────────────
struct ZoneMap {
    int8_t  min_uom;
    int8_t  max_uom;
    int16_t _pad;
    int32_t min_ddate;
    int32_t max_ddate;
};
static_assert(sizeof(ZoneMap) == 12, "ZoneMap size mismatch");

// ─── Phase-1 per-group accumulator ──────────────────────────────────────────
struct AggEntry {
    double  sum_value = 0.0;
    int64_t cnt       = 0;
};

// ─── Phase-2 re-aggregation entry (no heap strings) ─────────────────────────
struct ReAggEntry {
    double   sum_value        = 0.0;
    int64_t  cnt              = 0;
    uint32_t sample_adsh_code = 0;
    uint32_t sample_pre_row   = 0;
};

// ─── Final output row ────────────────────────────────────────────────────────
struct ResultRow {
    std::string name;
    std::string tag;
    std::string plabel;
    double      total_value;
    int64_t     cnt;
};

// ─── Load compact dict file → return code for target string ──────────────────
static int8_t load_dict_code(const std::string& path, const std::string& target) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open dict: " + path);
    uint8_t N = 0;
    ::read(fd, &N, 1);
    for (int i = 0; i < (int)N; i++) {
        int8_t  code = 0; uint8_t slen = 0;
        ::read(fd, &code, 1); ::read(fd, &slen, 1);
        char buf[256] = {}; ::read(fd, buf, slen);
        if (std::string(buf, slen) == target) { ::close(fd); return code; }
    }
    ::close(fd);
    throw std::runtime_error("Dict key not found: " + target + " in " + path);
}

// ─── FNV-64 of a string_view, chained ───────────────────────────────────────
static inline uint64_t fnv64_sv(std::string_view sv, uint64_t h) noexcept {
    for (unsigned char c : sv) { h ^= c; h *= 1099511628211ULL; }
    return h;
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

    // ── Phase 2: load sub_fy + build fy2023_bitmap ────────────────────────
    MmapColumn<int16_t> sub_fy(P("sub/fy.bin"));
    const size_t sub_N = sub_fy.count;  // 86135

    DenseBitmap fy2023_bits(sub_N);
    {
        GENDB_PHASE("dim_filter");
        const int16_t* fy_ptr = sub_fy.data;
        for (size_t i = 0; i < sub_N; i++) {
            if (fy_ptr[i] == 2023) fy2023_bits.set(i);
        }
    }

    // ── Open pre + num columns, issue async prefetch to overlap with CPU work
    MmapColumn<int8_t>  pre_stmt  (P("pre/stmt_code.bin"));
    MmapColumn<int32_t> pre_adsh  (P("pre/adsh_code.bin"));
    MmapColumn<int32_t> pre_tagver(P("pre/tagver_code.bin"));
    const size_t pre_N = pre_stmt.count;
    mmap_prefetch_all(pre_stmt, pre_adsh, pre_tagver);

    MmapColumn<int8_t>  num_uom   (P("num/uom_code.bin"));
    MmapColumn<int32_t> num_adsh  (P("num/adsh_code.bin"));
    MmapColumn<int32_t> num_tagver(P("num/tagver_code.bin"));
    MmapColumn<double>  num_value (P("num/value.bin"));
    mmap_prefetch_all(num_uom, num_adsh, num_tagver, num_value);

    int n_threads = omp_get_max_threads();

    // ── Phase 3: build_joins ───────────────────────────────────────────────
    // CRITICAL FIX: is_map(600000) → cap=1048576 × 16B = 16MB (L3-resident).
    // Was: is_map(1048576) → cap=2097152 × 16B = 32MB → cold after 86MB pre scan.
    //
    // Step A: parallel collect — 64 threads each scan ~150K rows, emit to thread-local
    //         vector. Parallel mmap accesses saturate I/O queue (better HDD throughput
    //         than single-thread sequential due to concurrent read requests).
    // Step B: serial merge in row-index order → keeps FIRST (smallest) pre_row per key.
    struct KeyRow { uint64_t key; uint32_t pre_row; };
    std::vector<std::vector<KeyRow>> thread_pairs(n_threads);

    // is_map must be accessible for main_scan — declare outside build_joins block
    CompactHashMap<uint64_t, uint32_t> is_map(600000);

    {
        GENDB_PHASE("build_joins");

        const int8_t*  ps_ptr = pre_stmt.data;
        const int32_t* pa_ptr = pre_adsh.data;
        const int32_t* pt_ptr = pre_tagver.data;

        // Step A: parallel collect IS+fy2023 pairs (saturates HDD I/O with 64 concurrent readers)
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

        // Step B: serial merge — threads in ascending index order → first pre_row per key
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
        struct stat st; fstat(zm_fd, &st);
        zm_file_size = st.st_size;
        void* zm_ptr = mmap(nullptr, zm_file_size, PROT_READ, MAP_PRIVATE, zm_fd, 0);
        if (zm_ptr == MAP_FAILED) throw std::runtime_error("mmap zone maps failed");
        const uint8_t* raw = (const uint8_t*)zm_ptr;
        memcpy(&zm_n_blocks, raw, sizeof(uint32_t));
        zm_data = reinterpret_cast<const ZoneMap*>(raw + sizeof(uint32_t));
    }

    // ── Phase 4: parallel_scan_num_with_zone_maps ─────────────────────────
    // CRITICAL FIX: CompactHashMap(8192) → cap=16384 × 28B = 458KB/thread (L2-resident).
    // Was CompactHashMap(32768) → cap=65536 × 28B = 1.8MB/thread × 64 = 115MB >> L3(44MB).
    // Key = (adsh_code<<32 | pre_row), value = {sum_value, cnt}
    static constexpr size_t BLOCK_SIZE = 100000;
    const size_t   num_N    = num_uom.count;
    const uint32_t n_blocks = zm_n_blocks;

    std::vector<CompactHashMap<uint64_t, AggEntry>> thread_aggs(n_threads,
        CompactHashMap<uint64_t, AggEntry>(8192));

    const int8_t*  uom_ptr    = num_uom.data;
    const int32_t* adsh_ptr   = num_adsh.data;
    const int32_t* tagver_ptr = num_tagver.data;
    const double*  value_ptr  = num_value.data;
    const int16_t* fy_ptr     = sub_fy.data;
    // Pre columns for duplicate forward scan (0.7% of qualifying rows)
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
                if (zm_data[b].min_uom > usd_code || zm_data[b].max_uom < usd_code)
                    continue;

                size_t row_start = (size_t)b * BLOCK_SIZE;
                size_t row_end   = std::min(row_start + BLOCK_SIZE, num_N);

                for (size_t row = row_start; row < row_end; row++) {
                    if (uom_ptr[row] != usd_code) continue;

                    int32_t tagver = tagver_ptr[row];
                    if (tagver == -1) continue;

                    double val = value_ptr[row];
                    if (std::isnan(val)) continue;

                    int32_t adsh = adsh_ptr[row];
                    if (fy_ptr[adsh] != 2023) continue;

                    uint64_t is_key = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tagver;
                    const uint32_t* pre_row_ptr = is_map.find(is_key);
                    if (!pre_row_ptr) continue;

                    uint32_t first_pr = *pre_row_ptr;

                    // Accumulate for first pre_row
                    uint64_t gk = ((uint64_t)(uint32_t)adsh << 32) | (uint64_t)first_pr;
                    AggEntry* ep = local_agg.find(gk);
                    if (ep) { ep->sum_value += val; ep->cnt++; }
                    else     local_agg.insert(gk, AggEntry{val, 1LL});

                    // Handle duplicate IS+fy2023 pre rows for same (adsh, tagver).
                    // pre sorted by (adsh_code, tagver_code) → duplicates contiguous.
                    for (uint32_t npr = first_pr + 1; npr < pre_N32; npr++) {
                        if (pa_ptr2[npr] != adsh || pt_ptr2[npr] != tagver) break;
                        if (ps_ptr2[npr] != is_code) continue;
                        uint64_t gk2 = ((uint64_t)(uint32_t)adsh << 32) | (uint64_t)npr;
                        AggEntry* ep2 = local_agg.find(gk2);
                        if (ep2) { ep2->sum_value += val; ep2->cnt++; }
                        else      local_agg.insert(gk2, AggEntry{val, 1LL});
                    }
                }
            }
        }
    }

    // ── Phase 5: merge_thread_local_agg ──────────────────────────────────
    // merged_agg: ~600K groups expected.
    // CompactHashMap(600000) → cap=1048576 × 28B = 29MB (L3-resident).
    CompactHashMap<uint64_t, AggEntry> merged_agg(600000);
    {
        GENDB_PHASE("merge_thread_local_agg");
        for (auto& local : thread_aggs) {
            for (auto [key, val] : local) {
                AggEntry* ep = merged_agg.find(key);
                if (ep) { ep->sum_value += val.sum_value; ep->cnt += val.cnt; }
                else     merged_agg.insert(key, val);
            }
        }
        thread_aggs.clear();
        thread_aggs.shrink_to_fit();
    }

    // ── Phase 6: hash_reaggregate_decode_topk_sort ───────────────────────
    // FIX: FNV-64-keyed CompactHashMap(250000) replaces string-keyed unordered_map.
    // Key = FNV-64(name_sv || 0x01 || tag_string || 0x01 || plabel_sv).
    // Zero heap allocation during reagg. Decode strings only for top-200 rows.
    MmapColumn<uint32_t> sub_name_offsets (P("sub/name_offsets.bin"));
    MmapColumn<char>     sub_name_data    (P("sub/name_data.bin"));
    MmapColumn<uint32_t> pre_plabel_offsets(P("pre/plabel_offsets.bin"));
    MmapColumn<char>     pre_plabel_data  (P("pre/plabel_data.bin"));
    MmapColumn<uint32_t> tag_offsets_col  (P("tag/tag_offsets.bin"));
    MmapColumn<char>     tag_data_col     (P("tag/tag_data.bin"));

    // CompactHashMap(250000) → cap=524288 × sizeof(Entry<u64,ReAggEntry>) = 524288 × 32B = 16.8MB
    CompactHashMap<uint64_t, ReAggEntry> final_reagg(250000);
    std::vector<ResultRow> results;

    {
        GENDB_PHASE("hash_reaggregate_decode_topk_sort");

        const uint32_t* name_off = sub_name_offsets.data;
        const char*     name_dat = sub_name_data.data;
        const uint32_t* plbl_off = pre_plabel_offsets.data;
        const char*     plbl_dat = pre_plabel_data.data;
        const uint32_t* tag_off  = tag_offsets_col.data;
        const char*     tag_dat  = tag_data_col.data;
        const int32_t*  ptv_ptr  = pre_tagver.data;

        static constexpr uint64_t FNV_OFFSET = 14695981039346656037ULL;

        // Step A: Re-aggregate merged_agg using FNV-64 composite key (no heap allocs)
        for (auto [key, agg] : merged_agg) {
            uint32_t adsh_code = (uint32_t)(key >> 32);
            uint32_t pre_row   = (uint32_t)(key & 0xFFFFFFFFULL);

            // Decode string views (zero-copy, zero heap)
            std::string_view name_sv(name_dat + name_off[adsh_code],
                                     name_off[adsh_code + 1] - name_off[adsh_code]);
            int32_t tvc = ptv_ptr[pre_row];
            std::string_view tag_sv(tag_dat + tag_off[tvc],
                                    tag_off[tvc + 1] - tag_off[tvc]);
            std::string_view plabel_sv(plbl_dat + plbl_off[pre_row],
                                       plbl_off[pre_row + 1] - plbl_off[pre_row]);

            // FNV-64 composite: name || 0x01 || tag || 0x01 || plabel
            uint64_t h = FNV_OFFSET;
            h = fnv64_sv(name_sv, h);
            h ^= 0x01u; h *= 1099511628211ULL;
            h = fnv64_sv(tag_sv, h);
            h ^= 0x01u; h *= 1099511628211ULL;
            h = fnv64_sv(plabel_sv, h);

            ReAggEntry* ep = final_reagg.find(h);
            if (ep) {
                ep->sum_value += agg.sum_value;
                ep->cnt       += agg.cnt;
            } else {
                final_reagg.insert(h, {agg.sum_value, agg.cnt, adsh_code, pre_row});
            }
        }
        merged_agg = CompactHashMap<uint64_t, AggEntry>();  // free 29MB

        // Step B: Collect all entries into flat vector for partial_sort
        struct Entry2 {
            double   sum_value;
            int64_t  cnt;
            uint32_t sample_adsh_code;
            uint32_t sample_pre_row;
        };
        std::vector<Entry2> all_entries;
        all_entries.reserve(final_reagg.size());
        for (auto [h, re] : final_reagg) {
            all_entries.push_back({re.sum_value, re.cnt,
                                   re.sample_adsh_code, re.sample_pre_row});
        }

        // Step C: Partial sort top-200 by total_value DESC
        constexpr size_t LIMIT = 200;
        if (all_entries.size() > LIMIT) {
            std::partial_sort(all_entries.begin(), all_entries.begin() + LIMIT,
                              all_entries.end(),
                [](const Entry2& a, const Entry2& b) { return a.sum_value > b.sum_value; });
            all_entries.resize(LIMIT);
        } else {
            std::sort(all_entries.begin(), all_entries.end(),
                [](const Entry2& a, const Entry2& b) { return a.sum_value > b.sum_value; });
        }

        // Step D: Decode strings only for the ≤200 output rows
        results.reserve(all_entries.size());
        for (const auto& e : all_entries) {
            uint32_t ac = e.sample_adsh_code;
            uint32_t pr = e.sample_pre_row;
            int32_t  tc = ptv_ptr[pr];
            results.push_back({
                std::string(name_dat + name_off[ac], name_off[ac + 1] - name_off[ac]),
                std::string(tag_dat  + tag_off[tc],  tag_off[tc + 1]  - tag_off[tc]),
                std::string(plbl_dat + plbl_off[pr], plbl_off[pr + 1] - plbl_off[pr]),
                e.sum_value, e.cnt
            });
        }
    }

    // ── Phase 7: output ───────────────────────────────────────────────────
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
                for (char c : field) { if (c == '"') fputc('"', fp); fputc(c, fp); }
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

    // Cleanup zone maps mmap
    if (zm_data) {
        munmap(const_cast<void*>(static_cast<const void*>(
            reinterpret_cast<const uint8_t*>(zm_data) - sizeof(uint32_t))), zm_file_size);
    }
    if (zm_fd >= 0) ::close(zm_fd);

    return 0;
}
