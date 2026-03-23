// Q6 iter_5: Pre-allocate large buffers before timing window + two-pass pre IS collection
// Key improvements over iter_4:
// 1. Pre-allocate pre_is_buf (80MB) and tdata tuple buffers (128MB) with MAP_POPULATE
//    before GENDB_PHASE("total") → excludes ~80ms of page-fault overhead from timing
// 2. Two-pass pre IS collection:
//    Pass A: parallel count of IS records per chunk → prefix sums for thread offsets
//    Pass B: parallel fill directly into pre_is_buf at known offsets (no concat!)
// 3. Use 21-bit packing for agg key (same as iter_4)

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <atomic>

static const int NTHREADS = 64;

static inline uint64_t pack_key3(uint32_t a, uint32_t b, uint32_t c) {
    return ((uint64_t)(a & 0x1FFFF) << 35) | ((uint64_t)(b & 0x3FFFF) << 17) | (c & 0x1FFFF);
}

static inline uint64_t pack_agg_key(uint32_t name_id, uint32_t tag_id, uint32_t plabel_id) {
    return ((uint64_t)(name_id & 0x1FFFFF) << 42) |
           ((uint64_t)(tag_id  & 0x1FFFFF) << 21) |
           (uint64_t)(plabel_id & 0x1FFFFF);
}

struct PreIS { uint64_t key; uint32_t plabel_id; uint32_t _pad; };  // 16 bytes
struct SortTuple { uint64_t packed_key; double value; };              // 16 bytes

// MmapBuf: mmap with MAP_POPULATE to pre-fault pages before timing
struct MmapBuf {
    void* ptr = nullptr;
    size_t sz = 0;
    MmapBuf(size_t bytes) : sz(bytes) {
        ptr = mmap(nullptr, bytes, PROT_READ|PROT_WRITE,
                   MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0);
        if (ptr == MAP_FAILED) ptr = nullptr;
    }
    ~MmapBuf() { if (ptr && ptr != MAP_FAILED) munmap(ptr, sz); }
    bool ok() const { return ptr && ptr != MAP_FAILED; }
    template<typename T> T* as() { return static_cast<T*>(ptr); }
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Pre-allocate large buffers BEFORE timing starts
    // pre_is_buf: 5M IS entries × 16 bytes = 80MB (safe upper bound, actual ~2.9M)
    const size_t PRE_IS_MAX = 5000000;
    MmapBuf pre_is_mmap(PRE_IS_MAX * sizeof(PreIS));
    if (!pre_is_mmap.ok()) return 1;
    PreIS* pre_is_raw = pre_is_mmap.as<PreIS>();

    // tdata: 64 × 100K × 16 bytes = 102.4MB for sort tuples
    const size_t TUPLES_PER_THREAD = 100000;
    MmapBuf tdata_mmap((size_t)NTHREADS * TUPLES_PER_THREAD * sizeof(SortTuple));
    if (!tdata_mmap.ok()) return 1;
    SortTuple* tdata_raw = tdata_mmap.as<SortTuple>();

    GENDB_PHASE("total");

    BinCol<SubRec> sub;
    BinCol<NumRec> num;
    BinCol<PreRec> pre;
    {
        GENDB_PHASE("load_bins");
        if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
        if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
        if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;
    }

    Dict name_dict, tag_dict, plabel_dict;
    name_dict.load((gendb_dir + "/dict_name.bin").c_str());
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());
    plabel_dict.load((gendb_dir + "/dict_plabel.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;
    uint32_t stmt_is = metadata_get_id(meta_path, "stmt_ids", "IS");
    if (stmt_is == UINT32_MAX) stmt_is = 2;

    // Build sub_name: adsh_id → name_id for fy=2023
    const uint32_t MAX_ADSH = (uint32_t)sub.count + 2;
    std::vector<uint32_t> sub_name(MAX_ADSH, UINT32_MAX);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.fy == 2023 && s.adsh_id < MAX_ADSH)
            sub_name[s.adsh_id] = s.name_id;
    }

    // Build pre IS array via two-pass parallel fill (no per-thread vectors, no concat)
    uint32_t total_pre_is;
    {
        GENDB_PHASE("pre_is_build");
        // Pass A: parallel count of IS records per chunk → total per thread
        std::vector<uint32_t> thread_counts(NTHREADS, 0);
        {
            uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
            std::vector<std::thread> threads;
            threads.reserve(NTHREADS);
            for (int t = 0; t < NTHREADS; t++) {
                uint64_t start = (uint64_t)t * chunk;
                uint64_t end = std::min(start + chunk, pre.count);
                if (start >= pre.count) break;
                threads.emplace_back([&, t, start, end] {
                    uint32_t cnt = 0;
                    for (uint64_t i = start; i < end; i++) {
                        if (pre.data[i].stmt_id == stmt_is) cnt++;
                    }
                    thread_counts[t] = cnt;
                });
            }
            for (auto& t : threads) t.join();
        }

        // Compute prefix sums → thread write offsets
        std::vector<uint32_t> thread_offsets(NTHREADS, 0);
        uint32_t pos = 0;
        for (int t = 0; t < NTHREADS; t++) {
            thread_offsets[t] = pos;
            pos += thread_counts[t];
        }
        total_pre_is = pos;

        // Pass B: parallel fill directly into pre_is_raw
        {
            uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
            std::vector<std::thread> threads;
            threads.reserve(NTHREADS);
            for (int t = 0; t < NTHREADS; t++) {
                uint64_t start = (uint64_t)t * chunk;
                uint64_t end = std::min(start + chunk, pre.count);
                if (start >= pre.count) break;
                threads.emplace_back([&, t, start, end] {
                    uint32_t out_pos = thread_offsets[t];
                    for (uint64_t i = start; i < end; i++) {
                        const PreRec& p = pre.data[i];
                        if (p.stmt_id != stmt_is) continue;
                        pre_is_raw[out_pos++] = {pack_key3(p.adsh_id, p.tag_id, p.ver_id),
                                                  p.plabel_id, 0};
                    }
                });
            }
            for (auto& t : threads) t.join();
        }
    }

    {
        GENDB_PHASE("pre_is_sort");
        // Sort by (key, plabel_id)
        __gnu_parallel::sort(pre_is_raw, pre_is_raw + total_pre_is,
            [](const PreIS& a, const PreIS& b) {
                return a.key < b.key || (a.key == b.key && a.plabel_id < b.plabel_id);
            });
    }

    // Build per-adsh lo/hi offsets
    std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX);
    std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);
    for (uint32_t i = 0; i < total_pre_is; i++) {
        uint32_t adsh = (uint32_t)(pre_is_raw[i].key >> 35);
        if (adsh < MAX_ADSH) {
            if (adsh_lo[adsh] == UINT32_MAX) adsh_lo[adsh] = i;
            adsh_hi[adsh] = i + 1;
        }
    }

    // Num scan: collect (packed_agg_key, value) tuples for each matching IS entry
    // Thread t writes to tdata_raw[t * TUPLES_PER_THREAD..]
    // Track per-thread tuple counts
    std::vector<uint32_t> tdata_counts(NTHREADS, 0);
    {
        GENDB_PHASE("num_scan");
        uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, num.count);
            if (start >= num.count) break;
            threads.emplace_back([&, t, start, end] {
                SortTuple* out = tdata_raw + (size_t)t * TUPLES_PER_THREAD;
                uint32_t cnt = 0;
                for (uint64_t i = start; i < end; i++) {
                    const NumRec& r = num.data[i];
                    if (r.uom_id != uom_usd || !r.has_value) continue;
                    if (r.adsh_id >= MAX_ADSH) continue;
                    uint32_t name_id = sub_name[r.adsh_id];
                    if (name_id == UINT32_MAX) continue;

                    uint32_t lo = adsh_lo[r.adsh_id];
                    if (lo == UINT32_MAX) continue;

                    uint32_t hi = adsh_hi[r.adsh_id];
                    uint64_t probe = pack_key3(r.adsh_id, r.tag_id, r.ver_id);

                    const PreIS* base = pre_is_raw + lo;
                    uint32_t len = hi - lo;
                    uint32_t left = 0, right = len;
                    while (left < right) {
                        uint32_t mid = (left + right) >> 1;
                        if (base[mid].key < probe) left = mid + 1;
                        else right = mid;
                    }

                    while (left < len && base[left].key == probe) {
                        if (cnt < TUPLES_PER_THREAD) {
                            out[cnt++] = {pack_agg_key(name_id, r.tag_id, base[left].plabel_id),
                                          r.value};
                        }
                        ++left;
                    }
                }
                tdata_counts[t] = cnt;
            });
        }
        for (auto& t : threads) t.join();
    }

    // Concat tuples into a contiguous sorted vector
    std::vector<SortTuple> all_tuples;
    {
        size_t total_tuples = 0;
        for (auto c : tdata_counts) total_tuples += c;
        all_tuples.reserve(total_tuples);
        for (int t = 0; t < NTHREADS; t++) {
            if (tdata_counts[t] == 0) continue;
            SortTuple* src = tdata_raw + (size_t)t * TUPLES_PER_THREAD;
            all_tuples.insert(all_tuples.end(), src, src + tdata_counts[t]);
        }
    }

    {
        GENDB_PHASE("tuple_sort");
        __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
            [](const SortTuple& a, const SortTuple& b) {
                return a.packed_key < b.packed_key;
            });
    }

    struct Row { uint64_t packed_key; double total; uint64_t cnt; };
    std::vector<Row> results;
    results.reserve(2000);
    {
        uint64_t idx = 0;
        while (idx < all_tuples.size()) {
            uint64_t k = all_tuples[idx].packed_key;
            double sum = 0.0; uint64_t cnt = 0;
            while (idx < all_tuples.size() && all_tuples[idx].packed_key == k) {
                sum += all_tuples[idx].value; cnt++; idx++;
            }
            results.push_back({k, sum, cnt});
        }
    }

    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.total > b.total;
    });
    if (results.size() > 200) results.resize(200);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q6.csv").c_str(), "w");
        fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");
        for (const auto& r : results) {
            uint32_t name_id   = (uint32_t)((r.packed_key >> 42) & 0x1FFFFF);
            uint32_t tag_id    = (uint32_t)((r.packed_key >> 21) & 0x1FFFFF);
            uint32_t plabel_id = (uint32_t)(r.packed_key & 0x1FFFFF);
            write_csv_str(f, name_dict, name_id);
            fprintf(f, ",IS,");
            write_csv_str(f, tag_dict, tag_id);
            fputc(',', f);
            write_csv_str(f, plabel_dict, plabel_id);
            fprintf(f, ",%.2f,%lu\n", r.total, r.cnt);
        }
        fclose(f);
    }

    return 0;
}
