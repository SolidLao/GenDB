#include <iostream>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <fstream>
#include <climits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

namespace {

// ─── Hash utilities ───────────────────────────────────────────────────────────

static inline uint64_t hash_int32(int32_t key) {
    return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
}

static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
    return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
}

// ─── Pre-built index slot structs ────────────────────────────────────────────

struct SubADSHSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t row_id;
    int32_t _pad0;
    int32_t _pad1;
};  // 16 bytes

struct PreTripleSlot {
    int32_t adsh_code;  // INT32_MIN = empty
    int32_t tag_code;
    int32_t ver_code;
    int32_t row_id;     // FIRST row in sorted pre for this key
};  // 16 bytes

// ─── Aggregation structures ───────────────────────────────────────────────────

struct Q6Key {
    int32_t name_code;
    int32_t tag_code;
    int32_t plabel_code;
    int16_t stmt_code;
    int16_t _pad;
};  // 16 bytes

static inline uint64_t hash_q6key(const Q6Key& k) {
    uint64_t h = hash_int32(k.name_code);
    h = hash_combine(h, hash_int32(k.tag_code));
    h = hash_combine(h, hash_int32(k.plabel_code));
    h = hash_combine(h, hash_int32((int32_t)k.stmt_code));
    return h;
}

// Scan-time partition entry: written by scan threads, consumed during merge.
// name_code is always >= 0 (valid dict code); INT32_MIN used as empty sentinel
// in the merge hash map.
struct PartEntry {
    Q6Key   key;    // 16 bytes
    int64_t cents;  // 8 bytes (C29: accumulate as int64_t cents)
};  // 24 bytes

// Per-partition hash map slot (stack-allocated inside merge loop).
// Empty sentinel: key.name_code == INT32_MIN (valid dict codes are >= 0).
struct PartSlot {
    Q6Key   key;        // 16 bytes
    int64_t sum_cents;  // 8 bytes
    int64_t count;      // 8 bytes
};  // 32 bytes

// P partitions for scan-time partitioned aggregation.
// 4096 slots × 32B = 128KB per partition hash map → fits in per-core L2 (256KB).
// Expected groups per partition: ~50000/64 = ~781, giving ~19% load factor.
static constexpr int      P         = 64;
static constexpr uint32_t PART_CAP  = 4096;
static constexpr uint32_t PART_MASK = PART_CAP - 1;

// ─── Dict loader: returns string_views into the persistent mmap region ────────
// Avoids 698K+ string heap allocations (plabel_dict: 698K entries, tag_dict: 198K entries).
// Caller must keep out_ptr/out_sz alive as long as the string_views are used.
// Caller is responsible for munmap(out_ptr, out_sz) when done.

static std::vector<std::string_view> load_dict_view(const std::string& path,
                                                     void*& out_ptr,
                                                     size_t& out_sz) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    if (sz == 0) {
        close(fd);
        out_ptr = nullptr;
        out_sz  = 0;
        return {};
    }
    void* m = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (m == MAP_FAILED) { perror("mmap dict"); exit(1); }
    madvise(m, sz, MADV_SEQUENTIAL);

    out_ptr = m;
    out_sz  = sz;

    const char* data = (const char*)m;
    const char* end  = data + sz;

    // Estimate entry count: average entry length ~73 chars for plabel, ~48 for tag
    std::vector<std::string_view> dict;
    dict.reserve(sz / 50 + 1);

    const char* p = data;
    while (p < end) {
        const char* nl = (const char*)memchr(p, '\n', (size_t)(end - p));
        if (!nl) {
            dict.emplace_back(p, (size_t)(end - p));
            break;
        }
        dict.emplace_back(p, (size_t)(nl - p));
        p = nl + 1;
    }
    return dict;
}

// ─── mmap helper ─────────────────────────────────────────────────────────────

static const uint8_t* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return (const uint8_t*)p;
}

// ─── Result row for output ───────────────────────────────────────────────────

struct ResultRow {
    int32_t name_code;
    int16_t stmt_code;
    int32_t tag_code;
    int32_t plabel_code;
    int64_t sum_cents;
    int64_t count;
};

// ─── Main query function ──────────────────────────────────────────────────────

} // end anonymous namespace

void run_q6(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ─── Data loading ────────────────────────────────────────────────────────
    size_t num_uom_sz, num_adsh_sz, num_tag_sz, num_ver_sz, num_val_sz;
    size_t sub_fy_sz, sub_name_sz;
    size_t pre_stmt_sz, pre_plabel_sz, pre_adsh_sz, pre_tag_sz, pre_ver_sz;
    size_t sub_idx_sz, pre_idx_sz;

    const int16_t* num_uom  = nullptr;
    const int32_t* num_adsh = nullptr;
    const int32_t* num_tag  = nullptr;
    const int32_t* num_ver  = nullptr;
    const double*  num_val  = nullptr;
    const int32_t* sub_fy   = nullptr;
    const int32_t* sub_name = nullptr;
    const int16_t* pre_stmt   = nullptr;
    const int32_t* pre_plabel = nullptr;
    const int32_t* pre_adsh   = nullptr;
    const int32_t* pre_tag    = nullptr;
    const int32_t* pre_ver    = nullptr;
    const uint8_t* sub_raw  = nullptr;
    const uint8_t* pre_raw  = nullptr;

    // Dict mmap backing: keeps string_view data alive for the duration of run_q6.
    // One per dict; null if dict was empty (unlikely).
    void*  uom_dict_mmap_ptr    = nullptr; size_t uom_dict_mmap_sz    = 0;
    void*  name_dict_mmap_ptr   = nullptr; size_t name_dict_mmap_sz   = 0;
    void*  stmt_dict_mmap_ptr   = nullptr; size_t stmt_dict_mmap_sz   = 0;
    void*  tag_dict_mmap_ptr    = nullptr; size_t tag_dict_mmap_sz    = 0;
    void*  plabel_dict_mmap_ptr = nullptr; size_t plabel_dict_mmap_sz = 0;

    std::vector<std::string_view> uom_dict, name_dict, stmt_dict, tag_dict, plabel_dict;
    int16_t usd_code = -1, is_code = -1;

    {
        GENDB_PHASE("data_loading");

        // Load dictionaries in parallel using fast mmap+memchr loader returning string_views.
        // This eliminates 896K+ heap allocations (698K plabel + 198K tag) that previously
        // consumed ~70ms. string_views point directly into the persistent mmap region.
        #pragma omp parallel sections num_threads(5)
        {
            #pragma omp section
            { uom_dict    = load_dict_view(gendb_dir + "/num/uom_dict.txt",
                                           uom_dict_mmap_ptr,    uom_dict_mmap_sz); }
            #pragma omp section
            { name_dict   = load_dict_view(gendb_dir + "/sub/name_dict.txt",
                                           name_dict_mmap_ptr,   name_dict_mmap_sz); }
            #pragma omp section
            { stmt_dict   = load_dict_view(gendb_dir + "/pre/stmt_dict.txt",
                                           stmt_dict_mmap_ptr,   stmt_dict_mmap_sz); }
            #pragma omp section
            { tag_dict    = load_dict_view(gendb_dir + "/shared/tag_dict.txt",
                                           tag_dict_mmap_ptr,    tag_dict_mmap_sz); }
            #pragma omp section
            { plabel_dict = load_dict_view(gendb_dir + "/pre/plabel_dict.txt",
                                           plabel_dict_mmap_ptr, plabel_dict_mmap_sz); }
        }

        for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
            if (uom_dict[i] == "USD") { usd_code = i; break; }
        for (int16_t i = 0; i < (int16_t)stmt_dict.size(); i++)
            if (stmt_dict[i] == "IS") { is_code = i; break; }

        // mmap columns
        num_uom  = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/num/uom.bin",     num_uom_sz));
        num_adsh = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/adsh.bin",    num_adsh_sz));
        num_tag  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/tag.bin",     num_tag_sz));
        num_ver  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/version.bin", num_ver_sz));
        num_val  = reinterpret_cast<const double*> (mmap_file(gendb_dir + "/num/value.bin",   num_val_sz));
        sub_fy   = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/fy.bin",      sub_fy_sz));
        sub_name = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/name.bin",    sub_name_sz));
        pre_stmt   = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/pre/stmt.bin",   pre_stmt_sz));
        pre_plabel = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/plabel.bin", pre_plabel_sz));
        pre_adsh   = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/adsh.bin",   pre_adsh_sz));
        pre_tag    = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/tag.bin",    pre_tag_sz));
        pre_ver    = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/pre/version.bin",pre_ver_sz));

        // mmap pre-built indexes
        sub_raw = mmap_file(gendb_dir + "/sub/indexes/sub_adsh_hash.bin",  sub_idx_sz);
        pre_raw = mmap_file(gendb_dir + "/pre/indexes/pre_triple_hash.bin", pre_idx_sz);

        // Parallel madvise for large indexes and columns (P27)
        #pragma omp parallel sections
        {
            #pragma omp section
            { madvise((void*)pre_raw, pre_idx_sz, MADV_WILLNEED); }
            #pragma omp section
            { madvise((void*)sub_raw, sub_idx_sz, MADV_WILLNEED); }
            #pragma omp section
            { madvise((void*)num_uom,  num_uom_sz,  MADV_SEQUENTIAL); }
            #pragma omp section
            { madvise((void*)num_adsh, num_adsh_sz, MADV_SEQUENTIAL); }
        }
        madvise((void*)num_tag,  num_tag_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_ver,  num_ver_sz,  MADV_SEQUENTIAL);
        madvise((void*)num_val,  num_val_sz,  MADV_SEQUENTIAL);
        madvise((void*)pre_stmt,   pre_stmt_sz,   MADV_RANDOM);
        madvise((void*)pre_plabel, pre_plabel_sz, MADV_RANDOM);
        madvise((void*)pre_adsh,   pre_adsh_sz,   MADV_RANDOM);
        madvise((void*)pre_tag,    pre_tag_sz,    MADV_RANDOM);
        madvise((void*)pre_ver,    pre_ver_sz,    MADV_RANDOM);
    }

    // Row counts
    const size_t num_N = num_uom_sz / sizeof(int16_t);
    const size_t pre_N = pre_adsh_sz / sizeof(int32_t);

    // ─── Index header parse (C32: at function scope) ─────────────────────────
    uint32_t sub_cap  = *(const uint32_t*)sub_raw;
    uint32_t sub_mask = sub_cap - 1;
    const SubADSHSlot* sub_ht = (const SubADSHSlot*)(sub_raw + 4);

    uint32_t pth_cap  = *(const uint32_t*)pre_raw;
    uint32_t pth_mask = pth_cap - 1;
    const PreTripleSlot* pth_ht = (const PreTripleSlot*)(pre_raw + 4);

    const int nthreads = omp_get_max_threads();

    // ─── Scan-time partitioned aggregation ───────────────────────────────────
    // Each scan thread writes qualifying tuples into P partition buffers.
    // Partition is chosen by hash(key) & (P-1), so all tuples for a given group
    // key go to the same partition (guaranteed by hash function determinism).
    // After scan, thread t aggregates ALL thread buffers for partition t into a
    // small cache-resident hash map (128KB, fits L2). Fully parallel, zero contention.
    //
    // Memory budget: total PartEntry ≈ 1.13M × 24B = ~27MB (vs 402MB for old approach).
    // Per-partition merge hash map: 4096 × 32B = 128KB (fits per-core L2 = 256KB).

    // all_bufs[tid][pid]: per-thread partition buffers
    std::vector<std::vector<std::vector<PartEntry>>> all_bufs(
        nthreads, std::vector<std::vector<PartEntry>>(P));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& my_bufs = all_bufs[tid];
            // Reserve initial capacity per partition to reduce vector reallocations.
            // Expected entries per thread per partition: ~1.13M / (64*64) ≈ 276
            for (int pid = 0; pid < P; pid++) my_bufs[pid].reserve(512);

            const size_t MORSEL = 100000;
            size_t total_morsels = (num_N + MORSEL - 1) / MORSEL;

            #pragma omp for schedule(dynamic, 1)
            for (size_t m = 0; m < total_morsels; m++) {
                size_t row_start = m * MORSEL;
                size_t row_end   = std::min(row_start + MORSEL, num_N);

                for (size_t i = row_start; i < row_end; i++) {
                    // Filter: uom == USD
                    if (num_uom[i] != usd_code) continue;
                    // Filter: value IS NOT NULL
                    double v = num_val[i];
                    if (std::isnan(v)) continue;

                    int32_t ak = num_adsh[i];
                    int32_t tc = num_tag[i];
                    int32_t vc = num_ver[i];

                    // Probe sub_adsh_hash → get sub row, check fy == 2023
                    uint32_t spos = (uint32_t)(hash_int32(ak) & sub_mask);
                    int32_t name_code = -1;
                    for (uint32_t probe = 0; probe < sub_cap; probe++) {
                        uint32_t slot = (spos + probe) & sub_mask;
                        if (sub_ht[slot].adsh_code == INT32_MIN) break;
                        if (sub_ht[slot].adsh_code == ak) {
                            int32_t sr = sub_ht[slot].row_id;
                            if (sub_fy[sr] == 2023) {
                                name_code = sub_name[sr];
                            }
                            break;
                        }
                    }
                    if (name_code < 0) continue;

                    // Probe pre_triple_hash (C24: bounded)
                    uint64_t ph = hash_combine(
                        hash_combine(hash_int32(ak), hash_int32(tc)),
                        hash_int32(vc));
                    uint32_t ppos = (uint32_t)(ph & pth_mask);
                    // Prefetch the hash table slot; sub_name read above gives ~20 cycles of hide time
                    __builtin_prefetch(&pth_ht[ppos], 0, 0);
                    int32_t first_row = -1;
                    for (uint32_t probe = 0; probe < pth_cap; probe++) {
                        uint32_t slot = (ppos + probe) & pth_mask;
                        if (pth_ht[slot].adsh_code == INT32_MIN) break;
                        if (pth_ht[slot].adsh_code == ak &&
                            pth_ht[slot].tag_code   == tc &&
                            pth_ht[slot].ver_code   == vc) {
                            first_row = pth_ht[slot].row_id;
                            break;
                        }
                    }
                    if (first_row < 0) continue;

                    // C29: convert value to int64_t cents before accumulation
                    int64_t iv = llround(v * 100.0);

                    // Multi-value scan: scan forward while (adsh, tag, ver) match
                    int32_t r = first_row;
                    while (r < (int32_t)pre_N &&
                           pre_adsh[r] == ak &&
                           pre_tag[r]  == tc &&
                           pre_ver[r]  == vc) {
                        if (pre_stmt[r] == is_code) {
                            Q6Key key;
                            key.name_code   = name_code;
                            key.tag_code    = tc;
                            key.plabel_code = pre_plabel[r];
                            key.stmt_code   = is_code;
                            key._pad        = 0;

                            // Route to partition by key hash (deterministic → same key
                            // always lands in same partition across all threads)
                            uint32_t pid = (uint32_t)(hash_q6key(key) & (uint32_t)(P - 1));
                            my_bufs[pid].push_back({key, iv});
                        }
                        r++;
                    }
                }
            }
        }
    }

    // ─── Parallel partitioned aggregation merge ───────────────────────────────
    // Thread p owns partition p: it reads all_bufs[0..nthreads-1][p] and
    // aggregates into a 128KB stack-allocated hash map.  Fully parallel, zero
    // contention.  Per-partition hash map fits in per-core L2 → cache-resident.
    std::vector<std::vector<ResultRow>> part_results(P);

    {
        GENDB_PHASE("aggregation_merge");

        #pragma omp parallel for num_threads(nthreads) schedule(static, 1)
        for (int p = 0; p < P; p++) {
            // Stack-allocate per-partition hash map: 4096 × 32B = 128KB.
            // Fits entirely in per-core L2 cache (256KB).
            PartSlot ht[PART_CAP];
            // C20: initialize via assignment loop, not memset
            for (uint32_t s = 0; s < PART_CAP; s++) ht[s].key.name_code = INT32_MIN;

            // Drain every thread's buffer for partition p
            for (int t = 0; t < nthreads; t++) {
                for (const PartEntry& e : all_bufs[t][p]) {
                    uint32_t pos = (uint32_t)(hash_q6key(e.key) & PART_MASK);
                    // C24: bounded probe
                    for (uint32_t probe = 0; probe < PART_CAP; probe++) {
                        uint32_t slot = (pos + probe) & PART_MASK;
                        if (ht[slot].key.name_code == INT32_MIN) {
                            // Empty slot: insert new group
                            ht[slot].key       = e.key;
                            ht[slot].sum_cents = e.cents;
                            ht[slot].count     = 1;
                            break;
                        }
                        // C15/C30: compare ALL four GROUP BY dimensions
                        if (ht[slot].key.name_code   == e.key.name_code   &&
                            ht[slot].key.tag_code     == e.key.tag_code    &&
                            ht[slot].key.plabel_code  == e.key.plabel_code &&
                            ht[slot].key.stmt_code    == e.key.stmt_code) {
                            ht[slot].sum_cents += e.cents;
                            ht[slot].count     += 1;
                            break;
                        }
                    }
                }
                // Free thread buffer memory as we go to reduce peak RSS
                { std::vector<PartEntry>().swap(all_bufs[t][p]); }
            }

            // Collect occupied slots into this partition's result
            auto& pr = part_results[p];
            pr.reserve(1024);  // expected ~781 groups per partition
            for (uint32_t s = 0; s < PART_CAP; s++) {
                if (ht[s].key.name_code == INT32_MIN) continue;
                ResultRow row;
                row.name_code   = ht[s].key.name_code;
                row.stmt_code   = ht[s].key.stmt_code;
                row.tag_code    = ht[s].key.tag_code;
                row.plabel_code = ht[s].key.plabel_code;
                row.sum_cents   = ht[s].sum_cents;
                row.count       = ht[s].count;
                pr.push_back(row);
            }
        }
    }

    // Concatenate all partition results into single vector
    std::vector<ResultRow> result_rows;
    {
        size_t total = 0;
        for (int p = 0; p < P; p++) total += part_results[p].size();
        result_rows.reserve(total);
        for (int p = 0; p < P; p++)
            for (auto& r : part_results[p]) result_rows.push_back(r);
    }

    // ─── Sort + LIMIT ─────────────────────────────────────────────────────────
    {
        GENDB_PHASE("sort_topk");

        size_t k = std::min((size_t)200, result_rows.size());
        // C33: stable tiebreaker (name string) to prevent non-determinism
        std::partial_sort(result_rows.begin(), result_rows.begin() + k, result_rows.end(),
            [&](const ResultRow& a, const ResultRow& b) {
                if (a.sum_cents != b.sum_cents) return a.sum_cents > b.sum_cents;
                return name_dict[a.name_code] < name_dict[b.name_code];
            });
        result_rows.resize(k);
    }

    // ─── Output ───────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q6.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); exit(1); }

        fprintf(fp, "name,stmt,tag,plabel,total_value,cnt\n");

        for (const auto& row : result_rows) {
            // C31: double-quote ALL string columns
            // Use %.*s with string_view (no null-terminator needed)
            const std::string_view& sv_name   = name_dict[row.name_code];
            const std::string_view& sv_stmt   = stmt_dict[row.stmt_code];
            const std::string_view& sv_tag    = tag_dict[row.tag_code];
            const std::string_view& sv_plabel = plabel_dict[row.plabel_code];
            fprintf(fp, "\"%.*s\",\"%.*s\",\"%.*s\",\"%.*s\",",
                (int)sv_name.size(),   sv_name.data(),
                (int)sv_stmt.size(),   sv_stmt.data(),
                (int)sv_tag.size(),    sv_tag.data(),
                (int)sv_plabel.size(), sv_plabel.data());
            // C29: output sum as integer cents / 100
            int64_t sc = row.sum_cents;
            fprintf(fp, "%lld.%02lld,%lld\n",
                (long long)(sc / 100),
                (long long)std::abs(sc % 100),
                (long long)row.count);
        }

        fclose(fp);
    }

    // ─── Cleanup dict mmaps (string_views no longer needed) ──────────────────
    if (uom_dict_mmap_ptr    && uom_dict_mmap_sz)    munmap(uom_dict_mmap_ptr,    uom_dict_mmap_sz);
    if (name_dict_mmap_ptr   && name_dict_mmap_sz)   munmap(name_dict_mmap_ptr,   name_dict_mmap_sz);
    if (stmt_dict_mmap_ptr   && stmt_dict_mmap_sz)   munmap(stmt_dict_mmap_ptr,   stmt_dict_mmap_sz);
    if (tag_dict_mmap_ptr    && tag_dict_mmap_sz)    munmap(tag_dict_mmap_ptr,    tag_dict_mmap_sz);
    if (plabel_dict_mmap_ptr && plabel_dict_mmap_sz) munmap(plabel_dict_mmap_ptr, plabel_dict_mmap_sz);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q6(gendb_dir, results_dir);
    return 0;
}
#endif
