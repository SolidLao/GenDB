// Q24: Anti-join (NOT EXISTS) + GROUP BY + HAVING + TOP-100
// SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
// FROM num n LEFT JOIN pre p ON n.tag=p.tag AND n.version=p.version AND n.adsh=p.adsh
// WHERE n.uom='USD' AND n.ddate BETWEEN 20230101 AND 20231231
//       AND n.value IS NOT NULL AND p.adsh IS NULL
// GROUP BY n.tag, n.version HAVING COUNT(*) > 10 ORDER BY cnt DESC LIMIT 100
//
// iter_1 optimizations over iter_0:
//   (1) main_scan:   software prefetch P=8 staging buffer — overlaps 402MB LLC misses (~60ns each)
//   (2) build_joins: parallel binary tree reduction (log2(64)=6 rounds vs 63 sequential merges)

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <vector>
#include <string>
#include <algorithm>
#include <atomic>
#include <climits>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ============================================================
// Constants
// ============================================================
static const int64_t NUM_ROWS   = 39401761LL;
static const int64_t BLOCK_SIZE = 100000LL;

// Thread-local aggregation hash map capacity
// ~20K groups expected; 2^16 = 65536 gives comfortable load factor
static const int32_t  HM_CAP  = (1 << 16);
static const uint32_t HM_MASK = (uint32_t)(HM_CAP - 1);

// Empty slot sentinel for HM key
static const uint64_t HM_EMPTY = UINT64_MAX;

// Software prefetch distance: issue prefetch P filtered rows ahead
// P=8 × ~5ns computation per row = ~40ns head start to overlap ~60ns LLC miss
static const int PREFETCH_DIST = 8;

// ============================================================
// Pre-existence hash slot (packed, 12 bytes — verbatim from build_indexes.cpp)
// ============================================================
#pragma pack(push, 1)
struct PreExistSlot {
    int32_t adsh;   // INT32_MIN = empty
    int32_t tag;
    int32_t ver;
};
#pragma pack(pop)
static_assert(sizeof(PreExistSlot) == 12, "PreExistSlot must be 12 bytes");

// ============================================================
// Zone map block (packed, 10 bytes — written field-by-field in build_indexes.cpp)
// ============================================================
#pragma pack(push, 1)
struct NumZoneBlock {
    int8_t  uom_min;
    int8_t  uom_max;
    int32_t ddate_min;
    int32_t ddate_max;
};
#pragma pack(pop)
static_assert(sizeof(NumZoneBlock) == 10, "NumZoneBlock must be 10 bytes");

// ============================================================
// Thread-local aggregation hash map entry
// Key: uint64_t = ((uint64_t)(uint32_t)tag_code << 32) | (uint32_t)ver_code
// ============================================================
struct HMEntry {
    uint64_t key;   // HM_EMPTY if slot is empty
    int64_t  cnt;
    double   sum;
};

static inline void hm_upsert(HMEntry* __restrict__ hm, uint64_t key, double val) {
    // Fibonacci hashing for good distribution
    uint64_t slot = ((key * 0x9E3779B97F4A7C15ULL) >> 48) & HM_MASK;
    while (hm[slot].key != HM_EMPTY && hm[slot].key != key)
        slot = (slot + 1) & HM_MASK;
    if (hm[slot].key == HM_EMPTY) {
        hm[slot].key = key;
        hm[slot].cnt = 0;
        hm[slot].sum = 0.0;
    }
    hm[slot].cnt++;
    hm[slot].sum += val;
}

static void hm_merge_into(HMEntry* __restrict__ dst, const HMEntry* __restrict__ src) {
    for (int32_t i = 0; i < HM_CAP; i++) {
        if (src[i].key == HM_EMPTY) continue;
        uint64_t key  = src[i].key;
        uint64_t slot = ((key * 0x9E3779B97F4A7C15ULL) >> 48) & HM_MASK;
        while (dst[slot].key != HM_EMPTY && dst[slot].key != key)
            slot = (slot + 1) & HM_MASK;
        if (dst[slot].key == HM_EMPTY) {
            dst[slot].key = key;
            dst[slot].cnt = 0;
            dst[slot].sum = 0.0;
        }
        dst[slot].cnt += src[i].cnt;
        dst[slot].sum += src[i].sum;
    }
}

// ============================================================
// hash3i — verbatim from build_indexes.cpp
// ============================================================
static inline uint64_t hash3i(int32_t a, int32_t b, int32_t c) {
    uint64_t h = 14695981039346656037ULL;
    uint32_t ua = (uint32_t)a, ub = (uint32_t)b, uc = (uint32_t)c;
    h ^= ua;       h *= 1099511628211ULL;
    h ^= (ua>>8);  h *= 1099511628211ULL;
    h ^= (ua>>16); h *= 1099511628211ULL;
    h ^= (ua>>24); h *= 1099511628211ULL;
    h ^= ub;       h *= 1099511628211ULL;
    h ^= (ub>>8);  h *= 1099511628211ULL;
    h ^= (ub>>16); h *= 1099511628211ULL;
    h ^= (ub>>24); h *= 1099511628211ULL;
    h ^= uc;       h *= 1099511628211ULL;
    h ^= (uc>>8);  h *= 1099511628211ULL;
    h ^= (uc>>16); h *= 1099511628211ULL;
    h ^= (uc>>24); h *= 1099511628211ULL;
    return h ? h : 1;
}

// ============================================================
// mmap helper
// ============================================================
static const void* mmap_ro(const char* path, size_t* out_sz) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); return nullptr; }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); close(fd); return nullptr; }
    *out_sz = (size_t)st.st_size;
    if (*out_sz == 0) { close(fd); return nullptr; }
    void* p = mmap(nullptr, *out_sz, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror(path); return nullptr; }
    return p;
}

// ============================================================
// Load a dict file: [n:uint32][len:uint16, bytes...]*n
// Returns vector of strings
// ============================================================
static std::vector<std::string> load_dict(const char* path) {
    size_t sz = 0;
    const uint8_t* d = (const uint8_t*)mmap_ro(path, &sz);
    if (!d) return {};
    if (sz < 4) return {};

    uint32_t n;
    memcpy(&n, d, 4);
    std::vector<std::string> result;
    result.reserve(n);

    size_t off = 4;
    for (uint32_t i = 0; i < n && off + 2 <= sz; i++) {
        uint16_t len;
        memcpy(&len, d + off, 2);
        off += 2;
        if (off + len > sz) break;
        result.emplace_back((const char*)d + off, len);
        off += len;
    }
    munmap((void*)d, sz);
    return result;
}

// ============================================================
// Prefetch staging buffer entry (for anti-join probe)
// Enqueue rows that pass filters; issue prefetch on enqueue;
// drain oldest after P rows (prefetch had P×~5ns=~40ns head start).
// ============================================================
struct PrefEntry {
    int32_t  adsh_code;
    int32_t  tag_code;
    int32_t  ver_code;
    uint32_t init_pos;   // initial probe position in pre_slots
    double   val;
};  // 28 bytes; P=8 buffer = 224 bytes, fits in L1

// ============================================================
// Group result for output
// ============================================================
struct GroupResult {
    int32_t tag_code;
    int32_t ver_code;
    int64_t cnt;
    double  sum;
};

// ============================================================
// main
// ============================================================
int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s gendb_dir results_dir\n", argv[0]);
        return 1;
    }
    const char* gdir = argv[1];
    const char* rdir = argv[2];
    char path[2048];

    GENDB_PHASE("total");

    // ----------------------------------------------------------
    // Phase: data_loading
    // ----------------------------------------------------------
    int8_t  usd_code = 0;   // USD is pre-seeded at index 0 in shared/uom.dict

    uint32_t            pre_cap    = 0;
    uint32_t            pre_mask   = 0;
    const PreExistSlot* pre_slots  = nullptr;
    size_t              pre_sz     = 0;
    const void*         pre_data   = nullptr;

    const int8_t*  col_uom     = nullptr;
    const int32_t* col_ddate   = nullptr;
    const double*  col_value   = nullptr;
    const int32_t* col_adsh    = nullptr;
    const int32_t* col_tag     = nullptr;
    const int32_t* col_version = nullptr;

    int32_t           n_blocks = 0;
    const NumZoneBlock* zblocks = nullptr;
    size_t              zm_sz  = 0;

    std::vector<std::string> tag_names;
    std::vector<std::string> ver_names;

    {
        GENDB_PHASE("data_loading");

        // 1. USD is pre-seeded at index 0 in shared/uom.dict — hardcoded, no dict scan
        usd_code = 0;

        // 2. Load num zone maps from indexes/num_zonemaps.bin
        //    Format: [n_blocks:int32][per block: uom_min:int8 uom_max:int8 ddate_min:int32 ddate_max:int32]
        //    (packed, 4 + n_blocks * 10 bytes)
        snprintf(path, sizeof(path), "%s/indexes/num_zonemaps.bin", gdir);
        {
            const void* zm_data = mmap_ro(path, &zm_sz);
            if (!zm_data) return 1;
            memcpy(&n_blocks, zm_data, 4);
            // Zone blocks start at byte 4, packed (10 bytes each)
            zblocks = (const NumZoneBlock*)((const char*)zm_data + 4);
            madvise((void*)zm_data, zm_sz, MADV_SEQUENTIAL);
        }

        // 3. mmap pre_existence_hash.bin
        //    Format: [capacity:uint32][n_entries:uint32][slots: PreExistSlot*capacity]
        snprintf(path, sizeof(path), "%s/indexes/pre_existence_hash.bin", gdir);
        {
            pre_data = mmap_ro(path, &pre_sz);
            if (!pre_data) return 1;
            uint32_t cap, nent;
            memcpy(&cap,  (const char*)pre_data + 0, 4);
            memcpy(&nent, (const char*)pre_data + 4, 4);
            pre_cap   = cap;
            pre_mask  = cap - 1;
            pre_slots = (const PreExistSlot*)((const char*)pre_data + 8);
            madvise((void*)pre_data, pre_sz, MADV_RANDOM);
        }

        // 4. mmap num columns
        size_t col_sz = 0;

        snprintf(path, sizeof(path), "%s/num/uom.bin", gdir);
        col_uom = (const int8_t*)mmap_ro(path, &col_sz);
        if (!col_uom) return 1;
        madvise((void*)col_uom, col_sz, MADV_SEQUENTIAL);

        snprintf(path, sizeof(path), "%s/num/ddate.bin", gdir);
        col_ddate = (const int32_t*)mmap_ro(path, &col_sz);
        if (!col_ddate) return 1;
        madvise((void*)col_ddate, col_sz, MADV_SEQUENTIAL);

        snprintf(path, sizeof(path), "%s/num/value.bin", gdir);
        col_value = (const double*)mmap_ro(path, &col_sz);
        if (!col_value) return 1;
        madvise((void*)col_value, col_sz, MADV_SEQUENTIAL);

        snprintf(path, sizeof(path), "%s/num/adsh.bin", gdir);
        col_adsh = (const int32_t*)mmap_ro(path, &col_sz);
        if (!col_adsh) return 1;
        madvise((void*)col_adsh, col_sz, MADV_SEQUENTIAL);

        snprintf(path, sizeof(path), "%s/num/tag.bin", gdir);
        col_tag = (const int32_t*)mmap_ro(path, &col_sz);
        if (!col_tag) return 1;
        madvise((void*)col_tag, col_sz, MADV_SEQUENTIAL);

        snprintf(path, sizeof(path), "%s/num/version.bin", gdir);
        col_version = (const int32_t*)mmap_ro(path, &col_sz);
        if (!col_version) return 1;
        madvise((void*)col_version, col_sz, MADV_SEQUENTIAL);

        // 5. Load tag and version dicts for output decode
        //    Format: [n:uint32][len:uint16, bytes...]*n
        snprintf(path, sizeof(path), "%s/shared/tag_numpre.dict", gdir);
        tag_names = load_dict(path);

        snprintf(path, sizeof(path), "%s/shared/version_numpre.dict", gdir);
        ver_names = load_dict(path);
    }

    // ----------------------------------------------------------
    // Phase: main_scan (parallel morsel-driven + software prefetch P=8)
    //
    // For each row passing filters we compute hash3i, issue __builtin_prefetch
    // for the initial pre_slots probe position, and store the row in a circular
    // staging buffer of size P=8. The oldest entry (issued P rows ago) is drained
    // with its cache line warm — overlapping the ~60ns LLC miss with computation.
    // ----------------------------------------------------------
    int nt = omp_get_max_threads();
    std::vector<HMEntry*> tl_maps(nt, nullptr);

    {
        GENDB_PHASE("main_scan");

        // Allocate and zero-init thread-local hash maps in parallel
        #pragma omp parallel for schedule(static) num_threads(nt)
        for (int t = 0; t < nt; t++) {
            tl_maps[t] = (HMEntry*)malloc((size_t)HM_CAP * sizeof(HMEntry));
            HMEntry* hm = tl_maps[t];
            for (int32_t i = 0; i < HM_CAP; i++) hm[i].key = HM_EMPTY;
        }

        std::atomic<int32_t> block_cursor(0);
        const int8_t   usd_c = usd_code;
        const uint32_t pmask = pre_mask;

        #pragma omp parallel num_threads(nt)
        {
            int tid = omp_get_thread_num();
            HMEntry* hm = tl_maps[tid];

            // Circular prefetch staging buffer — fits entirely in L1
            PrefEntry buf[PREFETCH_DIST];
            int head = 0;   // index of oldest entry (next to drain)
            int tail = 0;   // index of next empty slot
            int fill = 0;   // number of entries currently in buffer

            // Drain the oldest entry from the circular buffer.
            // By the time we drain, the prefetch issued P rows ago
            // has had P×~5ns ≈ 40ns to resolve the LLC miss.
            auto drain_one = [&]() __attribute__((always_inline)) {
                const PrefEntry& e = buf[head];
                uint32_t pos = e.init_pos;
                bool found = false;
                for (;;) {
                    int32_t sa = pre_slots[pos].adsh;
                    if (sa == INT32_MIN) break;         // empty → not in pre
                    if (sa == e.adsh_code &&
                        pre_slots[pos].tag == e.tag_code &&
                        pre_slots[pos].ver == e.ver_code) {
                        found = true;
                        break;
                    }
                    pos = (pos + 1) & pmask;
                }
                if (!found) {
                    uint64_t key = ((uint64_t)(uint32_t)e.tag_code << 32) | (uint32_t)e.ver_code;
                    hm_upsert(hm, key, e.val);
                }
                head = (head + 1 == PREFETCH_DIST) ? 0 : head + 1;
                fill--;
            };

            // Enqueue a filtered row: compute hash, prefetch, store in buffer.
            // If buffer is full, drain oldest first.
            auto enqueue = [&](int32_t a, int32_t t2, int32_t vc, double v) __attribute__((always_inline)) {
                uint64_t kh  = hash3i(a, t2, vc);
                uint32_t pos = (uint32_t)(kh & pmask);
                __builtin_prefetch(&pre_slots[pos], 0, 0);
                buf[tail].adsh_code = a;
                buf[tail].tag_code  = t2;
                buf[tail].ver_code  = vc;
                buf[tail].init_pos  = pos;
                buf[tail].val       = v;
                tail = (tail + 1 == PREFETCH_DIST) ? 0 : tail + 1;
                fill++;
                if (fill == PREFETCH_DIST) drain_one();
            };

            int32_t b;
            while ((b = block_cursor.fetch_add(1, std::memory_order_relaxed)) < n_blocks) {
                // Zone map: skip blocks where USD is absent
                const NumZoneBlock& zb = zblocks[b];
                if (zb.uom_max < usd_c || zb.uom_min > usd_c) continue;
                // Zone map: skip blocks entirely outside ddate range
                if (zb.ddate_max < 20230101 || zb.ddate_min > 20231231) continue;

                int64_t row_start = (int64_t)b * BLOCK_SIZE;
                int64_t row_end   = row_start + BLOCK_SIZE;
                if (row_end > NUM_ROWS) row_end = NUM_ROWS;

                for (int64_t row = row_start; row < row_end; row++) {
                    // Filter 1: uom = 'USD' (1-byte, cheapest)
                    if (col_uom[row] != usd_c) continue;

                    // Filter 2: ddate BETWEEN 20230101 AND 20231231
                    int32_t dd = col_ddate[row];
                    if (dd < 20230101 || dd > 20231231) continue;

                    // Filter 3: value IS NOT NULL
                    double v = col_value[row];
                    if (std::isnan(v)) continue;

                    // Row passes filters → enqueue for prefetch-buffered anti-join
                    int32_t a  = col_adsh[row];
                    int32_t t2 = col_tag[row];
                    int32_t vc = col_version[row];
                    enqueue(a, t2, vc, v);
                }

                // Drain remaining at block boundary to avoid stale prefetches
                while (fill > 0) drain_one();
            }

            // Drain any residual entries
            while (fill > 0) drain_one();
        } // end parallel
    }

    // ----------------------------------------------------------
    // Phase: build_joins (parallel binary tree reduction)
    //
    // log2(64)=6 rounds, each fully parallel with an OMP barrier between rounds.
    // At round r, stride=2^r: threads at multiples of 2^(r+1) merge from tid+2^r.
    // Reduces wall time from ~24ms (sequential) to ~1ms (log-parallel).
    // ----------------------------------------------------------
    {
        GENDB_PHASE("build_joins");

        // levels = ceil(log2(nt))
        int levels = 0;
        { int tmp = nt - 1; while (tmp > 0) { levels++; tmp >>= 1; } }

        #pragma omp parallel num_threads(nt)
        {
            int tid = omp_get_thread_num();

            for (int level = 0; level < levels; level++) {
                int step     = 1 << level;        // sender = receiver + step
                int src_step = 1 << (level + 1);  // receivers spaced src_step apart

                if (tid % src_step == 0) {
                    int src = tid + step;
                    if (src < nt && tl_maps[src] != nullptr) {
                        hm_merge_into(tl_maps[tid], tl_maps[src]);
                        free(tl_maps[src]);
                        tl_maps[src] = nullptr;
                    }
                }
                // Barrier: all threads finish level before advancing
                #pragma omp barrier
            }
        }
    }

    // ----------------------------------------------------------
    // Phase: output (HAVING + sort + decode + write CSV)
    // ----------------------------------------------------------
    {
        GENDB_PHASE("output");

        HMEntry* base = tl_maps[0];

        // Collect groups passing HAVING COUNT(*) > 10
        std::vector<GroupResult> results;
        results.reserve(512);
        for (int32_t i = 0; i < HM_CAP; i++) {
            if (base[i].key == HM_EMPTY) continue;
            if (base[i].cnt <= 10) continue;
            int32_t tc = (int32_t)(uint32_t)(base[i].key >> 32);
            int32_t vc = (int32_t)(uint32_t)(base[i].key & 0xFFFFFFFFULL);
            results.push_back({tc, vc, base[i].cnt, base[i].sum});
        }
        free(tl_maps[0]);
        tl_maps[0] = nullptr;

        // Sort by cnt DESC, LIMIT 100
        std::sort(results.begin(), results.end(), [](const GroupResult& a, const GroupResult& b) {
            return a.cnt > b.cnt;
        });
        if ((int64_t)results.size() > 100) results.resize(100);

        // Write output CSV
        snprintf(path, sizeof(path), "%s/Q24.csv", rdir);
        FILE* out = fopen(path, "w");
        if (!out) { perror(path); return 1; }

        fprintf(out, "tag,version,cnt,total\n");
        for (const auto& r : results) {
            const std::string& tag_str = (r.tag_code >= 0 && r.tag_code < (int32_t)tag_names.size())
                                          ? tag_names[r.tag_code] : "";
            const std::string& ver_str = (r.ver_code >= 0 && r.ver_code < (int32_t)ver_names.size())
                                          ? ver_names[r.ver_code] : "";
            fprintf(out, "%s,%s,%ld,%.2f\n",
                    tag_str.c_str(),
                    ver_str.c_str(),
                    (long)r.cnt,
                    r.sum);
        }
        fclose(out);
    }

    return 0;
}
