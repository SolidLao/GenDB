#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

// ============================================================
// Hash function from build_indexes.cpp (verbatim)
// ============================================================
static inline uint64_t hashKey3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= (uint64_t)c * 3266489917ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}

// ============================================================
// Pre index entry
// ============================================================
struct PreEntry {
    uint32_t sub_fk;
    uint32_t tag_code;
    uint32_t version_code;
    uint32_t row_idx;
};

// ============================================================
// Zone map entry (int32 min, int32 max per block)
// ============================================================
struct DdateZone {
    int32_t min_ddate;
    int32_t max_ddate;
};

// ============================================================
// Aggregation state
// ============================================================
struct AggState {
    int64_t cnt;
    double total;
};

// ============================================================
// Custom flat open-addressing hash map (plan-specified)
// 131072 slots × (8 key + 16 val) = ~3MB, fits in LLC
// ============================================================
static constexpr uint32_t HM_SLOTS = 131072;
static constexpr uint32_t HM_MASK = HM_SLOTS - 1;
// Sentinel = 0: keys array is zero-initialized via MAP_ANONYMOUS (free from kernel).
// Valid group keys are (tag_code << 32 | version_code). tag_code=0 AND version_code=0
// is extremely unlikely but we handle it: if both are 0, we use key=0 which would collide
// with sentinel. We remap key=0 to a reserved value.
static constexpr uint64_t HM_EMPTY = 0;

struct FlatHashMap {
    uint64_t* keys;   // 0 = empty sentinel
    AggState* vals;
    static constexpr size_t KEYS_BYTES = HM_SLOTS * sizeof(uint64_t);
    static constexpr size_t VALS_BYTES = HM_SLOTS * sizeof(AggState);

    FlatHashMap() {
        // Use MAP_ANONYMOUS for zero-initialized pages (lazy allocation, no memset cost)
        keys = (uint64_t*)mmap(nullptr, KEYS_BYTES, PROT_READ|PROT_WRITE,
                               MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        vals = (AggState*)mmap(nullptr, VALS_BYTES, PROT_READ|PROT_WRITE,
                               MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    }

    ~FlatHashMap() {
        munmap(keys, KEYS_BYTES);
        munmap(vals, VALS_BYTES);
    }

    // Encode group key: ensure non-zero (add 1 to avoid collision with sentinel)
    static inline uint64_t encode_key(uint32_t tc, uint32_t vc) {
        return (((uint64_t)tc << 32) | (uint64_t)vc) + 1;
    }
    static inline void decode_key(uint64_t k, uint32_t& tc, uint32_t& vc) {
        k -= 1;
        tc = (uint32_t)(k >> 32);
        vc = (uint32_t)(k & 0xFFFFFFFF);
    }

    __attribute__((always_inline))
    inline uint32_t find_or_insert(uint64_t key) {
        uint32_t slot = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 47) & HM_MASK;
        while (true) {
            uint64_t k = keys[slot];
            if (k == key) return slot;
            if (k == HM_EMPTY) { keys[slot] = key; return slot; }
            slot = (slot + 1) & HM_MASK;
        }
    }

    FlatHashMap(const FlatHashMap&) = delete;
    FlatHashMap& operator=(const FlatHashMap&) = delete;
};

// ============================================================
// Dictionary helper
// ============================================================
struct Dictionary {
    const uint64_t* offsets;
    const char* data;
    size_t offsets_file_size;
    size_t data_file_size;
    int off_fd, dat_fd;
    uint32_t num_entries;

    Dictionary() : offsets(nullptr), data(nullptr), offsets_file_size(0), data_file_size(0),
                   off_fd(-1), dat_fd(-1), num_entries(0) {}

    void open(const std::string& offsets_path, const std::string& data_path) {
        off_fd = ::open(offsets_path.c_str(), O_RDONLY);
        struct stat st;
        fstat(off_fd, &st);
        offsets_file_size = st.st_size;
        num_entries = (uint32_t)(offsets_file_size / sizeof(uint64_t)) - 1;
        void* p = mmap(nullptr, offsets_file_size, PROT_READ, MAP_PRIVATE, off_fd, 0);
        offsets = (const uint64_t*)p;

        dat_fd = ::open(data_path.c_str(), O_RDONLY);
        fstat(dat_fd, &st);
        data_file_size = st.st_size;
        void* p2 = mmap(nullptr, data_file_size, PROT_READ, MAP_PRIVATE, dat_fd, 0);
        data = (const char*)p2;
    }

    std::string decode(uint32_t code) const {
        if (code >= num_entries) return "";
        uint64_t start = offsets[code];
        uint64_t end = offsets[code + 1];
        return std::string(data + start, end - start);
    }

    ~Dictionary() {
        if (offsets) munmap((void*)offsets, offsets_file_size);
        if (data) munmap((void*)data, data_file_size);
        if (off_fd >= 0) ::close(off_fd);
        if (dat_fd >= 0) ::close(dat_fd);
    }
};

// ============================================================
// UOM offsets entry
// ============================================================
struct UomOffsetEntry {
    uint64_t start;
    uint64_t end;
};

void run_q24(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const int32_t DDATE_LO = 20230101;
    const int32_t DDATE_HI = 20231231;

    // ============================================================
    // Phase 1: Resolve USD code + row range + zone map + pre index
    // ============================================================
    uint8_t usd_code = 0;
    uint64_t usd_start = 0, usd_end = 0;

    uint64_t zm_num_blocks = 0;
    uint64_t zm_block_size = 0;
    std::vector<DdateZone> zones;

    uint64_t pre_num_buckets = 0;
    const uint64_t* pre_bucket_offsets = nullptr;
    const PreEntry* pre_entries = nullptr;
    void* pre_mmap_ptr = nullptr;
    size_t pre_mmap_size = 0;
    int pre_fd = -1;

    {
        GENDB_PHASE("data_loading");

        // 1. Resolve USD code
        {
            Dictionary uom_dict;
            uom_dict.open(gendb_dir + "/num/uom_dict_offsets.bin",
                         gendb_dir + "/num/uom_dict_data.bin");
            for (uint32_t i = 0; i < uom_dict.num_entries; i++) {
                uint64_t s = uom_dict.offsets[i];
                uint64_t e = uom_dict.offsets[i + 1];
                if (e - s == 3 && uom_dict.data[s] == 'U' && uom_dict.data[s+1] == 'S' && uom_dict.data[s+2] == 'D') {
                    usd_code = (uint8_t)i;
                    break;
                }
            }
        }

        // 2. USD row range
        {
            int fd = ::open((gendb_dir + "/num/uom_offsets.bin").c_str(), O_RDONLY);
            uint32_t num_entries;
            ::read(fd, &num_entries, 4);
            std::vector<UomOffsetEntry> entries(num_entries);
            ::read(fd, entries.data(), num_entries * sizeof(UomOffsetEntry));
            ::close(fd);
            if (usd_code < num_entries) {
                usd_start = entries[usd_code].start;
                usd_end = entries[usd_code].end;
            }
        }

        // 3. Zone map
        {
            int fd = ::open((gendb_dir + "/indexes/num_ddate_zonemap.bin").c_str(), O_RDONLY);
            ::read(fd, &zm_num_blocks, 8);
            ::read(fd, &zm_block_size, 8);
            zones.resize(zm_num_blocks);
            ::read(fd, zones.data(), zm_num_blocks * sizeof(DdateZone));
            ::close(fd);
        }

        // 4. mmap pre index with MADV_RANDOM
        {
            std::string path = gendb_dir + "/indexes/pre_by_adsh_tag_ver.idx";
            pre_fd = ::open(path.c_str(), O_RDONLY);
            struct stat st;
            fstat(pre_fd, &st);
            pre_mmap_size = st.st_size;
            pre_mmap_ptr = mmap(nullptr, pre_mmap_size, PROT_READ, MAP_PRIVATE, pre_fd, 0);
            madvise(pre_mmap_ptr, pre_mmap_size, MADV_RANDOM);

            const uint8_t* base = (const uint8_t*)pre_mmap_ptr;
            pre_num_buckets = *(const uint64_t*)(base);
            pre_bucket_offsets = (const uint64_t*)(base + 16);
            pre_entries = (const PreEntry*)(base + 16 + (pre_num_buckets + 1) * 8);
        }
    }

    // ============================================================
    // Phase 2: mmap num columns
    // ============================================================
    gendb::MmapColumn<int32_t> col_ddate;
    gendb::MmapColumn<double> col_value;
    gendb::MmapColumn<uint32_t> col_sub_fk;
    gendb::MmapColumn<uint32_t> col_tag_code;
    gendb::MmapColumn<uint32_t> col_version_code;

    {
        GENDB_PHASE("mmap_columns");
        col_ddate.open(gendb_dir + "/num/ddate.bin");
        col_value.open(gendb_dir + "/num/value.bin");
        col_sub_fk.open(gendb_dir + "/num/sub_fk.bin");
        col_tag_code.open(gendb_dir + "/num/tag_code.bin");
        col_version_code.open(gendb_dir + "/num/version_code.bin");
    }

    const int32_t* ddate = col_ddate.data;
    const double* value = col_value.data;
    const uint32_t* sub_fk = col_sub_fk.data;
    const uint32_t* tag_code = col_tag_code.data;
    const uint32_t* version_code = col_version_code.data;

    // ============================================================
    // Phase 3: Build qualifying block list using zone map
    // ============================================================
    struct BlockRange { uint64_t start, end; };
    std::vector<BlockRange> qualifying_blocks;
    {
        GENDB_PHASE("dim_filter");
        uint64_t first_block = usd_start / zm_block_size;
        uint64_t last_block = (usd_end > 0) ? (usd_end - 1) / zm_block_size : first_block;
        if (last_block >= zm_num_blocks) last_block = zm_num_blocks - 1;

        qualifying_blocks.reserve(last_block - first_block + 1);
        for (uint64_t b = first_block; b <= last_block; b++) {
            if (zones[b].max_ddate < DDATE_LO || zones[b].min_ddate > DDATE_HI) continue;
            uint64_t bstart = b * zm_block_size;
            uint64_t bend = (b + 1) * zm_block_size;
            if (bstart < usd_start) bstart = usd_start;
            if (bend > usd_end) bend = usd_end;
            if (bstart < bend) qualifying_blocks.push_back({bstart, bend});
        }
    }

    // ============================================================
    // Phase 4: Two-phase scan with flat hash maps
    // ============================================================
    int nthreads = std::min(omp_get_max_threads(), 32);

    // Allocate thread-local flat hash maps on heap
    std::vector<FlatHashMap*> local_maps(nthreads);
    for (int t = 0; t < nthreads; t++) {
        local_maps[t] = new FlatHashMap();
    }

    {
        GENDB_PHASE("main_scan");

        const uint64_t pre_mask = pre_num_buckets - 1;
        const uint64_t numQualBlocks = qualifying_blocks.size();

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            FlatHashMap* myMap = local_maps[tid];
            // Stack-allocated selection vector (max block size = 100000)
            uint32_t sel[100000];

            #pragma omp for schedule(dynamic, 1)
            for (uint64_t bi = 0; bi < numQualBlocks; bi++) {
                uint64_t rstart = qualifying_blocks[bi].start;
                uint64_t rend = qualifying_blocks[bi].end;
                uint64_t rowsInBlock = rend - rstart;

                // === PASS 1: Filter ddate + value → selection vector ===
                // Only reads ddate and value columns (sequential, cache-friendly)
                const int32_t* dd = ddate + rstart;
                const double* vv = value + rstart;
                uint32_t selCount = 0;

                for (uint64_t r = 0; r < rowsInBlock; r++) {
                    int32_t d = dd[r];
                    if (d >= DDATE_LO && d <= DDATE_HI && !std::isnan(vv[r])) {
                        sel[selCount++] = (uint32_t)r;
                    }
                }

                if (selCount == 0) continue;

                // === PASS 2: Anti-join probe + aggregate ===
                // Only loads sub_fk/tag_code/version_code for qualifying rows (~27% of block)
                const uint32_t* sfk = sub_fk + rstart;
                const uint32_t* tc = tag_code + rstart;
                const uint32_t* vc = version_code + rstart;
                const double* val = value + rstart;

                for (uint32_t si = 0; si < selCount; si++) {
                    // Software prefetch for i+8
                    if (si + 8 < selCount) {
                        uint32_t ri8 = sel[si + 8];
                        uint64_t h8 = hashKey3(sfk[ri8], tc[ri8], vc[ri8]);
                        uint64_t bucket8 = h8 & pre_mask;
                        __builtin_prefetch(&pre_bucket_offsets[bucket8], 0, 0);
                    }

                    uint32_t ri = sel[si];
                    uint32_t s = sfk[ri], t = tc[ri], v = vc[ri];

                    // Anti-join probe
                    uint64_t bucket = hashKey3(s, t, v) & pre_mask;
                    uint64_t bStart = pre_bucket_offsets[bucket];
                    uint64_t bEnd = pre_bucket_offsets[bucket + 1];
                    bool found = false;
                    for (uint64_t j = bStart; j < bEnd; j++) {
                        const PreEntry& e = pre_entries[j];
                        if (e.sub_fk == s && e.tag_code == t && e.version_code == v) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        // Passes anti-join → aggregate by (tag_code, version_code)
                        uint64_t groupKey = FlatHashMap::encode_key(t, v);
                        uint32_t slot = myMap->find_or_insert(groupKey);
                        myMap->vals[slot].cnt++;
                        myMap->vals[slot].total += val[ri];
                    }
                }
            }
        }
    }

    // ============================================================
    // Phase 5: Merge flat maps + HAVING + Top-K
    // ============================================================
    struct ResultRow {
        uint32_t tag_code;
        uint32_t version_code;
        int64_t cnt;
        double total;
    };

    std::vector<ResultRow> results;
    {
        GENDB_PHASE("merge_having_topk");

        // Merge all thread maps into first map
        FlatHashMap* merged = local_maps[0];
        for (int t = 1; t < nthreads; t++) {
            FlatHashMap* m = local_maps[t];
            for (uint32_t s = 0; s < HM_SLOTS; s++) {
                if (m->keys[s] == HM_EMPTY) continue;
                uint32_t slot = merged->find_or_insert(m->keys[s]);
                merged->vals[slot].cnt += m->vals[s].cnt;
                merged->vals[slot].total += m->vals[s].total;
            }
        }

        // Extract with HAVING cnt > 10
        results.reserve(1024);
        for (uint32_t s = 0; s < HM_SLOTS; s++) {
            if (merged->keys[s] == HM_EMPTY) continue;
            if (merged->vals[s].cnt <= 10) continue;
            uint32_t tc, vc;
            FlatHashMap::decode_key(merged->keys[s], tc, vc);
            results.push_back({
                tc, vc,
                merged->vals[s].cnt,
                merged->vals[s].total
            });
        }

        // Sort by cnt DESC, limit 100
        if (results.size() > 100) {
            std::partial_sort(results.begin(), results.begin() + 100, results.end(),
                [](const ResultRow& a, const ResultRow& b) { return a.cnt > b.cnt; });
            results.resize(100);
        } else {
            std::sort(results.begin(), results.end(),
                [](const ResultRow& a, const ResultRow& b) { return a.cnt > b.cnt; });
        }
    }

    // ============================================================
    // Phase 6: Decode and output
    // ============================================================
    {
        GENDB_PHASE("output");

        Dictionary tag_dict, ver_dict;
        tag_dict.open(gendb_dir + "/dicts/tag_dict_offsets.bin",
                     gendb_dir + "/dicts/tag_dict_data.bin");
        ver_dict.open(gendb_dir + "/dicts/version_dict_offsets.bin",
                     gendb_dir + "/dicts/version_dict_data.bin");

        std::string out_path = results_dir + "/Q24.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        fprintf(fp, "tag,version,cnt,total\n");
        for (auto& r : results) {
            std::string tag = tag_dict.decode(r.tag_code);
            std::string ver = ver_dict.decode(r.version_code);
            fprintf(fp, "%s,%s,%ld,%.2f\n", tag.c_str(), ver.c_str(), (long)r.cnt, r.total);
        }
        fclose(fp);
    }

    // Cleanup
    for (int t = 0; t < nthreads; t++) delete local_maps[t];
    if (pre_mmap_ptr) munmap(pre_mmap_ptr, pre_mmap_size);
    if (pre_fd >= 0) ::close(pre_fd);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    run_q24(argv[1], argv[2]);
    return 0;
}
#endif
