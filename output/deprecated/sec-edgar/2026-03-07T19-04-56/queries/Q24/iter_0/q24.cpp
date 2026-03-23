#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
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
// Aggregation state
// ============================================================
struct AggState {
    int64_t cnt;
    double total;
};

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
// Zone map entry for ddate (custom format per guide)
// ============================================================
struct DdateZone {
    int32_t min_ddate;
    int32_t max_ddate;
};

// ============================================================
// Dictionary helper: load offsets + data, decode code -> string
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
        // offsets (uint64_t per entry)
        off_fd = ::open(offsets_path.c_str(), O_RDONLY);
        struct stat st;
        fstat(off_fd, &st);
        offsets_file_size = st.st_size;
        num_entries = (uint32_t)(offsets_file_size / sizeof(uint64_t)) - 1;
        void* p = mmap(nullptr, offsets_file_size, PROT_READ, MAP_PRIVATE, off_fd, 0);
        offsets = (const uint64_t*)p;

        // data
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

    void close() {
        if (offsets) { munmap((void*)offsets, offsets_file_size); offsets = nullptr; }
        if (data) { munmap((void*)data, data_file_size); data = nullptr; }
        if (off_fd >= 0) { ::close(off_fd); off_fd = -1; }
        if (dat_fd >= 0) { ::close(dat_fd); dat_fd = -1; }
    }

    ~Dictionary() { close(); }
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

    // Constants
    const int32_t DDATE_LO = 20230101;
    const int32_t DDATE_HI = 20231231;
    const size_t BLOCK_SIZE = 100000;

    // ============================================================
    // Phase 1: Data Loading
    // ============================================================
    uint8_t usd_code = 0;
    uint64_t usd_start = 0, usd_end = 0;

    // Zone map
    uint64_t zm_num_blocks = 0;
    uint64_t zm_block_size = 0;
    std::vector<DdateZone> zones;

    // Pre index
    uint64_t pre_num_buckets = 0;
    uint64_t pre_total_entries = 0;
    const uint64_t* pre_bucket_offsets = nullptr;
    const PreEntry* pre_entries = nullptr;
    void* pre_mmap_ptr = nullptr;
    size_t pre_mmap_size = 0;
    int pre_fd = -1;

    {
        GENDB_PHASE("data_loading");

        // 1. Resolve USD code from uom_dict
        {
            Dictionary uom_dict;
            uom_dict.open(gendb_dir + "/num/uom_dict_offsets.bin",
                         gendb_dir + "/num/uom_dict_data.bin");
            bool found = false;
            for (uint32_t i = 0; i < uom_dict.num_entries; i++) {
                if (uom_dict.decode(i) == "USD") {
                    usd_code = (uint8_t)i;
                    found = true;
                    break;
                }
            }
            if (!found) {
                fprintf(stderr, "USD not found in uom_dict\n");
                return;
            }
        }

        // 2. Get USD row range from uom_offsets
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

        // 3. Load zone map
        {
            int fd = ::open((gendb_dir + "/indexes/num_ddate_zonemap.bin").c_str(), O_RDONLY);
            ::read(fd, &zm_num_blocks, 8);
            ::read(fd, &zm_block_size, 8);
            zones.resize(zm_num_blocks);
            ::read(fd, zones.data(), zm_num_blocks * sizeof(DdateZone));
            ::close(fd);
        }

        // 4. mmap pre_by_adsh_tag_ver index
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
            pre_total_entries = *(const uint64_t*)(base + 8);
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
        GENDB_PHASE("dim_filter");
        col_ddate.open(gendb_dir + "/num/ddate.bin");
        col_value.open(gendb_dir + "/num/value.bin");
        col_sub_fk.open(gendb_dir + "/num/sub_fk.bin");
        col_tag_code.open(gendb_dir + "/num/tag_code.bin");
        col_version_code.open(gendb_dir + "/num/version_code.bin");

        // Prefetch all columns needed during scan
        col_ddate.prefetch();
        col_value.prefetch();
        col_sub_fk.prefetch();
        col_tag_code.prefetch();
        col_version_code.prefetch();
    }

    // ============================================================
    // Phase 3: Build block skip list for USD range
    // ============================================================
    // Determine which blocks within [usd_start, usd_end) to scan
    // using zone map on ddate
    size_t first_block = usd_start / zm_block_size;
    size_t last_block = (usd_end > 0) ? (usd_end - 1) / zm_block_size : first_block;

    // Build list of (block_start_row, block_end_row) for qualifying blocks
    struct BlockRange {
        uint64_t start;
        uint64_t end;
    };
    std::vector<BlockRange> qualifying_blocks;
    {
        GENDB_PHASE("build_joins");
        qualifying_blocks.reserve(last_block - first_block + 1);
        for (size_t b = first_block; b <= last_block && b < zm_num_blocks; b++) {
            // Zone map skip
            if (zones[b].max_ddate < DDATE_LO || zones[b].min_ddate > DDATE_HI) {
                continue;
            }
            uint64_t bstart = b * zm_block_size;
            uint64_t bend = (b + 1) * zm_block_size;
            // Clip to USD range
            if (bstart < usd_start) bstart = usd_start;
            if (bend > usd_end) bend = usd_end;
            if (bstart < bend) {
                qualifying_blocks.push_back({bstart, bend});
            }
        }
    }

    // ============================================================
    // Phase 4: Main scan - parallel morsel-driven
    // ============================================================
    int nthreads = omp_get_max_threads();
    // Thread-local aggregation maps
    std::vector<std::unordered_map<uint64_t, AggState>> local_maps(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& my_map = local_maps[tid];
            my_map.reserve(50000);

            #pragma omp for schedule(dynamic, 1)
            for (size_t bi = 0; bi < qualifying_blocks.size(); bi++) {
                uint64_t rstart = qualifying_blocks[bi].start;
                uint64_t rend = qualifying_blocks[bi].end;

                for (uint64_t i = rstart; i < rend; i++) {
                    // Filter: ddate
                    int32_t dd = col_ddate[i];
                    if (dd < DDATE_LO || dd > DDATE_HI) continue;

                    // Filter: value IS NOT NULL
                    double val = col_value[i];
                    if (std::isnan(val)) continue;

                    // Anti-join: probe pre index
                    uint32_t sfk = col_sub_fk[i];
                    uint32_t tc = col_tag_code[i];
                    uint32_t vc = col_version_code[i];

                    uint64_t bucket = hashKey3(sfk, tc, vc) & (pre_num_buckets - 1);
                    uint64_t bStart = pre_bucket_offsets[bucket];
                    uint64_t bEnd = pre_bucket_offsets[bucket + 1];
                    bool found_in_pre = false;
                    for (uint64_t j = bStart; j < bEnd; j++) {
                        if (pre_entries[j].sub_fk == sfk &&
                            pre_entries[j].tag_code == tc &&
                            pre_entries[j].version_code == vc) {
                            found_in_pre = true;
                            break;
                        }
                    }
                    if (found_in_pre) continue; // anti-join: exclude matches

                    // Aggregate: group by (tag_code, version_code)
                    uint64_t key = ((uint64_t)tc << 32) | vc;
                    auto& agg = my_map[key];
                    agg.cnt++;
                    agg.total += val;
                }
            }
        }
    }

    // ============================================================
    // Phase 5: Merge thread-local maps + HAVING + Top-K
    // ============================================================
    struct ResultRow {
        uint32_t tag_code;
        uint32_t version_code;
        int64_t cnt;
        double total;
    };

    std::vector<ResultRow> results;
    {
        GENDB_PHASE("sort_topk");

        // Merge into global map
        std::unordered_map<uint64_t, AggState> global_map;
        global_map.reserve(100000);
        for (auto& lm : local_maps) {
            for (auto& [key, agg] : lm) {
                auto& g = global_map[key];
                g.cnt += agg.cnt;
                g.total += agg.total;
            }
        }
        local_maps.clear();

        // HAVING filter + collect
        results.reserve(global_map.size());
        for (auto& [key, agg] : global_map) {
            if (agg.cnt > 10) {
                uint32_t tc = (uint32_t)(key >> 32);
                uint32_t vc = (uint32_t)(key & 0xFFFFFFFF);
                results.push_back({tc, vc, agg.cnt, agg.total});
            }
        }

        // Sort by cnt DESC, limit 100
        if (results.size() > 100) {
            std::partial_sort(results.begin(), results.begin() + 100, results.end(),
                            [](const ResultRow& a, const ResultRow& b) {
                                return a.cnt > b.cnt;
                            });
            results.resize(100);
        } else {
            std::sort(results.begin(), results.end(),
                     [](const ResultRow& a, const ResultRow& b) {
                         return a.cnt > b.cnt;
                     });
        }
    }

    // ============================================================
    // Phase 6: Decode and output
    // ============================================================
    {
        GENDB_PHASE("output");

        // Load dictionaries for decode
        Dictionary tag_dict, ver_dict;
        tag_dict.open(gendb_dir + "/dicts/tag_dict_offsets.bin",
                     gendb_dir + "/dicts/tag_dict_data.bin");
        ver_dict.open(gendb_dir + "/dicts/version_dict_offsets.bin",
                     gendb_dir + "/dicts/version_dict_data.bin");

        // Write CSV
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

    // Cleanup pre index mmap
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
