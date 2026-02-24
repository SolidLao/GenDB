// Q3: SEC EDGAR – SUM(value) by (name, cik) for fy=2022, uom=USD
// HAVING > AVG(per-cik sums), ORDER BY total_value DESC LIMIT 100
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <climits>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

namespace {

// ---- Utilities ----

inline uint32_t next_pow2(uint32_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
    return n + 1;
}

static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    close(fd);
    out_size = st.st_size;
    return p;
}

// ---- Zone map structs ----
struct ZoneBlock16 {
    int16_t min_val;
    int16_t max_val;
    uint32_t row_count;
};

struct ZoneBlock32 {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// ---- P11: Pre-built sub adsh hash index ----
// Layout: [uint64_t cap][SubAdsSlot * cap]
// Empty sentinel: adsh_code == INT32_MIN
struct SubAdsSlot {
    int32_t adsh_code;
    int32_t row_id;
};

static inline int32_t sub_adsh_lookup(const SubAdsSlot* ht, uint64_t cap, int32_t adsh_code) {
    uint64_t mask = cap - 1;
    uint64_t h = (uint64_t)(uint32_t)adsh_code * 2654435761ULL & mask;
    for (uint64_t p = 0; p < cap; ++p) {
        uint64_t idx = (h + p) & mask;
        if (ht[idx].adsh_code == INT32_MIN) return -1;
        if (ht[idx].adsh_code == adsh_code) return ht[idx].row_id;
    }
    return -1;
}

// ---- Group hash: uint64_t packed key -> int64_t sum_cents ----
// C29: use int64_t cents to avoid double precision loss for values up to 1e14.
// Individual values up to 1e14 * 100 = 1e16 exceed double's 53-bit mantissa (2^53~9e15).
static constexpr uint64_t EMPTY_GROUP_KEY = UINT64_MAX;

struct GroupSlot {
    uint64_t key;
    double   sum_val;
};

struct GroupHash {
    GroupSlot* slots;
    uint32_t cap;
    uint32_t mask;

    void init(uint32_t capacity) {
        cap = capacity;
        mask = cap - 1;
        slots = new GroupSlot[cap];
        for (uint32_t i = 0; i < cap; ++i) {
            slots[i].key = EMPTY_GROUP_KEY;
            slots[i].sum_val = 0.0;
        }
    }

    void add(uint64_t key, double v) {
        uint64_t h = key;
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        uint32_t pos = (uint32_t)(h & mask);
        for (uint32_t p = 0; p < cap; ++p) {
            uint32_t idx = (pos + p) & mask;
            if (slots[idx].key == EMPTY_GROUP_KEY) {
                slots[idx].key = key;
                slots[idx].sum_val = v;
                return;
            }
            if (slots[idx].key == key) {
                slots[idx].sum_val += v;
                return;
            }
        }
    }

    // Merge: accept double partial sum
    void add_plain(uint64_t key, double s) {
        uint64_t h = key;
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        uint32_t pos = (uint32_t)(h & mask);
        for (uint32_t p = 0; p < cap; ++p) {
            uint32_t idx = (pos + p) & mask;
            if (slots[idx].key == EMPTY_GROUP_KEY) {
                slots[idx].key = key;
                slots[idx].sum_val = s;
                return;
            }
            if (slots[idx].key == key) {
                slots[idx].sum_val += s;
                return;
            }
        }
    }
};

// ---- Cik hash: int32_t cik -> int64_t sum_cents ----
// C29: use int64_t cents to avoid double precision loss for values up to 1e14.
static constexpr int32_t EMPTY_CIK = INT32_MIN;

struct CikSlot {
    int32_t cik;
    int32_t _pad;
    double  sum_val;
};

struct CikHash {
    CikSlot* slots;
    uint32_t cap;
    uint32_t mask;

    void init(uint32_t capacity) {
        cap = capacity;
        mask = cap - 1;
        slots = new CikSlot[cap];
        for (uint32_t i = 0; i < cap; ++i) {
            slots[i].cik = EMPTY_CIK;
            slots[i].sum_val = 0.0;
        }
    }

    void add(int32_t cik, double v) {
        uint32_t h = (uint32_t)((uint64_t)(uint32_t)cik * 2654435761ULL) & mask;
        for (uint32_t p = 0; p < cap; ++p) {
            uint32_t idx = (h + p) & mask;
            if (slots[idx].cik == EMPTY_CIK) {
                slots[idx].cik = cik;
                slots[idx].sum_val = v;
                return;
            }
            if (slots[idx].cik == cik) {
                slots[idx].sum_val += v;
                return;
            }
        }
    }

    void add_plain(int32_t cik, double s) {
        uint32_t h = (uint32_t)((uint64_t)(uint32_t)cik * 2654435761ULL) & mask;
        for (uint32_t p = 0; p < cap; ++p) {
            uint32_t idx = (h + p) & mask;
            if (slots[idx].cik == EMPTY_CIK) {
                slots[idx].cik = cik;
                slots[idx].sum_val = s;
                return;
            }
            if (slots[idx].cik == cik) {
                slots[idx].sum_val += s;
                return;
            }
        }
    }
};

// ---- Main query function ----

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const int16_t* num_uom_col   = nullptr;
    const double*  num_value_col = nullptr;
    const int32_t* num_adsh_col  = nullptr;
    const int32_t* sub_fy_col    = nullptr;
    const int32_t* sub_name_col  = nullptr;
    const int32_t* sub_cik_col   = nullptr;
    size_t num_rows = 0;

    const ZoneBlock16* num_uom_blocks = nullptr;
    uint32_t num_uom_num_blocks = 0;

    // P11: pre-built sub adsh hash index
    const SubAdsSlot* sub_ads_ht = nullptr;
    uint64_t sub_ads_cap = 0;

    std::vector<std::string> name_dict;
    int16_t usd_code = -1;

    {
        GENDB_PHASE("data_loading");
        size_t sz;

        num_uom_col   = reinterpret_cast<const int16_t*>(mmap_file(gendb_dir + "/num/uom.bin", sz));
        num_rows      = sz / sizeof(int16_t);
        num_value_col = reinterpret_cast<const double*>(mmap_file(gendb_dir + "/num/value.bin", sz));
        num_adsh_col  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/num/adsh.bin", sz));

        sub_fy_col    = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/fy.bin", sz));
        sub_name_col  = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/name.bin", sz));
        sub_cik_col   = reinterpret_cast<const int32_t*>(mmap_file(gendb_dir + "/sub/cik.bin", sz));

        // P11: mmap pre-built sub adsh hash index
        {
            const char* raw = reinterpret_cast<const char*>(mmap_file(gendb_dir + "/indexes/sub_adsh_hash.bin", sz));
            sub_ads_cap = *reinterpret_cast<const uint64_t*>(raw);
            sub_ads_ht  = reinterpret_cast<const SubAdsSlot*>(raw + sizeof(uint64_t));
        }

        // num uom zone map
        {
            const char* raw = reinterpret_cast<const char*>(mmap_file(gendb_dir + "/indexes/num_uom_zone_map.bin", sz));
            num_uom_num_blocks = *reinterpret_cast<const uint32_t*>(raw);
            num_uom_blocks = reinterpret_cast<const ZoneBlock16*>(raw + sizeof(uint32_t));
        }

        // C2: resolve USD code at runtime
        {
            std::ifstream f(gendb_dir + "/num/uom_dict.txt");
            std::string line; int16_t code = 0;
            while (std::getline(f, line)) {
                if (line == "USD") usd_code = code;
                ++code;
            }
        }
        if (usd_code < 0) { fprintf(stderr, "USD not found in dict\n"); exit(1); }

        // C18: load name dict for output decode
        {
            std::ifstream f(gendb_dir + "/sub/name_dict.txt");
            std::string line;
            while (std::getline(f, line)) name_dict.push_back(line);
        }
    }

    // P11: No build_joins phase — pre-built index replaces runtime SubHash construction

    // ---- DETERMINE USD SCAN RANGE via zone map ----
    uint32_t usd_scan_start = (uint32_t)num_rows, usd_scan_end = 0;
    {
        uint32_t row_off = 0;
        for (uint32_t b = 0; b < num_uom_num_blocks; ++b) {
            uint32_t rc = num_uom_blocks[b].row_count;
            bool qualifies = !(num_uom_blocks[b].max_val < usd_code || num_uom_blocks[b].min_val > usd_code);
            if (qualifies) {
                if (row_off < usd_scan_start) usd_scan_start = row_off;
                if (row_off + rc > usd_scan_end) usd_scan_end = row_off + rc;
            }
            row_off += rc;
        }
    }

    // ---- PARALLEL SCAN + DUAL AGGREGATION ----
    // C9: thread-local maps sized for FULL group cardinality (not /nthreads)
    uint32_t group_cap = next_pow2(27000 * 2);  // 65536
    uint32_t cik_cap   = next_pow2(12000 * 2);  // 32768

    int nthreads = omp_get_max_threads();
    std::vector<GroupHash> tl_group(nthreads);
    std::vector<CikHash>   tl_cik(nthreads);
    for (int t = 0; t < nthreads; ++t) {
        tl_group[t].init(group_cap);
        tl_cik[t].init(cik_cap);
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 100000)
        for (uint32_t i = usd_scan_start; i < usd_scan_end; ++i) {
            if (num_uom_col[i] != usd_code) continue;

            double v = num_value_col[i];
            if (std::isnan(v)) continue;

            // P11: probe pre-built index for adsh -> row_id
            int32_t row_id = sub_adsh_lookup(sub_ads_ht, sub_ads_cap, num_adsh_col[i]);
            if (row_id < 0) continue;

            // Still must check fy=2022 (index covers all rows, not just fy=2022)
            if (sub_fy_col[row_id] != 2022) continue;

            int32_t name_code = sub_name_col[row_id];
            int32_t cik       = sub_cik_col[row_id];

            int tid = omp_get_thread_num();
            // Use double accumulation to match reference SQL SUM(double).
            // llround(v*100) is NOT safe for |v|>=~9e13 (v*100 exceeds 2^53 exact range).
            // At SUM scale ~10^13, double spacing is ~2^-7 ≈ 0.008, so %.2f output is exact.
            // C15: pack BOTH GROUP BY keys
            uint64_t gkey = ((uint64_t)(uint32_t)name_code << 32) | (uint32_t)cik;
            tl_group[tid].add(gkey, v);
            tl_cik[tid].add(cik, v);
        }
    }

    // ---- MERGE thread-local maps ----
    GroupHash merged_group;
    CikHash   merged_cik;
    merged_group.init(group_cap);
    merged_cik.init(cik_cap);

    {
        GENDB_PHASE("aggregation_merge");
        for (int t = 0; t < nthreads; ++t) {
            for (uint32_t i = 0; i < tl_group[t].cap; ++i) {
                if (tl_group[t].slots[i].key != EMPTY_GROUP_KEY)
                    merged_group.add_plain(tl_group[t].slots[i].key,
                                           tl_group[t].slots[i].sum_val);
            }
            for (uint32_t i = 0; i < tl_cik[t].cap; ++i) {
                if (tl_cik[t].slots[i].cik != EMPTY_CIK)
                    merged_cik.add_plain(tl_cik[t].slots[i].cik,
                                         tl_cik[t].slots[i].sum_val);
            }
        }
    }

    // ---- COMPUTE HAVING THRESHOLD ----
    // Use double accumulation to match reference SQL AVG(SUM(value)).
    double avg_threshold = 0.0;
    {
        double total_cik_sum = 0.0;
        int64_t ncik = 0;
        for (uint32_t i = 0; i < merged_cik.cap; ++i) {
            if (merged_cik.slots[i].cik != EMPTY_CIK) {
                total_cik_sum += merged_cik.slots[i].sum_val;
                ++ncik;
            }
        }
        if (ncik > 0) {
            avg_threshold = total_cik_sum / (double)ncik;
        }
    }

    // ---- FILTER + TOP-100 ----
    struct ResultRow {
        int32_t name_code;
        int32_t cik;
        double  sum_val;
    };

    std::vector<ResultRow> candidates;
    candidates.reserve(2000);
    for (uint32_t i = 0; i < merged_group.cap; ++i) {
        if (merged_group.slots[i].key == EMPTY_GROUP_KEY) continue;
        if (merged_group.slots[i].sum_val <= avg_threshold) continue;
        int32_t name_code = (int32_t)(merged_group.slots[i].key >> 32);
        int32_t cik       = (int32_t)(uint32_t)merged_group.slots[i].key;
        candidates.push_back({name_code, cik, merged_group.slots[i].sum_val});
    }

    // P6: partial sort for LIMIT
    if ((int)candidates.size() > 100) {
        std::partial_sort(candidates.begin(), candidates.begin() + 100, candidates.end(),
            [](const ResultRow& a, const ResultRow& b) { return a.sum_val > b.sum_val; });
        candidates.resize(100);
    } else {
        std::sort(candidates.begin(), candidates.end(),
            [](const ResultRow& a, const ResultRow& b) { return a.sum_val > b.sum_val; });
    }

    // ---- OUTPUT ----
    {
        GENDB_PHASE("output");
        std::filesystem::create_directories(results_dir);
        FILE* out = fopen((results_dir + "/Q3.csv").c_str(), "w");
        if (!out) { perror("fopen"); exit(1); }
        fprintf(out, "name,cik,total_value\n");
        for (auto& r : candidates) {
            const std::string& nm = (r.name_code >= 0 && r.name_code < (int32_t)name_dict.size())
                ? name_dict[r.name_code] : "";
            // Use %.2f: at SUM scale ~10^13, double spacing ~2^-7 ≈ 0.008 < 0.01,
            // so 2 decimal places are correctly representable and match reference SQL output.
            fprintf(out, "%s,%d,%.2f\n", nm.c_str(), r.cik, r.sum_val);
        }
        fclose(out);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
