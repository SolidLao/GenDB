/*
 * Q3 - SEC EDGAR: SUM(value) GROUP BY (name, cik) with HAVING > AVG(per-cik sums)
 * Strategy:
 *   1. Build sub flat array sub_info[adsh_code]={cik,name_code} for fy=2022 rows
 *   2. Parallel scan num: uom='USD', for each matching row look up sub via flat array
 *   3. Thread-local dual aggregation:
 *      Map A: (name_code,cik) -> int64_t sum_cents  [outer GROUP BY]
 *      Map B: cik             -> int64_t sum_cents  [HAVING subquery GROUP BY]
 *   4. Merge thread-local maps
 *   5. Compute HAVING threshold = AVG(per-cik sum)
 *   6. Filter Map A entries, partial_sort top-100, output
 */

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <climits>

#include "timing_utils.h"

namespace {

// ─── Dictionary helpers ────────────────────────────────────────────────────

static int16_t find_code_i16(const std::string& path, const std::string& target) {
    std::ifstream f(path);
    std::string line;
    int16_t code = 0;
    while (std::getline(f, line)) {
        if (line == target) return code;
        ++code;
    }
    return -1;
}

static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> d;
    d.reserve(4096);
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) d.push_back(std::move(line));
    return d;
}

// ─── mmap helper ──────────────────────────────────────────────────────────

template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    if (st.st_size == 0) { close(fd); n_rows = 0; return nullptr; }
    auto* ptr = reinterpret_cast<const T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    n_rows = st.st_size / sizeof(T);
    close(fd);
    return ptr;
}

// ─── Hash table: Map A  key=uint64_t(name_code<<32|cik), val=int64_t cents ───
// Use int64_t cents accumulation: column max ~5.94e13 far exceeds double precision.

static constexpr uint32_t MAP_A_CAP  = 32768;  // next_power_of_2(14600*2)
static constexpr uint64_t A_SENTINEL = UINT64_MAX;

struct MapASlot {
    uint64_t key;
    int64_t  val;   // cents (value * 100 rounded) for precision
};

static inline uint32_t hash_a(uint64_t k) {
    // Fibonacci hashing
    return static_cast<uint32_t>((k * 11400714819323198485ULL) >> 32) & (MAP_A_CAP - 1);
}

static void upsert_a(MapASlot* __restrict__ map, uint64_t key, int64_t cents) {
    uint32_t h = hash_a(key);
    for (uint32_t p = 0; p < MAP_A_CAP; ++p) {
        uint32_t s = (h + p) & (MAP_A_CAP - 1);
        if (map[s].key == A_SENTINEL) {
            map[s].key = key;
            map[s].val = cents;
            return;
        }
        if (map[s].key == key) {
            map[s].val += cents;
            return;
        }
    }
    // Should never overflow with 14600 groups and CAP=32768
}

// ─── Hash table: Map B  key=int32_t cik, val=int64_t ─────────────────────

static constexpr uint32_t MAP_B_CAP   = 32768;
static constexpr int32_t  B_SENTINEL  = INT32_MIN;

struct MapBSlot {
    int32_t  key;
    int32_t  _pad;
    int64_t  val;
};

static inline uint32_t hash_b(int32_t k) {
    return (static_cast<uint32_t>(k) * 2654435761u) & (MAP_B_CAP - 1);
}

static void upsert_b(MapBSlot* __restrict__ map, int32_t key, int64_t cents) {
    uint32_t h = hash_b(key);
    for (uint32_t p = 0; p < MAP_B_CAP; ++p) {
        uint32_t s = (h + p) & (MAP_B_CAP - 1);
        if (map[s].key == B_SENTINEL) {
            map[s].key = key;
            map[s].val = cents;
            return;
        }
        if (map[s].key == key) {
            map[s].val += cents;
            return;
        }
    }
}

// ─── Sub lookup struct ────────────────────────────────────────────────────

struct SubInfo {
    int32_t cik;        // -1 = not fy2022
    int32_t name_code;
};

static constexpr int32_t SUB_ADSH_RANGE = 86135;

// ─── Main query ───────────────────────────────────────────────────────────

} // end anonymous namespace

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    // ── Data Loading ─────────────────────────────────────────────────────
    size_t num_rows = 0, sub_rows = 0;
    const int16_t* num_uom   = nullptr;
    const double*  num_value = nullptr;
    const int32_t* num_adsh  = nullptr;
    const int32_t* sub_adsh  = nullptr;
    const int32_t* sub_fy    = nullptr;
    const int32_t* sub_cik   = nullptr;
    const int32_t* sub_name  = nullptr;

    int16_t usd_code = -1;
    std::vector<std::string> name_dict;

    {
        GENDB_PHASE("data_loading");

        // Dictionaries (C2: load at runtime, never hardcode)
        usd_code  = find_code_i16(gendb_dir + "/num/uom_dict.txt", "USD");
        name_dict = load_dict(gendb_dir + "/sub/name_dict.txt");

        // num columns
        num_uom   = mmap_col<int16_t>(gendb_dir + "/num/uom.bin",   num_rows);
        size_t tmp;
        num_value = mmap_col<double>  (gendb_dir + "/num/value.bin", tmp);
        num_adsh  = mmap_col<int32_t> (gendb_dir + "/num/adsh.bin",  tmp);

        // sub columns
        sub_adsh  = mmap_col<int32_t>(gendb_dir + "/sub/adsh.bin",  sub_rows);
        sub_fy    = mmap_col<int32_t>(gendb_dir + "/sub/fy.bin",    tmp);
        sub_cik   = mmap_col<int32_t>(gendb_dir + "/sub/cik.bin",   tmp);
        sub_name  = mmap_col<int32_t>(gendb_dir + "/sub/name.bin",  tmp);
    }

    // ── Build Sub Flat Array (dim_filter) ─────────────────────────────────
    // sub_info[adsh_code] = {cik, name_code}, sentinel cik=-1 for non-fy2022
    std::vector<SubInfo> sub_info(SUB_ADSH_RANGE, SubInfo{-1, -1});  // C20: std::fill via constructor

    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < sub_rows; ++i) {
            if (sub_fy[i] != 2022) continue;
            int32_t ac = sub_adsh[i];
            if (ac < 0 || ac >= SUB_ADSH_RANGE) continue;
            sub_info[ac] = {sub_cik[i], sub_name[i]};
        }
    }

    // ── Parallel Scan + Thread-local Dual Aggregation ─────────────────────
    int nthreads = omp_get_max_threads();

    // Allocate thread-local hash maps on heap
    // Map A: (name_code<<32|cik) -> sum_cents
    // Map B: cik -> sum_cents
    std::vector<MapASlot> all_map_a(static_cast<size_t>(nthreads) * MAP_A_CAP);
    std::vector<MapBSlot> all_map_b(static_cast<size_t>(nthreads) * MAP_B_CAP);

    // C20: use std::fill for sentinel initialization
    std::fill(all_map_a.begin(), all_map_a.end(), MapASlot{A_SENTINEL, 0LL});
    std::fill(all_map_b.begin(), all_map_b.end(), MapBSlot{B_SENTINEL, 0, 0LL});

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(static) num_threads(nthreads)
        for (size_t i = 0; i < num_rows; ++i) {
            // Filter: uom = 'USD'
            if (num_uom[i] != usd_code) continue;

            // Join: look up sub via flat array
            int32_t ac = num_adsh[i];
            if (ac < 0 || ac >= SUB_ADSH_RANGE) continue;
            const SubInfo& si = sub_info[ac];
            if (si.cik < 0) continue;  // not fy=2022

            // value IS NOT NULL (filter non-finite)
            double val = num_value[i];
            if (!std::isfinite(val)) continue;

            // Both Map A and Map B use int64_t cents for precision.
            // Column max ~5.94e13 exceeds double precision threshold.
            int64_t cents = llround(val * 100.0);

            int tid = omp_get_thread_num();
            MapASlot* ma = all_map_a.data() + static_cast<size_t>(tid) * MAP_A_CAP;
            MapBSlot* mb = all_map_b.data() + static_cast<size_t>(tid) * MAP_B_CAP;

            // Map A: key = (name_code << 32) | cik (C15: both GROUP BY cols)
            uint64_t key_a = (static_cast<uint64_t>(static_cast<uint32_t>(si.name_code)) << 32)
                           |  static_cast<uint64_t>(static_cast<uint32_t>(si.cik));
            upsert_a(ma, key_a, cents);   // int64_t cents for precision

            // Map B: key = cik, int64_t cents for HAVING threshold
            upsert_b(mb, si.cik, cents);
        }
    }

    // ── Merge Thread-local Maps ───────────────────────────────────────────
    {
        GENDB_PHASE("aggregation_merge");

        // Merge into thread-0's maps sequentially (14600 groups × 64 threads = fast)
        MapASlot* ma0 = all_map_a.data();
        MapBSlot* mb0 = all_map_b.data();

        for (int t = 1; t < nthreads; ++t) {
            const MapASlot* mat = all_map_a.data() + static_cast<size_t>(t) * MAP_A_CAP;
            const MapBSlot* mbt = all_map_b.data() + static_cast<size_t>(t) * MAP_B_CAP;

            for (uint32_t s = 0; s < MAP_A_CAP; ++s) {
                if (mat[s].key != A_SENTINEL)
                    upsert_a(ma0, mat[s].key, mat[s].val);
            }
            for (uint32_t s = 0; s < MAP_B_CAP; ++s) {
                if (mbt[s].key != B_SENTINEL)
                    upsert_b(mb0, mbt[s].key, mbt[s].val);
            }
        }
    }

    // ── Compute HAVING Threshold ──────────────────────────────────────────
    // Use int64_t cents from Map B for accurate threshold.
    // HAVING: group_sum_cents > grand_sum_cents / num_ciks
    // Rearranged to avoid division: group_sum_cents * num_ciks > grand_sum_cents
    int64_t grand_sum_cents = 0;
    int64_t num_ciks        = 0;
    {
        const MapBSlot* mb0 = all_map_b.data();
        for (uint32_t s = 0; s < MAP_B_CAP; ++s) {
            if (mb0[s].key != B_SENTINEL) {
                grand_sum_cents += mb0[s].val;
                ++num_ciks;
            }
        }
    }

    // ── Collect candidates passing HAVING, decode, partial_sort top-100 ───
    struct ResultRow {
        std::string name;
        int32_t     cik;
        int64_t     sum_cents;    // int64_t cents for precision
    };

    std::vector<ResultRow> candidates;
    candidates.reserve(1024);

    {
        const MapASlot* ma0 = all_map_a.data();
        for (uint32_t s = 0; s < MAP_A_CAP; ++s) {
            if (ma0[s].key == A_SENTINEL) continue;

            int64_t grp_sum_cents = ma0[s].val;

            // HAVING: group_sum > AVG(per-cik sums)
            // Equivalent (no division): group_sum_cents * num_ciks > grand_sum_cents
            // Use __int128 to avoid overflow: max grp_sum_cents ~5.94e15, num_ciks ~14000 → product ~8.3e19 > INT64_MAX
            if (num_ciks <= 0 || (__int128)grp_sum_cents * num_ciks <= (__int128)grand_sum_cents) continue;

            int32_t cik       = static_cast<int32_t>(ma0[s].key & 0xFFFFFFFFULL);
            int32_t name_code = static_cast<int32_t>(ma0[s].key >> 32);

            const std::string& nm = (name_code >= 0 && name_code < static_cast<int32_t>(name_dict.size()))
                                  ? name_dict[name_code] : "";
            candidates.push_back({nm, cik, grp_sum_cents});
        }
    }

    // partial_sort top-100 by total_value DESC (P6)
    size_t topk = std::min(static_cast<size_t>(100), candidates.size());
    std::partial_sort(candidates.begin(), candidates.begin() + topk, candidates.end(),
        [](const ResultRow& a, const ResultRow& b) {
            return a.sum_cents > b.sum_cents;
        });

    // ── Output ────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        std::filesystem::create_directories(results_dir);
        std::ofstream out(results_dir + "/Q3.csv");
        out << "name,cik,total_value\n";

        char vbuf[64];
        for (size_t i = 0; i < topk; ++i) {
            const auto& row = candidates[i];

            // CSV output: name may contain commas → quote it
            const std::string& nm = row.name;
            bool needs_quote = (nm.find(',') != std::string::npos ||
                                nm.find('"') != std::string::npos ||
                                nm.find('\n') != std::string::npos);
            if (needs_quote) {
                out << '"';
                for (char c : nm) {
                    if (c == '"') out << '"';
                    out << c;
                }
                out << '"';
            } else {
                out << nm;
            }

            // Format value from int64_t cents to decimal with 2 decimal places
            bool neg = row.sum_cents < 0;
            int64_t c = neg ? -row.sum_cents : row.sum_cents;
            snprintf(vbuf, sizeof(vbuf), "%s%lld.%02lld",
                     neg ? "-" : "",
                     (long long)(c / 100), (long long)(c % 100));
            out << ',' << row.cik << ',' << vbuf << '\n';
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
