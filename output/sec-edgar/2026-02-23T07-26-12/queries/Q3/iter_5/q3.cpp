// Q3: SEC EDGAR – SUM(value) by (name, cik) for fy=2022, uom=USD
// HAVING > AVG(per-cik sums), ORDER BY total_value DESC LIMIT 100
//
// Precision strategy: Kahan-Neumaier compensated summation in long double.
// - long double on x86-64 = 80-bit extended (64-bit mantissa, ~18.5 sig digits)
// - Kahan error bound: ~2 * ε_ld * |S| ≈ 2 * 1.08e-19 * 5e14 ≈ 1e-4
// - This is well within %.2f rounding threshold (0.005)
// - Result is ORDER-INDEPENDENT — matches DuckDB regardless of scan order
//
// CSV quoting: RFC 4180 for fields containing commas (matching Python csv.writer).

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
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "timing_utils.h"

// ---- Utilities ----

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

// ---- Zone map struct ----
struct ZoneBlock16 {
    int16_t min_val;
    int16_t max_val;
    uint32_t row_count;
};

// ---- P11: Pre-built sub adsh hash index ----
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

// ---- Kahan-Neumaier compensated sum in long double ----
// long double on x86-64 GCC = 80-bit extended precision (64-bit mantissa)
// Kahan error ≈ 2 * 2^(-63) * |S| ≈ 1e-4 for |S| ≈ 5e14
// This is order-independent to within %.2f rounding threshold
struct KahanSumLD {
    long double sum;
    long double comp;
    KahanSumLD() : sum(0.0L), comp(0.0L) {}
    void add(long double v) {
        long double t = sum + v;
        if (std::fabs(sum) >= std::fabs(v))
            comp += (sum - t) + v;
        else
            comp += (v - t) + sum;
        sum = t;
    }
    double result() const { return (double)(sum + comp); }
};

// ---- CSV quoting helper (RFC 4180) ----
static void write_csv_field(FILE* f, const std::string& s) {
    if (s.find(',') != std::string::npos || s.find('"') != std::string::npos ||
        s.find('\n') != std::string::npos) {
        fputc('"', f);
        for (char c : s) {
            if (c == '"') fputc('"', f);
            fputc(c, f);
        }
        fputc('"', f);
    } else {
        fputs(s.c_str(), f);
    }
}

// ---- Main query function ----

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

    // ---- SINGLE-THREADED SCAN + DUAL AGGREGATION ----
    // Kahan-Neumaier in long double: order-independent to within %.2f precision.
    std::unordered_map<uint64_t, KahanSumLD> group_sums;   // (name_code, cik) -> sum
    std::unordered_map<int32_t, KahanSumLD>  cik_sums;     // cik -> sum (HAVING)
    group_sums.reserve(30000);
    cik_sums.reserve(15000);

    {
        GENDB_PHASE("main_scan");

        for (uint32_t i = usd_scan_start; i < usd_scan_end; ++i) {
            if (num_uom_col[i] != usd_code) continue;

            double v = num_value_col[i];
            if (std::isnan(v)) continue;

            int32_t row_id = sub_adsh_lookup(sub_ads_ht, sub_ads_cap, num_adsh_col[i]);
            if (row_id < 0) continue;

            if (sub_fy_col[row_id] != 2022) continue;

            int32_t name_code = sub_name_col[row_id];
            int32_t cik       = sub_cik_col[row_id];

            long double lv = (long double)v;
            uint64_t gkey = ((uint64_t)(uint32_t)name_code << 32) | (uint32_t)cik;
            group_sums[gkey].add(lv);
            cik_sums[cik].add(lv);
        }
    }

    // ---- COMPUTE HAVING THRESHOLD ----
    double avg_threshold = 0.0;
    {
        KahanSumLD total_kahan;
        for (auto& [cik, ks] : cik_sums) {
            total_kahan.add((long double)ks.result());
        }
        int64_t ncik = (int64_t)cik_sums.size();
        if (ncik > 0) {
            avg_threshold = (double)((long double)total_kahan.result() / (long double)ncik);
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
    for (auto& [gkey, ks] : group_sums) {
        double sv = ks.result();
        if (sv <= avg_threshold) continue;
        int32_t name_code = (int32_t)(gkey >> 32);
        int32_t cik       = (int32_t)(uint32_t)gkey;
        candidates.push_back({name_code, cik, sv});
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
            write_csv_field(out, nm);
            fprintf(out, ",%d,%.2f\n", r.cik, r.sum_val);
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
