#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

struct HashEntry64U32 {
    uint64_t key;
    uint32_t rowid;
    uint32_t pad;
};
static_assert(sizeof(HashEntry64U32) == 16, "HashEntry64U32 layout mismatch");

struct LineitemMatch {
    uint32_t rowid;
    uint8_t nationkey;
    int32_t supplycost_cents;
};

struct ResultRow {
    std::string nation;
    int year;
    int64_t sum_profit_1e4;
};

inline int32_t round_to_cents(double v) {
    return static_cast<int32_t>(v * 100.0 + 0.5);
}

std::vector<std::string> load_dict_strings(const std::string& path) {
    MmapColumn<uint8_t> d(path);
    if (d.file_size < sizeof(uint32_t)) {
        throw std::runtime_error("dictionary too small: " + path);
    }

    const uint8_t* p = d.data;
    const uint8_t* end = d.data + d.file_size;

    uint32_t n = 0;
    std::memcpy(&n, p, sizeof(uint32_t));
    p += sizeof(uint32_t);

    std::vector<std::string> out;
    out.reserve(n);

    for (uint32_t i = 0; i < n; ++i) {
        if (p + sizeof(uint32_t) > end) {
            throw std::runtime_error("dictionary truncated lengths: " + path);
        }
        uint32_t len = 0;
        std::memcpy(&len, p, sizeof(uint32_t));
        p += sizeof(uint32_t);

        if (p + len > end) {
            throw std::runtime_error("dictionary truncated payload: " + path);
        }
        out.emplace_back(reinterpret_cast<const char*>(p), len);
        p += len;
    }
    return out;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    GENDB_PHASE("total");

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    MmapColumn<uint8_t> has_green;
    MmapColumn<int16_t> year_codes;

    MmapColumn<int32_t> ps_partkey;
    MmapColumn<int32_t> ps_suppkey;
    MmapColumn<double> ps_supplycost;

    MmapColumn<int32_t> s_suppkey;
    MmapColumn<int32_t> s_nationkey;

    MmapColumn<int32_t> n_nationkey;
    MmapColumn<uint32_t> n_name_code;

    MmapColumn<int32_t> l_orderkey;
    MmapColumn<double> l_extendedprice;
    MmapColumn<uint16_t> l_discount_scaled100;
    MmapColumn<uint16_t> l_quantity_scaled100;

    MmapColumn<uint8_t> lineitem_partsupp_idx_raw;

    std::vector<std::string> nation_dict;

    {
        GENDB_PHASE("data_loading");

        has_green.open(gendb_dir + "/column_versions/part.p_name.has_green_by_partkey/codes.bin");
        year_codes.open(gendb_dir + "/column_versions/orders.o_orderdate.year_dense_by_orderkey/codes.bin");

        ps_partkey.open(gendb_dir + "/partsupp/ps_partkey.bin");
        ps_suppkey.open(gendb_dir + "/partsupp/ps_suppkey.bin");
        ps_supplycost.open(gendb_dir + "/partsupp/ps_supplycost.bin");

        s_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
        s_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");

        n_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
        n_name_code.open(gendb_dir + "/nation/n_name.bin");

        l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount_scaled100.open(gendb_dir + "/column_versions/lineitem.l_discount.int16_scaled_by_100/codes.bin");
        l_quantity_scaled100.open(gendb_dir + "/column_versions/lineitem.l_quantity.int16_scaled_by_100/codes.bin");

        lineitem_partsupp_idx_raw.open(gendb_dir + "/lineitem/lineitem_partsupp_hash.idx");

        nation_dict = load_dict_strings(gendb_dir + "/nation/n_name.dict");

        mmap_prefetch_all(has_green, year_codes,
                          ps_partkey, ps_suppkey, ps_supplycost,
                          s_suppkey, s_nationkey,
                          n_nationkey, n_name_code,
                          l_orderkey, l_extendedprice, l_discount_scaled100, l_quantity_scaled100,
                          lineitem_partsupp_idx_raw);
    }

    if (ps_partkey.count != ps_suppkey.count || ps_partkey.count != ps_supplycost.count) {
        throw std::runtime_error("partsupp column size mismatch");
    }
    if (s_suppkey.count != s_nationkey.count) {
        throw std::runtime_error("supplier column size mismatch");
    }
    if (n_nationkey.count != n_name_code.count) {
        throw std::runtime_error("nation column size mismatch");
    }
    if (l_orderkey.count != l_extendedprice.count || l_orderkey.count != l_discount_scaled100.count ||
        l_orderkey.count != l_quantity_scaled100.count) {
        throw std::runtime_error("lineitem column size mismatch");
    }

    if (lineitem_partsupp_idx_raw.file_size < 16) {
        throw std::runtime_error("lineitem_partsupp_hash.idx too small");
    }

    uint64_t idx_buckets = 0;
    uint64_t idx_n = 0;
    const uint8_t* idx_base = lineitem_partsupp_idx_raw.data;
    std::memcpy(&idx_buckets, idx_base, sizeof(uint64_t));
    std::memcpy(&idx_n, idx_base + sizeof(uint64_t), sizeof(uint64_t));

    if (idx_buckets == 0 || (idx_buckets & (idx_buckets - 1)) != 0) {
        throw std::runtime_error("lineitem_partsupp_hash.idx invalid bucket count");
    }

    const size_t offsets_bytes = static_cast<size_t>(idx_buckets + 1) * sizeof(uint64_t);
    const size_t entries_bytes = static_cast<size_t>(idx_n) * sizeof(HashEntry64U32);
    if (lineitem_partsupp_idx_raw.file_size < 16 + offsets_bytes + entries_bytes) {
        throw std::runtime_error("lineitem_partsupp_hash.idx layout mismatch");
    }

    const uint64_t* idx_offsets = reinterpret_cast<const uint64_t*>(idx_base + 16);
    const HashEntry64U32* idx_entries = reinterpret_cast<const HashEntry64U32*>(idx_base + 16 + offsets_bytes);

    std::vector<int32_t> green_partkeys;
    {
        GENDB_PHASE("dim_filter");
        green_partkeys.reserve(120000);
        for (size_t partkey = 1; partkey < has_green.count; ++partkey) {
            if (has_green.data[partkey] == 1) {
                green_partkeys.push_back(static_cast<int32_t>(partkey));
            }
        }
    }

    constexpr int kNationCount = 25;
    constexpr int kBaseYear = 1992;
    constexpr int kYearCount = 7;

    std::vector<int8_t> supp_to_nation;
    std::vector<uint32_t> nation_name_code_by_key;
    std::vector<LineitemMatch> matches;

    {
        GENDB_PHASE("build_joins");

        int32_t max_suppkey = 0;
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            if (s_suppkey.data[i] > max_suppkey) max_suppkey = s_suppkey.data[i];
        }
        supp_to_nation.assign(static_cast<size_t>(max_suppkey) + 1, static_cast<int8_t>(-1));

        for (size_t i = 0; i < s_suppkey.count; ++i) {
            const int32_t sk = s_suppkey.data[i];
            const int32_t nk = s_nationkey.data[i];
            if (sk > 0 && nk >= 0 && nk < kNationCount) {
                supp_to_nation[static_cast<size_t>(sk)] = static_cast<int8_t>(nk);
            }
        }

        int32_t max_nationkey = 0;
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            if (n_nationkey.data[i] > max_nationkey) max_nationkey = n_nationkey.data[i];
        }
        nation_name_code_by_key.assign(static_cast<size_t>(max_nationkey) + 1, 0);
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            const int32_t nk = n_nationkey.data[i];
            if (nk >= 0) {
                nation_name_code_by_key[static_cast<size_t>(nk)] = n_name_code.data[i];
            }
        }

        struct FilteredPS {
            int32_t partkey;
            int32_t suppkey;
            int32_t supplycost_cents;
            uint8_t nationkey;
        };

        std::vector<FilteredPS> filtered_ps;
        filtered_ps.reserve(450000);

        const int32_t* ps_pk = ps_partkey.data;
        const int32_t* ps_sk = ps_suppkey.data;

        for (size_t g = 0; g < green_partkeys.size(); ++g) {
            const int32_t pk = green_partkeys[g];
            const int32_t* lo = std::lower_bound(ps_pk, ps_pk + ps_partkey.count, pk);
            if (lo == ps_pk + ps_partkey.count || *lo != pk) continue;
            const int32_t* hi = std::upper_bound(lo, ps_pk + ps_partkey.count, pk);

            const size_t begin = static_cast<size_t>(lo - ps_pk);
            const size_t end = static_cast<size_t>(hi - ps_pk);

            for (size_t r = begin; r < end; ++r) {
                const int32_t sk = ps_sk[r];
                if (sk <= 0 || static_cast<size_t>(sk) >= supp_to_nation.size()) continue;
                const int8_t nk = supp_to_nation[static_cast<size_t>(sk)];
                if (nk < 0) continue;
                filtered_ps.push_back(FilteredPS{pk, sk, round_to_cents(ps_supplycost.data[r]), static_cast<uint8_t>(nk)});
            }
        }

        const int threads = std::max(1, omp_get_max_threads());
        std::vector<std::vector<LineitemMatch>> local_matches(static_cast<size_t>(threads));

#pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            std::vector<LineitemMatch>& out = local_matches[static_cast<size_t>(tid)];
            out.reserve(filtered_ps.size() / static_cast<size_t>(threads) + 4096);

#pragma omp for schedule(static)
            for (size_t i = 0; i < filtered_ps.size(); ++i) {
                const FilteredPS& ps = filtered_ps[i];
                const uint64_t packed = (static_cast<uint64_t>(static_cast<uint32_t>(ps.partkey)) << 32) |
                                        static_cast<uint32_t>(ps.suppkey);
                const uint64_t h = mix64(packed) & (idx_buckets - 1);
                const uint64_t begin = idx_offsets[h];
                const uint64_t end = idx_offsets[h + 1];

                for (uint64_t p = begin; p < end; ++p) {
                    if (idx_entries[p].key == packed) {
                        out.push_back(LineitemMatch{idx_entries[p].rowid, ps.nationkey, ps.supplycost_cents});
                    }
                }
            }
        }

        size_t total_matches = 0;
        for (const auto& v : local_matches) total_matches += v.size();
        matches.reserve(total_matches);
        for (auto& v : local_matches) {
            matches.insert(matches.end(), v.begin(), v.end());
            std::vector<LineitemMatch>().swap(v);
        }

        std::sort(matches.begin(), matches.end(), [](const LineitemMatch& a, const LineitemMatch& b) {
            return a.rowid < b.rowid;
        });
    }

    int64_t final_sums_1e4[kNationCount][kYearCount] = {};

    {
        GENDB_PHASE("main_scan");

        const int threads = std::max(1, omp_get_max_threads());
        std::vector<int64_t> thread_sums(static_cast<size_t>(threads) * kNationCount * kYearCount, 0);

        const int32_t* l_ok = l_orderkey.data;
        const double* l_ep = l_extendedprice.data;
        const uint16_t* l_dc = l_discount_scaled100.data;
        const uint16_t* l_qt = l_quantity_scaled100.data;

#pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            int64_t* local = thread_sums.data() + static_cast<size_t>(tid) * kNationCount * kYearCount;

#pragma omp for schedule(static)
            for (size_t i = 0; i < matches.size(); ++i) {
                const LineitemMatch& m = matches[i];
                const uint32_t rowid = m.rowid;
                if (rowid >= l_orderkey.count) continue;

                const int32_t orderkey = l_ok[rowid];
                if (orderkey <= 0 || static_cast<size_t>(orderkey) >= year_codes.count) continue;

                const int year = static_cast<int>(year_codes.data[orderkey]);
                const int y = year - kBaseYear;
                if (y < 0 || y >= kYearCount) continue;

                const int64_t ep_cents = round_to_cents(l_ep[rowid]);
                const int64_t discount = static_cast<int64_t>(l_dc[rowid]);
                const int64_t qty = static_cast<int64_t>(l_qt[rowid]);

                const int64_t amount_1e4 = ep_cents * (100 - discount) - static_cast<int64_t>(m.supplycost_cents) * qty;
                local[static_cast<int>(m.nationkey) * kYearCount + y] += amount_1e4;
            }
        }

        for (int t = 0; t < std::max(1, omp_get_max_threads()); ++t) {
            const int64_t* local = thread_sums.data() + static_cast<size_t>(t) * kNationCount * kYearCount;
            for (int n = 0; n < kNationCount; ++n) {
                for (int y = 0; y < kYearCount; ++y) {
                    final_sums_1e4[n][y] += local[n * kYearCount + y];
                }
            }
        }
    }

    std::vector<ResultRow> rows;
    rows.reserve(kNationCount * kYearCount);

    for (int nk = 0; nk < kNationCount; ++nk) {
        if (static_cast<size_t>(nk) >= nation_name_code_by_key.size()) continue;
        const uint32_t code = nation_name_code_by_key[static_cast<size_t>(nk)];
        if (static_cast<size_t>(code) >= nation_dict.size()) continue;

        const std::string& nation = nation_dict[code];
        for (int y = 0; y < kYearCount; ++y) {
            rows.push_back(ResultRow{nation, kBaseYear + y, final_sums_1e4[nk][y]});
        }
    }

    std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.nation != b.nation) return a.nation < b.nation;
        return a.year > b.year;
    });

    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q9.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            std::perror(("fopen: " + out_path).c_str());
            return 1;
        }

        std::fprintf(out, "nation,o_year,sum_profit\n");
        for (const ResultRow& r : rows) {
            const int64_t cents = (r.sum_profit_1e4 >= 0) ? (r.sum_profit_1e4 + 50) / 100 : (r.sum_profit_1e4 - 50) / 100;
            const bool neg = cents < 0;
            const uint64_t abs_cents = static_cast<uint64_t>(neg ? -cents : cents);
            const uint64_t dollars = abs_cents / 100;
            const uint64_t rem = abs_cents % 100;
            std::fprintf(out, "%s,%d,%s%llu.%02llu\n",
                         r.nation.c_str(),
                         r.year,
                         neg ? "-" : "",
                         static_cast<unsigned long long>(dollars),
                         static_cast<unsigned long long>(rem));
        }
        std::fclose(out);
    }

    return 0;
}
