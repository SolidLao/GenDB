#include <algorithm>
#include <cmath>
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
};

struct Row {
    std::string nation;
    int year;
    int64_t sum_profit_1e4;
};

inline int64_t to_i64_scaled100(double v) {
    return static_cast<int64_t>(std::llround(v * 100.0));
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
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    GENDB_PHASE("total");

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    MmapColumn<int32_t> ps_partkey;
    MmapColumn<int32_t> ps_suppkey;
    MmapColumn<double> ps_supplycost;

    MmapColumn<int32_t> l_orderkey;
    MmapColumn<double> l_extendedprice;
    MmapColumn<uint16_t> l_discount_scaled100;
    MmapColumn<uint16_t> l_quantity_scaled100;

    MmapColumn<int32_t> s_suppkey;
    MmapColumn<int32_t> s_nationkey;

    MmapColumn<int32_t> n_nationkey;
    MmapColumn<uint32_t> n_name_code;

    MmapColumn<int16_t> year_codes;
    MmapColumn<uint8_t> has_green_by_partkey;

    MmapColumn<uint8_t> lineitem_partsupp_idx;

    std::vector<std::string> nation_dict;

    {
        GENDB_PHASE("data_loading");

        ps_partkey.open(gendb_dir + "/partsupp/ps_partkey.bin");
        ps_suppkey.open(gendb_dir + "/partsupp/ps_suppkey.bin");
        ps_supplycost.open(gendb_dir + "/partsupp/ps_supplycost.bin");

        l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount_scaled100.open(gendb_dir + "/column_versions/lineitem.l_discount.int16_scaled_by_100/codes.bin");
        l_quantity_scaled100.open(gendb_dir + "/column_versions/lineitem.l_quantity.int16_scaled_by_100/codes.bin");

        s_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
        s_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");

        n_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
        n_name_code.open(gendb_dir + "/nation/n_name.bin");

        year_codes.open(gendb_dir + "/column_versions/orders.o_orderdate.year_dense_by_orderkey/codes.bin");
        has_green_by_partkey.open(gendb_dir + "/column_versions/part.p_name.has_green_by_partkey/codes.bin");

        lineitem_partsupp_idx.open(gendb_dir + "/lineitem/lineitem_partsupp_hash.idx");

        nation_dict = load_dict_strings(gendb_dir + "/nation/n_name.dict");

        mmap_prefetch_all(ps_partkey, ps_suppkey, ps_supplycost, l_orderkey, l_extendedprice, l_discount_scaled100,
                          l_quantity_scaled100, s_suppkey, s_nationkey, n_nationkey, n_name_code, year_codes,
                          has_green_by_partkey, lineitem_partsupp_idx);
    }

    std::vector<int32_t> green_partkeys;
    {
        GENDB_PHASE("dim_filter");
        green_partkeys.reserve(120000);
        for (size_t pk = 1; pk < has_green_by_partkey.count; ++pk) {
            if (has_green_by_partkey.data[pk] == 1) {
                green_partkeys.push_back(static_cast<int32_t>(pk));
            }
        }
    }

    std::vector<int32_t> supp_to_nation;
    std::vector<uint32_t> nation_code_by_key;
    {
        GENDB_PHASE("build_joins");

        int32_t max_suppkey = 0;
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            if (s_suppkey.data[i] > max_suppkey) max_suppkey = s_suppkey.data[i];
        }
        supp_to_nation.assign(static_cast<size_t>(max_suppkey) + 1, -1);
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            const int32_t sk = s_suppkey.data[i];
            if (sk >= 0) {
                supp_to_nation[static_cast<size_t>(sk)] = s_nationkey.data[i];
            }
        }

        int32_t max_nationkey = 0;
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            if (n_nationkey.data[i] > max_nationkey) max_nationkey = n_nationkey.data[i];
        }
        nation_code_by_key.assign(static_cast<size_t>(max_nationkey) + 1, 0U);
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            const int32_t nk = n_nationkey.data[i];
            if (nk >= 0) {
                nation_code_by_key[static_cast<size_t>(nk)] = n_name_code.data[i];
            }
        }
    }

    if (lineitem_partsupp_idx.file_size < 16) {
        throw std::runtime_error("lineitem_partsupp_hash.idx too small");
    }
    uint64_t idx_buckets = 0;
    uint64_t idx_n = 0;
    const uint8_t* idx_base = lineitem_partsupp_idx.data;
    std::memcpy(&idx_buckets, idx_base, sizeof(uint64_t));
    std::memcpy(&idx_n, idx_base + sizeof(uint64_t), sizeof(uint64_t));
    const size_t offsets_bytes = static_cast<size_t>(idx_buckets + 1) * sizeof(uint64_t);
    const size_t entries_bytes = static_cast<size_t>(idx_n) * sizeof(HashEntry64U32);
    if (lineitem_partsupp_idx.file_size < 16 + offsets_bytes + entries_bytes) {
        throw std::runtime_error("lineitem_partsupp_hash.idx layout mismatch");
    }
    const uint64_t* idx_offsets = reinterpret_cast<const uint64_t*>(idx_base + 16);
    const HashEntry64U32* idx_entries = reinterpret_cast<const HashEntry64U32*>(idx_base + 16 + offsets_bytes);

    constexpr int kBaseYear = 1992;
    constexpr int kYearCount = 7;
    constexpr int kNationCount = 25;
    constexpr int kGroups = kNationCount * kYearCount;

    const int threads = omp_get_max_threads();
    std::vector<int64_t> thread_sums_1e4(static_cast<size_t>(threads) * kGroups, 0);

    {
        GENDB_PHASE("main_scan");

        const int32_t* ps_pk = ps_partkey.data;
        const int32_t* ps_sk = ps_suppkey.data;
        const double* ps_sc = ps_supplycost.data;
        const int32_t* l_ok = l_orderkey.data;
        const double* l_ep = l_extendedprice.data;
        const uint16_t* l_dc100 = l_discount_scaled100.data;
        const uint16_t* l_qt100 = l_quantity_scaled100.data;

#pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            int64_t* local = thread_sums_1e4.data() + static_cast<size_t>(tid) * kGroups;

#pragma omp for schedule(dynamic, 128)
            for (size_t g = 0; g < green_partkeys.size(); ++g) {
                const int32_t pk = green_partkeys[g];

                const int32_t* lo = std::lower_bound(ps_pk, ps_pk + ps_partkey.count, pk);
                if (lo == ps_pk + ps_partkey.count || *lo != pk) continue;
                const int32_t* hi = std::upper_bound(lo, ps_pk + ps_partkey.count, pk);

                const size_t row_begin = static_cast<size_t>(lo - ps_pk);
                const size_t row_end = static_cast<size_t>(hi - ps_pk);

                for (size_t ps_row = row_begin; ps_row < row_end; ++ps_row) {
                    const int32_t suppkey = ps_sk[ps_row];
                    if (suppkey < 0 || static_cast<size_t>(suppkey) >= supp_to_nation.size()) continue;

                    const int32_t nationkey = supp_to_nation[static_cast<size_t>(suppkey)];
                    if (nationkey < 0 || nationkey >= kNationCount) continue;

                    const int64_t sc100 = to_i64_scaled100(ps_sc[ps_row]);
                    const uint64_t packed = (static_cast<uint64_t>(static_cast<uint32_t>(pk)) << 32) |
                                            static_cast<uint32_t>(suppkey);
                    const uint64_t h = mix64(packed) & (idx_buckets - 1);
                    const uint64_t begin = idx_offsets[h];
                    const uint64_t end = idx_offsets[h + 1];

                    for (uint64_t p = begin; p < end; ++p) {
                        if (idx_entries[p].key != packed) continue;
                        const uint32_t l_row = idx_entries[p].rowid;
                        if (l_row >= l_orderkey.count) continue;

                        const int32_t orderkey = l_ok[l_row];
                        if (orderkey < 0 || static_cast<size_t>(orderkey) >= year_codes.count) continue;
                        const int year = static_cast<int>(year_codes.data[orderkey]);
                        const int year_slot = year - kBaseYear;
                        if (year_slot < 0 || year_slot >= kYearCount) continue;

                        const int64_t ep100 = to_i64_scaled100(l_ep[l_row]);
                        const int64_t dc100 = static_cast<int64_t>(l_dc100[l_row]);
                        const int64_t qt100 = static_cast<int64_t>(l_qt100[l_row]);
                        const int64_t amount_1e4 = ep100 * (100 - dc100) - sc100 * qt100;

                        const int group = nationkey * kYearCount + year_slot;
                        local[group] += amount_1e4;
                    }
                }
            }
        }
    }

    int64_t final_sums_1e4[kNationCount][kYearCount] = {};
    for (int t = 0; t < threads; ++t) {
        const int64_t* local = thread_sums_1e4.data() + static_cast<size_t>(t) * kGroups;
        for (int n = 0; n < kNationCount; ++n) {
            for (int y = 0; y < kYearCount; ++y) {
                final_sums_1e4[n][y] += local[n * kYearCount + y];
            }
        }
    }

    std::vector<Row> rows;
    rows.reserve(kGroups);
    for (int nationkey = 0; nationkey < kNationCount; ++nationkey) {
        if (static_cast<size_t>(nationkey) >= nation_code_by_key.size()) continue;
        const uint32_t code = nation_code_by_key[static_cast<size_t>(nationkey)];
        if (static_cast<size_t>(code) >= nation_dict.size()) continue;

        const std::string& nation = nation_dict[code];
        for (int y = 0; y < kYearCount; ++y) {
            rows.push_back(Row{nation, kBaseYear + y, final_sums_1e4[nationkey][y]});
        }
    }

    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
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
        for (const Row& r : rows) {
            const int64_t cents = (r.sum_profit_1e4 >= 0) ? (r.sum_profit_1e4 + 50) / 100 : (r.sum_profit_1e4 - 50) / 100;
            const bool neg = cents < 0;
            const uint64_t abs_cents = static_cast<uint64_t>(neg ? -cents : cents);
            const uint64_t dollars = abs_cents / 100;
            const uint64_t rem = abs_cents % 100;
            std::fprintf(out, "%s,%d,%s%llu.%02llu\n", r.nation.c_str(), r.year, neg ? "-" : "",
                         static_cast<unsigned long long>(dollars), static_cast<unsigned long long>(rem));
        }
        std::fclose(out);
    }

    return 0;
}
