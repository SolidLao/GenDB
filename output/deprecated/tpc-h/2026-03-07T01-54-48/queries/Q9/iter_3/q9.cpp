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

static_assert(sizeof(HashEntry64U32) == 16, "HashEntry64U32 layout must match index file");

struct HashIndex64U32 {
    MmapColumn<uint8_t> raw;
    uint64_t buckets = 0;
    uint64_t n = 0;
    const uint64_t* offsets = nullptr;
    const HashEntry64U32* entries = nullptr;

    void load(const std::string& path) {
        raw.open(path);
        if (raw.file_size < sizeof(uint64_t) * 2) {
            throw std::runtime_error("index too small: " + path);
        }

        const uint8_t* p = raw.data;
        std::memcpy(&buckets, p, sizeof(uint64_t));
        std::memcpy(&n, p + sizeof(uint64_t), sizeof(uint64_t));

        if (buckets == 0 || (buckets & (buckets - 1)) != 0) {
            throw std::runtime_error("index buckets must be power-of-two: " + path);
        }

        const size_t offsets_bytes = static_cast<size_t>(buckets + 1) * sizeof(uint64_t);
        const size_t entries_bytes = static_cast<size_t>(n) * sizeof(HashEntry64U32);
        const size_t expected_bytes = sizeof(uint64_t) * 2 + offsets_bytes + entries_bytes;
        if (raw.file_size != expected_bytes) {
            throw std::runtime_error("index size mismatch: " + path);
        }

        offsets = reinterpret_cast<const uint64_t*>(p + sizeof(uint64_t) * 2);
        entries = reinterpret_cast<const HashEntry64U32*>(p + sizeof(uint64_t) * 2 + offsets_bytes);
    }

    inline bool find_unique(uint64_t key, uint32_t& rowid_out) const {
        const uint64_t h = mix64(key) & (buckets - 1);
        const uint64_t begin = offsets[h];
        const uint64_t end = offsets[h + 1];
        for (uint64_t i = begin; i < end; ++i) {
            if (entries[i].key == key) {
                rowid_out = entries[i].rowid;
                return true;
            }
        }
        return false;
    }
};

struct ResultRow {
    std::string nation;
    int year;
    int64_t sum_profit_1e4;
};

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

    MmapColumn<int32_t> l_partkey;
    MmapColumn<int32_t> l_suppkey;
    MmapColumn<int32_t> l_orderkey;
    MmapColumn<double> l_extendedprice;
    MmapColumn<double> l_discount;
    MmapColumn<double> l_quantity;

    MmapColumn<double> ps_supplycost;

    MmapColumn<int32_t> s_suppkey;
    MmapColumn<int32_t> s_nationkey;

    MmapColumn<int32_t> n_nationkey;
    MmapColumn<uint32_t> n_name_code;

    MmapColumn<int16_t> year_codes;
    MmapColumn<uint8_t> has_green_by_partkey;

    HashIndex64U32 partsupp_pk_hash;
    std::vector<std::string> nation_dict;

    {
        GENDB_PHASE("data_loading");

        l_partkey.open(gendb_dir + "/lineitem/l_partkey.bin");
        l_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
        l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");

        ps_supplycost.open(gendb_dir + "/partsupp/ps_supplycost.bin");

        s_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
        s_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");

        n_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
        n_name_code.open(gendb_dir + "/nation/n_name.bin");

        year_codes.open(gendb_dir + "/column_versions/orders.o_orderdate.year_dense_by_orderkey/codes.bin");
        has_green_by_partkey.open(gendb_dir + "/column_versions/part.p_name.has_green_by_partkey/codes.bin");

        partsupp_pk_hash.load(gendb_dir + "/partsupp/partsupp_pk_hash.idx");

        nation_dict = load_dict_strings(gendb_dir + "/nation/n_name.dict");

        mmap_prefetch_all(l_partkey, l_suppkey, l_orderkey, l_extendedprice, l_discount, l_quantity,
                          ps_supplycost,
                          s_suppkey, s_nationkey,
                          n_nationkey, n_name_code,
                          year_codes, has_green_by_partkey,
                          partsupp_pk_hash.raw);
    }

    {
        GENDB_PHASE("dim_filter");
        // Filter is applied in the lineitem scan via has_green_by_partkey[l_partkey].
    }

    std::vector<int16_t> supp_to_nation;
    std::vector<uint32_t> nation_code_by_key;

    {
        GENDB_PHASE("build_joins");

        int32_t max_suppkey = 0;
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            if (s_suppkey.data[i] > max_suppkey) max_suppkey = s_suppkey.data[i];
        }
        supp_to_nation.assign(static_cast<size_t>(max_suppkey) + 1, static_cast<int16_t>(-1));
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            const int32_t sk = s_suppkey.data[i];
            if (sk >= 0 && static_cast<size_t>(sk) < supp_to_nation.size()) {
                supp_to_nation[static_cast<size_t>(sk)] = static_cast<int16_t>(s_nationkey.data[i]);
            }
        }

        int32_t max_nationkey = 0;
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            if (n_nationkey.data[i] > max_nationkey) max_nationkey = n_nationkey.data[i];
        }
        nation_code_by_key.assign(static_cast<size_t>(max_nationkey) + 1, 0U);
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            const int32_t nk = n_nationkey.data[i];
            if (nk >= 0 && static_cast<size_t>(nk) < nation_code_by_key.size()) {
                nation_code_by_key[static_cast<size_t>(nk)] = n_name_code.data[i];
            }
        }
    }

    constexpr int kBaseYear = 1992;
    constexpr int kYearCount = 7;
    constexpr int kNationCount = 25;
    constexpr int kGroups = kNationCount * kYearCount;

    const size_t n_lineitem = l_partkey.count;
    if (l_suppkey.count != n_lineitem || l_orderkey.count != n_lineitem || l_extendedprice.count != n_lineitem ||
        l_discount.count != n_lineitem || l_quantity.count != n_lineitem) {
        throw std::runtime_error("lineitem column size mismatch");
    }

    const int threads = omp_get_max_threads();
    std::vector<int64_t> local_sums(static_cast<size_t>(threads) * kGroups, 0);

    {
        GENDB_PHASE("main_scan");

        const int32_t* l_pk = l_partkey.data;
        const int32_t* l_sk = l_suppkey.data;
        const int32_t* l_ok = l_orderkey.data;
        const double* l_ep = l_extendedprice.data;
        const double* l_dc = l_discount.data;
        const double* l_qt = l_quantity.data;

#pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            int64_t* agg = local_sums.data() + static_cast<size_t>(tid) * kGroups;

#pragma omp for schedule(dynamic, 65536)
            for (size_t i = 0; i < n_lineitem; ++i) {
                const int32_t partkey = l_pk[i];
                if (partkey <= 0 || static_cast<size_t>(partkey) >= has_green_by_partkey.count) continue;
                if (has_green_by_partkey.data[partkey] == 0) continue;

                const int32_t suppkey = l_sk[i];
                if (suppkey <= 0 || static_cast<size_t>(suppkey) >= supp_to_nation.size()) continue;
                const int nationkey = supp_to_nation[static_cast<size_t>(suppkey)];
                if (nationkey < 0 || nationkey >= kNationCount) continue;

                const int32_t orderkey = l_ok[i];
                if (orderkey <= 0 || static_cast<size_t>(orderkey) >= year_codes.count) continue;

                const int16_t year_code = year_codes.data[orderkey];
                int year_slot = -1;
                if (year_code >= 0 && year_code < kYearCount) {
                    year_slot = static_cast<int>(year_code);
                } else {
                    year_slot = static_cast<int>(year_code) - kBaseYear;
                }
                if (year_slot < 0 || year_slot >= kYearCount) continue;

                const uint64_t packed_key =
                    (static_cast<uint64_t>(static_cast<uint32_t>(partkey)) << 32) |
                    static_cast<uint32_t>(suppkey);

                uint32_t ps_rowid = 0;
                if (!partsupp_pk_hash.find_unique(packed_key, ps_rowid)) continue;
                if (ps_rowid >= ps_supplycost.count) continue;

                const int64_t ep100 = static_cast<int64_t>(std::llround(l_ep[i] * 100.0));
                const int64_t dc100 = static_cast<int64_t>(std::llround(l_dc[i] * 100.0));
                const int64_t sc100 = static_cast<int64_t>(std::llround(ps_supplycost.data[ps_rowid] * 100.0));
                const int64_t qt100 = static_cast<int64_t>(std::llround(l_qt[i] * 100.0));
                const int64_t amount_1e4 = ep100 * (100 - dc100) - sc100 * qt100;
                agg[nationkey * kYearCount + year_slot] += amount_1e4;
            }
        }
    }

    int64_t final_sums[kNationCount][kYearCount] = {};
    for (int t = 0; t < threads; ++t) {
        const int64_t* agg = local_sums.data() + static_cast<size_t>(t) * kGroups;
        for (int n = 0; n < kNationCount; ++n) {
            for (int y = 0; y < kYearCount; ++y) {
                final_sums[n][y] += agg[n * kYearCount + y];
            }
        }
    }

    std::vector<ResultRow> rows;
    rows.reserve(kGroups);
    for (int nationkey = 0; nationkey < kNationCount; ++nationkey) {
        if (static_cast<size_t>(nationkey) >= nation_code_by_key.size()) continue;
        const uint32_t name_code = nation_code_by_key[static_cast<size_t>(nationkey)];
        if (static_cast<size_t>(name_code) >= nation_dict.size()) continue;

        const std::string& nation = nation_dict[name_code];
        for (int y = 0; y < kYearCount; ++y) {
            rows.push_back(ResultRow{nation, kBaseYear + y, final_sums[nationkey][y]});
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
            std::fprintf(out, "%s,%d,%s%llu.%02llu\n", r.nation.c_str(), r.year, neg ? "-" : "",
                         static_cast<unsigned long long>(dollars), static_cast<unsigned long long>(rem));
        }
        std::fclose(out);
    }

    return 0;
}
