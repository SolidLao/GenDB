#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <omp.h>

#include "mmap_utils.h"
#include "timing_utils.h"
#include "date_utils.h"

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

inline bool contains_green(const char* s, size_t len) {
    if (len < 5) return false;
    for (size_t i = 0; i + 5 <= len; ++i) {
        if (s[i] == 'g' && s[i + 1] == 'r' && s[i + 2] == 'e' && s[i + 3] == 'e' && s[i + 4] == 'n') {
            return true;
        }
    }
    return false;
}

struct BloomFilter {
    std::vector<uint64_t> bits;
    uint64_t bit_mask;

    explicit BloomFilter(size_t bit_count_pow2) {
        size_t bits_count = 1;
        while (bits_count < bit_count_pow2) bits_count <<= 1;
        bits.resize(bits_count >> 6, 0ULL);
        bit_mask = static_cast<uint64_t>(bits_count - 1);
    }

    inline void add(uint32_t key) {
        const uint64_t h1 = mix64(key);
        const uint64_t h2 = mix64(h1 ^ 0x9e3779b97f4a7c15ULL);
        const uint64_t h3 = mix64(h1 ^ 0xc2b2ae3d27d4eb4fULL);
        bits[(h1 & bit_mask) >> 6] |= (1ULL << (h1 & 63));
        bits[(h2 & bit_mask) >> 6] |= (1ULL << (h2 & 63));
        bits[(h3 & bit_mask) >> 6] |= (1ULL << (h3 & 63));
    }

    inline bool maybe_contains(uint32_t key) const {
        const uint64_t h1 = mix64(key);
        const uint64_t h2 = mix64(h1 ^ 0x9e3779b97f4a7c15ULL);
        const uint64_t h3 = mix64(h1 ^ 0xc2b2ae3d27d4eb4fULL);
        return ((bits[(h1 & bit_mask) >> 6] >> (h1 & 63)) & 1ULL) &&
               ((bits[(h2 & bit_mask) >> 6] >> (h2 & 63)) & 1ULL) &&
               ((bits[(h3 & bit_mask) >> 6] >> (h3 & 63)) & 1ULL);
    }
};

struct HashEntry64U32 {
    uint64_t key;
    uint32_t rowid;
    uint32_t pad;
};

struct Row {
    std::string nation;
    int year;
    long double sum_profit;
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
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    MmapColumn<int32_t> p_partkey;
    MmapColumn<uint64_t> p_name_off;
    MmapColumn<uint8_t> p_name_dat;

    MmapColumn<int32_t> ps_partkey;
    MmapColumn<int32_t> ps_suppkey;
    MmapColumn<double> ps_supplycost;

    MmapColumn<int32_t> l_orderkey;
    MmapColumn<double> l_extendedprice;
    MmapColumn<double> l_discount;
    MmapColumn<double> l_quantity;

    MmapColumn<int32_t> s_suppkey;
    MmapColumn<int32_t> s_nationkey;

    MmapColumn<int32_t> o_orderkey;
    MmapColumn<int32_t> o_orderdate;

    MmapColumn<int32_t> n_nationkey;
    MmapColumn<uint32_t> n_name_code;

    MmapColumn<uint8_t> lineitem_partsupp_idx;

    std::vector<std::string> nation_dict;

    {
        GENDB_PHASE("data_loading");

        p_partkey.open(gendb_dir + "/part/p_partkey.bin");
        p_name_off.open(gendb_dir + "/part/p_name.off");
        p_name_dat.open(gendb_dir + "/part/p_name.dat");

        ps_partkey.open(gendb_dir + "/partsupp/ps_partkey.bin");
        ps_suppkey.open(gendb_dir + "/partsupp/ps_suppkey.bin");
        ps_supplycost.open(gendb_dir + "/partsupp/ps_supplycost.bin");

        l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");

        s_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
        s_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");

        o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
        o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");

        n_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
        n_name_code.open(gendb_dir + "/nation/n_name.bin");

        lineitem_partsupp_idx.open(gendb_dir + "/lineitem/lineitem_partsupp_hash.idx");

        nation_dict = load_dict_strings(gendb_dir + "/nation/n_name.dict");

        mmap_prefetch_all(p_partkey, p_name_off, p_name_dat,
                          ps_partkey, ps_suppkey, ps_supplycost,
                          l_orderkey, l_extendedprice, l_discount, l_quantity,
                          s_suppkey, s_nationkey,
                          o_orderkey, o_orderdate,
                          n_nationkey, n_name_code,
                          lineitem_partsupp_idx);
    }

    const size_t n_part = p_partkey.count;
    const int32_t max_partkey = 2000000;

    std::vector<uint8_t> green_part_bitset(static_cast<size_t>(max_partkey) + 1, 0);
    BloomFilter green_part_bloom(1ULL << 21);

    {
        GENDB_PHASE("dim_filter");

        if (p_name_off.count != n_part + 1) {
            throw std::runtime_error("part name offsets count mismatch");
        }

        for (size_t i = 0; i < n_part; ++i) {
            const int32_t pk = p_partkey.data[i];
            if (pk <= 0 || pk > max_partkey) continue;

            const uint64_t off0 = p_name_off.data[i];
            const uint64_t off1 = p_name_off.data[i + 1];
            if (off1 < off0 || off1 > p_name_dat.count) continue;

            const char* s = reinterpret_cast<const char*>(p_name_dat.data + off0);
            const size_t len = static_cast<size_t>(off1 - off0);

            if (contains_green(s, len)) {
                green_part_bitset[pk] = 1;
                green_part_bloom.add(static_cast<uint32_t>(pk));
            }
        }
    }

    std::vector<uint64_t> green_ps_keys;
    std::vector<double> green_ps_supplycost;

    std::vector<int32_t> supp_to_nation;
    std::vector<int16_t> order_to_year;

    std::vector<uint32_t> nation_code_by_key;

    {
        GENDB_PHASE("build_joins");

        const size_t n_ps = ps_partkey.count;
        green_ps_keys.reserve(450000);
        green_ps_supplycost.reserve(450000);

        for (size_t i = 0; i < n_ps; ++i) {
            const int32_t pk = ps_partkey.data[i];
            if (pk <= 0 || pk > max_partkey) continue;
            if (!green_part_bloom.maybe_contains(static_cast<uint32_t>(pk))) continue;
            if (!green_part_bitset[pk]) continue;

            const int32_t sk = ps_suppkey.data[i];
            const uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(pk)) << 32) |
                                 static_cast<uint32_t>(sk);
            green_ps_keys.push_back(key);
            green_ps_supplycost.push_back(ps_supplycost.data[i]);
        }

        int32_t max_suppkey = 0;
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            if (s_suppkey.data[i] > max_suppkey) max_suppkey = s_suppkey.data[i];
        }
        supp_to_nation.assign(static_cast<size_t>(max_suppkey) + 1, -1);
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            const int32_t sk = s_suppkey.data[i];
            if (sk >= 0 && sk <= max_suppkey) {
                supp_to_nation[sk] = s_nationkey.data[i];
            }
        }

        int32_t max_orderkey = 0;
        for (size_t i = 0; i < o_orderkey.count; ++i) {
            if (o_orderkey.data[i] > max_orderkey) max_orderkey = o_orderkey.data[i];
        }
        order_to_year.assign(static_cast<size_t>(max_orderkey) + 1, static_cast<int16_t>(-1));
        for (size_t i = 0; i < o_orderkey.count; ++i) {
            const int32_t ok = o_orderkey.data[i];
            if (ok < 0 || ok > max_orderkey) continue;
            order_to_year[ok] = static_cast<int16_t>(gendb::extract_year(o_orderdate.data[i]));
        }

        int32_t max_nationkey = 0;
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            if (n_nationkey.data[i] > max_nationkey) max_nationkey = n_nationkey.data[i];
        }
        nation_code_by_key.assign(static_cast<size_t>(max_nationkey) + 1, 0U);
        for (size_t i = 0; i < n_nationkey.count; ++i) {
            const int32_t nk = n_nationkey.data[i];
            if (nk >= 0 && nk <= max_nationkey) {
                nation_code_by_key[nk] = n_name_code.data[i];
            }
        }
    }

    const uint8_t* idx_base = lineitem_partsupp_idx.data;
    if (lineitem_partsupp_idx.file_size < 16) {
        throw std::runtime_error("lineitem_partsupp_hash.idx too small");
    }

    uint64_t idx_buckets = 0;
    uint64_t idx_n = 0;
    std::memcpy(&idx_buckets, idx_base, sizeof(uint64_t));
    std::memcpy(&idx_n, idx_base + sizeof(uint64_t), sizeof(uint64_t));

    const size_t offsets_bytes = static_cast<size_t>(idx_buckets + 1) * sizeof(uint64_t);
    const size_t entries_bytes = static_cast<size_t>(idx_n) * sizeof(HashEntry64U32);
    const size_t idx_needed = 16 + offsets_bytes + entries_bytes;
    if (lineitem_partsupp_idx.file_size < idx_needed) {
        throw std::runtime_error("lineitem_partsupp_hash.idx layout mismatch");
    }

    const uint64_t* idx_offsets = reinterpret_cast<const uint64_t*>(idx_base + 16);
    const HashEntry64U32* idx_entries = reinterpret_cast<const HashEntry64U32*>(idx_base + 16 + offsets_bytes);

    const int base_year = 1992;
    const int n_years = 7;
    const int n_nations = 25;
    const int groups = n_nations * n_years;

    const int nth = omp_get_max_threads();
    std::vector<long double> local_sum(static_cast<size_t>(nth) * groups, 0.0L);
    std::vector<uint64_t> local_cnt(static_cast<size_t>(nth) * groups, 0ULL);

    {
        GENDB_PHASE("main_scan");

        const size_t n_filtered = green_ps_keys.size();

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            long double* sum_arr = local_sum.data() + static_cast<size_t>(tid) * groups;
            uint64_t* cnt_arr = local_cnt.data() + static_cast<size_t>(tid) * groups;

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_filtered; ++i) {
                const uint64_t packed = green_ps_keys[i];
                const double sc = green_ps_supplycost[i];

                const int32_t suppkey = static_cast<int32_t>(packed & 0xffffffffULL);
                if (suppkey < 0 || static_cast<size_t>(suppkey) >= supp_to_nation.size()) continue;
                const int32_t nationkey = supp_to_nation[suppkey];
                if (nationkey < 0 || nationkey >= n_nations) continue;

                const uint64_t h = mix64(packed) & (idx_buckets - 1);
                const uint64_t begin = idx_offsets[h];
                const uint64_t end = idx_offsets[h + 1];

                for (uint64_t p = begin; p < end; ++p) {
                    if (idx_entries[p].key != packed) continue;

                    const uint32_t rowid = idx_entries[p].rowid;
                    if (rowid >= l_orderkey.count) continue;

                    const int32_t ok = l_orderkey.data[rowid];
                    if (ok < 0 || static_cast<size_t>(ok) >= order_to_year.size()) continue;
                    const int year = static_cast<int>(order_to_year[ok]);
                    const int y = year - base_year;
                    if (y < 0 || y >= n_years) continue;

                    const int g = nationkey * n_years + y;
                    const long double amount =
                        static_cast<long double>(l_extendedprice.data[rowid]) *
                            (1.0L - static_cast<long double>(l_discount.data[rowid])) -
                        static_cast<long double>(sc) *
                            static_cast<long double>(l_quantity.data[rowid]);
                    sum_arr[g] += amount;
                    cnt_arr[g] += 1;
                }
            }
        }
    }

    long double final_sum[25][7] = {};
    uint64_t final_cnt[25][7] = {};

    for (int t = 0; t < nth; ++t) {
        const long double* ts = local_sum.data() + static_cast<size_t>(t) * groups;
        const uint64_t* tc = local_cnt.data() + static_cast<size_t>(t) * groups;
        for (int n = 0; n < n_nations; ++n) {
            for (int y = 0; y < n_years; ++y) {
                const int g = n * n_years + y;
                final_sum[n][y] += ts[g];
                final_cnt[n][y] += tc[g];
            }
        }
    }

    std::vector<Row> rows;
    rows.reserve(175);

    for (int nk = 0; nk < n_nations; ++nk) {
        if (static_cast<size_t>(nk) >= nation_code_by_key.size()) continue;
        const uint32_t code = nation_code_by_key[nk];
        if (static_cast<size_t>(code) >= nation_dict.size()) continue;
        const std::string& nation = nation_dict[code];
        for (int y = 0; y < n_years; ++y) {
            if (final_cnt[nk][y] == 0) continue;
            rows.push_back(Row{nation, base_year + y, final_sum[nk][y]});
        }
    }

    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
        if (a.nation != b.nation) return a.nation < b.nation;
        return a.year > b.year;
    });

    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q9.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::perror(("fopen: " + out_path).c_str());
            return 1;
        }

        std::fprintf(f, "nation,o_year,sum_profit\n");
        for (const Row& r : rows) {
            std::fprintf(f, "%s,%d,%.2Lf\n", r.nation.c_str(), r.year, r.sum_profit);
        }
        std::fclose(f);
    }

    return 0;
}
