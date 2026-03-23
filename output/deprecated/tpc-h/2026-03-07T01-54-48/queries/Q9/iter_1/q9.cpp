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
    std::vector<uint64_t> words;
    uint64_t bit_count;

    explicit BloomFilter(uint64_t bits) : words(static_cast<size_t>((bits + 63) / 64), 0ULL), bit_count(bits) {}

    inline void set_bit(uint64_t h) {
        const uint64_t b = h % bit_count;
        words[static_cast<size_t>(b >> 6)] |= (1ULL << (b & 63));
    }

    inline bool test_bit(uint64_t h) const {
        const uint64_t b = h % bit_count;
        return (words[static_cast<size_t>(b >> 6)] >> (b & 63)) & 1ULL;
    }

    inline void add(uint32_t k) {
        const uint64_t h1 = mix64(k);
        const uint64_t h2 = mix64(h1 ^ 0x9e3779b97f4a7c15ULL);
        const uint64_t h3 = mix64(h1 ^ 0xc2b2ae3d27d4eb4fULL);
        set_bit(h1);
        set_bit(h2);
        set_bit(h3);
    }

    inline bool maybe_contains(uint32_t k) const {
        const uint64_t h1 = mix64(k);
        const uint64_t h2 = mix64(h1 ^ 0x9e3779b97f4a7c15ULL);
        const uint64_t h3 = mix64(h1 ^ 0xc2b2ae3d27d4eb4fULL);
        return test_bit(h1) && test_bit(h2) && test_bit(h3);
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

    MmapColumn<int32_t> n_nationkey;
    MmapColumn<uint32_t> n_name_code;

    MmapColumn<int16_t> order_year_codes;

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

        n_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
        n_name_code.open(gendb_dir + "/nation/n_name.bin");

        order_year_codes.open(gendb_dir + "/column_versions/orders.o_orderdate.year_dense_by_orderkey/codes.bin");

        lineitem_partsupp_idx.open(gendb_dir + "/lineitem/lineitem_partsupp_hash.idx");

        nation_dict = load_dict_strings(gendb_dir + "/nation/n_name.dict");

        mmap_prefetch_all(p_partkey, p_name_off, p_name_dat,
                          ps_partkey, ps_suppkey, ps_supplycost,
                          l_orderkey, l_extendedprice, l_discount, l_quantity,
                          s_suppkey, s_nationkey,
                          n_nationkey, n_name_code,
                          order_year_codes,
                          lineitem_partsupp_idx);
    }

    constexpr int32_t kMaxPartkey = 2000000;
    std::vector<uint8_t> green_part_bitset(static_cast<size_t>(kMaxPartkey) + 1, 0);
    BloomFilter green_part_bloom(1310720ULL);

    {
        GENDB_PHASE("dim_filter");

        if (p_name_off.count != p_partkey.count + 1) {
            throw std::runtime_error("part name offsets count mismatch");
        }

        for (size_t i = 0; i < p_partkey.count; ++i) {
            const int32_t pk = p_partkey.data[i];
            if (pk <= 0 || pk > kMaxPartkey) continue;

            const uint64_t off0 = p_name_off.data[i];
            const uint64_t off1 = p_name_off.data[i + 1];
            if (off1 < off0 || off1 > p_name_dat.count) continue;

            const char* s = reinterpret_cast<const char*>(p_name_dat.data + off0);
            const size_t len = static_cast<size_t>(off1 - off0);
            if (!contains_green(s, len)) continue;

            green_part_bitset[pk] = 1;
            green_part_bloom.add(static_cast<uint32_t>(pk));
        }
    }

    std::vector<uint64_t> green_ps_keys;
    std::vector<int32_t> green_ps_supplycost_100;

    std::vector<int32_t> supp_to_nation;
    std::vector<uint32_t> nation_code_by_key;

    {
        GENDB_PHASE("build_joins");

        green_ps_keys.reserve(400000);
        green_ps_supplycost_100.reserve(400000);

        for (size_t i = 0; i < ps_partkey.count; ++i) {
            const int32_t pk = ps_partkey.data[i];
            if (pk <= 0 || pk > kMaxPartkey) continue;
            if (!green_part_bloom.maybe_contains(static_cast<uint32_t>(pk))) continue;
            if (!green_part_bitset[pk]) continue;

            const int32_t sk = ps_suppkey.data[i];
            const uint64_t packed = (static_cast<uint64_t>(static_cast<uint32_t>(pk)) << 32) |
                                    static_cast<uint32_t>(sk);
            green_ps_keys.push_back(packed);
            green_ps_supplycost_100.push_back(static_cast<int32_t>(to_i64_scaled100(ps_supplycost.data[i])));
        }

        int32_t max_suppkey = 0;
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            if (s_suppkey.data[i] > max_suppkey) max_suppkey = s_suppkey.data[i];
        }
        supp_to_nation.assign(static_cast<size_t>(max_suppkey) + 1, -1);
        for (size_t i = 0; i < s_suppkey.count; ++i) {
            const int32_t sk = s_suppkey.data[i];
            if (sk >= 0 && sk <= max_suppkey) {
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
            if (nk >= 0 && nk <= max_nationkey) {
                nation_code_by_key[static_cast<size_t>(nk)] = n_name_code.data[i];
            }
        }
    }

    if (lineitem_partsupp_idx.file_size < 16) {
        throw std::runtime_error("lineitem_partsupp_hash.idx too small");
    }

    const uint8_t* idx_base = lineitem_partsupp_idx.data;
    uint64_t idx_buckets = 0;
    uint64_t idx_n = 0;
    std::memcpy(&idx_buckets, idx_base, sizeof(uint64_t));
    std::memcpy(&idx_n, idx_base + sizeof(uint64_t), sizeof(uint64_t));

    const size_t offsets_bytes = static_cast<size_t>(idx_buckets + 1) * sizeof(uint64_t);
    const size_t entries_bytes = static_cast<size_t>(idx_n) * sizeof(HashEntry64U32);
    const size_t needed_bytes = 16 + offsets_bytes + entries_bytes;
    if (lineitem_partsupp_idx.file_size < needed_bytes) {
        throw std::runtime_error("lineitem_partsupp_hash.idx layout mismatch");
    }

    const uint64_t* idx_offsets = reinterpret_cast<const uint64_t*>(idx_base + 16);
    const HashEntry64U32* idx_entries = reinterpret_cast<const HashEntry64U32*>(idx_base + 16 + offsets_bytes);

    constexpr int kBaseYear = 1992;
    constexpr int kYearCount = 7;
    constexpr int kNationCount = 25;
    constexpr int kGroups = kNationCount * kYearCount;

    const int thread_count = omp_get_max_threads();
    std::vector<int64_t> thread_sums_1e4(static_cast<size_t>(thread_count) * kGroups, 0);

    {
        GENDB_PHASE("main_scan");

        const size_t n_filtered = green_ps_keys.size();

#pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            int64_t* local = thread_sums_1e4.data() + static_cast<size_t>(tid) * kGroups;

#pragma omp for schedule(static)
            for (size_t i = 0; i < n_filtered; ++i) {
                const uint64_t packed = green_ps_keys[i];
                const int64_t sc100 = green_ps_supplycost_100[i];

                const int32_t suppkey = static_cast<int32_t>(packed & 0xffffffffULL);
                if (suppkey < 0 || static_cast<size_t>(suppkey) >= supp_to_nation.size()) continue;

                const int32_t nationkey = supp_to_nation[static_cast<size_t>(suppkey)];
                if (nationkey < 0 || nationkey >= kNationCount) continue;

                const uint64_t h = mix64(packed) & (idx_buckets - 1);
                const uint64_t begin = idx_offsets[h];
                const uint64_t end = idx_offsets[h + 1];

                for (uint64_t pos = begin; pos < end; ++pos) {
                    if (idx_entries[pos].key != packed) continue;

                    const uint32_t rowid = idx_entries[pos].rowid;
                    if (rowid >= l_orderkey.count) continue;

                    const int32_t orderkey = l_orderkey.data[rowid];
                    if (orderkey < 0 || static_cast<size_t>(orderkey) >= order_year_codes.count) continue;

                    const int year = static_cast<int>(order_year_codes.data[orderkey]);
                    const int year_slot = year - kBaseYear;
                    if (year_slot < 0 || year_slot >= kYearCount) continue;

                    const int g = nationkey * kYearCount + year_slot;
                    const int64_t e100 = to_i64_scaled100(l_extendedprice.data[rowid]);
                    const int64_t d100 = to_i64_scaled100(l_discount.data[rowid]);
                    const int64_t q100 = to_i64_scaled100(l_quantity.data[rowid]);
                    const int64_t amount_1e4 = e100 * (100 - d100) - sc100 * q100;
                    local[g] += amount_1e4;
                }
            }
        }
    }

    int64_t final_sum_1e4[kNationCount][kYearCount] = {};
    for (int t = 0; t < thread_count; ++t) {
        const int64_t* local = thread_sums_1e4.data() + static_cast<size_t>(t) * kGroups;
        for (int n = 0; n < kNationCount; ++n) {
            for (int y = 0; y < kYearCount; ++y) {
                final_sum_1e4[n][y] += local[n * kYearCount + y];
            }
        }
    }

    std::vector<Row> rows;
    rows.reserve(175);
    for (int nationkey = 0; nationkey < kNationCount; ++nationkey) {
        if (static_cast<size_t>(nationkey) >= nation_code_by_key.size()) continue;
        const uint32_t code = nation_code_by_key[static_cast<size_t>(nationkey)];
        if (static_cast<size_t>(code) >= nation_dict.size()) continue;

        const std::string& nation = nation_dict[code];
        for (int y = 0; y < kYearCount; ++y) {
            rows.push_back(Row{nation, kBaseYear + y, final_sum_1e4[nationkey][y]});
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
            const int64_t cents = (r.sum_profit_1e4 >= 0)
                                      ? (r.sum_profit_1e4 + 50) / 100
                                      : (r.sum_profit_1e4 - 50) / 100;
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
