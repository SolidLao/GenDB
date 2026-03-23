#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr int32_t kDateThreshold = 9204;  // DATE '1995-03-15'
constexpr uint32_t kCustomerKeyMax = 1500000;

struct OrdersCustkeyHashEntry {
    int64_t key;
    uint32_t rowid;
    uint32_t pad;
};
static_assert(sizeof(OrdersCustkeyHashEntry) == 16, "OrdersCustkeyHashEntry size mismatch");

struct ZoneMap {
    uint32_t block_size = 0;
    uint64_t nrows = 0;
    uint64_t blocks = 0;
    std::vector<int32_t> mins;
    std::vector<int32_t> maxs;
};

struct OrderPayload {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
};

struct ResultRow {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrderkeyToGroupMap {
    static constexpr int32_t kEmpty = 0;

    std::vector<int32_t> keys;
    std::vector<uint32_t> gids;
    uint32_t mask = 0;

    void init(size_t capacity_pow2) {
        keys.assign(capacity_pow2, kEmpty);
        gids.assign(capacity_pow2, 0);
        mask = static_cast<uint32_t>(capacity_pow2 - 1);
    }

    static inline uint32_t hash32(uint32_t x) {
        return x * 2654435761u;
    }

    void insert_or_assign(int32_t orderkey, uint32_t gid) {
        uint32_t pos = hash32(static_cast<uint32_t>(orderkey)) & mask;
        while (true) {
            const int32_t k = keys[pos];
            if (k == kEmpty) {
                keys[pos] = orderkey;
                gids[pos] = gid;
                return;
            }
            if (k == orderkey) {
                gids[pos] = gid;
                return;
            }
            pos = (pos + 1u) & mask;
        }
    }

    inline int32_t find_gid(int32_t orderkey) const {
        uint32_t pos = hash32(static_cast<uint32_t>(orderkey)) & mask;
        while (true) {
            const int32_t k = keys[pos];
            if (k == orderkey) return static_cast<int32_t>(gids[pos]);
            if (k == kEmpty) return -1;
            pos = (pos + 1u) & mask;
        }
    }
};

inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

inline uint32_t load_building_code(const std::string& dict_path) {
    std::ifstream in(dict_path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open dict: " + dict_path);

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(uint32_t));
    if (!in) throw std::runtime_error("dict read error: " + dict_path);

    for (uint32_t code = 0; code < n; ++code) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        if (!in) throw std::runtime_error("dict read length error: " + dict_path);

        std::string v(len, '\0');
        if (len > 0) {
            in.read(&v[0], static_cast<std::streamsize>(len));
            if (!in) throw std::runtime_error("dict read value error: " + dict_path);
        }

        if (v == "BUILDING") return code;
    }

    throw std::runtime_error("BUILDING not found in dictionary");
}

inline void set_bit(std::vector<uint8_t>& bits, uint32_t key) {
    bits[key >> 3] |= static_cast<uint8_t>(1u << (key & 7u));
}

inline bool test_bit(const std::vector<uint8_t>& bits, uint32_t key) {
    return (bits[key >> 3] & static_cast<uint8_t>(1u << (key & 7u))) != 0;
}

inline ZoneMap load_zonemap(const std::string& path) {
    ZoneMap zm;
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open zonemap: " + path);

    in.read(reinterpret_cast<char*>(&zm.block_size), sizeof(uint32_t));
    in.read(reinterpret_cast<char*>(&zm.nrows), sizeof(uint64_t));
    in.read(reinterpret_cast<char*>(&zm.blocks), sizeof(uint64_t));
    if (!in) throw std::runtime_error("zonemap header read failed: " + path);

    zm.mins.resize(static_cast<size_t>(zm.blocks));
    zm.maxs.resize(static_cast<size_t>(zm.blocks));

    if (zm.blocks > 0) {
        in.read(reinterpret_cast<char*>(zm.mins.data()),
                static_cast<std::streamsize>(zm.blocks * sizeof(int32_t)));
        in.read(reinterpret_cast<char*>(zm.maxs.data()),
                static_cast<std::streamsize>(zm.blocks * sizeof(int32_t)));
    }

    if (!in) throw std::runtime_error("zonemap arrays read failed: " + path);
    return zm;
}

inline bool better_result(const ResultRow& a, const ResultRow& b) {
    if (a.revenue != b.revenue) return a.revenue > b.revenue;
    if (a.orderdate != b.orderdate) return a.orderdate < b.orderdate;
    if (a.orderkey != b.orderkey) return a.orderkey < b.orderkey;
    return a.shippriority < b.shippriority;
}

inline size_t next_pow2(size_t x) {
    size_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: q3 <gendb_dir> <results_dir>\n");
        return 1;
    }

    const std::string gdir = argv[1];
    const std::string rdir = argv[2];

    try {
        GENDB_PHASE("total");
        init_date_tables();

        const int nthreads = std::max(1, omp_get_max_threads());
        omp_set_num_threads(nthreads);

        MmapColumn<uint32_t> c_mktsegment;
        MmapColumn<int32_t> c_custkey;

        MmapColumn<int32_t> o_orderkey;
        MmapColumn<int32_t> o_orderdate;
        MmapColumn<int32_t> o_shippriority;

        MmapColumn<int32_t> l_orderkey;
        MmapColumn<int32_t> l_shipdate;
        MmapColumn<double> l_extendedprice;
        MmapColumn<double> l_discount;

        uint32_t building_code = 0;
        ZoneMap l_ship_zm;

        uint64_t orders_hash_buckets = 0;
        std::vector<uint64_t> orders_hash_offsets;
        std::vector<OrdersCustkeyHashEntry> orders_hash_entries;

        {
            GENDB_PHASE("data_loading");
            c_mktsegment.open(gdir + "/customer/c_mktsegment.bin");
            c_custkey.open(gdir + "/customer/c_custkey.bin");

            o_orderkey.open(gdir + "/orders/o_orderkey.bin");
            o_orderdate.open(gdir + "/orders/o_orderdate.bin");
            o_shippriority.open(gdir + "/orders/o_shippriority.bin");

            l_orderkey.open(gdir + "/lineitem/l_orderkey.bin");
            l_shipdate.open(gdir + "/lineitem/l_shipdate.bin");
            l_extendedprice.open(gdir + "/lineitem/l_extendedprice.bin");
            l_discount.open(gdir + "/lineitem/l_discount.bin");

            building_code = load_building_code(gdir + "/customer/c_mktsegment.dict");
            l_ship_zm = load_zonemap(gdir + "/lineitem/lineitem_shipdate_zonemap.idx");

            std::ifstream in(gdir + "/orders/orders_custkey_hash.idx", std::ios::binary);
            if (!in) throw std::runtime_error("cannot open orders_custkey_hash.idx");

            uint64_t n = 0;
            in.read(reinterpret_cast<char*>(&orders_hash_buckets), sizeof(uint64_t));
            in.read(reinterpret_cast<char*>(&n), sizeof(uint64_t));
            if (!in || orders_hash_buckets == 0 || (orders_hash_buckets & (orders_hash_buckets - 1)) != 0) {
                throw std::runtime_error("invalid orders_custkey_hash header");
            }

            orders_hash_offsets.resize(static_cast<size_t>(orders_hash_buckets + 1));
            in.read(reinterpret_cast<char*>(orders_hash_offsets.data()),
                    static_cast<std::streamsize>((orders_hash_buckets + 1) * sizeof(uint64_t)));
            if (!in) throw std::runtime_error("orders_custkey_hash offsets read failed");

            orders_hash_entries.resize(static_cast<size_t>(n));
            if (n > 0) {
                in.read(reinterpret_cast<char*>(orders_hash_entries.data()),
                        static_cast<std::streamsize>(n * sizeof(OrdersCustkeyHashEntry)));
            }
            if (!in) throw std::runtime_error("orders_custkey_hash entries read failed");

            mmap_prefetch_all(c_mktsegment, c_custkey);
            mmap_prefetch_all(o_orderkey, o_orderdate, o_shippriority);
            mmap_prefetch_all(l_orderkey, l_shipdate, l_extendedprice, l_discount);
        }

        if (c_mktsegment.count != c_custkey.count) {
            throw std::runtime_error("customer column length mismatch");
        }
        if (o_orderkey.count != o_orderdate.count || o_orderkey.count != o_shippriority.count) {
            throw std::runtime_error("orders column length mismatch");
        }
        if (l_orderkey.count != l_shipdate.count || l_orderkey.count != l_extendedprice.count ||
            l_orderkey.count != l_discount.count) {
            throw std::runtime_error("lineitem column length mismatch");
        }

        std::vector<uint8_t> customer_filter_bits((kCustomerKeyMax + 8u) / 8u, 0);
        std::vector<uint32_t> building_custkeys;
        building_custkeys.reserve(400000);

        {
            GENDB_PHASE("dim_filter");
            const uint64_t n = c_mktsegment.count;
            for (uint64_t i = 0; i < n; ++i) {
                if (c_mktsegment.data[i] != building_code) continue;
                const uint32_t custkey = static_cast<uint32_t>(c_custkey.data[i]);
                if (custkey == 0 || custkey > kCustomerKeyMax) continue;
                if (test_bit(customer_filter_bits, custkey)) continue;
                set_bit(customer_filter_bits, custkey);
                building_custkeys.push_back(custkey);
            }
        }

        std::vector<OrderPayload> groups;
        OrderkeyToGroupMap orderkey_to_gid;

        {
            GENDB_PHASE("build_joins");
            std::vector<std::vector<OrderPayload>> locals(static_cast<size_t>(nthreads));

#pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& out = locals[static_cast<size_t>(tid)];
                out.reserve(32768);

#pragma omp for schedule(static)
                for (size_t idx = 0; idx < building_custkeys.size(); ++idx) {
                    const uint32_t custkey = building_custkeys[idx];
                    const uint64_t h = mix64(static_cast<uint32_t>(custkey)) & (orders_hash_buckets - 1);
                    const uint64_t begin = orders_hash_offsets[static_cast<size_t>(h)];
                    const uint64_t end = orders_hash_offsets[static_cast<size_t>(h + 1)];

                    for (uint64_t i = begin; i < end; ++i) {
                        const OrdersCustkeyHashEntry& e = orders_hash_entries[static_cast<size_t>(i)];
                        if (static_cast<uint32_t>(e.key) != custkey) continue;
                        if (e.rowid >= o_orderkey.count) continue;

                        const uint64_t rid = e.rowid;
                        const int32_t od = o_orderdate.data[rid];
                        if (od >= kDateThreshold) continue;

                        out.push_back(OrderPayload{o_orderkey.data[rid], od, o_shippriority.data[rid]});
                    }
                }
            }

            size_t total = 0;
            for (const auto& v : locals) total += v.size();
            groups.reserve(total);
            for (auto& v : locals) {
                groups.insert(groups.end(), v.begin(), v.end());
            }

            const size_t map_cap = next_pow2(std::max<size_t>(1024, groups.size() * 2));
            orderkey_to_gid.init(map_cap);
            for (uint32_t gid = 0; gid < groups.size(); ++gid) {
                orderkey_to_gid.insert_or_assign(groups[gid].orderkey, gid);
            }
        }

        std::vector<double> revenue(groups.size(), 0.0);

        {
            GENDB_PHASE("main_scan");
            std::vector<uint64_t> starts(static_cast<size_t>(nthreads + 1), 0);
            starts[0] = 0;
            starts[static_cast<size_t>(nthreads)] = l_orderkey.count;
            for (int t = 1; t < nthreads; ++t) {
                uint64_t s = (static_cast<uint64_t>(t) * l_orderkey.count) / static_cast<uint64_t>(nthreads);
                while (s < l_orderkey.count && s > 0 && l_orderkey.data[s - 1] == l_orderkey.data[s]) ++s;
                starts[static_cast<size_t>(t)] = s;
            }

#pragma omp parallel for schedule(static)
            for (int t = 0; t < nthreads; ++t) {
                const uint64_t start = starts[static_cast<size_t>(t)];
                const uint64_t end = starts[static_cast<size_t>(t + 1)];
                if (start >= end) continue;

                const uint64_t block_size = static_cast<uint64_t>(l_ship_zm.block_size);
                const uint64_t b0 = start / block_size;
                const uint64_t b1 = (end - 1) / block_size;

                for (uint64_t b = b0; b <= b1; ++b) {
                    if (l_ship_zm.maxs[static_cast<size_t>(b)] <= kDateThreshold) continue;

                    const uint64_t bs = b * block_size;
                    const uint64_t be = std::min(bs + block_size, static_cast<uint64_t>(l_orderkey.count));
                    const uint64_t rs = std::max(start, bs);
                    const uint64_t re = std::min(end, be);

                    for (uint64_t i = rs; i < re; ++i) {
                        if (l_shipdate.data[i] <= kDateThreshold) continue;

                        const int32_t gid = orderkey_to_gid.find_gid(l_orderkey.data[i]);
                        if (gid < 0) continue;

                        revenue[static_cast<size_t>(gid)] += l_extendedprice.data[i] * (1.0 - l_discount.data[i]);
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<ResultRow> rows;
            rows.reserve(groups.size());

            for (size_t i = 0; i < groups.size(); ++i) {
                if (revenue[i] == 0.0) continue;
                rows.push_back(ResultRow{groups[i].orderkey, revenue[i], groups[i].orderdate, groups[i].shippriority});
            }

            const size_t k = std::min<size_t>(10, rows.size());
            if (k > 0) {
                std::partial_sort(rows.begin(), rows.begin() + static_cast<std::ptrdiff_t>(k), rows.end(),
                                  better_result);
                rows.resize(k);
            }

            mkdir(rdir.c_str(), 0755);
            const std::string out_path = rdir + "/Q3.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) throw std::runtime_error("cannot open output: " + out_path);

            std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char dbuf[11];
            for (const auto& r : rows) {
                epoch_days_to_date_str(r.orderdate, dbuf);
                std::fprintf(out, "%d,%.2f,%s,%d\n", r.orderkey, r.revenue, dbuf, r.shippriority);
            }
            std::fclose(out);
        }

    } catch (const std::exception& ex) {
        std::fprintf(stderr, "q3 failed: %s\n", ex.what());
        return 1;
    }

    return 0;
}
