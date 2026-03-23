#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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

struct CustomerHashEntry {
    uint64_t key;
    uint32_t rowid;
    uint32_t pad;
};
static_assert(sizeof(CustomerHashEntry) == 16, "CustomerHashEntry size mismatch");

struct ZoneMap {
    uint32_t block_size = 0;
    uint64_t nrows = 0;
    uint64_t blocks = 0;
    std::vector<int32_t> mins;
    std::vector<int32_t> maxs;
};

struct OrderRow {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
};

struct ResultRow {
    int32_t orderkey;
    int64_t revenue_bp;
    int32_t orderdate;
    int32_t shippriority;
};

struct OrdersHashMap {
    static constexpr int32_t kEmpty = 0;  // TPC-H orderkeys are > 0

    std::vector<int32_t> keys;
    std::vector<int32_t> orderdates;
    std::vector<int32_t> shippriorities;
    std::vector<int64_t> revenues_bp;
    uint32_t mask = 0;

    void init(size_t capacity_pow2) {
        keys.assign(capacity_pow2, kEmpty);
        orderdates.assign(capacity_pow2, 0);
        shippriorities.assign(capacity_pow2, 0);
        revenues_bp.assign(capacity_pow2, 0);
        mask = static_cast<uint32_t>(capacity_pow2 - 1);
    }

    static inline uint32_t hash32(uint32_t x) {
        return x * 2654435761u;
    }

    bool insert(int32_t orderkey, int32_t orderdate, int32_t shippriority) {
        uint32_t pos = hash32(static_cast<uint32_t>(orderkey)) & mask;
        while (true) {
            int32_t k = keys[pos];
            if (k == kEmpty) {
                keys[pos] = orderkey;
                orderdates[pos] = orderdate;
                shippriorities[pos] = shippriority;
                return true;
            }
            if (k == orderkey) {
                orderdates[pos] = orderdate;
                shippriorities[pos] = shippriority;
                return true;
            }
            pos = (pos + 1u) & mask;
        }
    }

    inline int32_t find_slot(int32_t orderkey) const {
        uint32_t pos = hash32(static_cast<uint32_t>(orderkey)) & mask;
        while (true) {
            int32_t k = keys[pos];
            if (k == orderkey) return static_cast<int32_t>(pos);
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
    if (a.revenue_bp != b.revenue_bp) return a.revenue_bp > b.revenue_bp;
    if (a.orderdate != b.orderdate) return a.orderdate < b.orderdate;
    if (a.orderkey != b.orderkey) return a.orderkey < b.orderkey;
    return a.shippriority < b.shippriority;
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

        MmapColumn<int32_t> c_custkey;
        MmapColumn<int32_t> o_orderkey;
        MmapColumn<int32_t> o_custkey;
        MmapColumn<int32_t> o_orderdate;
        MmapColumn<int32_t> o_shippriority;
        MmapColumn<int32_t> l_orderkey;
        MmapColumn<int32_t> l_shipdate;
        MmapColumn<double> l_extendedprice;
        MmapColumn<double> l_discount;

        ZoneMap o_date_zm;
        ZoneMap l_ship_zm;
        uint32_t building_code = 0;

        {
            GENDB_PHASE("data_loading");
            c_custkey.open(gdir + "/customer/c_custkey.bin");
            o_orderkey.open(gdir + "/orders/o_orderkey.bin");
            o_custkey.open(gdir + "/orders/o_custkey.bin");
            o_orderdate.open(gdir + "/orders/o_orderdate.bin");
            o_shippriority.open(gdir + "/orders/o_shippriority.bin");
            l_orderkey.open(gdir + "/lineitem/l_orderkey.bin");
            l_shipdate.open(gdir + "/lineitem/l_shipdate.bin");
            l_extendedprice.open(gdir + "/lineitem/l_extendedprice.bin");
            l_discount.open(gdir + "/lineitem/l_discount.bin");

            o_date_zm = load_zonemap(gdir + "/orders/orders_orderdate_zonemap.idx");
            l_ship_zm = load_zonemap(gdir + "/lineitem/lineitem_shipdate_zonemap.idx");
            building_code = load_building_code(gdir + "/customer/c_mktsegment.dict");

            mmap_prefetch_all(o_orderkey, o_custkey, o_orderdate, o_shippriority);
            mmap_prefetch_all(l_orderkey, l_shipdate, l_extendedprice, l_discount);
        }

        if (o_orderkey.count != o_custkey.count || o_orderkey.count != o_orderdate.count ||
            o_orderkey.count != o_shippriority.count) {
            throw std::runtime_error("orders column length mismatch");
        }
        if (l_orderkey.count != l_shipdate.count || l_orderkey.count != l_extendedprice.count ||
            l_orderkey.count != l_discount.count) {
            throw std::runtime_error("lineitem column length mismatch");
        }

        std::vector<uint8_t> customer_filter_bits((kCustomerKeyMax + 8u) / 8u, 0);
        {
            GENDB_PHASE("dim_filter");
            std::ifstream in(gdir + "/customer/customer_mktsegment_hash.idx", std::ios::binary);
            if (!in) throw std::runtime_error("cannot open customer_mktsegment_hash.idx");

            uint64_t buckets = 0;
            uint64_t n = 0;
            in.read(reinterpret_cast<char*>(&buckets), sizeof(uint64_t));
            in.read(reinterpret_cast<char*>(&n), sizeof(uint64_t));
            if (!in || buckets == 0 || (buckets & (buckets - 1)) != 0) {
                throw std::runtime_error("invalid customer hash index header");
            }

            std::vector<uint64_t> offsets(static_cast<size_t>(buckets + 1));
            in.read(reinterpret_cast<char*>(offsets.data()),
                    static_cast<std::streamsize>((buckets + 1) * sizeof(uint64_t)));
            if (!in) throw std::runtime_error("customer hash index offsets read failed");

            std::vector<CustomerHashEntry> entries(static_cast<size_t>(n));
            if (n > 0) {
                in.read(reinterpret_cast<char*>(entries.data()),
                        static_cast<std::streamsize>(n * sizeof(CustomerHashEntry)));
            }
            if (!in) throw std::runtime_error("customer hash index entries read failed");

            const uint64_t h = mix64(static_cast<uint64_t>(building_code)) & (buckets - 1);
            const uint64_t begin = offsets[static_cast<size_t>(h)];
            const uint64_t end = offsets[static_cast<size_t>(h + 1)];

            for (uint64_t i = begin; i < end; ++i) {
                const CustomerHashEntry& e = entries[static_cast<size_t>(i)];
                if (e.key != building_code) continue;
                if (e.rowid >= c_custkey.count) continue;
                const uint32_t custkey = static_cast<uint32_t>(c_custkey.data[e.rowid]);
                if (custkey == 0 || custkey > kCustomerKeyMax) continue;
                set_bit(customer_filter_bits, custkey);
            }
        }

        std::vector<OrderRow> qualifying_orders;
        OrdersHashMap orders_hash;

        {
            GENDB_PHASE("build_joins");
            std::vector<std::vector<OrderRow>> locals(static_cast<size_t>(nthreads));

#pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& out = locals[static_cast<size_t>(tid)];
                out.reserve(32768);

#pragma omp for schedule(dynamic, 1)
                for (uint64_t b = 0; b < o_date_zm.blocks; ++b) {
                    if (o_date_zm.mins[static_cast<size_t>(b)] >= kDateThreshold) continue;

                    uint64_t start = b * static_cast<uint64_t>(o_date_zm.block_size);
                    uint64_t end = std::min(start + static_cast<uint64_t>(o_date_zm.block_size),
                                            static_cast<uint64_t>(o_orderkey.count));

                    for (uint64_t i = start; i < end; ++i) {
                        const int32_t od = o_orderdate.data[i];
                        if (od >= kDateThreshold) continue;

                        const uint32_t ck = static_cast<uint32_t>(o_custkey.data[i]);
                        if (ck == 0 || ck > kCustomerKeyMax) continue;
                        if (!test_bit(customer_filter_bits, ck)) continue;

                        out.push_back(OrderRow{o_orderkey.data[i], od, o_shippriority.data[i]});
                    }
                }
            }

            size_t total = 0;
            for (const auto& v : locals) total += v.size();
            qualifying_orders.reserve(total);
            for (auto& v : locals) {
                qualifying_orders.insert(qualifying_orders.end(), v.begin(), v.end());
            }

            const size_t map_cap = (1u << 21);  // pre-sized ~2M slots
            orders_hash.init(map_cap);
            for (const auto& r : qualifying_orders) {
                if (!orders_hash.insert(r.orderkey, r.orderdate, r.shippriority)) {
                    throw std::runtime_error("failed to insert into orders hash");
                }
            }
        }

        {
            GENDB_PHASE("main_scan");
#pragma omp parallel for schedule(dynamic, 1)
            for (uint64_t b = 0; b < l_ship_zm.blocks; ++b) {
                if (l_ship_zm.maxs[static_cast<size_t>(b)] <= kDateThreshold) continue;

                uint64_t start = b * static_cast<uint64_t>(l_ship_zm.block_size);
                uint64_t end = std::min(start + static_cast<uint64_t>(l_ship_zm.block_size),
                                        static_cast<uint64_t>(l_orderkey.count));

                for (uint64_t i = start; i < end; ++i) {
                    if (l_shipdate.data[i] <= kDateThreshold) continue;

                    const int32_t slot = orders_hash.find_slot(l_orderkey.data[i]);
                    if (slot < 0) continue;

                    const int64_t ep_cents = static_cast<int64_t>(__builtin_llround(l_extendedprice.data[i] * 100.0));
                    const int64_t disc_pct = static_cast<int64_t>(__builtin_llround(l_discount.data[i] * 100.0));
                    const int64_t contrib = ep_cents * (100 - disc_pct);
#pragma omp atomic update
                    orders_hash.revenues_bp[static_cast<size_t>(slot)] += contrib;
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<ResultRow> rows;
            rows.reserve(qualifying_orders.size());

            for (size_t i = 0; i < orders_hash.keys.size(); ++i) {
                if (orders_hash.keys[i] == OrdersHashMap::kEmpty) continue;
                int64_t rev_bp = orders_hash.revenues_bp[i];
                if (rev_bp == 0) continue;

                rows.push_back(ResultRow{
                    orders_hash.keys[i],
                    rev_bp,
                    orders_hash.orderdates[i],
                    orders_hash.shippriorities[i],
                });
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
                const double revenue = static_cast<double>(r.revenue_bp) / 10000.0;
                std::fprintf(out, "%d,%.2f,%s,%d\n", r.orderkey, revenue, dbuf, r.shippriority);
            }
            std::fclose(out);
        }

    } catch (const std::exception& ex) {
        std::fprintf(stderr, "q3 failed: %s\n", ex.what());
        return 1;
    }

    return 0;
}
