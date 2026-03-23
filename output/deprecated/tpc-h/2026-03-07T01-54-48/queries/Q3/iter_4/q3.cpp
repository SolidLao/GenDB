#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr int32_t kDateCutoff = 9204;  // DATE '1995-03-15'
constexpr uint32_t kMaxCustKey = 1500000;

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

struct ResultRow {
    int32_t orderkey;
    int64_t revenue_bp;
    int32_t orderdate;
    int32_t shippriority;
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
    if (!in) throw std::runtime_error("dict read header failed: " + dict_path);

    for (uint32_t code = 0; code < n; ++code) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        if (!in) throw std::runtime_error("dict read len failed: " + dict_path);

        std::string value(len, '\0');
        if (len > 0) {
            in.read(&value[0], static_cast<std::streamsize>(len));
            if (!in) throw std::runtime_error("dict read value failed: " + dict_path);
        }

        if (value == "BUILDING") return code;
    }

    throw std::runtime_error("BUILDING not found in dictionary");
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
        if (!in) throw std::runtime_error("zonemap arrays read failed: " + path);
    }

    return zm;
}

inline void set_bit(std::vector<uint8_t>& bits, uint32_t k) {
    bits[k >> 3] |= static_cast<uint8_t>(1u << (k & 7u));
}

inline bool test_bit(const std::vector<uint8_t>& bits, uint32_t k) {
    return (bits[k >> 3] & static_cast<uint8_t>(1u << (k & 7u))) != 0;
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
        MmapColumn<uint16_t> l_discount_x100;

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
            l_discount_x100.open(gdir +
                                "/column_versions/lineitem.l_discount.int16_scaled_by_100/codes.bin");

            o_date_zm = load_zonemap(gdir + "/orders/orders_orderdate_zonemap.idx");
            l_ship_zm = load_zonemap(gdir + "/lineitem/lineitem_shipdate_zonemap.idx");
            building_code = load_building_code(gdir + "/customer/c_mktsegment.dict");

            mmap_prefetch_all(c_custkey, o_orderkey, o_custkey, o_orderdate, o_shippriority);
            mmap_prefetch_all(l_orderkey, l_shipdate, l_extendedprice, l_discount_x100);
        }

        if (o_orderkey.count != o_custkey.count || o_orderkey.count != o_orderdate.count ||
            o_orderkey.count != o_shippriority.count) {
            throw std::runtime_error("orders column length mismatch");
        }
        if (l_orderkey.count != l_shipdate.count || l_orderkey.count != l_extendedprice.count ||
            l_orderkey.count != l_discount_x100.count) {
            throw std::runtime_error("lineitem column length mismatch");
        }

        std::vector<uint8_t> customer_bits((kMaxCustKey + 8u) / 8u, 0);
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
            if (!in) throw std::runtime_error("customer hash offsets read failed");

            std::vector<CustomerHashEntry> entries(static_cast<size_t>(n));
            if (n > 0) {
                in.read(reinterpret_cast<char*>(entries.data()),
                        static_cast<std::streamsize>(n * sizeof(CustomerHashEntry)));
                if (!in) throw std::runtime_error("customer hash entries read failed");
            }

            const uint64_t h = mix64(static_cast<uint64_t>(building_code)) & (buckets - 1);
            const uint64_t begin = offsets[static_cast<size_t>(h)];
            const uint64_t end = offsets[static_cast<size_t>(h + 1)];
            for (uint64_t i = begin; i < end; ++i) {
                const CustomerHashEntry& e = entries[static_cast<size_t>(i)];
                if (e.key != building_code) continue;
                if (e.rowid >= c_custkey.count) continue;
                const uint32_t custkey = static_cast<uint32_t>(c_custkey.data[e.rowid]);
                if (custkey == 0 || custkey > kMaxCustKey) continue;
                set_bit(customer_bits, custkey);
            }
        }

        const int32_t max_orderkey = (o_orderkey.count > 0) ? o_orderkey.data[o_orderkey.count - 1] : 0;
        if (max_orderkey < 0) throw std::runtime_error("invalid max orderkey");

        std::vector<uint8_t> qualifying_order(static_cast<size_t>(max_orderkey + 1), 0);
        std::vector<int32_t> orderdate_by_key(static_cast<size_t>(max_orderkey + 1), 0);
        std::vector<int32_t> shippriority_by_key(static_cast<size_t>(max_orderkey + 1), 0);

        {
            GENDB_PHASE("build_joins");
#pragma omp parallel for schedule(dynamic, 1)
            for (uint64_t b = 0; b < o_date_zm.blocks; ++b) {
                if (o_date_zm.mins[static_cast<size_t>(b)] >= kDateCutoff) continue;

                uint64_t start = b * static_cast<uint64_t>(o_date_zm.block_size);
                uint64_t end = std::min(start + static_cast<uint64_t>(o_date_zm.block_size),
                                        static_cast<uint64_t>(o_orderkey.count));

                for (uint64_t i = start; i < end; ++i) {
                    const int32_t od = o_orderdate.data[i];
                    if (od >= kDateCutoff) continue;

                    const uint32_t ck = static_cast<uint32_t>(o_custkey.data[i]);
                    if (ck == 0 || ck > kMaxCustKey) continue;
                    if (!test_bit(customer_bits, ck)) continue;

                    const int32_t ok = o_orderkey.data[i];
                    if (ok <= 0 || ok > max_orderkey) continue;
                    qualifying_order[static_cast<size_t>(ok)] = 1;
                    orderdate_by_key[static_cast<size_t>(ok)] = od;
                    shippriority_by_key[static_cast<size_t>(ok)] = o_shippriority.data[i];
                }
            }
        }

        std::vector<std::unordered_map<int32_t, int64_t>> thread_locals(static_cast<size_t>(nthreads));
        for (auto& m : thread_locals) {
            m.reserve(1u << 13);
        }

        {
            GENDB_PHASE("main_scan");
#pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& local = thread_locals[static_cast<size_t>(tid)];

#pragma omp for schedule(dynamic, 1)
                for (uint64_t b = 0; b < l_ship_zm.blocks; ++b) {
                    if (l_ship_zm.maxs[static_cast<size_t>(b)] <= kDateCutoff) continue;

                    uint64_t start = b * static_cast<uint64_t>(l_ship_zm.block_size);
                    uint64_t end = std::min(start + static_cast<uint64_t>(l_ship_zm.block_size),
                                            static_cast<uint64_t>(l_orderkey.count));

                    for (uint64_t i = start; i < end; ++i) {
                        if (l_shipdate.data[i] <= kDateCutoff) continue;

                        const int32_t ok = l_orderkey.data[i];
                        if (ok <= 0 || ok > max_orderkey) continue;
                        if (!qualifying_order[static_cast<size_t>(ok)]) continue;

                        const int64_t ep_cents =
                            static_cast<int64_t>(llround(l_extendedprice.data[i] * 100.0));
                        const int64_t disc_pct = static_cast<int64_t>(l_discount_x100.data[i]);
                        const int64_t contrib_bp = ep_cents * (100 - disc_pct);

                        local[ok] += contrib_bp;
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::unordered_map<int32_t, int64_t> revenues;
            size_t merge_cap = 0;
            for (const auto& m : thread_locals) merge_cap += m.size();
            revenues.reserve(merge_cap + 1024);

            for (const auto& local : thread_locals) {
                for (const auto& kv : local) {
                    revenues[kv.first] += kv.second;
                }
            }

            std::vector<ResultRow> rows;
            rows.reserve(revenues.size());
            for (const auto& kv : revenues) {
                if (kv.second == 0) continue;
                const int32_t ok = kv.first;
                rows.push_back(ResultRow{ok, kv.second, orderdate_by_key[static_cast<size_t>(ok)],
                                         shippriority_by_key[static_cast<size_t>(ok)]});
            }

            const size_t k = std::min<size_t>(10, rows.size());
            if (k > 0) {
                std::partial_sort(rows.begin(), rows.begin() + static_cast<std::ptrdiff_t>(k), rows.end(),
                                  better_result);
                rows.resize(k);
            }

            std::filesystem::create_directories(rdir);
            const std::string out_path = rdir + "/Q3.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) throw std::runtime_error("cannot open output: " + out_path);

            std::fprintf(out, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
            char date_buf[11];
            for (const auto& r : rows) {
                epoch_days_to_date_str(r.orderdate, date_buf);
                const double revenue = static_cast<double>(r.revenue_bp) / 10000.0;
                std::fprintf(out, "%d,%.2f,%s,%d\n", r.orderkey, revenue, date_buf, r.shippriority);
            }
            std::fclose(out);
        }

    } catch (const std::exception& ex) {
        std::fprintf(stderr, "q3 failed: %s\n", ex.what());
        return 1;
    }

    return 0;
}
