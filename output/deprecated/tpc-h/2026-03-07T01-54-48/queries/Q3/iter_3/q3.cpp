#include <algorithm>
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

struct AggPair {
    int32_t orderkey;
    int64_t revenue_bp;
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

        const int32_t max_ok_i32 = (o_orderkey.count == 0) ? 0 : o_orderkey.data[o_orderkey.count - 1];
        if (max_ok_i32 <= 0) {
            throw std::runtime_error("invalid max o_orderkey");
        }
        const uint32_t max_orderkey = static_cast<uint32_t>(max_ok_i32);
        std::vector<uint8_t> order_qual(static_cast<size_t>(max_orderkey + 1), 0);
        std::vector<int32_t> order_date_by_key(static_cast<size_t>(max_orderkey + 1), 0);
        std::vector<int32_t> order_shippriority_by_key(static_cast<size_t>(max_orderkey + 1), 0);
        std::vector<int64_t> revenue_bp_by_key(static_cast<size_t>(max_orderkey + 1), 0);
        std::vector<int32_t> qualifying_orderkeys;

        {
            GENDB_PHASE("build_joins");
            const uint64_t order_blocks = o_date_zm.blocks;
            std::vector<std::vector<int32_t>> local_keys(static_cast<size_t>(nthreads));
#pragma omp parallel for schedule(static)
            for (uint64_t b = 0; b < order_blocks; ++b) {
                auto& keys = local_keys[static_cast<size_t>(omp_get_thread_num())];
                if (o_date_zm.mins[static_cast<size_t>(b)] >= kDateThreshold) continue;

                const uint64_t start = b * static_cast<uint64_t>(o_date_zm.block_size);
                const uint64_t end = std::min(start + static_cast<uint64_t>(o_date_zm.block_size),
                                              static_cast<uint64_t>(o_orderkey.count));

                for (uint64_t i = start; i < end; ++i) {
                    const int32_t od = o_orderdate.data[i];
                    if (od >= kDateThreshold) continue;

                    const uint32_t ck = static_cast<uint32_t>(o_custkey.data[i]);
                    if (ck == 0 || ck > kCustomerKeyMax) continue;
                    if (!test_bit(customer_filter_bits, ck)) continue;

                    const int32_t ok = o_orderkey.data[i];
                    if (ok <= 0 || static_cast<uint32_t>(ok) > max_orderkey) continue;

                    const uint32_t uok = static_cast<uint32_t>(ok);
                    order_qual[uok] = 1;
                    order_date_by_key[uok] = od;
                    order_shippriority_by_key[uok] = o_shippriority.data[i];
                    keys.push_back(ok);
                }
            }
            size_t total_keys = 0;
            for (const auto& v : local_keys) total_keys += v.size();
            qualifying_orderkeys.reserve(total_keys);
            for (auto& v : local_keys) {
                qualifying_orderkeys.insert(qualifying_orderkeys.end(), v.begin(), v.end());
            }
        }

        {
            GENDB_PHASE("main_scan");
            const uint64_t line_blocks = l_ship_zm.blocks;
            std::vector<std::vector<AggPair>> local_agg(static_cast<size_t>(nthreads));

#pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                std::vector<AggPair>& out = local_agg[static_cast<size_t>(tid)];
                out.reserve(1u << 16);

                const uint64_t b_begin = (line_blocks * static_cast<uint64_t>(tid)) /
                                         static_cast<uint64_t>(nthreads);
                const uint64_t b_end = (line_blocks * static_cast<uint64_t>(tid + 1)) /
                                       static_cast<uint64_t>(nthreads);

                int32_t run_orderkey = -1;
                int64_t run_rev = 0;

                auto flush_run = [&]() {
                    if (run_orderkey > 0 && run_rev != 0) {
                        out.push_back(AggPair{run_orderkey, run_rev});
                    }
                    run_orderkey = -1;
                    run_rev = 0;
                };

                for (uint64_t b = b_begin; b < b_end; ++b) {
                    if (l_ship_zm.maxs[static_cast<size_t>(b)] <= kDateThreshold) continue;

                    const uint64_t start = b * static_cast<uint64_t>(l_ship_zm.block_size);
                    const uint64_t end = std::min(start + static_cast<uint64_t>(l_ship_zm.block_size),
                                                  static_cast<uint64_t>(l_orderkey.count));

                    for (uint64_t i = start; i < end; ++i) {
                        if (l_shipdate.data[i] <= kDateThreshold) continue;

                        const int32_t ok = l_orderkey.data[i];
                        if (ok <= 0 || static_cast<uint32_t>(ok) > max_orderkey) continue;
                        if (!order_qual[static_cast<uint32_t>(ok)]) continue;

                        const int64_t ep_cents =
                            static_cast<int64_t>(__builtin_llround(l_extendedprice.data[i] * 100.0));
                        const int64_t disc_pct =
                            static_cast<int64_t>(__builtin_llround(l_discount.data[i] * 100.0));
                        const int64_t contrib = ep_cents * (100 - disc_pct);

                        if (ok != run_orderkey) {
                            flush_run();
                            run_orderkey = ok;
                            run_rev = contrib;
                        } else {
                            run_rev += contrib;
                        }
                    }
                }
                flush_run();
            }

            for (const auto& part : local_agg) {
                for (const AggPair& a : part) {
                    revenue_bp_by_key[static_cast<uint32_t>(a.orderkey)] += a.revenue_bp;
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<ResultRow> rows;
            rows.reserve(1u << 20);

            for (const int32_t ok_i32 : qualifying_orderkeys) {
                const uint32_t ok = static_cast<uint32_t>(ok_i32);
                const int64_t rev = revenue_bp_by_key[ok];
                if (rev == 0) continue;

                rows.push_back(ResultRow{static_cast<int32_t>(ok), rev, order_date_by_key[ok],
                                         order_shippriority_by_key[ok]});
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
