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

inline void set_bit(std::vector<uint8_t>& bits, uint32_t key) {
    bits[key >> 3] |= static_cast<uint8_t>(1u << (key & 7u));
}

inline bool test_bit(const std::vector<uint8_t>& bits, uint32_t key) {
    return (bits[key >> 3] & static_cast<uint8_t>(1u << (key & 7u))) != 0;
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

        MmapColumn<uint32_t> c_mktsegment;
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
            c_mktsegment.open(gdir + "/customer/c_mktsegment.bin");
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

            mmap_prefetch_all(c_mktsegment, c_custkey, o_orderkey, o_custkey, o_orderdate, o_shippriority,
                              l_orderkey, l_shipdate, l_extendedprice, l_discount);
        }

        if (c_mktsegment.count != c_custkey.count) {
            throw std::runtime_error("customer column length mismatch");
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
            const size_t n = c_mktsegment.count;
            for (size_t i = 0; i < n; ++i) {
                if (c_mktsegment.data[i] != building_code) continue;
                const uint32_t ck = static_cast<uint32_t>(c_custkey.data[i]);
                if (ck == 0 || ck > kCustomerKeyMax) continue;
                set_bit(customer_filter_bits, ck);
            }
        }

        const int32_t max_orderkey = o_orderkey.count ? o_orderkey.data[o_orderkey.count - 1] : 0;
        if (max_orderkey < 0) throw std::runtime_error("invalid max orderkey");

        std::vector<uint8_t> order_selected(static_cast<size_t>(max_orderkey) + 1u, 0);
        std::vector<int32_t> order_date_by_key(static_cast<size_t>(max_orderkey) + 1u, 0);
        std::vector<int32_t> order_ship_by_key(static_cast<size_t>(max_orderkey) + 1u, 0);
        std::vector<int64_t> revenue_bp_by_key(static_cast<size_t>(max_orderkey) + 1u, 0);
        std::vector<int32_t> qualifying_orderkeys;
        qualifying_orderkeys.reserve(1500000);

        {
            GENDB_PHASE("build_joins");
            for (uint64_t b = 0; b < o_date_zm.blocks; ++b) {
                if (o_date_zm.mins[static_cast<size_t>(b)] >= kDateThreshold) continue;

                uint64_t row = b * static_cast<uint64_t>(o_date_zm.block_size);
                uint64_t row_end = std::min(row + static_cast<uint64_t>(o_date_zm.block_size),
                                            static_cast<uint64_t>(o_orderkey.count));

                for (; row < row_end; ++row) {
                    const int32_t od = o_orderdate.data[row];
                    if (od >= kDateThreshold) continue;

                    const uint32_t ck = static_cast<uint32_t>(o_custkey.data[row]);
                    if (ck == 0 || ck > kCustomerKeyMax) continue;
                    if (!test_bit(customer_filter_bits, ck)) continue;

                    const int32_t ok = o_orderkey.data[row];
                    if (ok <= 0 || ok > max_orderkey) continue;

                    order_selected[static_cast<size_t>(ok)] = 1;
                    order_date_by_key[static_cast<size_t>(ok)] = od;
                    order_ship_by_key[static_cast<size_t>(ok)] = o_shippriority.data[row];
                    qualifying_orderkeys.push_back(ok);
                }
            }
        }

        {
            GENDB_PHASE("main_scan");
            if (!qualifying_orderkeys.empty()) {
                const int workers = std::min<int>(nthreads, static_cast<int>(qualifying_orderkeys.size()));

#pragma omp parallel num_threads(workers)
                {
                    const int tid = omp_get_thread_num();
                    const size_t total = qualifying_orderkeys.size();
                    const size_t begin = (total * static_cast<size_t>(tid)) / static_cast<size_t>(workers);
                    const size_t end = (total * static_cast<size_t>(tid + 1)) / static_cast<size_t>(workers);
                    if (begin >= end) {
                        // Nothing for this thread.
                    } else {
                        const int32_t key_lo = qualifying_orderkeys[begin];
                        const int32_t key_hi = qualifying_orderkeys[end - 1];

                        const int32_t* lkey = l_orderkey.data;
                        const size_t lcount = l_orderkey.count;
                        size_t li = static_cast<size_t>(std::lower_bound(lkey, lkey + lcount, key_lo) - lkey);
                        const size_t li_end = static_cast<size_t>(
                            std::upper_bound(lkey, lkey + lcount, key_hi) - lkey);

                        size_t oi = begin;
                        while (oi < end && li < li_end) {
                            const int32_t ok = qualifying_orderkeys[oi];
                            li = static_cast<size_t>(std::lower_bound(lkey + li, lkey + li_end, ok) - lkey);
                            if (li >= li_end) break;

                            const int32_t lk = lkey[li];
                            if (lk != ok) {
                                ++oi;
                                continue;
                            }

                            int64_t sum_bp = 0;
                            while (li < li_end && lkey[li] == ok) {
                                const uint64_t block = static_cast<uint64_t>(li) /
                                                       static_cast<uint64_t>(l_ship_zm.block_size);
                                const size_t block_end = static_cast<size_t>(std::min(
                                    (block + 1) * static_cast<uint64_t>(l_ship_zm.block_size),
                                    static_cast<uint64_t>(li_end)));

                                if (l_ship_zm.maxs[static_cast<size_t>(block)] <= kDateThreshold) {
                                    li = block_end;
                                    continue;
                                }

                                for (; li < block_end && lkey[li] == ok; ++li) {
                                    if (l_shipdate.data[li] <= kDateThreshold) continue;
                                    const int64_t ep_cents = static_cast<int64_t>(
                                        __builtin_llround(l_extendedprice.data[li] * 100.0));
                                    const int64_t disc_pct = static_cast<int64_t>(
                                        __builtin_llround(l_discount.data[li] * 100.0));
                                    sum_bp += ep_cents * (100 - disc_pct);
                                }
                            }

                            revenue_bp_by_key[static_cast<size_t>(ok)] = sum_bp;
                            ++oi;
                        }
                    }
                }
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<ResultRow> rows;
            rows.reserve(qualifying_orderkeys.size());
            for (const int32_t ok : qualifying_orderkeys) {
                if (!order_selected[static_cast<size_t>(ok)]) continue;
                const int64_t rev = revenue_bp_by_key[static_cast<size_t>(ok)];
                if (rev == 0) continue;
                rows.push_back(ResultRow{ok, rev, order_date_by_key[static_cast<size_t>(ok)],
                                         order_ship_by_key[static_cast<size_t>(ok)]});
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
