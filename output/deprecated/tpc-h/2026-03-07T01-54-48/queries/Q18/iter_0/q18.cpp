#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

#ifdef _OPENMP
#include <omp.h>
#endif

namespace {

constexpr double kHavingThreshold = 300.0;
constexpr size_t kTopK = 100;
constexpr int kMaxPlanThreads = 64;

static inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

struct HashEntry {
    int64_t key;
    uint32_t rowid;
};

static_assert(sizeof(HashEntry) == 16, "HashEntry layout must match index file");

struct HashIndexI32 {
    gendb::MmapColumn<uint8_t> raw;
    uint64_t buckets = 0;
    uint64_t n = 0;
    const uint64_t* offsets = nullptr;
    const HashEntry* entries = nullptr;

    void load(const std::string& path) {
        raw.open(path);
        if (raw.file_size < (sizeof(uint64_t) * 2)) {
            throw std::runtime_error("index too small: " + path);
        }

        const uint8_t* p = raw.data;
        std::memcpy(&buckets, p, sizeof(uint64_t));
        std::memcpy(&n, p + sizeof(uint64_t), sizeof(uint64_t));
        if (buckets == 0 || (buckets & (buckets - 1)) != 0) {
            throw std::runtime_error("index buckets must be power-of-two: " + path);
        }

        const size_t offsets_bytes = static_cast<size_t>(buckets + 1) * sizeof(uint64_t);
        const size_t entries_bytes = static_cast<size_t>(n) * sizeof(HashEntry);
        const size_t expected = sizeof(uint64_t) * 2 + offsets_bytes + entries_bytes;
        if (raw.file_size != expected) {
            throw std::runtime_error("index size mismatch: " + path);
        }

        offsets = reinterpret_cast<const uint64_t*>(p + sizeof(uint64_t) * 2);
        entries = reinterpret_cast<const HashEntry*>(p + sizeof(uint64_t) * 2 + offsets_bytes);
    }

    inline bool find_unique(int32_t key, uint32_t& rowid_out) const {
        const uint64_t h = mix64(static_cast<uint32_t>(key)) & (buckets - 1);
        const uint64_t begin = offsets[h];
        const uint64_t end = offsets[h + 1];
        for (uint64_t i = begin; i < end; ++i) {
            if (entries[i].key == static_cast<int64_t>(key)) {
                rowid_out = entries[i].rowid;
                return true;
            }
        }
        return false;
    }

    template <typename Fn>
    inline void for_each_match(int32_t key, Fn&& fn) const {
        const uint64_t h = mix64(static_cast<uint32_t>(key)) & (buckets - 1);
        const uint64_t begin = offsets[h];
        const uint64_t end = offsets[h + 1];
        for (uint64_t i = begin; i < end; ++i) {
            if (entries[i].key == static_cast<int64_t>(key)) {
                fn(entries[i].rowid);
            }
        }
    }
};

struct JoinedOrder {
    int32_t o_orderkey;
    int32_t c_custkey;
    int32_t o_orderdate;
    double o_totalprice;
    uint32_t c_rowid;
};

struct AggRow {
    int32_t o_orderkey;
    int32_t c_custkey;
    int32_t o_orderdate;
    double o_totalprice;
    double sum_qty;
    uint32_t c_rowid;
};

struct OutputRow {
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double o_totalprice;
    double sum_qty;
};

inline bool output_better(const OutputRow& a, const OutputRow& b) {
    if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
    if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
    if (a.o_orderkey != b.o_orderkey) return a.o_orderkey < b.o_orderkey;
    if (a.c_custkey != b.c_custkey) return a.c_custkey < b.c_custkey;
    return a.c_name < b.c_name;
}

}  // namespace

int main(int argc, char** argv) {
    GENDB_PHASE("total");

    if (argc != 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    try {
        gendb::init_date_tables();

        gendb::MmapColumn<int32_t> l_orderkey;
        gendb::MmapColumn<double> l_quantity;
        gendb::MmapColumn<int32_t> o_orderkey;
        gendb::MmapColumn<int32_t> o_custkey;
        gendb::MmapColumn<int32_t> o_orderdate;
        gendb::MmapColumn<double> o_totalprice;
        gendb::MmapColumn<uint64_t> c_name_off;
        gendb::MmapColumn<char> c_name_dat;

        HashIndexI32 orders_pk;
        HashIndexI32 customer_pk;
        HashIndexI32 lineitem_orderkey_hash;

        {
            GENDB_PHASE("data_loading");
            l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
            l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
            o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
            o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
            o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
            o_totalprice.open(gendb_dir + "/orders/o_totalprice.bin");
            c_name_off.open(gendb_dir + "/customer/c_name.off");
            c_name_dat.open(gendb_dir + "/customer/c_name.dat");

            orders_pk.load(gendb_dir + "/orders/orders_pk_hash.idx");
            customer_pk.load(gendb_dir + "/customer/customer_pk_hash.idx");
            lineitem_orderkey_hash.load(gendb_dir + "/lineitem/lineitem_orderkey_hash.idx");

            gendb::mmap_prefetch_all(l_orderkey, l_quantity, o_orderkey, o_custkey, o_orderdate, o_totalprice);
        }

        if (l_orderkey.size() != l_quantity.size()) {
            throw std::runtime_error("lineitem size mismatch");
        }
        if (o_orderkey.size() != o_custkey.size() || o_orderkey.size() != o_orderdate.size() ||
            o_orderkey.size() != o_totalprice.size()) {
            throw std::runtime_error("orders size mismatch");
        }
        if (c_name_off.size() < 2) {
            throw std::runtime_error("invalid customer c_name.off");
        }
        if (c_name_off[c_name_off.size() - 1] != c_name_dat.size()) {
            throw std::runtime_error("customer c_name.off/c_name.dat mismatch");
        }

        int32_t max_orderkey = 0;
        for (size_t i = 0; i < o_orderkey.size(); ++i) {
            if (o_orderkey[i] > max_orderkey) max_orderkey = o_orderkey[i];
        }
        if (l_orderkey.size() > 0 && l_orderkey[l_orderkey.size() - 1] > max_orderkey) {
            max_orderkey = l_orderkey[l_orderkey.size() - 1];
        }
        std::vector<uint8_t> qualifying_bitset(static_cast<size_t>(max_orderkey) + 1, 0);
        std::vector<int32_t> qualifying_orderkeys;

        {
            GENDB_PHASE("dim_filter");
            const size_t n = l_orderkey.size();
            if (n > 0) {
                int32_t current_key = l_orderkey[0];
                double current_sum = 0.0;

                for (size_t i = 0; i < n; ++i) {
                    const int32_t ok = l_orderkey[i];
                    const double qty = l_quantity[i];

                    if (ok != current_key) {
                        if (current_key >= 0 && current_key <= max_orderkey && current_sum > kHavingThreshold) {
                            qualifying_bitset[static_cast<size_t>(current_key)] = 1;
                            qualifying_orderkeys.push_back(current_key);
                        }
                        current_key = ok;
                        current_sum = qty;
                    } else {
                        current_sum += qty;
                    }
                }

                if (current_key >= 0 && current_key <= max_orderkey && current_sum > kHavingThreshold) {
                    qualifying_bitset[static_cast<size_t>(current_key)] = 1;
                    qualifying_orderkeys.push_back(current_key);
                }
            }
        }

        std::vector<JoinedOrder> joined_orders;
        {
            GENDB_PHASE("build_joins");
#ifdef _OPENMP
            const int nthreads = std::min(kMaxPlanThreads, std::max(1, omp_get_max_threads()));
            std::vector<std::vector<JoinedOrder>> local(static_cast<size_t>(nthreads));
#pragma omp parallel num_threads(nthreads)
            {
                const int tid = omp_get_thread_num();
                auto& out = local[static_cast<size_t>(tid)];
                out.reserve(qualifying_orderkeys.size() / static_cast<size_t>(nthreads) + 8);

#pragma omp for schedule(dynamic, 64)
                for (int64_t i = 0; i < static_cast<int64_t>(qualifying_orderkeys.size()); ++i) {
                    const int32_t ok = qualifying_orderkeys[static_cast<size_t>(i)];

                    uint32_t o_row = 0;
                    if (!orders_pk.find_unique(ok, o_row)) continue;
                    if (o_row >= o_orderkey.size()) continue;

                    const int32_t ck = o_custkey[o_row];
                    uint32_t c_row = 0;
                    if (!customer_pk.find_unique(ck, c_row)) continue;
                    if (c_row + 1 >= c_name_off.size()) continue;

                    out.push_back(JoinedOrder{
                        o_orderkey[o_row], ck, o_orderdate[o_row], o_totalprice[o_row], c_row
                    });
                }
            }

            size_t total = 0;
            for (const auto& v : local) total += v.size();
            joined_orders.reserve(total);
            for (auto& v : local) {
                joined_orders.insert(joined_orders.end(), v.begin(), v.end());
            }
#else
            joined_orders.reserve(qualifying_orderkeys.size());
            for (int32_t ok : qualifying_orderkeys) {
                uint32_t o_row = 0;
                if (!orders_pk.find_unique(ok, o_row)) continue;
                if (o_row >= o_orderkey.size()) continue;

                const int32_t ck = o_custkey[o_row];
                uint32_t c_row = 0;
                if (!customer_pk.find_unique(ck, c_row)) continue;
                if (c_row + 1 >= c_name_off.size()) continue;

                joined_orders.push_back(JoinedOrder{
                    o_orderkey[o_row], ck, o_orderdate[o_row], o_totalprice[o_row], c_row
                });
            }
#endif
        }

        std::vector<AggRow> aggregated(joined_orders.size());
        {
            GENDB_PHASE("main_scan");
#ifdef _OPENMP
            const int nthreads = std::min(kMaxPlanThreads, std::max(1, omp_get_max_threads()));
#pragma omp parallel for num_threads(nthreads) schedule(dynamic, 64)
            for (int64_t i = 0; i < static_cast<int64_t>(joined_orders.size()); ++i) {
                const JoinedOrder& j = joined_orders[static_cast<size_t>(i)];
                double sum_qty = 0.0;
                lineitem_orderkey_hash.for_each_match(j.o_orderkey, [&](uint32_t li_row) {
                    if (li_row < l_quantity.size()) {
                        sum_qty += l_quantity[li_row];
                    }
                });

                aggregated[static_cast<size_t>(i)] =
                    AggRow{j.o_orderkey, j.c_custkey, j.o_orderdate, j.o_totalprice, sum_qty, j.c_rowid};
            }
#else
            for (size_t i = 0; i < joined_orders.size(); ++i) {
                const JoinedOrder& j = joined_orders[i];
                double sum_qty = 0.0;
                lineitem_orderkey_hash.for_each_match(j.o_orderkey, [&](uint32_t li_row) {
                    if (li_row < l_quantity.size()) {
                        sum_qty += l_quantity[li_row];
                    }
                });
                aggregated[i] = AggRow{j.o_orderkey, j.c_custkey, j.o_orderdate, j.o_totalprice, sum_qty, j.c_rowid};
            }
#endif
        }

        {
            GENDB_PHASE("output");
            std::vector<OutputRow> rows;
            rows.reserve(aggregated.size());

            for (const AggRow& a : aggregated) {
                const uint64_t begin = c_name_off[a.c_rowid];
                const uint64_t end = c_name_off[a.c_rowid + 1];
                if (begin > end || end > c_name_dat.size()) {
                    throw std::runtime_error("invalid customer c_name offsets");
                }

                rows.push_back(OutputRow{
                    std::string(c_name_dat.data + begin, c_name_dat.data + end),
                    a.c_custkey,
                    a.o_orderkey,
                    a.o_orderdate,
                    a.o_totalprice,
                    a.sum_qty
                });
            }

            std::sort(rows.begin(), rows.end(), output_better);
            if (rows.size() > kTopK) rows.resize(kTopK);

            std::filesystem::create_directories(results_dir);
            const std::string out_path = results_dir + "/Q18.csv";
            FILE* out = std::fopen(out_path.c_str(), "w");
            if (!out) {
                throw std::runtime_error("cannot open output: " + out_path);
            }

            std::fprintf(out, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
            char date_buf[11];
            for (const OutputRow& r : rows) {
                gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
                std::fprintf(out,
                             "%s,%d,%d,%s,%.2f,%.2f\n",
                             r.c_name.c_str(),
                             r.c_custkey,
                             r.o_orderkey,
                             date_buf,
                             r.o_totalprice,
                             r.sum_qty);
            }

            std::fclose(out);
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
