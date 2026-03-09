#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
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
constexpr int kPartitionThreads = 8;

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
};

struct JoinedRow {
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double o_totalprice;
    double sum_qty;
    uint32_t c_rowid;
};

inline bool output_better(const JoinedRow& a, const JoinedRow& b) {
    if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
    if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;
    if (a.o_orderkey != b.o_orderkey) return a.o_orderkey < b.o_orderkey;
    return a.c_custkey < b.c_custkey;
}

}  // namespace

int main(int argc, char** argv) {
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
        std::unique_ptr<uint16_t[]> sum_qty_by_orderkey;
        int32_t max_orderkey = 0;
        std::vector<int32_t> qualifying_orderkeys;
        std::vector<JoinedRow> joined_rows;

        {
            GENDB_PHASE("total");

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

            {
                GENDB_PHASE("dim_filter");
            const size_t n = l_orderkey.size();
            if (n == 0) {
                throw std::runtime_error("lineitem is empty");
            }
            max_orderkey = l_orderkey[n - 1];
            sum_qty_by_orderkey.reset(new uint16_t[static_cast<size_t>(max_orderkey) + 1]);
            qualifying_orderkeys.clear();

#ifdef _OPENMP
            const int nthreads =
                std::min(kPartitionThreads, std::min(kMaxPlanThreads, std::max(1, omp_get_max_threads())));
            std::vector<size_t> row_bounds(static_cast<size_t>(nthreads) + 1, 0);
            std::vector<int32_t> key_bounds(static_cast<size_t>(nthreads) + 1, 0);
            std::vector<std::vector<int32_t>> local_keys(static_cast<size_t>(nthreads));

            for (int t = 0; t <= nthreads; ++t) {
                const int64_t num = static_cast<int64_t>(max_orderkey + 1) * t;
                key_bounds[static_cast<size_t>(t)] = static_cast<int32_t>(num / nthreads);
            }
            key_bounds[0] = 0;
            key_bounds[static_cast<size_t>(nthreads)] = max_orderkey + 1;

            row_bounds[0] = 0;
            row_bounds[static_cast<size_t>(nthreads)] = n;
#pragma omp parallel for num_threads(nthreads) schedule(static)
            for (int t = 1; t < nthreads; ++t) {
                const int32_t threshold = key_bounds[static_cast<size_t>(t)];
                const int32_t* begin = l_orderkey.data;
                const int32_t* end = l_orderkey.data + n;
                const int32_t* it = std::lower_bound(begin, end, threshold);
                row_bounds[static_cast<size_t>(t)] = static_cast<size_t>(it - begin);
            }

#pragma omp parallel for num_threads(nthreads) schedule(static)
            for (int t = 0; t < nthreads; ++t) {
                auto& out = local_keys[static_cast<size_t>(t)];
                out.reserve(128);
                const size_t rs = row_bounds[static_cast<size_t>(t)];
                const size_t re = row_bounds[static_cast<size_t>(t + 1)];
                size_t i = rs;
                while (i < re) {
                    const int32_t k = l_orderkey[i];
                    double s = l_quantity[i];
                    ++i;
                    while (i < re && l_orderkey[i] == k) {
                        s += l_quantity[i];
                        ++i;
                    }
                    const uint16_t qs = static_cast<uint16_t>(s);
                    sum_qty_by_orderkey[static_cast<size_t>(k)] = qs;
                    if (qs > static_cast<uint16_t>(kHavingThreshold)) {
                        out.push_back(k);
                    }
                }
            }

            size_t q_total = 0;
            for (const auto& v : local_keys) q_total += v.size();
            qualifying_orderkeys.reserve(q_total);
            for (auto& v : local_keys) {
                qualifying_orderkeys.insert(qualifying_orderkeys.end(), v.begin(), v.end());
            }
#else
            size_t i = 0;
            while (i < n) {
                const int32_t k = l_orderkey[i];
                double s = l_quantity[i];
                ++i;
                while (i < n && l_orderkey[i] == k) {
                    s += l_quantity[i];
                    ++i;
                }
                const uint16_t qs = static_cast<uint16_t>(s);
                sum_qty_by_orderkey[static_cast<size_t>(k)] = qs;
                if (qs > static_cast<uint16_t>(kHavingThreshold)) {
                    qualifying_orderkeys.push_back(k);
                }
            }
#endif
            }

            {
                GENDB_PHASE("build_joins");
#ifdef _OPENMP
            const int nthreads =
                std::min(kPartitionThreads, std::min(kMaxPlanThreads, std::max(1, omp_get_max_threads())));
            std::vector<std::vector<JoinedRow>> local(static_cast<size_t>(nthreads));
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

                    out.push_back(JoinedRow{
                        ck,
                        o_orderkey[o_row],
                        o_orderdate[o_row],
                        o_totalprice[o_row],
                        static_cast<double>(sum_qty_by_orderkey[static_cast<size_t>(ok)]),
                        c_row,
                    });
                }
            }

            size_t total = 0;
            for (const auto& v : local) total += v.size();
            joined_rows.reserve(total);
            for (auto& v : local) {
                joined_rows.insert(joined_rows.end(), v.begin(), v.end());
            }
#else
            joined_rows.reserve(qualifying_orderkeys.size());
            for (int32_t ok : qualifying_orderkeys) {
                uint32_t o_row = 0;
                if (!orders_pk.find_unique(ok, o_row)) continue;
                if (o_row >= o_orderkey.size()) continue;

                const int32_t ck = o_custkey[o_row];
                uint32_t c_row = 0;
                if (!customer_pk.find_unique(ck, c_row)) continue;
                if (c_row + 1 >= c_name_off.size()) continue;

                joined_rows.push_back(JoinedRow{
                    ck,
                    o_orderkey[o_row],
                    o_orderdate[o_row],
                    o_totalprice[o_row],
                    static_cast<double>(sum_qty_by_orderkey[static_cast<size_t>(ok)]),
                    c_row,
                });
            }
#endif
            }

            {
                GENDB_PHASE("main_scan");
                if (joined_rows.size() > kTopK) {
                    std::partial_sort(joined_rows.begin(), joined_rows.begin() + kTopK, joined_rows.end(), output_better);
                    joined_rows.resize(kTopK);
                } else {
                    std::sort(joined_rows.begin(), joined_rows.end(), output_better);
                }
            }

            {
                GENDB_PHASE("output");
                std::filesystem::create_directories(results_dir);
                const std::string out_path = results_dir + "/Q18.csv";
                FILE* out = std::fopen(out_path.c_str(), "w");
                if (!out) {
                    throw std::runtime_error("cannot open output: " + out_path);
                }

                std::fprintf(out, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
                char date_buf[11];
                for (const JoinedRow& r : joined_rows) {
                    const uint64_t begin = c_name_off[r.c_rowid];
                    const uint64_t end = c_name_off[r.c_rowid + 1];
                    if (begin > end || end > c_name_dat.size()) {
                        std::fclose(out);
                        throw std::runtime_error("invalid customer c_name offsets");
                    }

                    gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
                    std::fprintf(out,
                                 "%.*s,%d,%d,%s,%.2f,%.2f\n",
                                 static_cast<int>(end - begin),
                                 c_name_dat.data + begin,
                                 r.c_custkey,
                                 r.o_orderkey,
                                 date_buf,
                                 r.o_totalprice,
                                 r.sum_qty);
                }

                std::fclose(out);
            }
        }

        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error: %s\n", e.what());
        return 1;
    }
}
