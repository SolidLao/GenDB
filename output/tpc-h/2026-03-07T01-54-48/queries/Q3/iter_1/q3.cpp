#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

#include <omp.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

constexpr int32_t kDateThreshold = 9204;  // DATE '1995-03-15'
constexpr uint32_t kCustomerKeyMax = 1500000;

inline uint64_t mix64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

struct MmapFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    void open(const std::string& path) {
        close();
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("cannot open: " + path);
        struct stat st;
        if (fstat(fd, &st) != 0) {
            ::close(fd);
            fd = -1;
            throw std::runtime_error("cannot stat: " + path);
        }
        size = static_cast<size_t>(st.st_size);
        if (size == 0) return;
        data = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            data = nullptr;
            ::close(fd);
            fd = -1;
            throw std::runtime_error("mmap failed: " + path);
        }
        madvise(data, size, MADV_RANDOM);
    }

    void close() {
        if (data && size) {
            ::munmap(data, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        data = nullptr;
        size = 0;
        fd = -1;
    }

    ~MmapFile() { close(); }

    MmapFile() = default;
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;
};

struct CustomerIndexEntry {
    uint64_t key;
    uint32_t rowid;
    uint32_t pad;
};
static_assert(sizeof(CustomerIndexEntry) == 16, "CustomerIndexEntry size must be 16 bytes");

struct HashIndexEntry {
    int64_t key;
    uint32_t rowid;
    uint32_t pad;
};
static_assert(sizeof(HashIndexEntry) == 16, "HashIndexEntry size must be 16 bytes");

struct CustomerHashIndexView {
    uint64_t buckets = 0;
    uint64_t n = 0;
    const uint64_t* offsets = nullptr;
    const CustomerIndexEntry* entries = nullptr;
};

struct HashIndexView {
    uint64_t buckets = 0;
    uint64_t n = 0;
    const uint64_t* offsets = nullptr;
    const HashIndexEntry* entries = nullptr;
};

CustomerHashIndexView parse_customer_index(const MmapFile& f) {
    if (f.size < 16) throw std::runtime_error("customer index too small");
    const char* p = static_cast<const char*>(f.data);
    CustomerHashIndexView v;
    std::memcpy(&v.buckets, p, sizeof(uint64_t));
    std::memcpy(&v.n, p + 8, sizeof(uint64_t));
    if (v.buckets == 0 || (v.buckets & (v.buckets - 1)) != 0) {
        throw std::runtime_error("customer index buckets invalid");
    }
    const size_t header_bytes = 16 + static_cast<size_t>(v.buckets + 1) * sizeof(uint64_t);
    const size_t payload_bytes = static_cast<size_t>(v.n) * sizeof(CustomerIndexEntry);
    if (f.size < header_bytes + payload_bytes) {
        throw std::runtime_error("customer index size mismatch");
    }
    v.offsets = reinterpret_cast<const uint64_t*>(p + 16);
    v.entries = reinterpret_cast<const CustomerIndexEntry*>(p + header_bytes);
    return v;
}

HashIndexView parse_hash_index(const MmapFile& f, const char* name) {
    if (f.size < 16) throw std::runtime_error(std::string(name) + " too small");
    const char* p = static_cast<const char*>(f.data);
    HashIndexView v;
    std::memcpy(&v.buckets, p, sizeof(uint64_t));
    std::memcpy(&v.n, p + 8, sizeof(uint64_t));
    if (v.buckets == 0 || (v.buckets & (v.buckets - 1)) != 0) {
        throw std::runtime_error(std::string(name) + " buckets invalid");
    }
    const size_t header_bytes = 16 + static_cast<size_t>(v.buckets + 1) * sizeof(uint64_t);
    const size_t payload_bytes = static_cast<size_t>(v.n) * sizeof(HashIndexEntry);
    if (f.size < header_bytes + payload_bytes) {
        throw std::runtime_error(std::string(name) + " size mismatch");
    }
    v.offsets = reinterpret_cast<const uint64_t*>(p + 16);
    v.entries = reinterpret_cast<const HashIndexEntry*>(p + header_bytes);
    return v;
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

struct QualOrder {
    int32_t orderkey;
    int32_t orderdate;
    int32_t shippriority;
};

struct AggHashMap {
    static constexpr int32_t kEmpty = 0;

    std::vector<int32_t> keys;
    std::vector<int32_t> orderdates;
    std::vector<int32_t> shippriorities;
    std::vector<int64_t> revenues_bp;
    uint32_t mask = 0;

    static inline uint32_t hash32(uint32_t x) {
        return static_cast<uint32_t>(mix64(x));
    }

    void init(size_t capacity_pow2) {
        keys.assign(capacity_pow2, kEmpty);
        orderdates.assign(capacity_pow2, 0);
        shippriorities.assign(capacity_pow2, 0);
        revenues_bp.assign(capacity_pow2, 0);
        mask = static_cast<uint32_t>(capacity_pow2 - 1);
    }

    std::pair<uint32_t, bool> insert_or_get(int32_t orderkey, int32_t orderdate, int32_t shippriority) {
        uint32_t pos = hash32(static_cast<uint32_t>(orderkey)) & mask;
        while (true) {
            const int32_t k = keys[pos];
            if (k == kEmpty) {
                keys[pos] = orderkey;
                orderdates[pos] = orderdate;
                shippriorities[pos] = shippriority;
                return {pos, true};
            }
            if (k == orderkey) {
                return {pos, false};
            }
            pos = (pos + 1u) & mask;
        }
    }
};

struct Task {
    int32_t orderkey;
    uint32_t slot;
};

struct ResultRow {
    int32_t orderkey;
    int64_t revenue_bp;
    int32_t orderdate;
    int32_t shippriority;
};

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
        MmapColumn<int32_t> o_orderdate;
        MmapColumn<int32_t> o_shippriority;
        MmapColumn<int32_t> l_shipdate;
        MmapColumn<double> l_extendedprice;
        MmapColumn<double> l_discount;

        MmapFile customer_idx_file;
        MmapFile orders_idx_file;
        MmapFile lineitem_idx_file;

        uint32_t building_code = 0;
        CustomerHashIndexView customer_idx;
        HashIndexView orders_idx;
        HashIndexView lineitem_idx;

        {
            GENDB_PHASE("data_loading");
            c_custkey.open(gdir + "/customer/c_custkey.bin");
            o_orderkey.open(gdir + "/orders/o_orderkey.bin");
            o_orderdate.open(gdir + "/orders/o_orderdate.bin");
            o_shippriority.open(gdir + "/orders/o_shippriority.bin");
            l_shipdate.open(gdir + "/lineitem/l_shipdate.bin");
            l_extendedprice.open(gdir + "/lineitem/l_extendedprice.bin");
            l_discount.open(gdir + "/lineitem/l_discount.bin");

            customer_idx_file.open(gdir + "/customer/customer_mktsegment_hash.idx");
            orders_idx_file.open(gdir + "/orders/orders_custkey_hash.idx");
            lineitem_idx_file.open(gdir + "/lineitem/lineitem_orderkey_hash.idx");

            customer_idx = parse_customer_index(customer_idx_file);
            orders_idx = parse_hash_index(orders_idx_file, "orders_custkey_hash");
            lineitem_idx = parse_hash_index(lineitem_idx_file, "lineitem_orderkey_hash");

            building_code = load_building_code(gdir + "/customer/c_mktsegment.dict");

            mmap_prefetch_all(c_custkey, o_orderkey, o_orderdate, o_shippriority, l_shipdate,
                              l_extendedprice, l_discount);
        }

        if (o_orderkey.count != o_orderdate.count || o_orderkey.count != o_shippriority.count) {
            throw std::runtime_error("orders column length mismatch");
        }
        if (l_shipdate.count != l_extendedprice.count || l_shipdate.count != l_discount.count) {
            throw std::runtime_error("lineitem column length mismatch");
        }

        std::vector<uint8_t> customer_filter_bits((kCustomerKeyMax + 8u) / 8u, 0);
        std::vector<uint32_t> filtered_custkeys;

        {
            GENDB_PHASE("dim_filter");
            const uint64_t h = mix64(static_cast<uint64_t>(building_code)) & (customer_idx.buckets - 1);
            const uint64_t begin = customer_idx.offsets[h];
            const uint64_t end = customer_idx.offsets[h + 1];

            filtered_custkeys.reserve(static_cast<size_t>(end - begin));
            for (uint64_t i = begin; i < end; ++i) {
                const CustomerIndexEntry& e = customer_idx.entries[i];
                if (e.key != building_code) continue;
                if (e.rowid >= c_custkey.count) continue;
                const uint32_t custkey = static_cast<uint32_t>(c_custkey.data[e.rowid]);
                if (custkey == 0 || custkey > kCustomerKeyMax) continue;
                set_bit(customer_filter_bits, custkey);
                filtered_custkeys.push_back(custkey);
            }
        }

        std::vector<QualOrder> qualifying_orders;
        AggHashMap agg;
        std::vector<Task> tasks;

        {
            GENDB_PHASE("build_joins");
            std::vector<std::vector<QualOrder>> locals(static_cast<size_t>(nthreads));

#pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                auto& out = locals[static_cast<size_t>(tid)];
                out.reserve(32768);

#pragma omp for schedule(dynamic, 1024)
                for (size_t i = 0; i < filtered_custkeys.size(); ++i) {
                    const uint32_t custkey = filtered_custkeys[i];
                    const uint64_t h = mix64(static_cast<uint32_t>(custkey)) & (orders_idx.buckets - 1);
                    const uint64_t begin = orders_idx.offsets[h];
                    const uint64_t end = orders_idx.offsets[h + 1];

                    for (uint64_t p = begin; p < end; ++p) {
                        const HashIndexEntry& e = orders_idx.entries[p];
                        if (static_cast<uint32_t>(e.key) != custkey) continue;
                        if (e.rowid >= o_orderkey.count) continue;

                        const int32_t od = o_orderdate.data[e.rowid];
                        if (od >= kDateThreshold) continue;

                        out.push_back(QualOrder{
                            o_orderkey.data[e.rowid],
                            od,
                            o_shippriority.data[e.rowid],
                        });
                    }
                }
            }

            size_t total = 0;
            for (const auto& v : locals) total += v.size();
            qualifying_orders.reserve(total);
            for (auto& v : locals) {
                qualifying_orders.insert(qualifying_orders.end(), v.begin(), v.end());
            }

            size_t cap = 1;
            const size_t target = std::max<size_t>(1024, qualifying_orders.size() * 2);
            while (cap < target) cap <<= 1;
            agg.init(cap);
            tasks.reserve(qualifying_orders.size());

            for (const QualOrder& q : qualifying_orders) {
                if (q.orderkey <= 0) continue;
                auto [slot, inserted] = agg.insert_or_get(q.orderkey, q.orderdate, q.shippriority);
                if (inserted) {
                    tasks.push_back(Task{q.orderkey, slot});
                }
            }
        }

        {
            GENDB_PHASE("main_scan");
#pragma omp parallel for schedule(dynamic, 2048)
            for (size_t i = 0; i < tasks.size(); ++i) {
                const int32_t orderkey = tasks[i].orderkey;
                const uint32_t slot = tasks[i].slot;

                const uint64_t h = mix64(static_cast<uint32_t>(orderkey)) & (lineitem_idx.buckets - 1);
                const uint64_t begin = lineitem_idx.offsets[h];
                const uint64_t end = lineitem_idx.offsets[h + 1];

                int64_t sum_bp = 0;
                for (uint64_t p = begin; p < end; ++p) {
                    const HashIndexEntry& e = lineitem_idx.entries[p];
                    if (e.key != orderkey) continue;
                    if (e.rowid >= l_shipdate.count) continue;
                    if (l_shipdate.data[e.rowid] <= kDateThreshold) continue;

                    const int64_t ep_cents = static_cast<int64_t>(
                        __builtin_llround(l_extendedprice.data[e.rowid] * 100.0));
                    const int64_t disc_pct = static_cast<int64_t>(
                        __builtin_llround(l_discount.data[e.rowid] * 100.0));
                    sum_bp += ep_cents * (100 - disc_pct);
                }
                agg.revenues_bp[slot] = sum_bp;
            }
        }

        {
            GENDB_PHASE("output");
            std::vector<ResultRow> rows;
            rows.reserve(tasks.size());

            for (const Task& t : tasks) {
                const int64_t rev = agg.revenues_bp[t.slot];
                if (rev == 0) continue;
                rows.push_back(ResultRow{
                    t.orderkey,
                    rev,
                    agg.orderdates[t.slot],
                    agg.shippriorities[t.slot],
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
