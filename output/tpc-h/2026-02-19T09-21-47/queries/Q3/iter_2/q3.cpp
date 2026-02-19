#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <tuple>
#include <algorithm>
#include <thread>
#include <atomic>
#include <climits>
#include <cstdlib>
#include <iostream>
#include "date_utils.h"
#include "timing_utils.h"

// ---- mmap helper (zone maps only — small files < 100 KB) ----
static void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return p;
}

// ---- Sequential read for a row range of a binary column file ----
// Reads rows [row_start, row_start+row_count) via a single sequential read() call.
// Returns malloc'd buffer (caller must free). Guarantees sequential disk I/O on HDD.
template<typename T>
static T* read_col_range(const std::string& path, size_t row_start, size_t row_count) {
    const size_t byte_start = row_start * sizeof(T);
    const size_t byte_count = row_count * sizeof(T);

    T* buf = static_cast<T*>(malloc(byte_count));
    if (!buf) { fprintf(stderr, "OOM: %zu bytes for %s\n", byte_count, path.c_str()); exit(1); }

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }

    // Hint: sequential access starting at byte_start
    posix_fadvise(fd, (off_t)byte_start, (off_t)byte_count, POSIX_FADV_SEQUENTIAL);

    if (byte_start > 0) {
        if (lseek(fd, (off_t)byte_start, SEEK_SET) == (off_t)-1) {
            perror("lseek"); close(fd); exit(1);
        }
    }

    // Large sequential reads avoid HDD seek overhead
    const size_t CHUNK = 64ul << 20; // 64 MB per syscall
    char* dst = reinterpret_cast<char*>(buf);
    size_t remaining = byte_count;
    while (remaining > 0) {
        ssize_t n = ::read(fd, dst, remaining < CHUNK ? remaining : CHUNK);
        if (n <= 0) { if (n < 0) perror("read"); break; }
        dst      += n;
        remaining -= (size_t)n;
    }
    close(fd);
    return buf;
}

// ---- Zone map entry (24 bytes) ----
struct ZoneMapEntry {
    double   min_val;
    double   max_val;
    uint32_t row_count;
    uint32_t _pad;
};

// ---- Orders join hash table: key=o_orderkey → {o_orderdate, o_shippriority} ----
struct OrdersMap {
    struct Slot {
        int32_t key;          // INT32_MIN = empty
        int32_t orderdate;
        int32_t shippriority;
        int32_t _pad;
    };

    Slot*    slots;
    uint32_t cap, mask;

    explicit OrdersMap(uint32_t cap_) : cap(cap_), mask(cap_ - 1) {
        slots = new Slot[cap_];
        for (uint32_t i = 0; i < cap_; ++i) slots[i].key = INT32_MIN;
    }
    ~OrdersMap() { delete[] slots; }

    static inline uint32_t fib_hash(int32_t k, unsigned bits) noexcept {
        return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> (64u - bits));
    }

    void insert(int32_t key, int32_t od, int32_t sp) noexcept {
        uint32_t h = fib_hash(key, 20) & mask;
        while (slots[h].key != INT32_MIN) h = (h + 1) & mask;
        slots[h] = {key, od, sp, 0};
    }

    const Slot* find(int32_t key) const noexcept {
        uint32_t h = fib_hash(key, 20) & mask;
        while (true) {
            const Slot& s = slots[h];
            if (s.key == key)       return &s;
            if (s.key == INT32_MIN) return nullptr;
            h = (h + 1) & mask;
        }
    }
};

// ---- Thread-local aggregation map: key=l_orderkey → (orderdate, shippriority, revenue) ----
struct AggMap {
    struct Slot {
        int32_t orderkey;    // INT32_MIN = empty
        int32_t orderdate;
        int32_t shippriority;
        int32_t _pad;
        double  revenue;
    };

    Slot*    slots;
    uint32_t cap, mask, sz;

    explicit AggMap(uint32_t cap_) : cap(cap_), mask(cap_ - 1), sz(0) {
        slots = new Slot[cap_];
        for (uint32_t i = 0; i < cap_; ++i) slots[i].orderkey = INT32_MIN;
    }
    ~AggMap() { delete[] slots; }

    static inline uint32_t fib_hash(int32_t k, unsigned bits) noexcept {
        return (uint32_t)(((uint64_t)(uint32_t)k * 0x9E3779B97F4A7C15ULL) >> (64u - bits));
    }

    void accumulate(int32_t ok, int32_t od, int32_t sp, double rev) noexcept {
        uint32_t h = fib_hash(ok, 18) & mask;
        while (true) {
            Slot& s = slots[h];
            if (s.orderkey == INT32_MIN) {
                s = {ok, od, sp, 0, rev};
                ++sz;
                return;
            }
            if (s.orderkey == ok) { s.revenue += rev; return; }
            h = (h + 1) & mask;
        }
    }

    void merge_into(AggMap& dest) const noexcept {
        for (uint32_t i = 0; i < cap; ++i) {
            const Slot& s = slots[i];
            if (s.orderkey != INT32_MIN)
                dest.accumulate(s.orderkey, s.orderdate, s.shippriority, s.revenue);
        }
    }
};

// ===================================================================

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    static constexpr int32_t DATE_THRESHOLD = 9204;   // epoch days for 1995-03-15
    static constexpr size_t  N_CUST         = 1500000;
    static constexpr size_t  N_LINEITEM     = 59986052;

    // ---- Phase 1: Build BUILDING-customer filter array ----
    bool* is_building = nullptr;
    {
        GENDB_PHASE("dim_filter");
        // Sequential reads for small customer columns (~1.5 MB + ~6 MB)
        const int8_t*  c_seg = read_col_range<int8_t> (gendb_dir + "/customer/c_mktsegment.bin", 0, N_CUST);
        const int32_t* c_ck  = read_col_range<int32_t>(gendb_dir + "/customer/c_custkey.bin",    0, N_CUST);

        is_building = new bool[1500001]();
        for (size_t i = 0; i < N_CUST; ++i)
            if (c_seg[i] == 1)  // BUILDING = dict code 1
                is_building[c_ck[i]] = true;

        free((void*)c_seg);
        free((void*)c_ck);
    }

    // ---- Phase 2: Scan orders → build qualifying orders hash map ----
    // Expected: ~630K qualifying rows (c_mktsegment=BUILDING AND o_orderdate<9204)
    OrdersMap*   orders_map  = new OrdersMap(1u << 20); // 1M slots, ~60% load
    uint32_t     num_threads = (uint32_t)std::thread::hardware_concurrency();
    {
        GENDB_PHASE("build_joins");

        // Zone map is tiny (~3.6 KB); mmap is fine
        size_t zm_sz;
        const uint8_t* zm_raw = (const uint8_t*)mmap_file(
            gendb_dir + "/orders/indexes/o_orderdate_zonemap.bin", zm_sz);
        uint32_t             num_blocks = *reinterpret_cast<const uint32_t*>(zm_raw);
        const ZoneMapEntry*  zm         = reinterpret_cast<const ZoneMapEntry*>(zm_raw + 4);

        // Data sorted ascending → break at first block with min_val >= threshold
        uint32_t qualifying_blocks = 0;
        for (uint32_t i = 0; i < num_blocks; ++i) {
            if (zm[i].min_val < (double)DATE_THRESHOLD) qualifying_blocks = i + 1;
            else break;
        }

        if (qualifying_blocks > 0) {
            // Exact row count for qualifying portion (last block may be partial)
            size_t orders_rows = (size_t)(qualifying_blocks - 1) * 100000
                                 + zm[qualifying_blocks - 1].row_count;

            // Sequential reads: ~25 MB per column, 4 columns = ~100 MB total
            // Each read() call issues a single contiguous sequential disk read — no seeks
            const int32_t* o_od = read_col_range<int32_t>(gendb_dir + "/orders/o_orderdate.bin",    0, orders_rows);
            const int32_t* o_ok = read_col_range<int32_t>(gendb_dir + "/orders/o_orderkey.bin",     0, orders_rows);
            const int32_t* o_ck = read_col_range<int32_t>(gendb_dir + "/orders/o_custkey.bin",      0, orders_rows);
            const int32_t* o_sp = read_col_range<int32_t>(gendb_dir + "/orders/o_shippriority.bin", 0, orders_rows);

            // Parallel morsel scan — data fully in RAM, pure CPU-bound
            std::vector<std::vector<std::tuple<int32_t,int32_t,int32_t>>> tl(num_threads);
            for (auto& v : tl) v.reserve(16384);

            std::atomic<uint32_t> blk(0);
            auto worker = [&](uint32_t tid) {
                auto& res = tl[tid];
                uint32_t b;
                while ((b = blk.fetch_add(1, std::memory_order_relaxed)) < qualifying_blocks) {
                    size_t r0 = (size_t)b * 100000;
                    size_t r1 = r0 + zm[b].row_count;
                    if (r1 > orders_rows) r1 = orders_rows;
                    for (size_t r = r0; r < r1; ++r) {
                        if (o_od[r] < DATE_THRESHOLD && is_building[o_ck[r]])
                            res.emplace_back(o_ok[r], o_od[r], o_sp[r]);
                    }
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            for (uint32_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
            for (auto& th : threads) th.join();

            // Sequential insert of ~630K rows into hash map
            for (auto& v : tl)
                for (auto& [ok, od, sp] : v)
                    orders_map->insert(ok, od, sp);

            free((void*)o_od); free((void*)o_ok);
            free((void*)o_ck); free((void*)o_sp);
        }

        munmap((void*)zm_raw, zm_sz);
    }

    // ---- Phase 3: Scan lineitem → probe orders map → aggregate revenue ----
    std::vector<AggMap*> tl_agg(num_threads);
    for (uint32_t t = 0; t < num_threads; ++t)
        tl_agg[t] = new AggMap(1u << 18); // 262144 slots per thread (~150K groups total)
    {
        GENDB_PHASE("main_scan");

        // Zone map tiny (~14.4 KB); mmap is fine
        size_t lzm_sz;
        const uint8_t* lzm_raw = (const uint8_t*)mmap_file(
            gendb_dir + "/lineitem/indexes/l_shipdate_zonemap.bin", lzm_sz);
        uint32_t            l_num_blocks = *reinterpret_cast<const uint32_t*>(lzm_raw);
        const ZoneMapEntry* lzm          = reinterpret_cast<const ZoneMapEntry*>(lzm_raw + 4);

        // Skip blocks fully before threshold (max_val <= 9204) — ~46% of blocks
        uint32_t first_qual = l_num_blocks;
        for (uint32_t i = 0; i < l_num_blocks; ++i) {
            if (lzm[i].max_val > (double)DATE_THRESHOLD) { first_qual = i; break; }
        }
        uint32_t qual_blocks = l_num_blocks - first_qual;

        if (qual_blocks > 0) {
            // Read only the qualifying portion: ~32.4M rows out of 60M
            // l_row_start ≈ 276 * 100000 = 27,600,000
            // Sequential reads: ~130 MB × 2 (int32) + ~260 MB × 2 (double) = ~780 MB total
            const size_t l_row_start = (size_t)first_qual * 100000;
            const size_t l_row_count = N_LINEITEM - l_row_start;

            // Load columns sequentially — one file at a time, guaranteed sequential HDD I/O
            const int32_t* l_sd = read_col_range<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",      l_row_start, l_row_count);
            const int32_t* l_ok = read_col_range<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",      l_row_start, l_row_count);
            const double*  l_ep = read_col_range<double> (gendb_dir + "/lineitem/l_extendedprice.bin", l_row_start, l_row_count);
            const double*  l_dc = read_col_range<double> (gendb_dir + "/lineitem/l_discount.bin",      l_row_start, l_row_count);

            // Parallel morsel scan — data in RAM, pure CPU-bound
            std::atomic<uint32_t> blk(0);
            auto worker = [&](uint32_t tid) {
                AggMap* agg = tl_agg[tid];
                uint32_t b_rel;
                while ((b_rel = blk.fetch_add(1, std::memory_order_relaxed)) < qual_blocks) {
                    uint32_t b = first_qual + b_rel;
                    // Buffer offset: row b*100000 in file → offset (b*100000 - l_row_start) in buffer
                    size_t r0 = (size_t)b * 100000 - l_row_start;
                    size_t r1 = r0 + lzm[b].row_count;
                    if (r1 > l_row_count) r1 = l_row_count;
                    for (size_t r = r0; r < r1; ++r) {
                        if (l_sd[r] > DATE_THRESHOLD) {
                            const OrdersMap::Slot* s = orders_map->find(l_ok[r]);
                            if (s) {
                                agg->accumulate(l_ok[r], s->orderdate, s->shippriority,
                                                l_ep[r] * (1.0 - l_dc[r]));
                            }
                        }
                    }
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(num_threads);
            for (uint32_t t = 0; t < num_threads; ++t) threads.emplace_back(worker, t);
            for (auto& th : threads) th.join();

            free((void*)l_sd); free((void*)l_ok);
            free((void*)l_ep); free((void*)l_dc);
        }

        munmap((void*)lzm_raw, lzm_sz);
    }

    // ---- Phase 4: Merge thread-local aggregates, top-10, output ----
    {
        GENDB_PHASE("output");

        AggMap* global = new AggMap(1u << 20); // 1M slots for ~150K groups
        for (uint32_t t = 0; t < num_threads; ++t) {
            tl_agg[t]->merge_into(*global);
            delete tl_agg[t];
        }
        tl_agg.clear();

        struct Row { int32_t orderkey, orderdate, shippriority; double revenue; };
        std::vector<Row> results;
        results.reserve(global->sz);
        for (uint32_t i = 0; i < global->cap; ++i) {
            const auto& s = global->slots[i];
            if (s.orderkey != INT32_MIN)
                results.push_back({s.orderkey, s.orderdate, s.shippriority, s.revenue});
        }

        // Top-10: ORDER BY revenue DESC, o_orderdate ASC
        size_t top_k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + top_k, results.end(),
            [](const Row& a, const Row& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.orderdate < b.orderdate;
            });

        FILE* f = fopen((results_dir + "/Q3.csv").c_str(), "w");
        if (!f) { perror("fopen"); exit(1); }
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[12];
        for (size_t i = 0; i < top_k; ++i) {
            const auto& r = results[i];
            gendb::epoch_days_to_date_str(r.orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n", r.orderkey, r.revenue, date_buf, r.shippriority);
        }
        fclose(f);
        delete global;
    }

    delete[] is_building;
    delete orders_map;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_Q3(argv[1], argc > 2 ? argv[2] : ".");
    return 0;
}
#endif
