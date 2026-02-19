// Q18: Large Volume Customer
// Strategy:
//   Phase 1: Parallel scan of l_orderkey + l_quantity, build thread-local qty maps,
//            merge, filter HAVING SUM(l_quantity) > 300 (scaled: 30000) -> qualifying_set
//   Phase 2: Parallel scan of orders columns, semi-join with qualifying_set
//   Phase 3: Point lookup c_name via mmap hash_c_custkey index
//   Phase 4: Multi-value lookup l_quantity sum via mmap hash_l_orderkey index
//   Phase 5: Sort by o_totalprice DESC, o_orderdate ASC, LIMIT 100

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ============================================================================
// Custom open-addressing hash map for phase1 qty accumulation
// Uses linear probing, pre-sized for ~15M unique orderkeys
// ============================================================================
struct QtyMap {
    struct Slot {
        int32_t key;
        int64_t value;
    };
    static constexpr int32_t EMPTY_KEY = 0; // orderkeys start at 1

    std::vector<Slot> table;
    uint32_t mask;
    size_t count;

    QtyMap() : mask(0), count(0) {}

    void init(size_t cap) {
        // cap must be power of 2
        table.assign(cap, {EMPTY_KEY, 0LL});
        mask = (uint32_t)(cap - 1);
        count = 0;
    }

    inline void add(int32_t key, int64_t qty) {
        uint32_t pos = (uint32_t)((uint32_t)key * 0x9E3779B9U) & mask;
        while (true) {
            int32_t k = table[pos].key;
            if (k == key) {
                table[pos].value += qty;
                return;
            }
            if (k == EMPTY_KEY) {
                table[pos].key = key;
                table[pos].value = qty;
                count++;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    // Merge another QtyMap into this one
    void merge_from(const QtyMap& other) {
        for (uint32_t i = 0; i <= other.mask; i++) {
            if (other.table[i].key != EMPTY_KEY) {
                add(other.table[i].key, other.table[i].value);
            }
        }
    }
};

// ============================================================================
// Tiny hash set for ~100 qualifying orderkeys
// ============================================================================
struct SmallSet {
    // Use gendb::CompactHashSet<int32_t> - at ~57 entries fits in L1 cache
    gendb::CompactHashSet<int32_t> impl;

    SmallSet() : impl(256) {}

    inline bool contains(int32_t key) const { return impl.contains(key); }
    inline void insert(int32_t key) { impl.insert(key); }
    size_t size() const { return impl.size(); }
};

// ============================================================================
// Result row
// ============================================================================
struct ResultRow {
    char    c_name[22];    // "Customer#XXXXXXXXX" max 18 chars + null
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;  // scaled by 100
    int64_t sum_qty;       // scaled by 100
};

// ============================================================================
// mmap helper that returns raw pointer + size
// ============================================================================
struct MmapFile {
    void*  ptr  = nullptr;
    size_t size = 0;
    int    fd   = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        if (size == 0) return;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        madvise(ptr, size, MADV_RANDOM);
    }

    ~MmapFile() {
        if (ptr && size) munmap(ptr, size);
        if (fd >= 0) ::close(fd);
    }

    template<typename T>
    const T* as() const { return static_cast<const T*>(ptr); }
};

// ============================================================================
// hash_c_custkey lookup: [uint32_t table_size] then slots [int32_t key, uint32_t pos]
// Empty slot: key = INT32_MIN
// Hash function: Fibonacci hashing with 64-bit multiply-shift
// ============================================================================
struct CustKeyIndex {
    uint32_t table_size;
    uint32_t shift;
    struct Slot { int32_t key; uint32_t pos; };
    const Slot* slots;
    MmapFile file;

    void open(const std::string& path) {
        file.open(path);
        const uint32_t* raw = file.as<uint32_t>();
        table_size = raw[0];
        // shift = 64 - log2(table_size)
        shift = (uint32_t)(64 - __builtin_clzll((unsigned long long)table_size));
        slots = reinterpret_cast<const Slot*>(raw + 1);
    }

    // Returns row index into c_custkey / c_name arrays, or UINT32_MAX if not found
    uint32_t lookup(int32_t custkey) const {
        uint32_t mask = table_size - 1;
        uint32_t pos = (uint32_t)(((uint64_t)(uint32_t)custkey * 0x9E3779B97F4A7C15ULL) >> shift) & mask;
        while (true) {
            int32_t k = slots[pos].key;
            if (k == custkey) return slots[pos].pos;
            if (k == INT32_MIN) return UINT32_MAX; // empty
            pos = (pos + 1) & mask;
        }
    }
};

// ============================================================================
// hash_l_orderkey multi-value lookup
// Layout: [uint32_t num_rows][uint32_t table_size][slots: 12B each][positions: 4B each]
// Slot: [int32_t key, uint32_t offset, uint32_t count]
// Empty slot key: INT32_MIN
// positions[0] = num_rows (sentinel); data at positions[offset+1 .. offset+count]
// Hash function: Fibonacci hashing with 64-bit multiply-shift
// ============================================================================
struct OrderkeyIndex {
    uint32_t num_rows;
    uint32_t table_size;
    uint32_t shift;
    struct Slot { int32_t key; uint32_t offset; uint32_t count; };
    const Slot*     slots;
    const uint32_t* positions;
    MmapFile file;

    void open(const std::string& path) {
        file.open(path);
        const uint32_t* raw = file.as<uint32_t>();
        num_rows   = raw[0];
        table_size = raw[1];
        shift = (uint32_t)(64 - __builtin_clzll((unsigned long long)table_size));
        // slots start at byte offset 8
        slots = reinterpret_cast<const Slot*>(
            static_cast<const uint8_t*>(file.ptr) + 8);
        // positions start after slots; positions[0] is sentinel (num_rows), data at [offset+1]
        positions = reinterpret_cast<const uint32_t*>(
            static_cast<const uint8_t*>(file.ptr) + 8 + (size_t)table_size * 12);
    }

    // Returns span of positions for orderkey, or empty if not found
    struct Span { const uint32_t* ptr; uint32_t count; };
    Span lookup(int32_t orderkey) const {
        uint32_t mask = table_size - 1;
        uint32_t pos = (uint32_t)(((uint64_t)(uint32_t)orderkey * 0x9E3779B97F4A7C15ULL) >> shift) & mask;
        while (true) {
            const Slot& s = slots[pos];
            if (s.key == orderkey) return {positions + s.offset + 1, s.count};
            if (s.key == INT32_MIN) return {nullptr, 0}; // empty
            pos = (pos + 1) & mask;
        }
    }
};

// ============================================================================
// Main query implementation
// ============================================================================
void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int nthreads = (int)std::thread::hardware_concurrency();

    // -------------------------------------------------------------------------
    // Phase 1: Parallel scan of l_orderkey + l_quantity, build qty sum per orderkey
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> l_orderkey(gendb_dir + "/lineitem/l_orderkey.bin");
    gendb::MmapColumn<int64_t> l_quantity(gendb_dir + "/lineitem/l_quantity.bin");

    // Kick off prefetch
    l_orderkey.prefetch();
    l_quantity.prefetch();

    const size_t nrows_li = l_orderkey.count;
    // Hash table size: next power-of-2 >= nrows_li * 4/3
    // ~60M rows, ~15M unique keys → use 33554432 (2^25) for thread-locals too small
    // Use 16M per thread-local map (each thread sees ~60M/nthreads rows)
    // Global map: 2^25 = 33554432
    constexpr size_t GLOBAL_CAP = 1u << 25; // 33554432

    // Thread-local maps: smaller, sized for nrows_li/nthreads unique keys
    // Each thread sees ~60M/64 = 937K rows → likely ~234K unique keys → use 2^19=524288
    constexpr size_t LOCAL_CAP  = 1u << 19; // 524288

    SmallSet qualifying_set;

    {
        GENDB_PHASE("phase1_lineitem_scan");

        std::vector<QtyMap> local_maps(nthreads);
        for (int t = 0; t < nthreads; t++) {
            local_maps[t].init(LOCAL_CAP);
        }

        // Parallel morsel-driven scan
        const size_t morsel_size = 65536; // rows per morsel
        std::atomic<size_t> morsel_idx{0};

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                QtyMap& lmap = local_maps[t];
                const int32_t* ok = l_orderkey.data;
                const int64_t* qty = l_quantity.data;
                while (true) {
                    size_t start = morsel_idx.fetch_add(morsel_size, std::memory_order_relaxed);
                    if (start >= nrows_li) break;
                    size_t end = std::min(start + morsel_size, nrows_li);
                    for (size_t i = start; i < end; i++) {
                        lmap.add(ok[i], qty[i]);
                    }
                    // If local map is getting full (>75% load), pre-emptively resize
                    // Actually LOCAL_CAP=524288 at 75% = 393K; each thread sees ~234K unique → fine
                }
            });
        }
        for (auto& th : threads) th.join();

        // Merge thread-local maps into global map
        QtyMap global_map;
        global_map.init(GLOBAL_CAP);

        for (int t = 0; t < nthreads; t++) {
            global_map.merge_from(local_maps[t]);
            // Free local map memory
            local_maps[t].table.clear();
            local_maps[t].table.shrink_to_fit();
        }

        // Filter: qualifying orderkeys where sum_qty > 300.00 (l_quantity scale_factor=2, raw threshold=30000)
        qualifying_set.impl.reserve(256);
        const int64_t THRESHOLD = 30000LL; // l_quantity scale_factor=2: 300.00 * 100 = 30000

        for (uint32_t i = 0; i <= global_map.mask; i++) {
            if (global_map.table[i].key != QtyMap::EMPTY_KEY &&
                global_map.table[i].value > THRESHOLD) {
                qualifying_set.insert(global_map.table[i].key);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 2: Parallel scan of orders, semi-join with qualifying_set
    // -------------------------------------------------------------------------
    gendb::MmapColumn<int32_t> o_orderkey  (gendb_dir + "/orders/o_orderkey.bin");
    gendb::MmapColumn<int32_t> o_custkey   (gendb_dir + "/orders/o_custkey.bin");
    gendb::MmapColumn<int32_t> o_orderdate (gendb_dir + "/orders/o_orderdate.bin");
    gendb::MmapColumn<int64_t> o_totalprice(gendb_dir + "/orders/o_totalprice.bin");

    o_orderkey.prefetch();
    o_custkey.prefetch();
    o_orderdate.prefetch();
    o_totalprice.prefetch();

    struct OrderMatch {
        int32_t o_orderkey;
        int32_t o_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
    };

    std::vector<OrderMatch> matching_orders;

    {
        GENDB_PHASE("phase2_orders_scan");

        const size_t nrows_ord = o_orderkey.count;
        const size_t morsel_size = 65536;
        std::atomic<size_t> morsel_idx{0};
        std::mutex results_mutex;

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&]() {
                std::vector<OrderMatch> local_matches;
                local_matches.reserve(8);
                const int32_t* ok   = o_orderkey.data;
                const int32_t* ck   = o_custkey.data;
                const int32_t* od   = o_orderdate.data;
                const int64_t* tp   = o_totalprice.data;

                while (true) {
                    size_t start = morsel_idx.fetch_add(morsel_size, std::memory_order_relaxed);
                    if (start >= nrows_ord) break;
                    size_t end = std::min(start + morsel_size, nrows_ord);
                    for (size_t i = start; i < end; i++) {
                        if (qualifying_set.contains(ok[i])) {
                            local_matches.push_back({ok[i], ck[i], od[i], tp[i]});
                        }
                    }
                }
                if (!local_matches.empty()) {
                    std::lock_guard<std::mutex> lock(results_mutex);
                    matching_orders.insert(matching_orders.end(),
                                           local_matches.begin(), local_matches.end());
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // -------------------------------------------------------------------------
    // Phase 3 & 4: Customer lookup + lineitem qty sum (sequential, tiny ~100 rows)
    // -------------------------------------------------------------------------
    std::vector<ResultRow> results;
    results.reserve(matching_orders.size());

    {
        GENDB_PHASE("phase3_phase4_index_lookups");

        // Load pre-built indexes via mmap
        CustKeyIndex cust_idx;
        cust_idx.open(gendb_dir + "/indexes/hash_c_custkey.bin");

        OrderkeyIndex li_idx;
        li_idx.open(gendb_dir + "/indexes/hash_l_orderkey.bin");

        // Load l_quantity column for position-based access
        gendb::MmapColumn<int64_t> l_qty_col(gendb_dir + "/lineitem/l_quantity.bin");
        l_qty_col.advise_random();

        // Load c_name raw bytes for position-based access
        // c_name format: [int32_t length][char... name_bytes] = 22 bytes per entry
        MmapFile c_name_file;
        c_name_file.open(gendb_dir + "/customer/c_name.bin");
        const uint8_t* c_name_raw = static_cast<const uint8_t*>(c_name_file.ptr);
        constexpr size_t C_NAME_ENTRY_SIZE = 22; // 4 bytes length + 18 bytes name

        for (const auto& ord : matching_orders) {
            ResultRow row;
            row.c_custkey   = ord.o_custkey;
            row.o_orderkey  = ord.o_orderkey;
            row.o_orderdate = ord.o_orderdate;
            row.o_totalprice = ord.o_totalprice;

            // Lookup c_name
            uint32_t cust_pos = cust_idx.lookup(ord.o_custkey);
            if (cust_pos != UINT32_MAX) {
                const uint8_t* entry = c_name_raw + cust_pos * C_NAME_ENTRY_SIZE;
                int32_t name_len;
                memcpy(&name_len, entry, 4);
                if (name_len < 0) name_len = 0;
                if (name_len > 21) name_len = 21;
                memcpy(row.c_name, entry + 4, name_len);
                row.c_name[name_len] = '\0';
            } else {
                row.c_name[0] = '\0';
            }

            // Lookup sum(l_quantity) from lineitem index
            auto span = li_idx.lookup(ord.o_orderkey);
            int64_t sum_qty = 0;
            for (uint32_t j = 0; j < span.count; j++) {
                uint32_t li_pos = span.ptr[j];
                sum_qty += l_qty_col.data[li_pos];
            }
            row.sum_qty = sum_qty;

            results.push_back(row);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 5: Sort by o_totalprice DESC, o_orderdate ASC, LIMIT 100
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("sort_topk");

        // Use TopKHeap for O(n log 100) instead of O(n log n) full sort
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice)
                return a.o_totalprice > b.o_totalprice; // DESC
            return a.o_orderdate < b.o_orderdate;       // ASC
        };
        gendb::TopKHeap<ResultRow, decltype(cmp)> heap(100, cmp);
        for (auto& row : results) heap.push(row);
        results = heap.sorted();
    }

    // -------------------------------------------------------------------------
    // Phase 6: Output CSV
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q18.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) throw std::runtime_error("Cannot open output: " + out_path);

        fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[12];
        for (const auto& row : results) {
            gendb::epoch_days_to_date_str(row.o_orderdate, date_buf);
            // o_totalprice and sum_qty both have scale_factor=2
            // Format: 558289.17 (2 decimal places)
            int64_t tp_int  = row.o_totalprice / 100;
            int64_t tp_frac = row.o_totalprice % 100;
            if (tp_frac < 0) tp_frac = -tp_frac;
            int64_t sq_int  = row.sum_qty / 100;
            int64_t sq_frac = row.sum_qty % 100;
            if (sq_frac < 0) sq_frac = -sq_frac;

            fprintf(fp, "%s,%d,%d,%s,%lld.%02lld,%lld.%02lld\n",
                    row.c_name,
                    row.c_custkey,
                    row.o_orderkey,
                    date_buf,
                    (long long)tp_int, (long long)tp_frac,
                    (long long)sq_int, (long long)sq_frac);
        }

        fclose(fp);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
