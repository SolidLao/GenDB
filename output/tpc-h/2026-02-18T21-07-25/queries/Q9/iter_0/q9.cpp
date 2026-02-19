// Q9: Product Type Profit Measure
// Strategy:
//   1. Nation (25 rows): load into flat array[25] → nation_name
//   2. Supplier (100K rows): flat array[suppkey] → nation_idx (direct index)
//   3. Part (2M rows): scan p_name for '%green%', build 256KB bitset on p_partkey
//   4. Load prebuilt idx_orders_orderkey (HashIndex: key→row_pos, mmap)
//   5. Load prebuilt idx_partsupp_part_supp (MultiValueHashIndex on ps_partkey, mmap)
//      + mmap ps_suppkey + ps_supplycost for lookup after probe
//   6. Parallel scan lineitem (60M rows): bitset filter → supplier lookup → orders idx → partsupp idx → accumulate
//   7. Merge thread-local agg arrays → sort → output CSV

#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
#include <fstream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "date_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Prebuilt index layouts (from build_indexes.cpp):
//
// HashIndex (idx_orders_orderkey):
//   [uint32_t capacity]
//   [capacity × Entry{int32_t key, uint32_t position}]
//   key == -1 → empty slot
//
// MultiValueHashIndex (idx_partsupp_part_supp, built on ps_partkey):
//   [uint32_t capacity]
//   [capacity × HashEntry{int32_t key, uint32_t offset, uint32_t count}]
//   key == -1 → empty slot
//   [uint32_t pos_count]
//   [pos_count × uint32_t positions]   (row indices into partsupp)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Hash function matching build_indexes.cpp: (uint64_t)key * 0x9E3779B97F4A7C15 >> 32
// ---------------------------------------------------------------------------
inline uint32_t hash_int32_idx(int32_t key, uint32_t mask) {
    uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
    return (uint32_t)(h >> 32) & mask;
}

// ---------------------------------------------------------------------------
// Load variable-length string column (format: uint32_t len, char data...)
// Returns vector of strings (only 25 or 2M, both manageable)
// ---------------------------------------------------------------------------
static std::vector<std::string> load_string_column(const std::string& path) {
    std::vector<std::string> result;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open: " + path);
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (ptr == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);

    const char* data = static_cast<const char*>(ptr);
    size_t pos = 0;
    while (pos + 4 <= sz) {
        uint32_t len;
        memcpy(&len, data + pos, 4);
        pos += 4;
        if (pos + len > sz) break;
        result.emplace_back(data + pos, len);
        pos += len;
    }
    munmap(ptr, sz);
    return result;
}

// ---------------------------------------------------------------------------
// MmapRaw: generic raw mmap for index files (not typed columns)
// ---------------------------------------------------------------------------
struct MmapRaw {
    void*  ptr  = nullptr;
    size_t size = 0;
    int    fd   = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open index: " + path);
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) {
            ::close(fd);
            throw std::runtime_error("mmap failed: " + path);
        }
        madvise(ptr, size, MADV_RANDOM);
    }

    ~MmapRaw() {
        if (ptr && size) munmap(ptr, size);
        if (fd >= 0) ::close(fd);
    }

    const uint8_t* data() const { return static_cast<const uint8_t*>(ptr); }
};

// ---------------------------------------------------------------------------
// Orders HashIndex probe: key → row position in orders table
// Layout: [uint32_t cap][cap × {int32_t key, uint32_t pos}]
// ---------------------------------------------------------------------------
struct OrdersHashIndex {
    struct Entry { int32_t key; uint32_t pos; };

    uint32_t      capacity;
    uint32_t      mask;
    const Entry*  table;  // points into mmap

    void init(const uint8_t* data) {
        memcpy(&capacity, data, 4);
        mask  = capacity - 1;
        table = reinterpret_cast<const Entry*>(data + 4);
    }

    // Returns row index into orders columns, or UINT32_MAX if not found
    uint32_t find(int32_t key) const {
        uint32_t h = hash_int32_idx(key, mask);
        while (table[h].key != -1) {
            if (table[h].key == key) return table[h].pos;
            h = (h + 1) & mask;
        }
        return UINT32_MAX;
    }
};

// ---------------------------------------------------------------------------
// PartSupp MultiValueHashIndex probe: ps_partkey → span of positions
// Then we scan positions checking ps_suppkey == target to find ps_supplycost
// Layout: [uint32_t cap][cap × {int32_t key, uint32_t offset, uint32_t count}]
//         [uint32_t pos_count][uint32_t positions[]]
// ---------------------------------------------------------------------------
struct PartsuppHashIndex {
    struct HashEntry { int32_t key; uint32_t offset; uint32_t count; };

    uint32_t           capacity;
    uint32_t           mask;
    const HashEntry*   hash_table;
    uint32_t           pos_count;
    const uint32_t*    positions;

    void init(const uint8_t* data) {
        memcpy(&capacity, data, 4);
        mask       = capacity - 1;
        hash_table = reinterpret_cast<const HashEntry*>(data + 4);
        size_t ht_bytes = (size_t)capacity * sizeof(HashEntry);
        const uint8_t* after_ht = data + 4 + ht_bytes;
        memcpy(&pos_count, after_ht, 4);
        positions  = reinterpret_cast<const uint32_t*>(after_ht + 4);
    }

    // Returns (offset, count) into positions array for partkey, or {0,0} if not found
    std::pair<uint32_t,uint32_t> find_partkey(int32_t partkey) const {
        uint32_t h = hash_int32_idx(partkey, mask);
        while (hash_table[h].key != -1) {
            if (hash_table[h].key == partkey)
                return {hash_table[h].offset, hash_table[h].count};
            h = (h + 1) & mask;
        }
        return {0, 0};
    }
};

// ---------------------------------------------------------------------------
// Year slot mapping: 1992-1998 → slots 0-6
// ---------------------------------------------------------------------------
static constexpr int MIN_YEAR = 1992;
static constexpr int MAX_YEAR = 1998;
static constexpr int NUM_YEARS = MAX_YEAR - MIN_YEAR + 1; // 7
static constexpr int NUM_NATIONS = 25;
static constexpr int AGG_SIZE = NUM_NATIONS * 10; // 10 year slots to be safe

inline int year_to_slot(int year) {
    int s = year - MIN_YEAR;
    if (s < 0) s = 0;
    if (s >= 10) s = 9;
    return s;
}

// ---------------------------------------------------------------------------
// Main query function
// ---------------------------------------------------------------------------
void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string& d = gendb_dir;

    // -----------------------------------------------------------------------
    // Phase 1: Load dimension tables (nation + supplier) + build part bitset
    // -----------------------------------------------------------------------
    std::string nation_names[NUM_NATIONS];   // nationkey → name
    uint8_t supp_nation[100001] = {};        // suppkey → nation_idx (1-indexed keys)

    {
        GENDB_PHASE("dim_filter");

        // --- Nation: 25 rows, load names into flat array ---
        {
            gendb::MmapColumn<int32_t> n_nationkey(d + "/nation/n_nationkey.bin");
            auto n_name = load_string_column(d + "/nation/n_name.bin");
            for (size_t i = 0; i < n_nationkey.count; i++) {
                int32_t nk = n_nationkey.data[i];
                if (nk >= 0 && nk < NUM_NATIONS) {
                    nation_names[nk] = n_name[i];
                }
            }
        }

        // --- Supplier: 100K rows, build suppkey → nation_idx map ---
        {
            gendb::MmapColumn<int32_t> s_suppkey(d + "/supplier/s_suppkey.bin");
            gendb::MmapColumn<int32_t> s_nationkey(d + "/supplier/s_nationkey.bin");
            size_t n = s_suppkey.count;
            for (size_t i = 0; i < n; i++) {
                int32_t sk = s_suppkey.data[i];
                int32_t nk = s_nationkey.data[i];
                if (sk >= 0 && sk <= 100000 && nk >= 0 && nk < NUM_NATIONS) {
                    supp_nation[sk] = (uint8_t)nk;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Build part bitset (p_name LIKE '%green%')
    // Bitset: 2M+1 bits = 256KB, use uint64_t words
    // -----------------------------------------------------------------------
    // 2M part keys, word-aligned
    static constexpr size_t PART_MAX_KEY = 2000001;
    static constexpr size_t BITSET_WORDS = (PART_MAX_KEY + 63) / 64;
    std::vector<uint64_t> part_bitset(BITSET_WORDS, 0);

    {
        GENDB_PHASE("build_joins"); // part bitset phase
        gendb::MmapColumn<int32_t> p_partkey(d + "/part/p_partkey.bin");
        auto p_name = load_string_column(d + "/part/p_name.bin");
        size_t n = p_partkey.count;
        for (size_t i = 0; i < n; i++) {
            // Substring match for 'green'
            if (p_name[i].find("green") != std::string::npos) {
                int32_t pk = p_partkey.data[i];
                if (pk >= 0 && (size_t)pk < PART_MAX_KEY) {
                    part_bitset[pk >> 6] |= (1ULL << (pk & 63));
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3: Load prebuilt indexes (mmap, 0ms build cost)
    // -----------------------------------------------------------------------
    MmapRaw orders_idx_raw, partsupp_idx_raw;
    OrdersHashIndex   orders_idx;
    PartsuppHashIndex partsupp_idx;

    // Load o_orderdate column (needed for date lookup after orders index probe)
    gendb::MmapColumn<int32_t> o_orderdate(d + "/orders/o_orderdate.bin");

    // Load partsupp key/cost columns (needed after partsupp index probe)
    gendb::MmapColumn<int32_t> ps_suppkey(d + "/partsupp/ps_suppkey.bin");
    gendb::MmapColumn<int64_t> ps_supplycost(d + "/partsupp/ps_supplycost.bin");

    {
        GENDB_PHASE("index_load");
        orders_idx_raw.open(d + "/indexes/idx_orders_orderkey.bin");
        orders_idx.init(orders_idx_raw.data());

        partsupp_idx_raw.open(d + "/indexes/idx_partsupp_part_supp.bin");
        partsupp_idx.init(partsupp_idx_raw.data());

        // Prefetch lineitem columns into page cache
        // (done below after mmap opens)
    }

    // -----------------------------------------------------------------------
    // Phase 4: Parallel lineitem scan with fused joins + aggregation
    // -----------------------------------------------------------------------
    // Open lineitem columns
    gendb::MmapColumn<int32_t> l_orderkey(d + "/lineitem/l_orderkey.bin");
    gendb::MmapColumn<int32_t> l_partkey(d  + "/lineitem/l_partkey.bin");
    gendb::MmapColumn<int32_t> l_suppkey(d  + "/lineitem/l_suppkey.bin");
    gendb::MmapColumn<int64_t> l_extendedprice(d + "/lineitem/l_extendedprice.bin");
    gendb::MmapColumn<int64_t> l_discount(d  + "/lineitem/l_discount.bin");
    gendb::MmapColumn<int64_t> l_quantity(d  + "/lineitem/l_quantity.bin");

    // Prefetch all lineitem columns concurrently (HDD: overlap I/O)
    mmap_prefetch_all(l_orderkey, l_partkey, l_suppkey,
                      l_extendedprice, l_discount, l_quantity);

    const size_t total_rows = l_orderkey.count;
    const int    num_threads = (int)std::thread::hardware_concurrency();
    const size_t morsel_size = 65536;

    // Global aggregation: [nation_idx * 10 + year_slot] → sum_profit (scaled int64)
    // Scale: l_extendedprice * (100 - l_discount) is in units of 100^2 = 10000
    //        ps_supplycost * l_quantity is in units of 100^2 = 10000
    // So amount is in units of 1/10000 of currency unit
    // We'll keep full precision and divide by 10000 at output
    std::vector<std::vector<int64_t>> thread_agg(num_threads,
                                                  std::vector<int64_t>(AGG_SIZE, 0));

    std::atomic<size_t> morsel_idx{0};

    auto worker = [&](int tid) {
        auto& local_agg = thread_agg[tid];

        // Local copies of pointers for hot loop
        const int32_t* lk = l_orderkey.data;
        const int32_t* lp = l_partkey.data;
        const int32_t* ls = l_suppkey.data;
        const int64_t* le = l_extendedprice.data;
        const int64_t* ld = l_discount.data;
        const int64_t* lq = l_quantity.data;

        const uint64_t* pbits = part_bitset.data();
        const int32_t*  od    = o_orderdate.data;
        const int32_t*  psk   = ps_suppkey.data;
        const int64_t*  psc   = ps_supplycost.data;

        while (true) {
            size_t start = morsel_idx.fetch_add(morsel_size, std::memory_order_relaxed);
            if (start >= total_rows) break;
            size_t end = std::min(start + morsel_size, total_rows);

            for (size_t i = start; i < end; i++) {
                // Step 1: Part bitset filter (branch-free test, ~99% early exit)
                int32_t pk = lp[i];
                if ((size_t)pk >= PART_MAX_KEY) continue;
                if (!((pbits[pk >> 6] >> (pk & 63)) & 1)) continue;

                // Step 2: Supplier → nation lookup
                int32_t sk = ls[i];
                if (sk < 0 || sk > 100000) continue;
                uint8_t nation_idx = supp_nation[sk];

                // Step 3: Orders index → o_orderdate → year
                int32_t ok = lk[i];
                uint32_t orow = orders_idx.find(ok);
                if (orow == UINT32_MAX) continue;
                int32_t odate = od[orow];
                if (odate < 0 || odate >= 30000) continue;
                int year = gendb::extract_year(odate);
                int yslot = year_to_slot(year);

                // Step 4: PartSupp index → ps_supplycost
                // Index is on ps_partkey; scan positions to find matching suppkey
                auto [ps_off, ps_cnt] = partsupp_idx.find_partkey(pk);
                if (ps_cnt == 0) continue;

                int64_t supply_cost = 0;
                bool found_ps = false;
                const uint32_t* ps_positions = partsupp_idx.positions;
                for (uint32_t pi = ps_off; pi < ps_off + ps_cnt; pi++) {
                    uint32_t row = ps_positions[pi];
                    if (psk[row] == sk) {
                        supply_cost = psc[row];
                        found_ps = true;
                        break;
                    }
                }
                if (!found_ps) continue;

                // Step 5: Compute amount (all scaled by 100, so scale^2=10000)
                // amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
                // In scaled ints (scale=100):
                //   l_extendedprice * (100 - l_discount) / 100  [→ divide by 100 to get scale=100]
                //   ps_supplycost * l_quantity / 100             [→ divide by 100 to get scale=100]
                // Keep in scale^2=10000 by not dividing yet:
                int64_t ep  = le[i];  // scale 100
                int64_t dis = ld[i];  // scale 100 (e.g., 5 means 0.05)
                int64_t qty = lq[i];  // scale 100
                // revenue = ep * (100 - dis)   [scale = 100 * 100 = 10000]
                // cost    = supply_cost * qty   [scale = 100 * 100 = 10000]
                int64_t revenue = ep * (100LL - dis);
                int64_t cost    = supply_cost * qty;
                int64_t amount  = revenue - cost;

                // Step 6: Accumulate into flat aggregation array
                int agg_idx = nation_idx * 10 + yslot;
                local_agg[agg_idx] += amount;
            }
        }
    };

    {
        GENDB_PHASE("main_scan");
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& th : threads) th.join();
    }

    // -----------------------------------------------------------------------
    // Phase 5: Merge thread-local aggregation arrays
    // -----------------------------------------------------------------------
    std::vector<int64_t> global_agg(AGG_SIZE, 0);
    {
        for (int t = 0; t < num_threads; t++) {
            for (int j = 0; j < AGG_SIZE; j++) {
                global_agg[j] += thread_agg[t][j];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 6: Collect results, sort, output CSV
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct ResultRow {
            std::string nation;
            int         o_year;
            int64_t     sum_profit; // in scale 10000
        };

        std::vector<ResultRow> rows;
        rows.reserve(NUM_NATIONS * NUM_YEARS);

        for (int ni = 0; ni < NUM_NATIONS; ni++) {
            if (nation_names[ni].empty()) continue;
            for (int ys = 0; ys < 10; ys++) {
                int64_t profit = global_agg[ni * 10 + ys];
                if (profit == 0) continue;
                int year = MIN_YEAR + ys;
                if (year < MIN_YEAR || year > MAX_YEAR) continue;
                rows.push_back({nation_names[ni], year, profit});
            }
        }

        // Sort: nation ASC, o_year DESC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            int cmp = a.nation.compare(b.nation);
            if (cmp != 0) return cmp < 0;
            return a.o_year > b.o_year;
        });

        // Write CSV
        std::string out_path = results_dir + "/Q9.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output: " + out_path);

        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : rows) {
            // Divide by 10000 for output (scale^2 → 2 decimal places)
            int64_t whole = r.sum_profit / 10000LL;
            int64_t frac  = r.sum_profit % 10000LL;
            // Handle negative amounts correctly
            if (frac < 0) { whole--; frac += 10000; }
            // Round to 2 decimal places (divide frac by 100)
            int64_t cents = frac / 100;
            int64_t subcents = frac % 100;
            if (subcents >= 50) cents++;
            if (cents >= 100) { whole++; cents -= 100; }
            fprintf(f, "%s,%d,%lld.%02lld\n",
                    r.nation.c_str(),
                    r.o_year,
                    (long long)whole,
                    (long long)cents);
        }
        fclose(f);
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
    run_Q9(gendb_dir, results_dir);
    return 0;
}
#endif
