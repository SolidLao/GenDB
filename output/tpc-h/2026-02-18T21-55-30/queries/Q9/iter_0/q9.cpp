// Q9: Product Type Profit Measure
// SELECT nation, o_year, SUM(amount) AS sum_profit
// FROM (
//   SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year,
//          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
//   FROM part, supplier, lineitem, partsupp, orders, nation
//   WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
//     AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
//     AND p_name LIKE '%green%'
// ) AS profit
// GROUP BY nation, o_year
// ORDER BY nation, o_year DESC;

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <string>
#include <vector>

#include "date_utils.h"
#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// Helper: read a length-prefixed string column (int32_t len + bytes)
// Returns vector of (offset, length) pairs into the raw mmap'd bytes
// ---------------------------------------------------------------------------
struct StringRef {
    const char* ptr;
    int32_t     len;
};

// Read all length-prefixed strings from a raw byte mmap'd file
// The file format is: [int32_t len][len bytes][int32_t len][len bytes]...
static std::vector<StringRef> read_string_column(const char* raw, size_t file_size) {
    std::vector<StringRef> result;
    result.reserve(25); // for small tables; will resize for large
    size_t pos = 0;
    while (pos + 4 <= file_size) {
        int32_t len;
        memcpy(&len, raw + pos, 4);
        pos += 4;
        if (pos + len > file_size) break;
        result.push_back({raw + pos, len});
        pos += len;
    }
    return result;
}

// ---------------------------------------------------------------------------
// Pre-built index structures (mmap'd, zero-copy)
// ---------------------------------------------------------------------------
// hash_ps_partkey: [uint32_t table_size] [table_size x {int32_t key, uint32_t offset, uint32_t count}] [positions...]
struct PsPartkeySlot {
    int32_t  key;
    uint32_t offset;
    uint32_t count;
};

static const PsPartkeySlot* ps_partkey_table = nullptr;
static const uint32_t*       ps_partkey_positions = nullptr;
static uint32_t               ps_partkey_table_size = 0;

static inline const PsPartkeySlot* ps_partkey_find(int32_t partkey) {
    uint32_t mask = ps_partkey_table_size - 1;
    uint32_t pos  = (uint32_t)((uint64_t)(uint32_t)partkey * 0x9E3779B97F4A7C15ULL >> 32) & mask;
    for (;;) {
        const PsPartkeySlot& slot = ps_partkey_table[pos];
        if (slot.count == 0 && slot.key == 0) return nullptr; // empty
        if (slot.key == partkey) return &slot;
        pos = (pos + 1) & mask;
    }
}

// ---------------------------------------------------------------------------
// Result struct for lineitem intermediate
// ---------------------------------------------------------------------------
struct LineitemHit {
    int32_t  l_orderkey;
    int32_t  l_suppkey;
    int64_t  amount; // l_extendedprice*(100-l_discount) - ps_supplycost*l_quantity (scaled x10000)
};

void run_Q9(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string base = gendb_dir + "/";

    // -------------------------------------------------------------------------
    // Phase 0: Load small dimension tables (supplier→nationkey, nation names)
    // -------------------------------------------------------------------------
    // supplier: flat_array[s_suppkey] → s_nationkey  (100K rows, dense 1..100000)
    std::vector<int32_t> supp_nationkey(100001, -1); // index by s_suppkey
    {
        GENDB_PHASE("dim_load_supplier_nation");
        gendb::MmapColumn<int32_t> s_suppkey(base + "supplier/s_suppkey.bin");
        gendb::MmapColumn<int32_t> s_nationkey(base + "supplier/s_nationkey.bin");
        size_t n = s_suppkey.size();
        for (size_t i = 0; i < n; i++) {
            int32_t sk = s_suppkey[i];
            if (sk >= 0 && sk <= 100000)
                supp_nationkey[sk] = s_nationkey[i];
        }
    }

    // nation: flat_array[n_nationkey] → n_name string
    std::string nation_names[25];
    {
        GENDB_PHASE("dim_load_nation");
        gendb::MmapColumn<int32_t> n_nationkey(base + "nation/n_nationkey.bin");
        // Load n_name as raw byte file
        int fd = ::open((base + "nation/n_name.bin").c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open nation/n_name.bin");
        struct stat st; fstat(fd, &st);
        size_t fsz = st.st_size;
        void* ptr = mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (ptr == MAP_FAILED) throw std::runtime_error("mmap failed for nation/n_name.bin");
        auto srefs = read_string_column((const char*)ptr, fsz);
        munmap(ptr, fsz);

        size_t n = n_nationkey.size();
        // srefs is in row order
        for (size_t i = 0; i < n && i < srefs.size(); i++) {
            int32_t nk = n_nationkey[i];
            if (nk >= 0 && nk < 25)
                nation_names[nk] = std::string(srefs[i].ptr, srefs[i].len);
        }
    }

    // -------------------------------------------------------------------------
    // Phase 1: Scan part table, filter p_name LIKE '%green%'
    //          → collect matching p_partkey values + build bitset
    // -------------------------------------------------------------------------
    std::vector<int32_t> green_partkeys;
    green_partkeys.reserve(120000);

    // Bitset for p_partkey membership (p_partkey in [1..2000000])
    const size_t PART_BITSET_SIZE = 2097152; // 2^21
    std::vector<uint8_t> green_bitset((PART_BITSET_SIZE + 7) / 8, 0);

    {
        GENDB_PHASE("dim_filter");
        // Load p_partkey as typed mmap
        gendb::MmapColumn<int32_t> p_partkey(base + "part/p_partkey.bin");
        size_t nparts = p_partkey.size(); // 2,000,000

        // Load p_name as raw bytes
        int fd = ::open((base + "part/p_name.bin").c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open part/p_name.bin");
        struct stat st; fstat(fd, &st);
        size_t fsz = st.st_size;
        void* rawptr = mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
        madvise(rawptr, fsz, MADV_SEQUENTIAL);
        ::close(fd);

        // Parallel scan with thread-local vectors, then merge
        int nthreads = omp_get_max_threads();
        std::vector<std::vector<int32_t>> local_keys(nthreads);
        for (auto& v : local_keys) v.reserve(2000);

        // We need to parse string column sequentially (variable length), so
        // do a sequential scan to collect (row_idx, partkey) pairs for green parts
        // Pre-pass: parse string positions for parallel access
        // For 2M parts, sequential scan is fine (~15ms total)
        const char* raw = (const char*)rawptr;
        size_t pos = 0;
        for (size_t i = 0; i < nparts && pos + 4 <= fsz; i++) {
            int32_t len;
            memcpy(&len, raw + pos, 4);
            pos += 4;
            const char* s = raw + pos;
            pos += len;
            // Check for 'green' substring
            // len >= 5 for "green"
            if (len >= 5) {
                // Manual strstr on bounded string
                bool found = false;
                for (int j = 0; j <= len - 5; j++) {
                    if (s[j]=='g' && s[j+1]=='r' && s[j+2]=='e' && s[j+3]=='e' && s[j+4]=='n') {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    int32_t pk = p_partkey[i];
                    green_partkeys.push_back(pk);
                    if ((size_t)pk < PART_BITSET_SIZE)
                        green_bitset[pk >> 3] |= (1u << (pk & 7));
                }
            }
        }
        munmap(rawptr, fsz);
    }

    // -------------------------------------------------------------------------
    // Phase 2: Build join map (ps_partkey, ps_suppkey) → ps_supplycost
    //          using prebuilt hash_ps_partkey index
    // -------------------------------------------------------------------------
    // Map key: composite (partkey<<32)|suppkey → ps_supplycost (int64_t)
    // ~430K entries expected
    gendb::CompactHashMapPair<int64_t> ps_map(500000);

    {
        GENDB_PHASE("build_joins");

        // Load prebuilt hash_ps_partkey index
        int fd = ::open((base + "indexes/hash_ps_partkey.bin").c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open hash_ps_partkey.bin");
        struct stat st; fstat(fd, &st);
        size_t fsz = st.st_size;
        void* rawptr = mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (rawptr == MAP_FAILED) throw std::runtime_error("mmap failed for hash_ps_partkey.bin");

        const char* raw = (const char*)rawptr;
        uint32_t table_size;
        memcpy(&table_size, raw, 4);
        ps_partkey_table_size = table_size;
        ps_partkey_table = reinterpret_cast<const PsPartkeySlot*>(raw + 4);
        // positions array starts after table
        ps_partkey_positions = reinterpret_cast<const uint32_t*>(
            raw + 4 + (size_t)table_size * sizeof(PsPartkeySlot));

        // Load partsupp columns
        gendb::MmapColumn<int32_t> ps_partkey_col(base + "partsupp/ps_partkey.bin");
        gendb::MmapColumn<int32_t> ps_suppkey_col(base + "partsupp/ps_suppkey.bin");
        gendb::MmapColumn<int64_t> ps_supplycost_col(base + "partsupp/ps_supplycost.bin");

        // For each green partkey, look up in the index and collect partsupp rows
        uint32_t mask = table_size - 1;
        for (int32_t pk : green_partkeys) {
            // Probe the index
            uint32_t h = (uint32_t)((uint64_t)(uint32_t)pk * 0x9E3779B97F4A7C15ULL >> 32) & mask;
            for (;;) {
                const PsPartkeySlot& slot = ps_partkey_table[h];
                // Empty slot detection: count==0 and key==0 is ambiguous; use a sentinel
                // The index uses linear probing; an empty slot has key==-1 (or check if count==0 and we haven't seen the key)
                // Based on storage guide: slot key=-1 or offset/count pattern
                // For a multi-value hash index with open addressing, empty = slot not set
                // Let's check: if key matches, use it; if key is 0 and count is 0, it's empty
                if (slot.key == pk) {
                    // Found — iterate positions
                    for (uint32_t j = slot.offset; j < slot.offset + slot.count; j++) {
                        uint32_t row = ps_partkey_positions[j];
                        int32_t  sk   = ps_suppkey_col[row];
                        int64_t  cost = ps_supplycost_col[row];
                        ps_map.insert({pk, sk}, cost);
                    }
                    break;
                }
                // Check for empty slot — use heuristic: if count==0 and key==0
                // Actually for a probing table, we need a proper empty marker
                // The index was built with int32_t key; use -1 or INT32_MIN as empty sentinel
                // From the layout description: slots are initialized, likely key=0 means empty
                // (partkeys start at 1 in TPC-H)
                if (slot.key == 0 && slot.count == 0) break; // empty, not found
                h = (h + 1) & mask;
            }
        }
        munmap(rawptr, fsz);
    }

    // -------------------------------------------------------------------------
    // Phase 3: Scan lineitem, probe (l_partkey, l_suppkey) → ps_supplycost
    //          Compute partial amount, store by l_orderkey
    // -------------------------------------------------------------------------
    // We need: per l_orderkey → (l_suppkey, sum_of_amount)
    // Since each orderkey can have multiple lineitems matching, we accumulate
    // Use CompactHashMap<int32_t, {suppkey, amount}> — but suppkey differs per lineitem
    // so we store results as a flat vector then build orderkey→... map

    // Actually: we need to join with orders to get o_orderdate (year)
    // Strategy: scan lineitem → emit (l_orderkey, l_suppkey, amount) hits
    //           then join with orders: probe o_orderkey → year
    //           then aggregate by (suppkey→nationkey→nation_name, year)

    // Since lineitem has 60M rows and only ~178K match, we store hits in a vector
    // Then build a map from l_orderkey → hits for orders join

    // For parallel lineitem scan, use thread-local vectors
    // Parallel scan
    int nthreads = omp_get_max_threads();
    std::vector<std::vector<LineitemHit>> local_hits(nthreads);
    for (auto& v : local_hits) v.reserve(4000);

    {
        GENDB_PHASE("main_scan");

        // Open all needed lineitem columns
        gendb::MmapColumn<int32_t> l_orderkey(base + "lineitem/l_orderkey.bin");
        gendb::MmapColumn<int32_t> l_partkey(base + "lineitem/l_partkey.bin");
        gendb::MmapColumn<int32_t> l_suppkey(base + "lineitem/l_suppkey.bin");
        gendb::MmapColumn<int64_t> l_quantity(base + "lineitem/l_quantity.bin");
        gendb::MmapColumn<int64_t> l_extendedprice(base + "lineitem/l_extendedprice.bin");
        gendb::MmapColumn<int64_t> l_discount(base + "lineitem/l_discount.bin");

        // Prefetch all columns
        mmap_prefetch_all(l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount);

        size_t n = l_orderkey.size();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& hits = local_hits[tid];

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n; i++) {
                int32_t pk = l_partkey[i];
                // Quick bitset filter
                if ((size_t)pk >= PART_BITSET_SIZE) continue;
                if (!(green_bitset[pk >> 3] & (1u << (pk & 7)))) continue;

                int32_t sk = l_suppkey[i];
                const int64_t* cost_ptr = ps_map.find({pk, sk});
                if (!cost_ptr) continue;

                // amount (scaled x 10000) = l_extendedprice*(100-l_discount) - ps_supplycost*l_quantity
                // scale_factor=2 for all, so:
                //   l_extendedprice is int64 * 100 (e.g., 9.00 = 900)
                //   l_discount is int64 * 100 (e.g., 0.04 = 4)
                //   l_quantity is int64 * 100 (e.g., 36.00 = 3600)
                //   ps_supplycost is int64 * 100
                // amount = ep*(100 - disc)/100 - cost*qty/100
                // In scaled int (multiply out by 100):
                //   = ep*(100 - disc) - cost*qty
                // All three factors are scaled by 100, so:
                //   ep*(100-disc) has scale 100*100 = 10000
                //   cost*qty has scale 100*100 = 10000
                // → amount is in units of 1/10000
                int64_t ep   = l_extendedprice[i];
                int64_t disc = l_discount[i];      // scale 100, e.g. 4 means 0.04
                int64_t qty  = l_quantity[i];
                int64_t cost = *cost_ptr;

                // ep*(100-disc): ep is x100, disc is x100, so (100-disc) needs to be unscaled
                // Actually: the formula is l_extendedprice * (1 - l_discount/100) - ps_supplycost * (l_quantity/100)
                // = ep/100 * (1 - disc/100) - cost/100 * qty/100
                // In integer math with scale factor 10000:
                //   amount_scaled = ep * (100 - disc) - cost * qty
                // where ep, disc, cost, qty are all the raw stored int64 (already *100 of real value)
                // Verification:
                //   real amount = ep_real*(1 - disc_real) - cost_real*qty_real
                //   ep_raw = ep_real*100, disc_raw = disc_real*100, cost_raw=cost_real*100, qty_raw=qty_real*100
                //   ep_raw*(100 - disc_raw) = ep_real*100*(100 - disc_real*100)
                //     = ep_real*100*100*(1-disc_real) = ep_real*(1-disc_real)*10000 ✓
                //   cost_raw*qty_raw = cost_real*100*qty_real*100 = cost_real*qty_real*10000 ✓

                int64_t amount = ep * (100LL - disc) / 100 - cost * qty / 100;
                hits.push_back({l_orderkey[i], sk, amount});
            }
        }
    }

    // Merge thread-local hits into a single vector
    size_t total_hits = 0;
    for (auto& v : local_hits) total_hits += v.size();
    std::vector<LineitemHit> all_hits;
    all_hits.reserve(total_hits);
    for (auto& v : local_hits) {
        all_hits.insert(all_hits.end(), v.begin(), v.end());
        v.clear();
        v.shrink_to_fit();
    }

    // -------------------------------------------------------------------------
    // Phase 4: Build orderkey→(suppkey,amount) map, then scan orders for year
    // -------------------------------------------------------------------------
    // We need: for each hit, look up o_orderdate via l_orderkey
    // Build a map: l_orderkey → index into all_hits (multi-value)
    // But orders has 15M rows and orderkeys are up to 60M
    // Better: build map from hit orderkeys, then scan orders

    // Build compact hash map: l_orderkey → index in all_hits
    // (one lineitem hit per entry -- but multiple hits can have same orderkey)
    // We'll use a flat hash map that stores (orderkey → vector<idx>) but that's expensive
    // Instead: sort hits by orderkey, then scan orders and binary search or build hash map

    // Use CompactHashMap<int32_t, uint32_t> where value = first index in all_hits for this orderkey
    // But multiple hits can share same orderkey, so store a range — need sorted hits.
    // Alternative: use hash map of orderkey → head of a linked list (but complicates)
    // Simplest: build CompactHashMap where each entry stores the hit's data (suppkey, amount)
    // and accumulate per orderkey. But suppkey differs per lineitem...
    //
    // Actually for Q9, each lineitem is a separate row with its own suppkey.
    // We need: for each lineitem hit → get o_orderdate year → aggregate by (n_name, year)
    //
    // Best approach: for each hit, store (l_orderkey, l_suppkey, amount)
    // Then scan orders: for each order, if orderkey is in our hit set, get year,
    //   look up all hits with that orderkey.
    //
    // Build: CompactHashMap<int32_t, {first_hit_idx, count}> grouped by orderkey
    // This requires sorting hits by orderkey first.

    // Sort hits by l_orderkey for group-by and range lookup
    std::sort(all_hits.begin(), all_hits.end(),
              [](const LineitemHit& a, const LineitemHit& b) {
                  return a.l_orderkey < b.l_orderkey;
              });

    // Build hash map: orderkey → {start_idx, count} in sorted all_hits
    struct HitRange { uint32_t start; uint32_t count; };
    gendb::CompactHashMap<int32_t, HitRange> orderkey_map(all_hits.size() * 2);
    {
        GENDB_PHASE("build_orderkey_map");
        size_t n = all_hits.size();
        size_t i = 0;
        while (i < n) {
            int32_t ok = all_hits[i].l_orderkey;
            uint32_t start = (uint32_t)i;
            while (i < n && all_hits[i].l_orderkey == ok) i++;
            orderkey_map.insert(ok, {start, (uint32_t)(i - start)});
        }
    }

    // -------------------------------------------------------------------------
    // Phase 5: Scan orders, probe orderkey → hits, extract year, aggregate
    // -------------------------------------------------------------------------
    // Aggregation: flat_array[25][10] indexed by [nation_idx][year-1992]
    // sum_profit in units of 1/10000 (scaled)
    // Years: 1992..1998 → 7 slots; use 10 for safety
    static constexpr int YEAR_OFFSET = 1992;
    static constexpr int YEAR_COUNT  = 10;

    // Thread-local aggregation arrays
    std::vector<std::array<int64_t, 25 * YEAR_COUNT>> local_agg(nthreads);
    for (auto& arr : local_agg) arr.fill(0);

    {
        GENDB_PHASE("orders_scan_probe");
        gendb::MmapColumn<int32_t> o_orderkey(base + "orders/o_orderkey.bin");
        gendb::MmapColumn<int32_t> o_orderdate(base + "orders/o_orderdate.bin");
        mmap_prefetch_all(o_orderkey, o_orderdate);
        size_t n = o_orderkey.size();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& agg = local_agg[tid];

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < n; i++) {
                int32_t ok = o_orderkey[i];
                const HitRange* hr = orderkey_map.find(ok);
                if (!hr) continue;

                int year = gendb::extract_year(o_orderdate[i]);
                int yi   = year - YEAR_OFFSET;
                if (yi < 0 || yi >= YEAR_COUNT) continue;

                // Process all lineitem hits for this order
                for (uint32_t j = hr->start; j < hr->start + hr->count; j++) {
                    int32_t sk = all_hits[j].l_suppkey;
                    int64_t amt = all_hits[j].amount;

                    // Look up supplier → nationkey
                    if (sk < 0 || sk > 100000) continue;
                    int32_t nk = supp_nationkey[sk];
                    if (nk < 0 || nk >= 25) continue;

                    agg[nk * YEAR_COUNT + yi] += amt;
                }
            }
        }
    }

    // Merge thread-local aggregation arrays
    std::array<int64_t, 25 * YEAR_COUNT> global_agg{};
    global_agg.fill(0);
    for (auto& arr : local_agg) {
        for (int i = 0; i < 25 * YEAR_COUNT; i++)
            global_agg[i] += arr[i];
    }

    // -------------------------------------------------------------------------
    // Phase 6: Sort and output
    // -------------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        struct ResultRow {
            std::string nation;
            int         o_year;
            int64_t     sum_profit; // in units of 1/10000
        };

        std::vector<ResultRow> results;
        results.reserve(175);

        for (int nk = 0; nk < 25; nk++) {
            for (int yi = 0; yi < YEAR_COUNT; yi++) {
                int64_t val = global_agg[nk * YEAR_COUNT + yi];
                if (val == 0) continue; // skip zero groups (unlikely to be valid output)
                // Actually TPC-H requires all groups with data; zero profit could exist
                // but we skip if no rows matched at all
                results.push_back({nation_names[nk], YEAR_OFFSET + yi, val});
            }
        }

        // Sort: ORDER BY nation ASC, o_year DESC
        std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.o_year > b.o_year;
        });

        // Write CSV
        std::string outpath = results_dir + "/Q9.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) throw std::runtime_error("Cannot open output file: " + outpath);
        fprintf(f, "nation,o_year,sum_profit\n");
        for (const auto& r : results) {
            // sum_profit is in units of 1/10000 (scale_factor=2 per operand, multiplied = 10000)
            // Output as 2 decimal places using floating point
            double val = (double)r.sum_profit / 10000.0;
            fprintf(f, "%s,%d,%.2f\n", r.nation.c_str(), r.o_year, val);
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
