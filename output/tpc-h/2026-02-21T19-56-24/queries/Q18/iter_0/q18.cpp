// Q18: Large Volume Customer
// Strategy:
//   1. Parallel scan lineitem (l_orderkey + l_quantity) with thread-local hash maps
//      → HAVING SUM(l_quantity) > 300 → ~700 qualifying orderkeys
//   2. For each qualifying orderkey:
//      - Probe orders_orderkey_hash (SV) → o_custkey, o_orderdate, o_totalprice
//      - Probe customer_custkey_hash (SV) → c_name, c_custkey
//      - Probe lineitem_orderkey_hash (MV) → sum l_quantity for outer query
//   3. Sort by o_totalprice DESC, o_orderdate ASC; output top 100

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <omp.h>
#include "date_utils.h"
#include "timing_utils.h"

// ---- Index entry structs ----
struct SvEntry {
    int32_t  key;
    uint32_t row_idx;
};

struct MvEntry {
    int32_t  key;
    uint32_t offset;
    uint32_t count;
};

// ---- Hash probe helpers ----
// C24 fix: bounded for-loops capped at capacity to prevent infinite loops on full tables
static inline uint32_t sv_probe(const SvEntry* entries, uint32_t cap, uint32_t mask, int32_t key) {
    uint32_t slot = (static_cast<uint64_t>(static_cast<uint32_t>(key)) * 2654435761ULL >> 32) & mask;
    for (uint32_t p = 0; p < cap; ++p) {
        if (entries[slot].key == INT32_MIN || entries[slot].key == key) break;
        slot = (slot + 1) & mask;
    }
    return slot;
}

static inline uint32_t mv_probe(const MvEntry* entries, uint32_t cap, uint32_t mask, int32_t key) {
    uint32_t slot = (static_cast<uint64_t>(static_cast<uint32_t>(key)) * 2654435761ULL >> 32) & mask;
    for (uint32_t p = 0; p < cap; ++p) {
        if (entries[slot].key == INT32_MIN || entries[slot].key == key) break;
        slot = (slot + 1) & mask;
    }
    return slot;
}

// ---- mmap helper ----
static const void* mmap_file(const std::string& path, size_t& out_size, int& out_fd,
                              bool populate = false) {
    out_fd = open(path.c_str(), O_RDONLY);
    if (out_fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(out_fd, &st);
    out_size = static_cast<size_t>(st.st_size);
    int flags = MAP_PRIVATE | (populate ? MAP_POPULATE : 0);
    void* ptr = mmap(nullptr, out_size, PROT_READ, flags, out_fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    return ptr;
}

// ---- Result row ----
struct ResultRow {
    char     c_name[26];   // 25 chars + null
    int32_t  c_custkey;
    int32_t  o_orderkey;
    int32_t  o_orderdate;
    double   o_totalprice;
    double   sum_qty;
};

void run_Q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const std::string li_dir   = gendb_dir + "/lineitem/";
    const std::string ord_dir  = gendb_dir + "/orders/";
    const std::string cust_dir = gendb_dir + "/customer/";
    const std::string idx_dir  = gendb_dir + "/indexes/";

    // ========================================================================
    // Phase 0: mmap lineitem columns for subquery (populate for sequential scan)
    // ========================================================================
    const int32_t* l_orderkey = nullptr;
    const double*  l_quantity = nullptr;
    size_t l_nrows = 0;
    int fd_lok = -1, fd_lq = -1;
    size_t sz_lok = 0, sz_lq = 0;

    {
        GENDB_PHASE("data_loading");

        l_orderkey = reinterpret_cast<const int32_t*>(
            mmap_file(li_dir + "l_orderkey.bin", sz_lok, fd_lok, /*populate=*/true));
        posix_fadvise(fd_lok, 0, sz_lok, POSIX_FADV_SEQUENTIAL);
        l_nrows = sz_lok / sizeof(int32_t);

        l_quantity = reinterpret_cast<const double*>(
            mmap_file(li_dir + "l_quantity.bin", sz_lq, fd_lq, /*populate=*/true));
        posix_fadvise(fd_lq, 0, sz_lq, POSIX_FADV_SEQUENTIAL);
    }

    // ========================================================================
    // Phase 1: Subquery — parallel SUM(l_quantity) per l_orderkey
    //          HAVING SUM > 300 → qualifying_orderkeys
    // P1 fix: single shared custom open-addressing hash table with atomic ops.
    //   key sentinel = 0 (valid: TPC-H orderkeys are >= 1).
    //   l_quantity is integer-valued in TPC-H, so accumulate as int64_t for
    //   lock-free atomic fetch_add; compare threshold as int64 (300 * 100 if
    //   scaled, or 300 directly since quantities are whole numbers).
    // ========================================================================
    std::vector<int32_t> qualifying_orderkeys;

    {
        GENDB_PHASE("subquery_precompute");

        // Size table: next power-of-2 >= l_nrows/2 gives ~50% load for ~l_nrows/4 unique keys
        uint32_t subq_cap = 1u << 20; // minimum 1M slots
        {
            size_t target = l_nrows / 2;
            while (static_cast<size_t>(subq_cap) < target) subq_cap <<= 1;
        }
        uint32_t subq_mask = subq_cap - 1;

        // calloc gives zero-initialized memory (key=0 = sentinel, val=0)
        int32_t* subq_keys = static_cast<int32_t*>(calloc(subq_cap, sizeof(int32_t)));
        int64_t* subq_vals = static_cast<int64_t*>(calloc(subq_cap, sizeof(int64_t)));
        if (!subq_keys || !subq_vals) { perror("calloc subq_table"); exit(1); }

        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < l_nrows; i++) {
            const int32_t key = l_orderkey[i];
            // l_quantity is integer-valued (1–50) in TPC-H stored as double
            const int64_t qty = static_cast<int64_t>(l_quantity[i]);

            uint32_t slot = (static_cast<uint32_t>(key) * 2654435761u) & subq_mask;
            for (uint32_t p = 0; p < subq_cap; ++p) {
                int32_t cur = __atomic_load_n(&subq_keys[slot], __ATOMIC_RELAXED);
                if (cur == key) {
                    __atomic_fetch_add(&subq_vals[slot], qty, __ATOMIC_RELAXED);
                    break;
                } else if (cur == 0) {
                    int32_t expected = 0;
                    if (__atomic_compare_exchange_n(&subq_keys[slot], &expected, key,
                                                   false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                        __atomic_fetch_add(&subq_vals[slot], qty, __ATOMIC_RELAXED);
                        break;
                    }
                    // CAS failed: re-read and check if same key won
                    cur = __atomic_load_n(&subq_keys[slot], __ATOMIC_RELAXED);
                    if (cur == key) {
                        __atomic_fetch_add(&subq_vals[slot], qty, __ATOMIC_RELAXED);
                        break;
                    }
                    // Different key inserted here — linear probe without advancing
                    // (fall through to slot increment below)
                }
                slot = (slot + 1) & subq_mask;
                if (p + 1 == subq_cap) {
                    // Table exhausted — should never happen at <50% load
                    std::abort();
                }
            }
        }

        // Unmap l_orderkey — not needed for outer query
        munmap(const_cast<int32_t*>(l_orderkey), sz_lok);
        close(fd_lok);
        fd_lok = -1;

        // Collect qualifying keys (HAVING SUM(l_quantity) > 300)
        qualifying_orderkeys.reserve(1024);
        for (uint32_t s = 0; s < subq_cap; ++s) {
            if (subq_keys[s] != 0 && subq_vals[s] > 300) {
                qualifying_orderkeys.push_back(subq_keys[s]);
            }
        }

        free(subq_keys);
        free(subq_vals);
    }

    // ========================================================================
    // Phase 2: Load indexes and order/customer columns (no MAP_POPULATE —
    //          random access on ~700 keys)
    // ========================================================================

    // orders_orderkey_hash (SV): [num_rows][capacity][SvEntry x capacity]
    int fd_ord_idx; size_t sz_ord_idx;
    const uint32_t* ord_raw = reinterpret_cast<const uint32_t*>(
        mmap_file(idx_dir + "orders_orderkey_hash.bin", sz_ord_idx, fd_ord_idx));
    uint32_t ord_cap  = ord_raw[1];
    uint32_t ord_mask = ord_cap - 1;
    const SvEntry* ord_entries = reinterpret_cast<const SvEntry*>(ord_raw + 2);

    // customer_custkey_hash (SV): [num_rows][capacity][SvEntry x capacity]
    int fd_cust_idx; size_t sz_cust_idx;
    const uint32_t* cust_raw = reinterpret_cast<const uint32_t*>(
        mmap_file(idx_dir + "customer_custkey_hash.bin", sz_cust_idx, fd_cust_idx));
    uint32_t cust_cap  = cust_raw[1];
    uint32_t cust_mask = cust_cap - 1;
    const SvEntry* cust_entries = reinterpret_cast<const SvEntry*>(cust_raw + 2);

    // lineitem_orderkey_hash (MV): [num_positions][num_unique][capacity]
    //                              [uint32_t positions[num_positions]]
    //                              [MvEntry[capacity]]
    int fd_li_idx; size_t sz_li_idx;
    const uint32_t* li_raw = reinterpret_cast<const uint32_t*>(
        mmap_file(idx_dir + "lineitem_orderkey_hash.bin", sz_li_idx, fd_li_idx));
    uint32_t li_num_pos = li_raw[0];
    // li_raw[1] = num_unique, li_raw[2] = capacity
    uint32_t li_cap     = li_raw[2];
    uint32_t li_mask    = li_cap - 1;
    const uint32_t* li_positions = li_raw + 3;
    const MvEntry*  li_entries   = reinterpret_cast<const MvEntry*>(li_positions + li_num_pos);

    // orders payload columns
    int fd_o_custkey; size_t sz_o_custkey;
    const int32_t* o_custkey = reinterpret_cast<const int32_t*>(
        mmap_file(ord_dir + "o_custkey.bin", sz_o_custkey, fd_o_custkey));

    int fd_o_orderdate; size_t sz_o_orderdate;
    const int32_t* o_orderdate = reinterpret_cast<const int32_t*>(
        mmap_file(ord_dir + "o_orderdate.bin", sz_o_orderdate, fd_o_orderdate));

    int fd_o_totalprice; size_t sz_o_totalprice;
    const double* o_totalprice = reinterpret_cast<const double*>(
        mmap_file(ord_dir + "o_totalprice.bin", sz_o_totalprice, fd_o_totalprice));

    // customer payload columns
    int fd_c_name; size_t sz_c_name;
    const char* c_name_data = reinterpret_cast<const char*>(
        mmap_file(cust_dir + "c_name.bin", sz_c_name, fd_c_name));

    int fd_c_custkey; size_t sz_c_custkey;
    const int32_t* c_custkey_col = reinterpret_cast<const int32_t*>(
        mmap_file(cust_dir + "c_custkey.bin", sz_c_custkey, fd_c_custkey));

    // ========================================================================
    // Phase 3: Main scan — process qualifying orderkeys via index point lookups
    // ========================================================================
    std::vector<ResultRow> results;
    results.reserve(qualifying_orderkeys.size());

    {
        GENDB_PHASE("main_scan");

        for (int32_t okey : qualifying_orderkeys) {
            // --- orders lookup ---
            uint32_t oslot = sv_probe(ord_entries, ord_cap, ord_mask, okey);
            if (ord_entries[oslot].key != okey) continue; // shouldn't happen
            uint32_t ord_row = ord_entries[oslot].row_idx;

            int32_t custkey  = o_custkey[ord_row];
            int32_t odate    = o_orderdate[ord_row];
            double  oprice   = o_totalprice[ord_row];

            // --- customer lookup ---
            uint32_t cslot = sv_probe(cust_entries, cust_cap, cust_mask, custkey);
            if (cust_entries[cslot].key != custkey) continue;
            uint32_t cust_row = cust_entries[cslot].row_idx;

            // --- lineitem MV lookup → SUM(l_quantity) for outer query ---
            uint32_t lslot = mv_probe(li_entries, li_cap, li_mask, okey);
            if (li_entries[lslot].key != okey) continue;
            uint32_t li_off = li_entries[lslot].offset;
            uint32_t li_cnt = li_entries[lslot].count;

            double sum_qty = 0.0;
            for (uint32_t j = 0; j < li_cnt; j++) {
                sum_qty += l_quantity[li_positions[li_off + j]];
            }

            // --- build result row ---
            ResultRow row;
            const char* name_src = c_name_data + cust_row * 25;
            memcpy(row.c_name, name_src, 25);
            row.c_name[25] = '\0';
            // Trim trailing null padding
            int nlen = 25;
            while (nlen > 0 && row.c_name[nlen - 1] == '\0') --nlen;
            row.c_name[nlen] = '\0';

            row.c_custkey   = c_custkey_col[cust_row];
            row.o_orderkey  = okey;
            row.o_orderdate = odate;
            row.o_totalprice = oprice;
            row.sum_qty     = sum_qty;

            results.push_back(row);
        }
    }

    // ========================================================================
    // Phase 4: Sort top-100 and write CSV output
    // ========================================================================
    {
        // P6 fix: partial_sort is O(n log k) instead of O(n log n)
        GENDB_PHASE("sort_topk");
        auto cmp = [](const ResultRow& a, const ResultRow& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        };
        size_t topk = std::min(results.size(), size_t(100));
        std::partial_sort(results.begin(), results.begin() + topk, results.end(), cmp);
        results.resize(topk);
    }

    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q18.csv";
        std::ofstream out(out_path);
        if (!out) { std::cerr << "Cannot open " << out_path << "\n"; exit(1); }

        out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";

        char date_buf[16];
        for (const auto& r : results) {
            gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
            out << r.c_name << ","
                << r.c_custkey << ","
                << r.o_orderkey << ","
                << date_buf << ","
                << std::fixed << std::setprecision(2) << r.o_totalprice << ","
                << std::fixed << std::setprecision(2) << r.sum_qty << "\n";
        }
    }

    // ---- Cleanup ----
    munmap(const_cast<double*>(l_quantity), sz_lq);
    close(fd_lq);

    munmap(const_cast<uint32_t*>(ord_raw),  sz_ord_idx);  close(fd_ord_idx);
    munmap(const_cast<uint32_t*>(cust_raw), sz_cust_idx); close(fd_cust_idx);
    munmap(const_cast<uint32_t*>(li_raw),   sz_li_idx);   close(fd_li_idx);

    munmap(const_cast<int32_t*>(o_custkey),    sz_o_custkey);    close(fd_o_custkey);
    munmap(const_cast<int32_t*>(o_orderdate),  sz_o_orderdate);  close(fd_o_orderdate);
    munmap(const_cast<double*>(o_totalprice),  sz_o_totalprice); close(fd_o_totalprice);
    munmap(const_cast<char*>(c_name_data),     sz_c_name);       close(fd_c_name);
    munmap(const_cast<int32_t*>(c_custkey_col),sz_c_custkey);    close(fd_c_custkey);
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q18(gendb_dir, results_dir);
    return 0;
}
#endif
