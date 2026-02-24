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

        // Keep l_orderkey mapped — needed for Phase 3 lineitem re-scan (page-cached)

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
    // Phase 2: Load orders + customer columns with MAP_POPULATE.
    //          4 orders files prefetched concurrently by the OS → fast HDD streaming.
    //          Avoids cold random page faults from probing large index files on HDD.
    // ========================================================================
    int fd_o_okey; size_t sz_o_okey;
    const int32_t* o_orderkey_col = reinterpret_cast<const int32_t*>(
        mmap_file(ord_dir + "o_orderkey.bin", sz_o_okey, fd_o_okey, /*populate=*/true));
    posix_fadvise(fd_o_okey, 0, sz_o_okey, POSIX_FADV_SEQUENTIAL);

    int fd_o_ck; size_t sz_o_ck;
    const int32_t* o_custkey_col = reinterpret_cast<const int32_t*>(
        mmap_file(ord_dir + "o_custkey.bin", sz_o_ck, fd_o_ck, /*populate=*/true));
    posix_fadvise(fd_o_ck, 0, sz_o_ck, POSIX_FADV_SEQUENTIAL);

    int fd_o_od; size_t sz_o_od;
    const int32_t* o_orderdate_col = reinterpret_cast<const int32_t*>(
        mmap_file(ord_dir + "o_orderdate.bin", sz_o_od, fd_o_od, /*populate=*/true));
    posix_fadvise(fd_o_od, 0, sz_o_od, POSIX_FADV_SEQUENTIAL);

    int fd_o_tp; size_t sz_o_tp;
    const double* o_totalprice_col = reinterpret_cast<const double*>(
        mmap_file(ord_dir + "o_totalprice.bin", sz_o_tp, fd_o_tp, /*populate=*/true));
    posix_fadvise(fd_o_tp, 0, sz_o_tp, POSIX_FADV_SEQUENTIAL);

    const size_t n_orders = sz_o_okey / sizeof(int32_t);

    int fd_c_name; size_t sz_c_name;
    const char* c_name_data = reinterpret_cast<const char*>(
        mmap_file(cust_dir + "c_name.bin", sz_c_name, fd_c_name, /*populate=*/true));
    posix_fadvise(fd_c_name, 0, sz_c_name, POSIX_FADV_SEQUENTIAL);

    int fd_c_custkey; size_t sz_c_custkey;
    const int32_t* c_custkey_col = reinterpret_cast<const int32_t*>(
        mmap_file(cust_dir + "c_custkey.bin", sz_c_custkey, fd_c_custkey, /*populate=*/true));
    posix_fadvise(fd_c_custkey, 0, sz_c_custkey, POSIX_FADV_SEQUENTIAL);

    const size_t n_cust = sz_c_custkey / sizeof(int32_t);

    // ========================================================================
    // Phase 3: Main scan — sequential scans replace cold random index probes.
    //   3a: Parallel scan of orders (15M rows) to find ~700 qualifying rows.
    //   3b: Sequential scan of customer (1.5M rows) to get c_name.
    //   3c: Parallel re-scan of lineitem (60M rows, page-cached from Phase 1)
    //       to compute SUM(l_quantity) per qualifying orderkey.
    // ========================================================================
    std::vector<ResultRow> results;

    {
        GENDB_PHASE("main_scan");

        const size_t n_qual = qualifying_orderkeys.size();

        // Build qualifying orderkey hash map: key → result index.
        // Sentinel = 0 (TPC-H orderkeys are >= 1).
        uint32_t qmap_cap = 4096;
        while (qmap_cap < n_qual * 4) qmap_cap <<= 1;
        const uint32_t qmap_mask = qmap_cap - 1;
        std::vector<int32_t>  qmap_keys(qmap_cap, 0);
        std::vector<uint32_t> qmap_ridx(qmap_cap, 0);

        for (uint32_t i = 0; i < (uint32_t)n_qual; i++) {
            int32_t k = qualifying_orderkeys[i];
            uint32_t s = (uint32_t(k) * 2654435761u) & qmap_mask;
            for (uint32_t p = 0; p < qmap_cap; ++p) {
                if (qmap_keys[s] == 0) break;
                s = (s + 1) & qmap_mask;
                if (p + 1 == qmap_cap) { std::cerr << "qmap build exhausted\n"; std::abort(); }
            }
            qmap_keys[s] = k;
            qmap_ridx[s] = i;
        }

        results.resize(n_qual);
        std::vector<int32_t> order_custkeys(n_qual, 0);
        for (size_t i = 0; i < n_qual; i++) results[i].sum_qty = 0.0;

        // --- 3a: Parallel sequential scan of orders ---
        // o_orderkey is PK → at most one match per qualifying key → no write races.
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_orders; i++) {
            int32_t ok = o_orderkey_col[i];
            uint32_t s = (uint32_t(ok) * 2654435761u) & qmap_mask;
            for (uint32_t p = 0; p < qmap_cap; ++p) {
                if (qmap_keys[s] == 0 || qmap_keys[s] == ok) break;
                s = (s + 1) & qmap_mask;
                if (p + 1 == qmap_cap) { std::cerr << "qmap probe (3a) exhausted\n"; std::abort(); }
            }
            if (qmap_keys[s] == ok) {
                uint32_t idx = qmap_ridx[s];
                order_custkeys[idx]       = o_custkey_col[i];
                results[idx].o_orderkey   = ok;
                results[idx].o_orderdate  = o_orderdate_col[i];
                results[idx].o_totalprice = o_totalprice_col[i];
            }
        }

        munmap(const_cast<int32_t*>(o_orderkey_col),  sz_o_okey); close(fd_o_okey);
        munmap(const_cast<int32_t*>(o_custkey_col),   sz_o_ck);   close(fd_o_ck);
        munmap(const_cast<int32_t*>(o_orderdate_col), sz_o_od);   close(fd_o_od);
        munmap(const_cast<double*>(o_totalprice_col), sz_o_tp);   close(fd_o_tp);

        // --- 3b: Build custkey lookup map and scan customer sequentially ---
        uint32_t cmap_cap = 4096;
        while (cmap_cap < n_qual * 4) cmap_cap <<= 1;
        const uint32_t cmap_mask = cmap_cap - 1;
        std::vector<int32_t> cmap_keys(cmap_cap, 0);  // 0 = empty (TPC-H custkeys >= 1)
        std::vector<char>    cmap_names(cmap_cap * 26, '\0');
        std::vector<int32_t> cmap_ckeys(cmap_cap, 0); // 0 = not yet stored

        // Register which custkeys are needed
        for (uint32_t i = 0; i < (uint32_t)n_qual; i++) {
            int32_t ck = order_custkeys[i];
            if (ck == 0) continue;
            uint32_t s = (uint32_t(ck) * 2654435761u) & cmap_mask;
            for (uint32_t p = 0; p < cmap_cap; ++p) {
                if (cmap_keys[s] == 0 || cmap_keys[s] == ck) break;
                s = (s + 1) & cmap_mask;
                if (p + 1 == cmap_cap) { std::cerr << "cmap build exhausted\n"; std::abort(); }
            }
            cmap_keys[s] = ck;
        }

        // Sequential scan of customer: find c_name for each needed custkey
        for (size_t i = 0; i < n_cust; i++) {
            int32_t ck = c_custkey_col[i];
            uint32_t s = (uint32_t(ck) * 2654435761u) & cmap_mask;
            for (uint32_t p = 0; p < cmap_cap; ++p) {
                if (cmap_keys[s] == 0 || cmap_keys[s] == ck) break;
                s = (s + 1) & cmap_mask;
                if (p + 1 == cmap_cap) { std::cerr << "cmap probe (3b scan) exhausted\n"; std::abort(); }
            }
            if (cmap_keys[s] == ck && cmap_ckeys[s] == 0) {
                cmap_ckeys[s] = ck;
                const char* nm = c_name_data + i * 25;
                memcpy(&cmap_names[s * 26], nm, 25);
                cmap_names[s * 26 + 25] = '\0';
                int nlen = 25;
                while (nlen > 0 && cmap_names[s * 26 + nlen - 1] == '\0') --nlen;
                cmap_names[s * 26 + nlen] = '\0';
            }
        }

        munmap(const_cast<char*>(c_name_data),      sz_c_name);    close(fd_c_name);
        munmap(const_cast<int32_t*>(c_custkey_col), sz_c_custkey); close(fd_c_custkey);

        // Apply customer info to each result row
        for (uint32_t i = 0; i < (uint32_t)n_qual; i++) {
            int32_t ck = order_custkeys[i];
            uint32_t s = (uint32_t(ck) * 2654435761u) & cmap_mask;
            for (uint32_t p = 0; p < cmap_cap; ++p) {
                if (cmap_keys[s] == 0 || cmap_keys[s] == ck) break;
                s = (s + 1) & cmap_mask;
                if (p + 1 == cmap_cap) { std::cerr << "cmap probe (3b apply) exhausted\n"; std::abort(); }
            }
            if (cmap_keys[s] == ck) {
                memcpy(results[i].c_name, &cmap_names[s * 26], 26);
                results[i].c_custkey = cmap_ckeys[s];
            }
        }

        // --- 3c: MV index point-lookups for outer SUM(l_quantity) ---
        // Use lineitem_orderkey_hash (MV) index: ~600 qualifying keys × avg 4 items
        // → ~2400 row accesses vs 60M-row full rescan. (P11/P8 fix)
        {
            GENDB_PHASE("phase3c_mv_lookup");

            // Load MV hash index
            int fd_mv_hash; size_t sz_mv_hash;
            const MvEntry* mv_hash = reinterpret_cast<const MvEntry*>(
                mmap_file(idx_dir + "lineitem_orderkey_hash.bin", sz_mv_hash, fd_mv_hash));
            const uint32_t mv_cap  = static_cast<uint32_t>(sz_mv_hash / sizeof(MvEntry));
            const uint32_t mv_mask = mv_cap - 1;

            // Load MV positions array (row indices into l_quantity)
            int fd_mv_pos; size_t sz_mv_pos;
            const uint32_t* mv_positions = reinterpret_cast<const uint32_t*>(
                mmap_file(idx_dir + "lineitem_orderkey_positions.bin", sz_mv_pos, fd_mv_pos));

            // For each qualifying orderkey, probe MV index → sum l_quantity via positions
            #pragma omp parallel for schedule(dynamic, 32)
            for (size_t i = 0; i < n_qual; i++) {
                int32_t ok = results[i].o_orderkey;
                uint32_t slot = mv_probe(mv_hash, mv_cap, mv_mask, ok);
                if (mv_hash[slot].key == ok) {
                    uint32_t off = mv_hash[slot].offset;
                    uint32_t cnt = mv_hash[slot].count;
                    double s = 0.0;
                    for (uint32_t j = 0; j < cnt; ++j)
                        s += l_quantity[mv_positions[off + j]];
                    results[i].sum_qty = s;
                }
            }

            munmap(const_cast<MvEntry*>(mv_hash),         sz_mv_hash); close(fd_mv_hash);
            munmap(const_cast<uint32_t*>(mv_positions),   sz_mv_pos);  close(fd_mv_pos);
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
    munmap(const_cast<int32_t*>(l_orderkey), sz_lok); close(fd_lok);
    munmap(const_cast<double*>(l_quantity),  sz_lq);  close(fd_lq);
    // orders and customer columns are munmapped within Phase 3
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
