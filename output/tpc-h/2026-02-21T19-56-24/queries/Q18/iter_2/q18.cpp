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
#include <thread>
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

    // ---- Declare all file handles and pointers up front ----
    // Lineitem columns (for subquery sequential scan)
    const int32_t* l_orderkey  = nullptr;
    const double*  l_quantity  = nullptr;
    size_t l_nrows = 0;
    int fd_lok = -1, fd_lq = -1;
    size_t sz_lok = 0, sz_lq = 0;

    // Orders orderkey hash index (SV)
    int fd_ord_idx = -1;       size_t sz_ord_idx = 0;
    const uint32_t* ord_raw    = nullptr;

    // Customer custkey hash index (SV)
    int fd_cust_idx = -1;      size_t sz_cust_idx = 0;
    const uint32_t* cust_raw   = nullptr;

    // Orders payload columns
    int fd_o_custkey = -1;     size_t sz_o_custkey = 0;
    const int32_t* o_custkey   = nullptr;
    int fd_o_orderdate = -1;   size_t sz_o_orderdate = 0;
    const int32_t* o_orderdate = nullptr;
    int fd_o_totalprice = -1;  size_t sz_o_totalprice = 0;
    const double*  o_totalprice = nullptr;

    // Customer payload columns
    int fd_c_name = -1;        size_t sz_c_name = 0;
    const char*    c_name_data  = nullptr;
    int fd_c_custkey = -1;     size_t sz_c_custkey = 0;
    const int32_t* c_custkey_col = nullptr;

    // ========================================================================
    // Phase 0: data_loading — MAP_POPULATE all needed files in parallel.
    //   Background thread loads indexes + orders/customer columns (621 MB).
    //   Main thread loads lineitem l_orderkey + l_quantity (720 MB sequential).
    //   Overlap I/O so secondary files are in memory before main_scan.
    // ========================================================================
    {
        GENDB_PHASE("data_loading");

        // Background thread: MAP_POPULATE secondary files (indexes + payload cols)
        std::thread bg_load([&]() {
            struct stat st;

            // orders_orderkey_hash (SV, ~257 MB): MAP_POPULATE for random access
            fd_ord_idx = open((idx_dir + "orders_orderkey_hash.bin").c_str(), O_RDONLY);
            if (fd_ord_idx < 0) { perror("orders_orderkey_hash"); exit(1); }
            fstat(fd_ord_idx, &st); sz_ord_idx = static_cast<size_t>(st.st_size);
            ord_raw = reinterpret_cast<const uint32_t*>(
                mmap(nullptr, sz_ord_idx, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_ord_idx, 0));
            if (ord_raw == MAP_FAILED) { perror("mmap ord_idx"); exit(1); }

            // customer_custkey_hash (SV, ~33 MB): MAP_POPULATE for random access
            fd_cust_idx = open((idx_dir + "customer_custkey_hash.bin").c_str(), O_RDONLY);
            if (fd_cust_idx < 0) { perror("customer_custkey_hash"); exit(1); }
            fstat(fd_cust_idx, &st); sz_cust_idx = static_cast<size_t>(st.st_size);
            cust_raw = reinterpret_cast<const uint32_t*>(
                mmap(nullptr, sz_cust_idx, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_cust_idx, 0));
            if (cust_raw == MAP_FAILED) { perror("mmap cust_idx"); exit(1); }

            // o_custkey (~58 MB)
            fd_o_custkey = open((ord_dir + "o_custkey.bin").c_str(), O_RDONLY);
            if (fd_o_custkey < 0) { perror("o_custkey"); exit(1); }
            fstat(fd_o_custkey, &st); sz_o_custkey = static_cast<size_t>(st.st_size);
            o_custkey = reinterpret_cast<const int32_t*>(
                mmap(nullptr, sz_o_custkey, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_o_custkey, 0));
            if (o_custkey == MAP_FAILED) { perror("mmap o_custkey"); exit(1); }

            // o_orderdate (~58 MB)
            fd_o_orderdate = open((ord_dir + "o_orderdate.bin").c_str(), O_RDONLY);
            if (fd_o_orderdate < 0) { perror("o_orderdate"); exit(1); }
            fstat(fd_o_orderdate, &st); sz_o_orderdate = static_cast<size_t>(st.st_size);
            o_orderdate = reinterpret_cast<const int32_t*>(
                mmap(nullptr, sz_o_orderdate, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_o_orderdate, 0));
            if (o_orderdate == MAP_FAILED) { perror("mmap o_orderdate"); exit(1); }

            // o_totalprice (~115 MB)
            fd_o_totalprice = open((ord_dir + "o_totalprice.bin").c_str(), O_RDONLY);
            if (fd_o_totalprice < 0) { perror("o_totalprice"); exit(1); }
            fstat(fd_o_totalprice, &st); sz_o_totalprice = static_cast<size_t>(st.st_size);
            o_totalprice = reinterpret_cast<const double*>(
                mmap(nullptr, sz_o_totalprice, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_o_totalprice, 0));
            if (o_totalprice == MAP_FAILED) { perror("mmap o_totalprice"); exit(1); }

            // c_name (~36 MB)
            fd_c_name = open((cust_dir + "c_name.bin").c_str(), O_RDONLY);
            if (fd_c_name < 0) { perror("c_name"); exit(1); }
            fstat(fd_c_name, &st); sz_c_name = static_cast<size_t>(st.st_size);
            c_name_data = reinterpret_cast<const char*>(
                mmap(nullptr, sz_c_name, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_c_name, 0));
            if (c_name_data == MAP_FAILED) { perror("mmap c_name"); exit(1); }

            // c_custkey (~6 MB)
            fd_c_custkey = open((cust_dir + "c_custkey.bin").c_str(), O_RDONLY);
            if (fd_c_custkey < 0) { perror("c_custkey"); exit(1); }
            fstat(fd_c_custkey, &st); sz_c_custkey = static_cast<size_t>(st.st_size);
            c_custkey_col = reinterpret_cast<const int32_t*>(
                mmap(nullptr, sz_c_custkey, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_c_custkey, 0));
            if (c_custkey_col == MAP_FAILED) { perror("mmap c_custkey"); exit(1); }
        });

        // Main thread: lineitem columns (sequential MAP_POPULATE, 720 MB)
        l_orderkey = reinterpret_cast<const int32_t*>(
            mmap_file(li_dir + "l_orderkey.bin", sz_lok, fd_lok, /*populate=*/true));
        posix_fadvise(fd_lok, 0, sz_lok, POSIX_FADV_SEQUENTIAL);
        l_nrows = sz_lok / sizeof(int32_t);

        l_quantity = reinterpret_cast<const double*>(
            mmap_file(li_dir + "l_quantity.bin", sz_lq, fd_lq, /*populate=*/true));
        posix_fadvise(fd_lq, 0, sz_lq, POSIX_FADV_SEQUENTIAL);

        bg_load.join(); // wait for all secondary files to be in memory
    }

    // Extract index layouts (after bg_load has populated pointers)
    uint32_t ord_cap   = ord_raw[1];
    uint32_t ord_mask  = ord_cap - 1;
    const SvEntry* ord_entries  = reinterpret_cast<const SvEntry*>(ord_raw  + 2);

    uint32_t cust_cap  = cust_raw[1];
    uint32_t cust_mask = cust_cap - 1;
    const SvEntry* cust_entries = reinterpret_cast<const SvEntry*>(cust_raw + 2);

    // ========================================================================
    // Phase 1: Subquery — parallel SUM(l_quantity) per l_orderkey
    //          HAVING SUM > 300 → qualifying rows with pre-computed sum_qty.
    //
    //  Key insight: subq_vals[s] IS the SUM(l_quantity) for that orderkey.
    //  The outer query needs the same SUM per orderkey group → reuse directly.
    //  No lineitem MV index needed for the outer query.
    // ========================================================================

    // Store qualifying key + its sum_qty together
    struct QualRow { int32_t key; double sum_qty; };
    std::vector<QualRow> qualifying_rows;

    {
        GENDB_PHASE("subquery_precompute");

        // Size table: next power-of-2 >= l_nrows/2 gives ~50% load for ~15M unique keys
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
                    // Different key inserted — fall through to probe next slot
                }
                slot = (slot + 1) & subq_mask;
                if (p + 1 == subq_cap) { std::abort(); } // table full (impossible at <50% load)
            }
        }

        // Unmap l_orderkey — no longer needed
        munmap(const_cast<int32_t*>(l_orderkey), sz_lok);
        close(fd_lok);
        fd_lok = -1;

        // Collect qualifying keys WITH their sum_qty (from the same hash table).
        // subq_vals[s] = SUM(l_quantity) for that orderkey — identical to outer query's
        // SUM(l_quantity) grouped by o_orderkey. No lineitem MV index scan needed.
        qualifying_rows.reserve(1024);
        for (uint32_t s = 0; s < subq_cap; ++s) {
            if (subq_keys[s] != 0 && subq_vals[s] > 300) {
                qualifying_rows.push_back({subq_keys[s],
                                           static_cast<double>(subq_vals[s])});
            }
        }

        free(subq_keys);
        free(subq_vals);
    }

    // ========================================================================
    // Phase 2: Main scan — pure in-memory index lookups.
    //   All indexes and columns are MAP_POPULATE'd → zero I/O page faults.
    //   sum_qty is already known from subquery → no lineitem MV index needed.
    // ========================================================================
    std::vector<ResultRow> results;
    results.reserve(qualifying_rows.size());

    {
        GENDB_PHASE("main_scan");

        for (const auto& qr : qualifying_rows) {
            const int32_t okey = qr.key;

            // --- orders index lookup (SV hash, all in memory) ---
            uint32_t oslot = sv_probe(ord_entries, ord_cap, ord_mask, okey);
            if (ord_entries[oslot].key != okey) continue;
            uint32_t ord_row = ord_entries[oslot].row_idx;

            const int32_t custkey = o_custkey[ord_row];
            const int32_t odate   = o_orderdate[ord_row];
            const double  oprice  = o_totalprice[ord_row];

            // --- customer index lookup (SV hash, all in memory) ---
            uint32_t cslot = sv_probe(cust_entries, cust_cap, cust_mask, custkey);
            if (cust_entries[cslot].key != custkey) continue;
            uint32_t cust_row = cust_entries[cslot].row_idx;

            // --- build result row (sum_qty from subquery — no MV index!) ---
            ResultRow row;
            const char* name_src = c_name_data + cust_row * 25;
            memcpy(row.c_name, name_src, 25);
            row.c_name[25] = '\0';
            int nlen = 25;
            while (nlen > 0 && row.c_name[nlen - 1] == '\0') --nlen;
            row.c_name[nlen] = '\0';

            row.c_custkey    = c_custkey_col[cust_row];
            row.o_orderkey   = okey;
            row.o_orderdate  = odate;
            row.o_totalprice = oprice;
            row.sum_qty      = qr.sum_qty;  // pre-computed in subquery pass

            results.push_back(row);
        }
    }

    // ========================================================================
    // Phase 3: Sort top-100 and write CSV output
    // ========================================================================
    {
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
    munmap(const_cast<double*>(l_quantity),      sz_lq);          close(fd_lq);
    munmap(const_cast<uint32_t*>(ord_raw),       sz_ord_idx);     close(fd_ord_idx);
    munmap(const_cast<uint32_t*>(cust_raw),      sz_cust_idx);    close(fd_cust_idx);
    munmap(const_cast<int32_t*>(o_custkey),      sz_o_custkey);   close(fd_o_custkey);
    munmap(const_cast<int32_t*>(o_orderdate),    sz_o_orderdate); close(fd_o_orderdate);
    munmap(const_cast<double*>(o_totalprice),    sz_o_totalprice);close(fd_o_totalprice);
    munmap(const_cast<char*>(c_name_data),       sz_c_name);      close(fd_c_name);
    munmap(const_cast<int32_t*>(c_custkey_col),  sz_c_custkey);   close(fd_c_custkey);
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
