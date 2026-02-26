// build_indexes.cpp — Build all persistent indexes from binary columnar data.
//
// Indexes produced:
//   Zone maps  : lineitem/l_shipdate, orders/o_orderdate  (both cols are sorted)
//   PK hashes  : orders.o_orderkey, customer.c_custkey, part.p_partkey,
//                supplier.s_suppkey, nation.n_nationkey
//   Composite PK hash : partsupp.(ps_partkey, ps_suppkey)
//   FK sorted pairs   : lineitem.l_orderkey, orders.o_custkey
//
// All indexes built in parallel via std::thread.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <climits>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <algorithm>
#include <thread>
#include <stdexcept>

// ─── Utilities ─────────────────────────────────────────────────────────────

static void mkdirp(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

// Derive row count from file size and element size
static size_t row_count(const std::string& path, size_t elem_size) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        fprintf(stderr, "Cannot stat: %s\n", path.c_str());
        return 0;
    }
    return (size_t)st.st_size / elem_size;
}

// Read a full binary column into a vector
template<typename T>
static std::vector<T> read_col(const std::string& path, size_t n) {
    std::vector<T> col(n);
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open: " + path);
    size_t rd = fread(col.data(), sizeof(T), n, f);
    fclose(f);
    if (rd != n) throw std::runtime_error("Short read: " + path);
    return col;
}

// ─── Zone Map ──────────────────────────────────────────────────────────────
// File format:
//   uint64_t  n_blocks
//   { int32_t min_val; int32_t max_val; } [n_blocks]
//
// Column must be sorted for zone maps to be maximally effective.

static void build_zone_map(const std::string& col_path, const std::string& out_path,
                            size_t n_rows, size_t block_size) {
    auto col = read_col<int32_t>(col_path, n_rows);

    size_t n_blocks = (n_rows + block_size - 1) / block_size;
    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create: " + out_path);

    uint64_t nb = (uint64_t)n_blocks;
    fwrite(&nb, 8, 1, f);

    for (size_t b = 0; b < n_blocks; b++) {
        size_t lo = b * block_size;
        size_t hi = std::min(lo + block_size, n_rows);
        int32_t mn = col[lo], mx = col[lo];
        for (size_t i = lo + 1; i < hi; i++) {
            if (col[i] < mn) mn = col[i];
            if (col[i] > mx) mx = col[i];
        }
        fwrite(&mn, 4, 1, f);
        fwrite(&mx, 4, 1, f);
    }
    fclose(f);
    printf("[zone_map] %s: %zu blocks\n", out_path.c_str(), n_blocks); fflush(stdout);
}

// ─── PK Hash (int32 key → row_id) ─────────────────────────────────────────
// Open-addressing linear-probe hash table.
// File format:
//   uint64_t  num_entries
//   uint64_t  bucket_count
//   { int32_t key; int32_t row_id; } [bucket_count]
// Empty sentinel: key == INT32_MIN
// Hash function : (uint32_t(key) * 2654435761u) % bucket_count
// Load factor   : ~0.6

static void build_pk_hash(const std::string& key_path, const std::string& out_path,
                           size_t n_rows) {
    auto keys = read_col<int32_t>(key_path, n_rows);

    // bucket_count: first odd number >= n_rows/0.6
    uint64_t bc = (uint64_t)((double)n_rows / 0.6) + 2;
    if (bc % 2 == 0) bc++;

    struct Bucket { int32_t key; int32_t row_id; };
    const int32_t EMPTY = INT32_MIN;
    std::vector<Bucket> table((size_t)bc, {EMPTY, -1});

    for (size_t i = 0; i < n_rows; i++) {
        uint32_t k = (uint32_t)keys[i];
        uint64_t h = ((uint64_t)k * 2654435761ULL) % bc;
        while (table[(size_t)h].key != EMPTY)
            h = (h + 1 < bc) ? h + 1 : 0;
        table[(size_t)h] = {keys[i], (int32_t)i};
    }

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create: " + out_path);
    uint64_t ne = (uint64_t)n_rows;
    fwrite(&ne, 8, 1, f);
    fwrite(&bc, 8, 1, f);
    fwrite(table.data(), sizeof(Bucket), (size_t)bc, f);
    fclose(f);
    printf("[pk_hash] %s: %zu entries, %llu buckets\n",
           out_path.c_str(), n_rows, (unsigned long long)bc); fflush(stdout);
}

// ─── Composite PK Hash (int64 key → row_id) ────────────────────────────────
// Key encoding : int64_t k = (int64_t(ps_partkey) << 32) | uint32_t(ps_suppkey)
// Hash function: splitmix64(k) % bucket_count
// File format:
//   uint64_t  num_entries
//   uint64_t  bucket_count
//   { int64_t key; int32_t row_id; int32_t pad; } [bucket_count]
// Empty sentinel: key == INT64_MIN

static uint64_t splitmix64(uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static void build_composite_pk_hash(const std::string& key1_path,
                                     const std::string& key2_path,
                                     const std::string& out_path,
                                     size_t n_rows) {
    auto k1 = read_col<int32_t>(key1_path, n_rows);
    auto k2 = read_col<int32_t>(key2_path, n_rows);

    uint64_t bc = (uint64_t)((double)n_rows / 0.6) + 2;
    if (bc % 2 == 0) bc++;

    struct Bucket { int64_t key; int32_t row_id; int32_t pad; };
    const int64_t EMPTY = INT64_MIN;
    std::vector<Bucket> table((size_t)bc, {EMPTY, -1, 0});

    for (size_t i = 0; i < n_rows; i++) {
        int64_t key = ((int64_t)k1[i] << 32) | (uint32_t)k2[i];
        uint64_t h  = splitmix64((uint64_t)key) % bc;
        while (table[(size_t)h].key != EMPTY)
            h = (h + 1 < bc) ? h + 1 : 0;
        table[(size_t)h] = {key, (int32_t)i, 0};
    }

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create: " + out_path);
    uint64_t ne = (uint64_t)n_rows;
    fwrite(&ne, 8, 1, f);
    fwrite(&bc, 8, 1, f);
    fwrite(table.data(), sizeof(Bucket), (size_t)bc, f);
    fclose(f);
    printf("[composite_hash] %s: %zu entries, %llu buckets\n",
           out_path.c_str(), n_rows, (unsigned long long)bc); fflush(stdout);
}

// ─── FK Sorted Pairs (key → multiple row_ids) ─────────────────────────────
// File format:
//   uint64_t  num_pairs
//   { int32_t key; int32_t row_id; } [num_pairs]   sorted ascending by key
//
// Usage: binary-search for first occurrence of a key, iterate while key matches.

static void build_fk_sorted(const std::string& key_path, const std::string& out_path,
                              size_t n_rows) {
    auto keys = read_col<int32_t>(key_path, n_rows);

    struct Pair { int32_t key; int32_t row_id; };
    std::vector<Pair> pairs(n_rows);
    for (size_t i = 0; i < n_rows; i++)
        pairs[i] = {keys[i], (int32_t)i};

    std::sort(pairs.begin(), pairs.end(), [](const Pair& a, const Pair& b) {
        return a.key < b.key;
    });

    FILE* f = fopen(out_path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create: " + out_path);
    uint64_t n = (uint64_t)n_rows;
    fwrite(&n, 8, 1, f);
    fwrite(pairs.data(), sizeof(Pair), n_rows, f);
    fclose(f);
    printf("[fk_sorted] %s: %zu pairs\n", out_path.c_str(), n_rows); fflush(stdout);
}

// ─── Main ──────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string db(argv[1]);
    std::string idx = db + "/indexes";
    mkdirp(idx);

    // Derive row counts from ingested binary columns
    auto rc = [&](const char* tbl, const char* col, size_t esz) {
        return row_count(db + "/" + tbl + "/" + col, esz);
    };

    size_t N_LI = rc("lineitem", "l_orderkey.bin",     4);
    size_t N_OR = rc("orders",   "o_orderkey.bin",     4);
    size_t N_CU = rc("customer", "c_custkey.bin",      4);
    size_t N_PA = rc("part",     "p_partkey.bin",      4);
    size_t N_PS = rc("partsupp", "ps_partkey.bin",     4);
    size_t N_SU = rc("supplier", "s_suppkey.bin",      4);
    size_t N_NA = rc("nation",   "n_nationkey.bin",    4);

    printf("[build_indexes] row counts: lineitem=%zu orders=%zu customer=%zu "
           "part=%zu partsupp=%zu supplier=%zu nation=%zu\n",
           N_LI, N_OR, N_CU, N_PA, N_PS, N_SU, N_NA); fflush(stdout);

    const size_t BLOCK = 100000;

    auto run = [](const char* name, auto fn) {
        return std::thread([=]() {
            try { fn(); }
            catch (std::exception& e) {
                fprintf(stderr, "[%s] ERROR: %s\n", name, e.what());
            }
        });
    };

    std::vector<std::thread> threads;

    // ── Zone maps (sorted columns) ──────────────────────────────────────────
    threads.push_back(run("zonemap_shipdate",
        [&]{ build_zone_map(db + "/lineitem/l_shipdate.bin",
                            idx + "/lineitem_shipdate_zonemap.bin", N_LI, BLOCK); }));
    threads.push_back(run("zonemap_orderdate",
        [&]{ build_zone_map(db + "/orders/o_orderdate.bin",
                            idx + "/orders_orderdate_zonemap.bin", N_OR, BLOCK); }));

    // ── PK hash tables ──────────────────────────────────────────────────────
    threads.push_back(run("hash_orders_orderkey",
        [&]{ build_pk_hash(db + "/orders/o_orderkey.bin",
                           idx + "/orders_orderkey_hash.bin", N_OR); }));
    threads.push_back(run("hash_customer_custkey",
        [&]{ build_pk_hash(db + "/customer/c_custkey.bin",
                           idx + "/customer_custkey_hash.bin", N_CU); }));
    threads.push_back(run("hash_part_partkey",
        [&]{ build_pk_hash(db + "/part/p_partkey.bin",
                           idx + "/part_partkey_hash.bin", N_PA); }));
    threads.push_back(run("hash_supplier_suppkey",
        [&]{ build_pk_hash(db + "/supplier/s_suppkey.bin",
                           idx + "/supplier_suppkey_hash.bin", N_SU); }));
    threads.push_back(run("hash_nation_nationkey",
        [&]{ build_pk_hash(db + "/nation/n_nationkey.bin",
                           idx + "/nation_nationkey_hash.bin", N_NA); }));

    // ── Composite PK hash (partsupp) ────────────────────────────────────────
    threads.push_back(run("hash_partsupp_pk",
        [&]{ build_composite_pk_hash(db + "/partsupp/ps_partkey.bin",
                                     db + "/partsupp/ps_suppkey.bin",
                                     idx + "/partsupp_pk_hash.bin", N_PS); }));

    // ── FK sorted pairs ─────────────────────────────────────────────────────
    threads.push_back(run("fk_lineitem_orderkey",
        [&]{ build_fk_sorted(db + "/lineitem/l_orderkey.bin",
                             idx + "/lineitem_orderkey_sorted.bin", N_LI); }));
    threads.push_back(run("fk_orders_custkey",
        [&]{ build_fk_sorted(db + "/orders/o_custkey.bin",
                             idx + "/orders_custkey_sorted.bin", N_OR); }));

    for (auto& t : threads) t.join();

    printf("[build_indexes] All indexes complete.\n");
    return 0;
}
