#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <filesystem>
#include <algorithm>
#include <thread>
#include <chrono>
#include <mutex>

namespace fs = std::filesystem;

// ============================================================
// Utility
// ============================================================
static std::mutex g_print_mtx;
static void report(const char* idx, const char* msg) {
    std::lock_guard<std::mutex> lk(g_print_mtx);
    printf("[%-30s] %s\n", idx, msg);
    fflush(stdout);
}

static uint64_t read_row_count(const std::string& meta_path) {
    FILE* fp = fopen(meta_path.c_str(), "r");
    if (!fp) { perror(meta_path.c_str()); exit(1); }
    uint64_t n = 0;
    fscanf(fp, "row_count=%lu", &n);
    fclose(fp);
    return n;
}

template<typename T>
static std::vector<T> read_bin(const std::string& path, size_t count) {
    std::vector<T> v(count);
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) { perror(path.c_str()); exit(1); }
    size_t rd = fread(v.data(), sizeof(T), count, fp);
    if (rd != count) { fprintf(stderr, "Short read: %s (%zu/%zu)\n", path.c_str(), rd, count); exit(1); }
    fclose(fp);
    return v;
}

template<typename T>
static void write_bin(const std::string& path, const std::vector<T>& v) {
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(1); }
    if (!v.empty()) fwrite(v.data(), sizeof(T), v.size(), fp);
    fclose(fp);
}

static void write_bin_raw(const std::string& path, const void* data, size_t bytes) {
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(1); }
    fwrite(data, 1, bytes, fp);
    fclose(fp);
}

// ============================================================
// Dense PK index: key_value -> row_id
// For tables where PK is a single int32 column.
// Array of int32_t, indexed by key value. -1 = not present.
// ============================================================
static void build_dense_pk_index(const std::string& storage_dir,
                                  const std::string& table,
                                  const std::string& pk_col) {
    std::string meta_path = storage_dir + "/" + table + "/meta.txt";
    uint64_t nrows = read_row_count(meta_path);

    auto keys = read_bin<int32_t>(storage_dir + "/" + table + "/" + pk_col + ".bin", nrows);

    // Find max key
    int32_t max_key = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (keys[i] > max_key) max_key = keys[i];
    }

    // Build dense array
    std::vector<int32_t> index(max_key + 1, -1);
    for (size_t i = 0; i < nrows; i++) {
        index[keys[i]] = (int32_t)i;
    }

    std::string out_path = storage_dir + "/indexes/" + table + "_pk_index.bin";
    write_bin(out_path, index);

    char msg[256];
    snprintf(msg, sizeof(msg), "done: %lu rows, max_key=%d, array_size=%d",
             (unsigned long)nrows, max_key, max_key + 1);
    report((table + "_pk_idx").c_str(), msg);
}

// ============================================================
// Dense range index: key_value -> (start_row, count)
// For sorted tables where we want to find all rows matching a key.
// ============================================================
struct RangeEntry {
    uint32_t start;
    uint32_t count;
};

static void build_dense_range_index(const std::string& storage_dir,
                                     const std::string& table,
                                     const std::string& key_col,
                                     const std::string& index_name) {
    std::string meta_path = storage_dir + "/" + table + "/meta.txt";
    uint64_t nrows = read_row_count(meta_path);

    auto keys = read_bin<int32_t>(storage_dir + "/" + table + "/" + key_col + ".bin", nrows);

    // Verify sorted
    for (size_t i = 1; i < nrows; i++) {
        if (keys[i] < keys[i-1]) {
            fprintf(stderr, "ERROR: %s/%s.bin is not sorted at row %zu\n",
                    table.c_str(), key_col.c_str(), i);
            exit(1);
        }
    }

    // Find max key
    int32_t max_key = 0;
    for (size_t i = 0; i < nrows; i++) {
        if (keys[i] > max_key) max_key = keys[i];
    }

    // Build range index
    std::vector<RangeEntry> index(max_key + 1, {0, 0});

    if (nrows > 0) {
        size_t run_start = 0;
        for (size_t i = 1; i <= nrows; i++) {
            if (i == nrows || keys[i] != keys[run_start]) {
                int32_t k = keys[run_start];
                index[k] = { (uint32_t)run_start, (uint32_t)(i - run_start) };
                run_start = i;
            }
        }
    }

    std::string out_path = storage_dir + "/indexes/" + index_name + ".bin";
    write_bin_raw(out_path, index.data(), index.size() * sizeof(RangeEntry));

    // Also write the max_key as metadata
    FILE* mfp = fopen((storage_dir + "/indexes/" + index_name + "_meta.txt").c_str(), "w");
    fprintf(mfp, "max_key=%d\nentry_size=8\nformat=start_u32,count_u32\n", max_key);
    fclose(mfp);

    char msg[256];
    snprintf(msg, sizeof(msg), "done: max_key=%d, array_entries=%d", max_key, max_key + 1);
    report(index_name.c_str(), msg);
}

// ============================================================
// Zone map: (min, max) per block of rows
// ============================================================
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
};

static void build_zone_map(const std::string& storage_dir,
                            const std::string& table,
                            const std::string& col,
                            uint32_t block_size) {
    std::string meta_path = storage_dir + "/" + table + "/meta.txt";
    uint64_t nrows = read_row_count(meta_path);

    auto vals = read_bin<int32_t>(storage_dir + "/" + table + "/" + col + ".bin", nrows);

    uint32_t nblocks = (uint32_t)((nrows + block_size - 1) / block_size);
    std::vector<ZoneMapEntry> zm(nblocks);

    for (uint32_t b = 0; b < nblocks; b++) {
        uint64_t start = (uint64_t)b * block_size;
        uint64_t end = std::min(start + block_size, nrows);
        int32_t mn = vals[start], mx = vals[start];
        for (uint64_t i = start + 1; i < end; i++) {
            if (vals[i] < mn) mn = vals[i];
            if (vals[i] > mx) mx = vals[i];
        }
        zm[b] = { mn, mx };
    }

    std::string out_path = storage_dir + "/indexes/" + table + "_" + col + "_zonemap.bin";
    write_bin(out_path, zm);

    // Write metadata
    FILE* mfp = fopen((storage_dir + "/indexes/" + table + "_" + col + "_zonemap_meta.txt").c_str(), "w");
    fprintf(mfp, "block_size=%u\nnum_blocks=%u\nentry_size=8\nformat=min_i32,max_i32\n",
            block_size, nblocks);
    fclose(mfp);

    char msg[256];
    snprintf(msg, sizeof(msg), "done: %u blocks of %u rows", nblocks, block_size);
    report((table + "_" + col + "_zm").c_str(), msg);
}

// ============================================================
// Composite key index for partsupp: (ps_partkey, ps_suppkey) -> row_id
// Hash index stored as: array of (key_hash -> row_id) with open addressing
// ============================================================
static void build_partsupp_composite_index(const std::string& storage_dir) {
    std::string meta_path = storage_dir + "/partsupp/meta.txt";
    uint64_t nrows = read_row_count(meta_path);

    auto partkeys = read_bin<int32_t>(storage_dir + "/partsupp/ps_partkey.bin", nrows);
    auto suppkeys = read_bin<int32_t>(storage_dir + "/partsupp/ps_suppkey.bin", nrows);

    // Open-addressing hash table: size = next power of 2 above 2*nrows
    uint64_t capacity = 1;
    while (capacity < nrows * 2) capacity <<= 1;
    uint64_t mask = capacity - 1;

    // Hash table: each entry = (partkey, suppkey, row_id) or empty marker
    struct Entry {
        int32_t partkey;
        int32_t suppkey;
        int32_t row_id;
        int32_t pad; // alignment
    };
    std::vector<Entry> table(capacity, {-1, -1, -1, 0});

    auto hash_fn = [&](int32_t pk, int32_t sk) -> uint64_t {
        uint64_t h = (uint64_t)(uint32_t)pk * 2654435761ULL ^ (uint64_t)(uint32_t)sk * 40503ULL;
        return h;
    };

    for (size_t i = 0; i < nrows; i++) {
        uint64_t h = hash_fn(partkeys[i], suppkeys[i]) & mask;
        while (table[h].row_id != -1) {
            h = (h + 1) & mask;
        }
        table[h] = { partkeys[i], suppkeys[i], (int32_t)i, 0 };
    }

    std::string out_path = storage_dir + "/indexes/partsupp_composite_hash.bin";
    write_bin_raw(out_path, table.data(), capacity * sizeof(Entry));

    FILE* mfp = fopen((storage_dir + "/indexes/partsupp_composite_hash_meta.txt").c_str(), "w");
    fprintf(mfp, "capacity=%lu\nmask=%lu\nentry_size=16\nformat=partkey_i32,suppkey_i32,row_id_i32,pad_i32\n"
            "hash=pk*2654435761^sk*40503\n",
            (unsigned long)capacity, (unsigned long)mask);
    fclose(mfp);

    char msg[256];
    snprintf(msg, sizeof(msg), "done: %lu entries, capacity=%lu, load=%.2f",
             (unsigned long)nrows, (unsigned long)capacity, (double)nrows/capacity);
    report("partsupp_composite_hash", msg);
}

// ============================================================
// Customer-to-orders reverse index: custkey -> (start, count) in orders
// (Requires orders sorted by o_custkey, or we build a hash)
// Actually orders is sorted by o_orderkey, not o_custkey.
// So we build: custkey -> list of order row_ids using a grouped approach.
// ============================================================
static void build_custkey_to_orders_index(const std::string& storage_dir) {
    uint64_t norders = read_row_count(storage_dir + "/orders/meta.txt");
    auto custkeys = read_bin<int32_t>(storage_dir + "/orders/o_custkey.bin", norders);

    // Find max custkey
    int32_t max_ck = 0;
    for (size_t i = 0; i < norders; i++) {
        if (custkeys[i] > max_ck) max_ck = custkeys[i];
    }

    // Count per custkey
    std::vector<uint32_t> counts(max_ck + 1, 0);
    for (size_t i = 0; i < norders; i++) counts[custkeys[i]]++;

    // Build offsets (prefix sum)
    std::vector<uint32_t> offsets(max_ck + 2, 0);
    for (int32_t k = 0; k <= max_ck; k++) offsets[k+1] = offsets[k] + counts[k];

    // Fill row_ids
    std::vector<uint32_t> row_ids(norders);
    std::vector<uint32_t> pos(max_ck + 1, 0);
    for (size_t i = 0; i < norders; i++) {
        int32_t ck = custkeys[i];
        row_ids[offsets[ck] + pos[ck]] = (uint32_t)i;
        pos[ck]++;
    }

    write_bin(storage_dir + "/indexes/custkey_to_orders_offsets.bin", offsets);
    write_bin(storage_dir + "/indexes/custkey_to_orders_rows.bin", row_ids);

    FILE* mfp = fopen((storage_dir + "/indexes/custkey_to_orders_meta.txt").c_str(), "w");
    fprintf(mfp, "max_custkey=%d\nnum_orders=%lu\nformat=offsets_u32[max_custkey+2],row_ids_u32[num_orders]\n",
            max_ck, (unsigned long)norders);
    fclose(mfp);

    report("custkey_to_orders", "done");
}

// ============================================================
// Main
// ============================================================
int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <storage_dir>\n", argv[0]);
        return 1;
    }
    std::string dir = argv[1];
    fs::create_directories(dir + "/indexes");

    auto t0 = std::chrono::high_resolution_clock::now();

    // Group 1: Dense PK indexes (parallel)
    std::thread t1([&]{ build_dense_pk_index(dir, "nation", "n_nationkey"); });
    std::thread t2([&]{ build_dense_pk_index(dir, "region", "r_regionkey"); });
    std::thread t3([&]{ build_dense_pk_index(dir, "supplier", "s_suppkey"); });
    std::thread t4([&]{ build_dense_pk_index(dir, "customer", "c_custkey"); });
    std::thread t5([&]{ build_dense_pk_index(dir, "part", "p_partkey"); });
    std::thread t6([&]{ build_dense_pk_index(dir, "orders", "o_orderkey"); });
    t1.join(); t2.join(); t3.join(); t4.join(); t5.join(); t6.join();

    // Group 2: Range indexes and zone maps (parallel)
    std::thread t7([&]{ build_dense_range_index(dir, "lineitem", "l_orderkey", "lineitem_orderkey_idx"); });
    std::thread t8([&]{ build_dense_range_index(dir, "partsupp", "ps_partkey", "partsupp_partkey_idx"); });
    std::thread t9([&]{ build_zone_map(dir, "lineitem", "l_shipdate", 65536); });
    std::thread t10([&]{ build_zone_map(dir, "orders", "o_orderdate", 65536); });
    std::thread t11([&]{ build_partsupp_composite_index(dir); });
    std::thread t12([&]{ build_custkey_to_orders_index(dir); });
    t7.join(); t8.join(); t9.join(); t10.join(); t11.join(); t12.join();

    auto t1_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t1_end - t0).count();
    printf("\nIndex building complete in %.1f seconds\n", elapsed);
    return 0;
}
