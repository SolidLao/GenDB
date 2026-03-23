// build_indexes.cpp
// Builds all indexes from binary columnar data in the gendb directory.
//
// Indexes built:
//   orders/orders_by_orderkey.bin      - dense array: o_orderkey -> row_idx
//   customer/customer_by_custkey.bin   - dense array: c_custkey  -> row_idx
//   part/part_by_partkey.bin           - dense array: p_partkey  -> row_idx
//   supplier/supplier_by_suppkey.bin   - dense array: s_suppkey  -> row_idx
//   nation/nation_by_nationkey.bin     - dense array: n_nationkey-> row_idx
//   partsupp/partsupp_hash_index.bin   - open-addressing hash table: (ps_partkey,ps_suppkey)->row_idx
//   lineitem/l_shipdate_zone_map.bin   - zone map: block -> {min_date, max_date}

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Read entire binary column file into a vector
// ---------------------------------------------------------------------------
template<typename T>
static std::vector<T> read_column(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { perror(("read_column: " + path).c_str()); return {}; }
    fseek(f, 0, SEEK_END);
    long bytes = ftell(f);
    fseek(f, 0, SEEK_SET);
    size_t n = (size_t)bytes / sizeof(T);
    std::vector<T> v(n);
    fread(v.data(), sizeof(T), n, f);
    fclose(f);
    return v;
}

// Write raw binary array
template<typename T>
static void write_array(const std::string& path, const T* data, size_t count) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(("write_array: " + path).c_str()); return; }
    fwrite(data, sizeof(T), count, f);
    fclose(f);
}

// ---------------------------------------------------------------------------
// Build dense array index: pk_col[row] -> array[pk] = row_idx
// ---------------------------------------------------------------------------
static void build_dense_index(const std::string& pk_path,
                               const std::string& out_path,
                               int32_t max_key) {
    auto pk = read_column<int32_t>(pk_path);
    if (pk.empty()) { fprintf(stderr, "empty: %s\n", pk_path.c_str()); return; }

    size_t arr_size = (size_t)max_key + 1;
    std::vector<int32_t> arr(arr_size, -1);
    for (size_t i = 0; i < pk.size(); i++) {
        int32_t k = pk[i];
        if (k >= 0 && k <= max_key) arr[k] = (int32_t)i;
    }
    write_array(out_path, arr.data(), arr_size);
    printf("[index] wrote %s (%zu entries)\n", out_path.c_str(), arr_size);
    fflush(stdout);
}

// ---------------------------------------------------------------------------
// Hash table for partsupp: (ps_partkey, ps_suppkey) -> row_idx
//
// Hash function: Fibonacci hashing of a 64-bit composite key.
//   key = (uint64_t)ps_partkey * 200001ULL + ps_suppkey
//   slot = (key * 11400714819323198485ULL) >> (64 - LOG2_CAP)
//   Linear probing on collision; empty sentinel key = 0 (partkey 0 is invalid).
//
// Capacity: 2^24 = 16,777,216 (load factor <= 0.48 for 8M entries)
// ---------------------------------------------------------------------------

static const uint32_t LOG2_CAP   = 24;
static const uint64_t CAPACITY   = (uint64_t)1 << LOG2_CAP;
static const uint64_t FIB_CONST  = 11400714819323198485ULL;
static const uint64_t EMPTY_KEY  = 0ULL;

#pragma pack(push, 1)
struct HTSlot {
    uint64_t key;
    int32_t  row_idx;
    int32_t  pad;        // alignment padding -> 16 bytes per slot
};
#pragma pack(pop)

static_assert(sizeof(HTSlot) == 16, "HTSlot must be 16 bytes");

static void build_partsupp_hash_index(const std::string& gendb_dir) {
    std::string ps_dir = gendb_dir + "/partsupp";
    auto partkey    = read_column<int32_t>(ps_dir + "/ps_partkey.bin");
    auto suppkey    = read_column<int32_t>(ps_dir + "/ps_suppkey.bin");
    if (partkey.empty()) return;

    size_t N = partkey.size();
    printf("[partsupp hash] building hash table for %zu rows, capacity=%llu\n",
           N, (unsigned long long)CAPACITY);
    fflush(stdout);

    std::vector<HTSlot> ht(CAPACITY, {EMPTY_KEY, -1, 0});

    uint64_t collisions = 0;
    for (size_t i = 0; i < N; i++) {
        uint64_t key  = (uint64_t)partkey[i] * 200001ULL + (uint64_t)suppkey[i];
        uint64_t slot = (key * FIB_CONST) >> (64 - LOG2_CAP);
        while (ht[slot].key != EMPTY_KEY) {
            slot = (slot + 1) & (CAPACITY - 1);
            collisions++;
        }
        ht[slot].key     = key;
        ht[slot].row_idx = (int32_t)i;
    }

    printf("[partsupp hash] collisions=%llu (%.2f%%)\n",
           (unsigned long long)collisions, 100.0 * collisions / N);

    // Write: header (uint64_t capacity, uint64_t count) then slots
    std::string out_path = ps_dir + "/partsupp_hash_index.bin";
    FILE* f = fopen(out_path.c_str(), "wb");
    uint64_t hdr[2] = {CAPACITY, (uint64_t)N};
    fwrite(hdr, sizeof(uint64_t), 2, f);
    fwrite(ht.data(), sizeof(HTSlot), CAPACITY, f);
    fclose(f);

    printf("[partsupp hash] wrote %s (%.1f MB)\n",
           out_path.c_str(), (double)(sizeof(uint64_t)*2 + sizeof(HTSlot)*CAPACITY) / (1<<20));
    fflush(stdout);
}

// ---------------------------------------------------------------------------
// Zone map for lineitem l_shipdate (already sorted ascending)
// Format: uint32_t num_blocks, uint32_t block_size,
//         then num_blocks * {int32_t min_date, int32_t max_date}
// ---------------------------------------------------------------------------
static void build_shipdate_zone_map(const std::string& gendb_dir) {
    std::string li_dir  = gendb_dir + "/lineitem";
    auto shipdate = read_column<int32_t>(li_dir + "/l_shipdate.bin");
    if (shipdate.empty()) return;

    const uint32_t BLOCK_SIZE = 100000;
    size_t N = shipdate.size();
    uint32_t num_blocks = (uint32_t)((N + BLOCK_SIZE - 1) / BLOCK_SIZE);

    struct ZoneEntry { int32_t min_date; int32_t max_date; };
    std::vector<ZoneEntry> zones(num_blocks);

    for (uint32_t b = 0; b < num_blocks; b++) {
        size_t start = (size_t)b * BLOCK_SIZE;
        size_t stop  = std::min(start + BLOCK_SIZE, N);
        int32_t mn = shipdate[start], mx = shipdate[start];
        for (size_t i = start + 1; i < stop; i++) {
            if (shipdate[i] < mn) mn = shipdate[i];
            if (shipdate[i] > mx) mx = shipdate[i];
        }
        zones[b] = {mn, mx};
    }

    std::string out_path = li_dir + "/l_shipdate_zone_map.bin";
    FILE* f = fopen(out_path.c_str(), "wb");
    fwrite(&num_blocks,  sizeof(uint32_t), 1, f);
    fwrite(&BLOCK_SIZE,  sizeof(uint32_t), 1, f);
    fwrite(zones.data(), sizeof(ZoneEntry), num_blocks, f);
    fclose(f);

    printf("[zone map] wrote %s: %u blocks of %u rows\n",
           out_path.c_str(), num_blocks, BLOCK_SIZE);
    fflush(stdout);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb = argv[1];

    printf("Building indexes in %s\n", gendb.c_str()); fflush(stdout);

    // Dense array indexes (quick, run in parallel)
    std::thread t_orders(   build_dense_index,
        gendb + "/orders/o_orderkey.bin",
        gendb + "/orders/orders_by_orderkey.bin",
        60000000);  // TPC-H SF10 max orderkey

    std::thread t_customer( build_dense_index,
        gendb + "/customer/c_custkey.bin",
        gendb + "/customer/customer_by_custkey.bin",
        1500000);

    std::thread t_part(     build_dense_index,
        gendb + "/part/p_partkey.bin",
        gendb + "/part/part_by_partkey.bin",
        2000000);

    std::thread t_supplier( build_dense_index,
        gendb + "/supplier/s_suppkey.bin",
        gendb + "/supplier/supplier_by_suppkey.bin",
        100000);

    std::thread t_nation(   build_dense_index,
        gendb + "/nation/n_nationkey.bin",
        gendb + "/nation/nation_by_nationkey.bin",
        24);

    // partsupp hash table and zone map can run in parallel too
    std::thread t_partsupp([&gendb]() { build_partsupp_hash_index(gendb); });
    std::thread t_zonemap( [&gendb]() { build_shipdate_zone_map(gendb); });

    t_orders.join();
    t_customer.join();
    t_part.join();
    t_supplier.join();
    t_nation.join();
    t_partsupp.join();
    t_zonemap.join();

    printf("All indexes built successfully.\n");
    return 0;
}
