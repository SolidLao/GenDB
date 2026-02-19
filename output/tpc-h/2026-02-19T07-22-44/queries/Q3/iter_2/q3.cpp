#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <fstream>
#include <climits>

#include "date_utils.h"
#include "timing_utils.h"

// ============================================================
// Compact open-addressing hash set for int32_t
// ============================================================
struct HashSet32 {
    std::vector<int32_t> slots;
    uint32_t mask;
    static constexpr int32_t EMPTY = INT32_MIN;

    HashSet32() : mask(0) {}
    HashSet32(uint32_t capacity) {
        uint32_t sz = 1;
        while (sz < capacity * 2) sz <<= 1;
        slots.assign(sz, EMPTY);
        mask = sz - 1;
    }

    inline void insert(int32_t key) {
        uint32_t h = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
        uint32_t idx = h & mask;
        while (slots[idx] != EMPTY && slots[idx] != key) idx = (idx + 1) & mask;
        slots[idx] = key;
    }

    inline bool contains(int32_t key) const {
        uint32_t h = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
        uint32_t idx = h & mask;
        while (slots[idx] != EMPTY && slots[idx] != key) idx = (idx + 1) & mask;
        return slots[idx] == key;
    }
};

// ============================================================
// Compact open-addressing hash map: int32_t -> {int32_t, int32_t}
// ============================================================
struct OrderInfo {
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct HashMap32ToOrder {
    struct Bucket {
        int32_t key;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };
    std::vector<Bucket> slots;
    uint32_t mask;
    static constexpr int32_t EMPTY = INT32_MIN;

    HashMap32ToOrder() : mask(0) {}
    HashMap32ToOrder(uint32_t capacity) {
        uint32_t sz = 1;
        while (sz < capacity * 2) sz <<= 1;
        slots.resize(sz);
        for (auto& b : slots) b.key = EMPTY;
        mask = sz - 1;
    }

    inline void insert(int32_t key, int32_t odate, int32_t opri) {
        uint32_t h = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
        uint32_t idx = h & mask;
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx + 1) & mask;
        slots[idx].key = key;
        slots[idx].o_orderdate = odate;
        slots[idx].o_shippriority = opri;
    }

    inline const Bucket* find(int32_t key) const {
        uint32_t h = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
        uint32_t idx = h & mask;
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx + 1) & mask;
        if (slots[idx].key == key) return &slots[idx];
        return nullptr;
    }
};

// ============================================================
// Bloom filter
// ============================================================
struct BloomFilter {
    std::vector<uint64_t> bits;
    uint32_t mask;

    BloomFilter(uint32_t nbits) {
        uint32_t sz = 1;
        while (sz < (nbits + 63) / 64) sz <<= 1;
        bits.assign(sz, 0);
        mask = sz * 64 - 1;
    }

    inline void add(int32_t key) {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h >> 32) & mask;
        uint32_t h2 = (uint32_t)(h * 0xC4CEB9FE1A85EC53ULL >> 32) & mask;
        uint32_t h3 = (uint32_t)((h ^ (h >> 17)) * 0x517CC1B727220A95ULL >> 32) & mask;
        bits[h1 >> 6] |= (1ULL << (h1 & 63));
        bits[h2 >> 6] |= (1ULL << (h2 & 63));
        bits[h3 >> 6] |= (1ULL << (h3 & 63));
    }

    inline bool likely_contains(int32_t key) const {
        uint64_t h = (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
        uint32_t h1 = (uint32_t)(h >> 32) & mask;
        uint32_t h2 = (uint32_t)(h * 0xC4CEB9FE1A85EC53ULL >> 32) & mask;
        uint32_t h3 = (uint32_t)((h ^ (h >> 17)) * 0x517CC1B727220A95ULL >> 32) & mask;
        return (bits[h1 >> 6] >> (h1 & 63) & 1) &&
               (bits[h2 >> 6] >> (h2 & 63) & 1) &&
               (bits[h3 >> 6] >> (h3 & 63) & 1);
    }
};

// ============================================================
// Aggregation map: int32_t -> {double revenue, int32_t odate, int32_t opri}
// ============================================================
struct AggEntry {
    int32_t key;       // l_orderkey
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct AggMap {
    std::vector<AggEntry> slots;
    uint32_t mask;
    static constexpr int32_t EMPTY = INT32_MIN;

    AggMap() : mask(0) {}
    AggMap(uint32_t capacity) {
        uint32_t sz = 1;
        while (sz < capacity * 2) sz <<= 1;
        slots.resize(sz);
        for (auto& e : slots) e.key = EMPTY;
        mask = sz - 1;
    }

    inline void update(int32_t key, double contrib, int32_t odate, int32_t opri) {
        uint32_t h = ((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32;
        uint32_t idx = h & mask;
        while (slots[idx].key != EMPTY && slots[idx].key != key) idx = (idx + 1) & mask;
        if (slots[idx].key == EMPTY) {
            slots[idx].key = key;
            slots[idx].revenue = contrib;
            slots[idx].o_orderdate = odate;
            slots[idx].o_shippriority = opri;
        } else {
            slots[idx].revenue += contrib;
        }
    }
};

// ============================================================
// mmap helper
// ============================================================
template<typename T>
struct MmapCol {
    const T* data = nullptr;
    size_t n = 0;
    size_t sz = 0;
    int fd = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return; }
        struct stat st; fstat(fd, &st);
        sz = st.st_size;
        n = sz / sizeof(T);
        data = reinterpret_cast<const T*>(mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0));
    }
    ~MmapCol() {
        if (data && data != MAP_FAILED) munmap((void*)data, sz);
        if (fd >= 0) close(fd);
    }
};

// Zone map block info
struct ZoneBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t block_size;
};

void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    const int32_t DATE_THRESHOLD = 9204; // 1995-03-15
    const int NUM_THREADS = std::thread::hardware_concurrency();

    // -------------------------------------------------------
    // Phase 1: Filter customer, build custkey set
    // -------------------------------------------------------
    HashSet32 cust_set(700000);
    {
        GENDB_PHASE("dim_filter");

        // Load c_mktsegment dictionary
        std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
        std::ifstream dict_f(dict_path);
        std::vector<std::string> dict_entries;
        std::string line;
        while (std::getline(dict_f, line)) dict_entries.push_back(line);

        int8_t building_code = -1;
        for (int i = 0; i < (int)dict_entries.size(); i++) {
            if (dict_entries[i] == "BUILDING") { building_code = (int8_t)i; break; }
        }

        MmapCol<int8_t>  c_mkt;  c_mkt.open(gendb_dir + "/customer/c_mktsegment.bin");
        MmapCol<int32_t> c_cust; c_cust.open(gendb_dir + "/customer/c_custkey.bin");

        size_t n = c_mkt.n;

        // Parallel scan
        std::vector<std::vector<int32_t>> local_keys(NUM_THREADS);
        std::vector<std::thread> threads;
        size_t chunk = (n + NUM_THREADS - 1) / NUM_THREADS;

        for (int t = 0; t < NUM_THREADS; t++) {
            threads.emplace_back([&, t]() {
                size_t start = t * chunk;
                size_t end = std::min(start + chunk, n);
                auto& lk = local_keys[t];
                lk.reserve(chunk / 5);
                for (size_t i = start; i < end; i++) {
                    if (c_mkt.data[i] == building_code) {
                        lk.push_back(c_cust.data[i]);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Merge into shared set
        for (auto& lk : local_keys)
            for (int32_t k : lk)
                cust_set.insert(k);
    }

    // -------------------------------------------------------
    // Phase 2: Build orders join map using zone map
    // -------------------------------------------------------
    // Use ~16M capacity for orders (1.44M qualifying, but o_orderkey can be up to 15M*4)
    HashMap32ToOrder orders_map(3000000);
    BloomFilter bloom(16 * 1024 * 1024 * 8); // 16MB = 128M bits
    {
        GENDB_PHASE("build_joins");

        // Load zone map
        std::string zm_path = gendb_dir + "/indexes/orders_orderdate_zonemap.bin";
        int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        uint32_t num_blocks = 0;
        std::vector<ZoneBlock> zone_blocks;
        if (zm_fd >= 0) {
            read(zm_fd, &num_blocks, 4);
            zone_blocks.resize(num_blocks);
            read(zm_fd, zone_blocks.data(), num_blocks * 12);
            close(zm_fd);
        }

        MmapCol<int32_t> o_orderdate;   o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
        MmapCol<int32_t> o_custkey;     o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
        MmapCol<int32_t> o_orderkey;    o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
        MmapCol<int32_t> o_shippriority; o_shippriority.open(gendb_dir + "/orders/o_shippriority.bin");

        // Collect qualifying rows per block (zone-map guided)
        // Build list of blocks to process
        struct BlockRange { uint32_t start; uint32_t size; };
        std::vector<BlockRange> process_blocks;
        process_blocks.reserve(num_blocks);
        uint32_t row_offset = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            uint32_t bsz = zone_blocks[b].block_size;
            // Skip if block_min >= DATE_THRESHOLD (no rows can satisfy o_orderdate < 9204)
            if (zone_blocks[b].min_val < DATE_THRESHOLD) {
                process_blocks.push_back({row_offset, bsz});
            }
            row_offset += bsz;
        }

        // Parallel processing of qualifying blocks
        std::atomic<size_t> block_idx{0};
        // Use partitioned insertion: partition by hash(o_orderkey) % NUM_THREADS
        // Each thread maintains local vectors, then we merge into shared map
        std::vector<std::vector<std::tuple<int32_t,int32_t,int32_t>>> local_orders(NUM_THREADS);
        for (int t = 0; t < NUM_THREADS; t++)
            local_orders[t].reserve(1500000 / NUM_THREADS + 10000);

        std::vector<std::thread> threads;
        for (int t = 0; t < NUM_THREADS; t++) {
            threads.emplace_back([&, t]() {
                auto& lo = local_orders[t];
                while (true) {
                    size_t bi = block_idx.fetch_add(1, std::memory_order_relaxed);
                    if (bi >= process_blocks.size()) break;
                    uint32_t start = process_blocks[bi].start;
                    uint32_t bsz = process_blocks[bi].size;
                    for (uint32_t i = 0; i < bsz; i++) {
                        uint32_t row = start + i;
                        if (o_orderdate.data[row] < DATE_THRESHOLD) {
                            int32_t ck = o_custkey.data[row];
                            if (cust_set.contains(ck)) {
                                lo.emplace_back(o_orderkey.data[row], o_orderdate.data[row], o_shippriority.data[row]);
                            }
                        }
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Merge into orders_map and bloom filter
        for (auto& lo : local_orders) {
            for (auto& [ok, od, op] : lo) {
                orders_map.insert(ok, od, op);
                bloom.add(ok);
            }
        }
    }

    // -------------------------------------------------------
    // Phase 3: Scan lineitem with zone map, aggregate
    // -------------------------------------------------------
    // Thread-local aggregation maps
    int NPARTS = NUM_THREADS;
    std::vector<AggMap> agg_maps(NPARTS, AggMap(200000));
    {
        GENDB_PHASE("main_scan");

        // Load lineitem zone map
        std::string zm_path = gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin";
        int zm_fd = ::open(zm_path.c_str(), O_RDONLY);
        uint32_t num_blocks = 0;
        std::vector<ZoneBlock> zone_blocks;
        if (zm_fd >= 0) {
            read(zm_fd, &num_blocks, 4);
            zone_blocks.resize(num_blocks);
            read(zm_fd, zone_blocks.data(), num_blocks * 12);
            close(zm_fd);
        }

        MmapCol<int32_t> l_shipdate;      l_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        MmapCol<int32_t> l_orderkey;      l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        MmapCol<double>  l_extendedprice; l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        MmapCol<double>  l_discount;      l_discount.open(gendb_dir + "/lineitem/l_discount.bin");

        struct BlockRange { uint32_t start; uint32_t size; };
        std::vector<BlockRange> process_blocks;
        process_blocks.reserve(num_blocks);
        uint32_t row_offset = 0;
        for (uint32_t b = 0; b < num_blocks; b++) {
            uint32_t bsz = zone_blocks[b].block_size;
            // Skip if block_max <= DATE_THRESHOLD (no rows satisfy l_shipdate > 9204)
            if (zone_blocks[b].max_val > DATE_THRESHOLD) {
                process_blocks.push_back({row_offset, bsz});
            }
            row_offset += bsz;
        }

        std::atomic<size_t> block_idx{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < NUM_THREADS; t++) {
            threads.emplace_back([&, t]() {
                while (true) {
                    size_t bi = block_idx.fetch_add(1, std::memory_order_relaxed);
                    if (bi >= process_blocks.size()) break;
                    uint32_t start = process_blocks[bi].start;
                    uint32_t bsz = process_blocks[bi].size;
                    for (uint32_t i = 0; i < bsz; i++) {
                        uint32_t row = start + i;
                        if (l_shipdate.data[row] <= DATE_THRESHOLD) continue;
                        int32_t ok = l_orderkey.data[row];
                        if (!bloom.likely_contains(ok)) continue;
                        const auto* oe = orders_map.find(ok);
                        if (!oe) continue;
                        double contrib = l_extendedprice.data[row] * (1.0 - l_discount.data[row]);
                        // Partition by orderkey
                        uint32_t part = ((uint64_t)(uint32_t)ok * 0x9E3779B97F4A7C15ULL >> 32) % (uint32_t)NPARTS;
                        agg_maps[part].update(ok, contrib, oe->o_orderdate, oe->o_shippriority);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // -------------------------------------------------------
    // Phase 4: Merge aggregation maps, top-10
    // -------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Collect all entries
        std::vector<AggEntry> results;
        results.reserve(2000000);
        for (auto& am : agg_maps) {
            for (auto& e : am.slots) {
                if (e.key != AggMap::EMPTY) {
                    results.push_back(e);
                }
            }
        }

        // Sort: revenue DESC, o_orderdate ASC
        std::partial_sort(results.begin(),
                          results.begin() + std::min((size_t)10, results.size()),
                          results.end(),
                          [](const AggEntry& a, const AggEntry& b) {
                              if (a.revenue != b.revenue) return a.revenue > b.revenue;
                              return a.o_orderdate < b.o_orderdate;
                          });

        // Write CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[16];
        int limit = std::min((int)results.size(), 10);
        for (int i = 0; i < limit; i++) {
            const auto& e = results[i];
            gendb::epoch_days_to_date_str(e.o_orderdate, date_buf);
            fprintf(f, "%d,%.2f,%s,%d\n", e.key, e.revenue, date_buf, e.o_shippriority);
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
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
