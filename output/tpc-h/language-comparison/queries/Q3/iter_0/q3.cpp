// Q3: Shipping Priority — GenDB generated code
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
#include <queue>
#include "mmap_utils.h"
#include "timing_utils.h"
#include "date_utils.h"

using namespace gendb;

// ---- Aggregation entry ----
struct AggEntry {
    int32_t orderkey;
    double  revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// ---- Open-addressing hash map for aggregation ----
struct AggMap {
    struct Slot {
        int32_t key;      // orderkey, 0 = empty
        double  revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };
    std::vector<Slot> slots;
    uint32_t mask;

    void init(uint32_t capacity) {
        // round up to power of 2
        uint32_t cap = 1;
        while (cap < capacity) cap <<= 1;
        mask = cap - 1;
        slots.resize(cap);
        memset(slots.data(), 0, cap * sizeof(Slot));
    }

    void insert(int32_t ok, double rev, int32_t odate, int32_t shippr) {
        uint32_t h = (uint32_t)ok * 2654435761u;
        while (true) {
            uint32_t idx = h & mask;
            auto& s = slots[idx];
            if (s.key == 0) {
                s.key = ok;
                s.revenue = rev;
                s.o_orderdate = odate;
                s.o_shippriority = shippr;
                return;
            }
            if (s.key == ok) {
                s.revenue += rev;
                return;
            }
            h++;
        }
    }

    void merge_from(const AggMap& other) {
        for (auto& s : other.slots) {
            if (s.key != 0) {
                insert(s.key, s.revenue, s.o_orderdate, s.o_shippriority);
            }
        }
    }
};

// ---- Lineitem orderkey index entry ----
struct LiIdxEntry {
    uint32_t start;
    uint32_t count;
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb(argv[1]);
    std::string results(argv[2]);

    GENDB_PHASE_MS("total", total_ms);

    // ---- Constants ----
    const int32_t DATE_19950315 = 9204;

    // ==== Phase 1: Data Loading ====
    double load_ms = 0;
    {
        GENDB_PHASE_MS("data_loading", _lms);

        // We mmap lazily below, but declare here
        load_ms = 0; // will be set by destructor
    }

    // ---- mmap all columns ----
    MmapColumn<uint8_t>  c_mktsegment(gendb + "/customer/c_mktsegment.bin");
    MmapColumn<int32_t>  c_custkey(gendb + "/customer/c_custkey.bin");
    MmapColumn<int32_t>  o_orderkey(gendb + "/orders/o_orderkey.bin");
    MmapColumn<int32_t>  o_custkey(gendb + "/orders/o_custkey.bin");
    MmapColumn<int32_t>  o_orderdate(gendb + "/orders/o_orderdate.bin");
    MmapColumn<int32_t>  o_shippriority(gendb + "/orders/o_shippriority.bin");
    MmapColumn<int32_t>  l_shipdate(gendb + "/lineitem/l_shipdate.bin");
    MmapColumn<double>   l_extendedprice(gendb + "/lineitem/l_extendedprice.bin");
    MmapColumn<double>   l_discount(gendb + "/lineitem/l_discount.bin");

    // Advise random for lineitem (index-probed)
    l_shipdate.advise_random();
    l_extendedprice.advise_random();
    l_discount.advise_random();

    // ---- Load lineitem orderkey index ----
    MmapColumn<char> li_idx_raw(gendb + "/indexes/lineitem_orderkey_index.bin");
    const uint32_t li_max_ok = *(const uint32_t*)li_idx_raw.data;
    const LiIdxEntry* li_idx = (const LiIdxEntry*)(li_idx_raw.data + 4);

    // ==== Phase 2: Build customer BUILDING bitset ====
    std::vector<uint64_t> cust_bitset;
    {
        GENDB_PHASE("build_customer_bitset");

        // Load dictionary
        uint8_t building_code = 255;
        {
            FILE* df = fopen((gendb + "/customer/c_mktsegment_dict.bin").c_str(), "r");
            if (!df) { fprintf(stderr, "Cannot open mktsegment dict\n"); return 1; }
            char line[256];
            while (fgets(line, sizeof(line), df)) {
                int code; char name[64];
                if (sscanf(line, "%d|%s", &code, name) == 2) {
                    if (strcmp(name, "BUILDING") == 0) {
                        building_code = (uint8_t)code;
                        break;
                    }
                }
            }
            fclose(df);
        }
        if (building_code == 255) { fprintf(stderr, "BUILDING code not found\n"); return 1; }

        // Build bitset indexed by custkey value (dense 1..1500000)
        size_t n_cust = c_custkey.count;
        // Find max custkey for bitset sizing
        int32_t max_ck = 0;
        for (size_t i = 0; i < n_cust; i++) {
            if (c_custkey[i] > max_ck) max_ck = c_custkey[i];
        }
        size_t bitset_words = (max_ck + 64) / 64;
        cust_bitset.resize(bitset_words, 0);

        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == building_code) {
                int32_t ck = c_custkey[i];
                cust_bitset[ck >> 6] |= (1ULL << (ck & 63));
            }
        }
    }

    // ==== Phase 3: Parallel scan orders + probe lineitem ====
    size_t n_orders = o_orderkey.count;
    unsigned n_threads = std::thread::hardware_concurrency();
    if (n_threads == 0) n_threads = 4;
    if (n_threads > 64) n_threads = 64;

    std::vector<AggMap> thread_maps(n_threads);
    {
        GENDB_PHASE("scan_orders_probe_lineitem");

        const size_t MORSEL = 100000;
        std::atomic<size_t> next_morsel(0);

        auto worker = [&](unsigned tid) {
            auto& lmap = thread_maps[tid];
            lmap.init(1 << 18); // 256K slots initial

            const int32_t* ok_data = o_orderkey.data;
            const int32_t* ock_data = o_custkey.data;
            const int32_t* od_data = o_orderdate.data;
            const int32_t* osp_data = o_shippriority.data;
            const int32_t* lsd_data = l_shipdate.data;
            const double*  lep_data = l_extendedprice.data;
            const double*  ld_data  = l_discount.data;
            const uint64_t* bset = cust_bitset.data();

            while (true) {
                size_t start = next_morsel.fetch_add(MORSEL, std::memory_order_relaxed);
                if (start >= n_orders) break;
                size_t end = std::min(start + MORSEL, n_orders);

                for (size_t i = start; i < end; i++) {
                    // Filter: o_orderdate < DATE_19950315
                    int32_t odate = od_data[i];
                    if (odate >= DATE_19950315) continue;

                    // Check customer bitset
                    int32_t ck = ock_data[i];
                    if (!(bset[ck >> 6] & (1ULL << (ck & 63)))) continue;

                    // Qualifying order — probe lineitem index
                    int32_t ok = ok_data[i];
                    if ((uint32_t)ok > li_max_ok) continue;
                    uint32_t li_start = li_idx[ok].start;
                    uint32_t li_count = li_idx[ok].count;
                    if (li_count == 0) continue;

                    int32_t shippr = osp_data[i];

                    for (uint32_t j = li_start; j < li_start + li_count; j++) {
                        if (lsd_data[j] > DATE_19950315) {
                            double rev = lep_data[j] * (1.0 - ld_data[j]);
                            lmap.insert(ok, rev, odate, shippr);
                        }
                    }
                }
            }
        };

        std::vector<std::thread> threads;
        for (unsigned t = 0; t < n_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& t : threads) t.join();
    }

    // ==== Phase 4: Merge and Top-K ====
    {
        GENDB_PHASE("merge_topk");

        // Merge all thread maps into thread_maps[0]
        for (unsigned t = 1; t < n_threads; t++) {
            thread_maps[0].merge_from(thread_maps[t]);
            // Free memory
            thread_maps[t].slots.clear();
            thread_maps[t].slots.shrink_to_fit();
        }

        // Top-10 using min-heap
        // Comparator: revenue DESC, o_orderdate ASC
        // Min-heap: pop smallest revenue (or largest date if tied)
        auto cmp = [](const AggEntry& a, const AggEntry& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue; // min-heap: larger stays
            return a.o_orderdate < b.o_orderdate; // for same revenue, smaller date stays
        };
        std::priority_queue<AggEntry, std::vector<AggEntry>, decltype(cmp)> heap(cmp);

        auto& slots = thread_maps[0].slots;
        for (auto& s : slots) {
            if (s.key == 0) continue;
            AggEntry e{s.key, s.revenue, s.o_orderdate, s.o_shippriority};
            if (heap.size() < 10) {
                heap.push(e);
            } else if (e.revenue > heap.top().revenue ||
                       (e.revenue == heap.top().revenue && e.o_orderdate < heap.top().o_orderdate)) {
                heap.pop();
                heap.push(e);
            }
        }

        // Extract top-10 in sorted order
        std::vector<AggEntry> top10;
        while (!heap.empty()) {
            top10.push_back(heap.top());
            heap.pop();
        }
        // Sort: revenue DESC, o_orderdate ASC
        std::sort(top10.begin(), top10.end(), [](const AggEntry& a, const AggEntry& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        });

        // ==== Output ====
        {
            GENDB_PHASE("output");
            init_date_tables();

            std::string outpath = results + "/Q3.csv";
            FILE* fp = fopen(outpath.c_str(), "w");
            if (!fp) { fprintf(stderr, "Cannot open output %s\n", outpath.c_str()); return 1; }
            fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

            char datebuf[16];
            for (auto& e : top10) {
                epoch_days_to_date_str(e.o_orderdate, datebuf);
                fprintf(fp, "%d,%.4f,%s,%d\n", e.orderkey, e.revenue, datebuf, e.o_shippriority);
            }
            fclose(fp);
        }
    }

    return 0;
}
