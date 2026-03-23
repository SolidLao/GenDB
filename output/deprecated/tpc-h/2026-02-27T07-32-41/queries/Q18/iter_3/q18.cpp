// Q18 iter_3: Two-phase radix partition scatter — NO atomics, no false sharing
// Phase 1: Each thread scatters its lineitem chunk into P=8 per-thread partition buffers
// Phase 2: Per-partition sequential accumulation (single writer per partition range)
// Phase 3: Scan qty_sum to find qualifying orderkeys
// Phase 4: Scan orders for matching records, sort, output
// Usage: ./q18 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <omp.h>
#include "mmap_utils.h"
#include "timing_utils.h"
#include "date_utils.h"
using namespace gendb;

static const int32_t BASE_DATE = 8035;
struct Name26 { char s[26]; };

// Packed entry for partition buffers: 5 bytes per row
struct __attribute__((packed)) Entry {
    int32_t key;
    uint8_t qty;
};

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    GENDB_PHASE("total");
    gendb::init_date_tables();

    std::string gd = argv[1];
    std::string rd = argv[2];

    const int MAX_OKEY = 60000001;
    const int P = 8;  // 8 partitions, each covers 7.5M keys → 15MB qty_sum fits in L3!
    const int PART_SIZE = (MAX_OKEY + P - 1) / P;  // 7500001

    int nth = omp_get_max_threads();

    // ===================================================================
    // Step 1: Two-phase radix partition scatter of lineitem
    // ===================================================================

    // Thread-local partition buffers: tbufs[tid][part] = vector of Entry
    // Each thread processes n/T rows and scatters to P partition vectors
    // Phase 1a: Count pass (to allocate exact sizes → avoid realloc)
    MmapColumn<int32_t> orderkey(gd+"/lineitem/l_orderkey.bin");
    MmapColumn<uint8_t> quantity(gd+"/lineitem/l_quantity.bin");
    mmap_prefetch_all(orderkey, quantity);

    size_t n = orderkey.count;
    const int32_t* ok = orderkey.data;
    const uint8_t*  qt = quantity.data;

    // Count[tid][p] = how many entries thread tid has for partition p
    std::vector<std::vector<int32_t>> counts(nth, std::vector<int32_t>(P, 0));

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& cnt = counts[tid];
        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < n; i++) {
            int32_t k = ok[i];
            if (k > 0 && k < MAX_OKEY)
                cnt[k / PART_SIZE]++;
        }
    }

    // Compute per-thread, per-partition offsets (prefix sum within each partition)
    // global_offset[t][p] = start position of thread t's data in partition p's global buffer
    // global_size[p] = total entries for partition p
    std::vector<int32_t> global_size(P, 0);
    std::vector<std::vector<int32_t>> global_offset(nth, std::vector<int32_t>(P, 0));
    for (int p = 0; p < P; p++) {
        int off = 0;
        for (int t = 0; t < nth; t++) {
            global_offset[t][p] = off;
            off += counts[t][p];
        }
        global_size[p] = off;
    }

    // Allocate partition buffers (one flat array per partition)
    std::vector<Entry*> bufs(P);
    for (int p = 0; p < P; p++)
        bufs[p] = new Entry[global_size[p]];

    // Phase 1b: Scatter pass — each thread writes to its slice of each partition
    // Write pointers: start at global_offset[tid][p]
    std::vector<std::vector<int32_t>> wpos(nth, std::vector<int32_t>(P, 0));
    for (int t = 0; t < nth; t++)
        for (int p = 0; p < P; p++)
            wpos[t][p] = global_offset[t][p];

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& wp = wpos[tid];
        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < n; i++) {
            int32_t k = ok[i];
            if (k > 0 && k < MAX_OKEY) {
                int p = k / PART_SIZE;
                bufs[p][wp[p]++] = {k, qt[i]};
            }
        }
    }

    // ===================================================================
    // Step 2: Per-partition sequential accumulation (NO atomics!)
    // Each partition p: one thread, local qty_sum[PART_SIZE] (~15MB)
    // All threads run simultaneously (8 threads × 15MB = 120MB L3 total)
    // ===================================================================

    // One big flat qty_sum (120MB virtual, 30MB physical for occupied entries)
    // We use separate per-partition arrays so each thread's working set = PART_SIZE × 2B = 15MB
    std::vector<uint16_t*> part_sum(P);
    for (int p = 0; p < P; p++) {
        part_sum[p] = (uint16_t*)calloc(PART_SIZE, sizeof(uint16_t));
    }

    // Process partitions in parallel (one thread per partition)
    // With P=8 and nth=64, assign multiple partitions per thread? No — use omp task or manual split
    // Simple: just use #pragma omp parallel for with P iterations
    #pragma omp parallel for schedule(static) num_threads(P)
    for (int p = 0; p < P; p++) {
        uint16_t* psum = part_sum[p];
        int32_t base = p * PART_SIZE;
        Entry* buf = bufs[p];
        int32_t sz = global_size[p];
        for (int32_t i = 0; i < sz; i++) {
            int32_t rel = buf[i].key - base;
            psum[rel] += buf[i].qty;
        }
        delete[] bufs[p];
        bufs[p] = nullptr;
    }

    // ===================================================================
    // Step 3: Scan qty_sum arrays to find qualifying orderkeys (sum > 300)
    // Parallel scan across all partitions
    // ===================================================================
    std::vector<std::vector<int32_t>> tq_par(nth);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local = tq_par[tid];
        local.reserve(10);

        // Distribute work across all nth threads over P partitions
        // Each thread handles its share of the total keys
        #pragma omp for schedule(static) nowait
        for (int32_t i = 1; i < MAX_OKEY; i++) {
            int p = i / PART_SIZE;
            int rel = i - p * PART_SIZE;
            if (part_sum[p][rel] > 300)
                local.push_back(i);
        }
    }

    // Merge qualifying keys
    std::vector<int32_t> qualifying;
    qualifying.reserve(200);
    for (auto& v : tq_par)
        qualifying.insert(qualifying.end(), v.begin(), v.end());
    std::sort(qualifying.begin(), qualifying.end());

    // Build lookup: for a given orderkey, get its qty_sum
    // Use the part_sum arrays for lookup
    auto get_qty = [&](int32_t k) -> uint16_t {
        if (k <= 0 || k >= MAX_OKEY) return 0;
        int p = k / PART_SIZE;
        return part_sum[p][k - p * PART_SIZE];
    };

    // ===================================================================
    // Step 4: Scan orders to collect qualifying records
    // ===================================================================
    struct OrderRec {
        int32_t  orderkey;
        int32_t  custkey;
        uint16_t orderdate;
        int32_t  totalprice;
        uint16_t sum_qty;
    };

    std::vector<std::vector<OrderRec>> tords(nth);

    {
        MmapColumn<int32_t>  okey(gd+"/orders/o_orderkey.bin");
        MmapColumn<int32_t>  ckey(gd+"/orders/o_custkey.bin");
        MmapColumn<uint16_t> odat(gd+"/orders/o_orderdate.bin");
        MmapColumn<int32_t>  otp (gd+"/orders/o_totalprice.bin");
        mmap_prefetch_all(okey, ckey, odat, otp);

        size_t no = okey.count;
        const int32_t*  OK = okey.data;
        const int32_t*  CK = ckey.data;
        const uint16_t* OD = odat.data;
        const int32_t*  TP = otp.data;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = tords[tid];
            local.reserve(5);

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < no; i++) {
                int32_t k = OK[i];
                if (k > 0 && k < MAX_OKEY) {
                    uint16_t qs = get_qty(k);
                    if (qs > 300) {
                        local.push_back({k, CK[i], OD[i], TP[i], qs});
                    }
                }
            }
        }
    }

    // Merge order records
    std::vector<OrderRec> order_recs;
    order_recs.reserve(200);
    for (auto& v : tords)
        order_recs.insert(order_recs.end(), v.begin(), v.end());

    // Free part_sum arrays
    for (int p = 0; p < P; p++) {
        free(part_sum[p]);
        part_sum[p] = nullptr;
    }

    // Load customer names
    MmapColumn<Name26> cname(gd+"/customer/c_name.bin");

    // Sort by totalprice DESC, orderdate ASC, limit 100
    std::sort(order_recs.begin(), order_recs.end(), [](const OrderRec& a, const OrderRec& b){
        if (a.totalprice != b.totalprice) return a.totalprice > b.totalprice;
        return a.orderdate < b.orderdate;
    });

    {
        GENDB_PHASE("output");
        FILE* f = fopen((rd+"/Q18.csv").c_str(), "w");
        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
        int limit = std::min((int)order_recs.size(), 100);
        char datebuf[16];
        for (int i = 0; i < limit; i++) {
            auto& rec = order_recs[i];
            int32_t ck = rec.custkey;
            const char* name = (ck >= 1 && ck-1 < (int32_t)cname.count)
                               ? cname.data[ck-1].s : "";
            gendb::epoch_days_to_date_str(BASE_DATE + rec.orderdate, datebuf);
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                    name, ck, rec.orderkey, datebuf,
                    rec.totalprice / 100.0,
                    (double)rec.sum_qty);
        }
        fclose(f);
    }
    return 0;
}
