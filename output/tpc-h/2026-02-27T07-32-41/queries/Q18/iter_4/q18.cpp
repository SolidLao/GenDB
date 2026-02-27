// Q18 iter_4: Sequential scan of lineitem_by_okey (sorted by orderkey)
// ZERO atomics, ZERO false sharing, SEQUENTIAL memory access!
// Each thread handles a contiguous range of orderkeys using okey_offsets for bounds.
// Expected: ~5-10ms total (vs 208ms with random atomic scatter)
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

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    GENDB_PHASE("total");
    gendb::init_date_tables();

    std::string gd = argv[1];
    std::string rd = argv[2];

    const int MAX_OKEY = 60000001;

    // Load secondary index (lineitem sorted by orderkey)
    MmapColumn<int32_t> l_okey(gd+"/lineitem_by_okey/l_orderkey.bin");
    MmapColumn<uint8_t> l_qty (gd+"/lineitem_by_okey/l_quantity.bin");
    MmapColumn<uint32_t> okey_offsets(gd+"/lineitem_by_okey/okey_offsets.bin");
    mmap_prefetch_all(l_okey, l_qty);
    // Note: okey_offsets is used for parallelization bounds (small reads)

    size_t n = l_okey.count;  // 59986052

    // ===================================================================
    // Step 1: Parallel sequential accumulation — NO atomics!
    // Each thread handles a contiguous range of ORDERKEYS.
    // Using okey_offsets, each thread knows its ROW range in secondary index.
    // ===================================================================
    int nth = omp_get_max_threads();

    struct QualKey { int32_t okey; uint16_t sum_qty; };
    std::vector<std::vector<QualKey>> tq(nth);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& local = tq[tid];
        local.reserve(5);

        // Divide orderkey range [1, MAX_OKEY) among threads
        int32_t k_per_thread = (MAX_OKEY + nth - 1) / nth;
        int32_t k_start = tid * k_per_thread + 1;  // 1-indexed
        int32_t k_end   = std::min((int32_t)((tid+1) * k_per_thread + 1), (int32_t)MAX_OKEY);

        if (k_start >= MAX_OKEY) goto done;

        {
            // Row range in secondary index for this thread's orderkey range
            size_t row_start = (k_start < MAX_OKEY) ? (size_t)okey_offsets.data[k_start] : n;
            size_t row_end   = (k_end   < MAX_OKEY) ? (size_t)okey_offsets.data[k_end]   : n;

            // Sequential scan of sorted rows — purely sequential memory access!
            const int32_t* OK  = l_okey.data;
            const uint8_t*  QT = l_qty.data;

            int32_t cur_okey = -1;
            uint32_t cur_sum = 0;

            for (size_t i = row_start; i < row_end; i++) {
                int32_t k = OK[i];
                if (k != cur_okey) {
                    // Check previous key
                    if (cur_okey > 0 && cur_sum > 300)
                        local.push_back({cur_okey, (uint16_t)cur_sum});
                    cur_okey = k;
                    cur_sum = 0;
                }
                cur_sum += QT[i];
            }
            // Check last key
            if (cur_okey > 0 && cur_sum > 300)
                local.push_back({cur_okey, (uint16_t)cur_sum});
        }
        done:;
    }

    // Merge qualifying keys
    std::vector<QualKey> qualifying;
    qualifying.reserve(200);
    for (auto& v : tq)
        qualifying.insert(qualifying.end(), v.begin(), v.end());

    // Build lookup: okey → sum_qty (for orders scan)
    // qualifying is ~100-200 entries — use sorted + binary search
    std::sort(qualifying.begin(), qualifying.end(), [](const QualKey& a, const QualKey& b){
        return a.okey < b.okey;
    });

    auto get_qty = [&](int32_t k) -> uint16_t {
        // Binary search in qualifying (tiny array)
        int lo = 0, hi = (int)qualifying.size();
        while (lo < hi) {
            int m = (lo+hi)/2;
            if (qualifying[m].okey < k) lo = m+1;
            else hi = m;
        }
        return (lo < (int)qualifying.size() && qualifying[lo].okey == k) ? qualifying[lo].sum_qty : 0;
    };

    // ===================================================================
    // Step 2: Parallel scan of orders to collect qualifying records
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

        // Build proper open-addressing hash table: qualifying okey -> sum_qty
        // Size: next power-of-2 >= 4 * qualifying.size() for ~25% load (few collisions)
        uint32_t ht_size = 256;
        while (ht_size < (uint32_t)qualifying.size() * 4) ht_size <<= 1;
        uint32_t ht_mask = ht_size - 1;
        std::vector<int32_t>  ht_keys(ht_size, -1);
        std::vector<uint16_t> ht_vals(ht_size, 0);
        for (const auto& q : qualifying) {
            uint32_t slot = ((uint32_t)q.okey * 2654435761u) & ht_mask;
            while (ht_keys[slot] != -1) slot = (slot+1) & ht_mask;
            ht_keys[slot] = q.okey;
            ht_vals[slot] = q.sum_qty;
        }

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = tords[tid];
            local.reserve(5);

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < no; i++) {
                int32_t ok = OK[i];
                if (ok > 0 && ok < MAX_OKEY) {
                    uint32_t slot = ((uint32_t)ok * 2654435761u) & ht_mask;
                    while (ht_keys[slot] != -1 && ht_keys[slot] != ok)
                        slot = (slot+1) & ht_mask;
                    if (ht_keys[slot] == ok) {
                        local.push_back({ok, CK[i], OD[i], TP[i], ht_vals[slot]});
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
