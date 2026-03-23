// Q3: Shipping Priority — TPC-H (optimized: no merge, thread-local top-10)
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
#include "mmap_utils.h"
#include "timing_utils.h"
#include "date_utils.h"

using namespace gendb;

static constexpr int32_t DATE_CUT = 9204; // 1995-03-15
static constexpr uint32_t HT_CAP = 32768; // 32K slots, ~768KB per thread
static constexpr uint32_t HT_MASK = HT_CAP - 1;
static constexpr size_t MORSEL = 100000;
static constexpr int TOPK = 10;

struct LiIdx { uint32_t start, count; };

struct ResultRow {
    int32_t orderkey;
    double revenue;
    int32_t orderdate;
    int32_t shippriority;
};

// Sort: revenue DESC, orderdate ASC
static bool result_cmp(const ResultRow& a, const ResultRow& b) {
    if (a.revenue != b.revenue) return a.revenue > b.revenue;
    return a.orderdate < b.orderdate;
}

struct alignas(64) ThreadLocal {
    struct Slot {
        int32_t key; // 0 = empty
        double revenue;
        int32_t orderdate;
        int32_t shippriority;
    };
    Slot ht[HT_CAP];
    ResultRow top[TOPK];
    int top_count;
};

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gdir(argv[1]);
    std::string rdir(argv[2]);

    GENDB_PHASE_MS("total", total_ms);
    init_date_tables();

    // ==== Data Loading ====
    MmapColumn<uint8_t>  c_mktsegment(gdir + "/customer/c_mktsegment.bin");
    MmapColumn<int32_t>  c_custkey(gdir + "/customer/c_custkey.bin");
    MmapColumn<int32_t>  o_orderkey(gdir + "/orders/o_orderkey.bin");
    MmapColumn<int32_t>  o_custkey(gdir + "/orders/o_custkey.bin");
    MmapColumn<int32_t>  o_orderdate(gdir + "/orders/o_orderdate.bin");
    MmapColumn<int32_t>  o_shippriority(gdir + "/orders/o_shippriority.bin");
    MmapColumn<int32_t>  l_shipdate(gdir + "/lineitem/l_shipdate.bin");
    MmapColumn<double>   l_extendedprice(gdir + "/lineitem/l_extendedprice.bin");
    MmapColumn<double>   l_discount(gdir + "/lineitem/l_discount.bin");
    l_shipdate.advise_random();
    l_extendedprice.advise_random();
    l_discount.advise_random();

    // Lineitem orderkey index
    MmapColumn<char> li_raw(gdir + "/indexes/lineitem_orderkey_index.bin");
    const uint32_t li_max = *(const uint32_t*)li_raw.data;
    const LiIdx* li_idx = (const LiIdx*)(li_raw.data + 4);

    // Load BUILDING code from dictionary
    uint8_t building_code = 255;
    {
        FILE* df = fopen((gdir + "/customer/c_mktsegment_dict.bin").c_str(), "r");
        if (!df) { fprintf(stderr, "Cannot open dict\n"); return 1; }
        char line[256];
        while (fgets(line, sizeof(line), df)) {
            int code; char name[64];
            if (sscanf(line, "%d|%63s", &code, name) == 2 && strcmp(name, "BUILDING") == 0) {
                building_code = (uint8_t)code; break;
            }
        }
        fclose(df);
    }

    // ==== Build Customer BUILDING Bitset ====
    size_t n_cust = c_custkey.count;
    int32_t max_ck = 0;
    for (size_t i = 0; i < n_cust; i++)
        if (c_custkey[i] > max_ck) max_ck = c_custkey[i];
    size_t bset_words = (max_ck / 64) + 1;
    std::vector<uint64_t> cust_bset(bset_words, 0);
    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < n_cust; i++) {
            if (c_mktsegment[i] == building_code) {
                int32_t ck = c_custkey[i];
                cust_bset[ck >> 6] |= (1ULL << (ck & 63));
            }
        }
    }

    // ==== Parallel Scan Orders + Probe Lineitem ====
    unsigned nth = std::thread::hardware_concurrency();
    if (nth == 0) nth = 4;
    size_t n_orders = o_orderkey.count;

    // Allocate thread-local storage
    std::vector<ThreadLocal*> tls(nth);
    for (unsigned t = 0; t < nth; t++) {
        tls[t] = new ThreadLocal();
        memset(tls[t]->ht, 0, sizeof(tls[t]->ht));
        tls[t]->top_count = 0;
    }

    std::atomic<size_t> morsel_ctr{0};

    {
        GENDB_PHASE("main_scan");

        auto worker = [&](unsigned tid) {
            auto* local = tls[tid];
            auto* ht = local->ht;
            const uint64_t* bset = cust_bset.data();
            const int32_t* ok_d = o_orderkey.data;
            const int32_t* oc_d = o_custkey.data;
            const int32_t* od_d = o_orderdate.data;
            const int32_t* sp_d = o_shippriority.data;
            const int32_t* ls_d = l_shipdate.data;
            const double*  lp_d = l_extendedprice.data;
            const double*  ld_d = l_discount.data;

            while (true) {
                size_t start = morsel_ctr.fetch_add(MORSEL, std::memory_order_relaxed);
                if (start >= n_orders) break;
                size_t end = start + MORSEL;
                if (end > n_orders) end = n_orders;

                for (size_t i = start; i < end; i++) {
                    int32_t odate = od_d[i];
                    if (odate >= DATE_CUT) continue;

                    int32_t ck = oc_d[i];
                    if (!(bset[ck >> 6] & (1ULL << (ck & 63)))) continue;

                    int32_t ok = ok_d[i];
                    uint32_t uok = (uint32_t)ok;
                    if (uok > li_max) continue;
                    uint32_t ls = li_idx[uok].start;
                    uint32_t lc = li_idx[uok].count;
                    if (lc == 0) continue;

                    int32_t spri = sp_d[i];
                    double rev_sum = 0.0;
                    bool any = false;
                    uint32_t le = ls + lc;
                    for (uint32_t j = ls; j < le; j++) {
                        if (ls_d[j] > DATE_CUT) {
                            rev_sum += lp_d[j] * (1.0 - ld_d[j]);
                            any = true;
                        }
                    }
                    if (!any) continue;

                    // Insert into thread-local HT
                    uint32_t h = ((uint32_t)ok * 2654435761u) & HT_MASK;
                    while (true) {
                        if (ht[h].key == 0) {
                            ht[h].key = ok;
                            ht[h].revenue = rev_sum;
                            ht[h].orderdate = odate;
                            ht[h].shippriority = spri;
                            break;
                        }
                        if (ht[h].key == ok) {
                            ht[h].revenue += rev_sum;
                            break;
                        }
                        h = (h + 1) & HT_MASK;
                    }
                }
            }

            // Extract local top-10
            ResultRow* top = local->top;
            int tc = 0;
            for (uint32_t h = 0; h < HT_CAP; h++) {
                if (ht[h].key == 0) continue;
                ResultRow r{ht[h].key, ht[h].revenue, ht[h].orderdate, ht[h].shippriority};
                if (tc < TOPK) {
                    top[tc++] = r;
                    if (tc == TOPK) {
                        // Build min-heap
                        std::make_heap(top, top + TOPK, result_cmp);
                    }
                } else {
                    // Compare with heap top (smallest in top-10)
                    if (result_cmp(r, top[0])) {
                        // r is "better" — replace
                        std::pop_heap(top, top + TOPK, result_cmp);
                        top[TOPK - 1] = r;
                        std::push_heap(top, top + TOPK, result_cmp);
                    }
                }
            }
            local->top_count = tc;
        };

        std::vector<std::thread> threads;
        for (unsigned t = 0; t < nth; t++) threads.emplace_back(worker, t);
        for (auto& t : threads) t.join();
    }

    // ==== Merge thread-local top-10s ====
    std::vector<ResultRow> global;
    global.reserve(nth * TOPK);
    for (unsigned t = 0; t < nth; t++) {
        for (int k = 0; k < tls[t]->top_count; k++)
            global.push_back(tls[t]->top[k]);
        delete tls[t];
    }
    std::partial_sort(global.begin(),
                      global.begin() + std::min((int)global.size(), TOPK),
                      global.end(), result_cmp);
    if ((int)global.size() > TOPK) global.resize(TOPK);

    // ==== Output ====
    {
        GENDB_PHASE("output");
        std::string outpath = rdir + "/Q3.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) { fprintf(stderr, "Cannot open %s\n", outpath.c_str()); return 1; }
        fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char db[16];
        for (auto& r : global) {
            epoch_days_to_date_str(r.orderdate, db);
            fprintf(fp, "%d,%.4f,%s,%d\n", r.orderkey, r.revenue, db, r.shippriority);
        }
        fclose(fp);
    }

    return 0;
}
