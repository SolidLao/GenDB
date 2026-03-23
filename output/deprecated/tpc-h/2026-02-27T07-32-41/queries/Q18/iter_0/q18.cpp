// Q18: Large Volume Customer
// Find orders where sum(l_quantity) > 300, join with customer+orders, top-100
// Usage: ./q18 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <algorithm>
#include "mmap_utils.h"
#include "timing_utils.h"
#include "date_utils.h"

using namespace gendb;

static const int32_t BASE_DATE = 8035;

struct Name26 { char s[26]; };

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: q18 <gendb> <results>\n"); return 1; }
    GENDB_PHASE("total");

    gendb::init_date_tables();

    std::string gendb = argv[1];
    std::string rdir  = argv[2];

    // Step 1: Scan lineitem, accumulate sum_qty per orderkey using dense array
    // orderkey range: 1..60000000 for SF10 (sparse, 25% filled)
    const int MAX_OKEY = 60000001;
    uint16_t* qty_sum = (uint16_t*)calloc(MAX_OKEY, sizeof(uint16_t));

    {
        MmapColumn<int32_t> orderkey(gendb + "/lineitem/l_orderkey.bin");
        MmapColumn<uint8_t> quantity(gendb + "/lineitem/l_quantity.bin");
        mmap_prefetch_all(orderkey, quantity);

        size_t n = orderkey.count;
        const int32_t* ok = orderkey.data;
        const uint8_t* qt = quantity.data;
        for (size_t i = 0; i < n; i++) {
            int32_t key = ok[i];
            if (key < MAX_OKEY)
                qty_sum[key] += qt[i];
        }
    }

    // Step 2: Find qualifying orderkeys (sum_qty > 300)
    std::vector<int32_t> qualifying;
    qualifying.reserve(200);
    for (int32_t i = 1; i < MAX_OKEY; i++) {
        if (qty_sum[i] > 300) qualifying.push_back(i);
    }

    // Build a small hash set for fast lookup
    // Since qualifying is tiny, use sorted vector + binary search
    std::sort(qualifying.begin(), qualifying.end());

    // Step 3: Scan orders, collect info for qualifying orderkeys
    struct OrderRec {
        int32_t  orderkey;
        int32_t  custkey;
        uint16_t orderdate;
        int32_t  totalprice;
        uint16_t sum_qty; // from qty_sum array
    };
    std::vector<OrderRec> order_recs;
    order_recs.reserve(qualifying.size());

    {
        MmapColumn<int32_t>  orderkey    (gendb + "/orders/o_orderkey.bin");
        MmapColumn<int32_t>  custkey     (gendb + "/orders/o_custkey.bin");
        MmapColumn<uint16_t> orderdate   (gendb + "/orders/o_orderdate.bin");
        MmapColumn<int32_t>  totalprice  (gendb + "/orders/o_totalprice.bin");

        size_t n = orderkey.count;
        for (size_t i = 0; i < n; i++) {
            int32_t ok = orderkey.data[i];
            if (ok < MAX_OKEY && qty_sum[ok] > 300) {
                order_recs.push_back({ok, custkey.data[i], orderdate.data[i],
                                      totalprice.data[i], qty_sum[ok]});
            }
        }
    }
    free(qty_sum);

    // Step 4: Load customer names (dense array indexed by custkey-1)
    // c_name.bin stores Name26 structs in custkey order (1..1500000)
    MmapColumn<Name26> cname(gendb + "/customer/c_name.bin");

    // Step 5: Sort by totalprice DESC, orderdate ASC, limit 100
    std::sort(order_recs.begin(), order_recs.end(), [](const OrderRec& a, const OrderRec& b){
        if (a.totalprice != b.totalprice) return a.totalprice > b.totalprice;
        return a.orderdate < b.orderdate;
    });

    {
        GENDB_PHASE("output");
        std::string out = rdir + "/Q18.csv";
        FILE* f = fopen(out.c_str(), "w");
        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        int limit = std::min((int)order_recs.size(), 100);
        char datebuf[16];
        for (int i = 0; i < limit; i++) {
            auto& r = order_recs[i];
            int32_t ck = r.custkey;
            const char* name = (ck >= 1 && ck-1 < (int32_t)cname.count)
                               ? cname.data[ck-1].s : "";
            gendb::epoch_days_to_date_str(BASE_DATE + r.orderdate, datebuf);
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                    name, ck, r.orderkey, datebuf,
                    r.totalprice / 100.0,
                    (double)r.sum_qty);
        }
        fclose(f);
    }
    return 0;
}
