// Q3: Shipping Priority
// Join customer(BUILDING) + orders(date<1995-03-15) + lineitem(shipdate>1995-03-15)
// Group by orderkey, sum revenue, top-10 by revenue desc, orderdate asc
// Usage: ./q3 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <algorithm>
#include "mmap_utils.h"
#include "hash_utils.h"
#include "timing_utils.h"
#include "date_utils.h"

using namespace gendb;

// Date constants (stored as uint16, days from 1992-01-01, BASE=8035)
// 1995-03-15: epoch=9204, stored=9204-8035=1169
static const uint16_t ODATE_CUT = 1169;  // o_orderdate < 1995-03-15
static const uint16_t SDATE_CUT = 1169;  // l_shipdate > 1995-03-15

struct OrderInfo {
    uint16_t orderdate;
    uint8_t  shippriority;
};

struct Result {
    int32_t  orderkey;
    double   revenue;
    uint16_t orderdate;
    int32_t  shippriority;
};

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: q3 <gendb> <results>\n"); return 1; }
    GENDB_PHASE("total");

    gendb::init_date_tables();

    std::string gendb = argv[1];
    std::string rdir  = argv[2];

    // Step 1: Load customer, build bitset of BUILDING custkeys
    // c_custkey is 1..1500000 for SF10
    const int MAX_CUST = 1500001;
    uint8_t* is_building = (uint8_t*)calloc(MAX_CUST, 1);

    {
        MmapColumn<int32_t> custkey(gendb + "/customer/c_custkey.bin");
        MmapColumn<uint8_t> mktseg (gendb + "/customer/c_mktseg.bin");
        size_t n = custkey.count;
        for (size_t i = 0; i < n; i++) {
            if (mktseg.data[i] == 'B') // 'B' for BUILDING
                is_building[custkey.data[i]] = 1;
        }
    }

    // Step 2: Scan orders, filter o_orderdate < ODATE_CUT AND is_building[custkey]
    // Build hash map: orderkey -> OrderInfo
    CompactHashMap<int32_t, OrderInfo> orders_map(6000000);

    {
        MmapColumn<int32_t>  orderkey    (gendb + "/orders/o_orderkey.bin");
        MmapColumn<int32_t>  custkey     (gendb + "/orders/o_custkey.bin");
        MmapColumn<uint16_t> orderdate   (gendb + "/orders/o_orderdate.bin");
        MmapColumn<uint8_t>  shippriority(gendb + "/orders/o_shippriority.bin");

        size_t n = orderkey.count;
        for (size_t i = 0; i < n; i++) {
            int32_t ck = custkey.data[i];
            if (orderdate.data[i] < ODATE_CUT && ck < MAX_CUST && is_building[ck]) {
                OrderInfo info{orderdate.data[i], shippriority.data[i]};
                orders_map.insert(orderkey.data[i], info);
            }
        }
    }

    free(is_building);

    // Step 3: Scan lineitem from SDATE_CUT, accumulate revenue by orderkey
    // Use separate hash map for revenue
    CompactHashMap<int32_t, double> revenue_map(orders_map.size());

    {
        MmapColumn<uint16_t> shipdate(gendb + "/lineitem/l_shipdate.bin");
        MmapColumn<int32_t>  orderkey(gendb + "/lineitem/l_orderkey.bin");
        MmapColumn<int32_t>  extprice(gendb + "/lineitem/l_extprice.bin");
        MmapColumn<uint8_t>  discount(gendb + "/lineitem/l_discount.bin");

        mmap_prefetch_all(shipdate, orderkey, extprice, discount);

        size_t n = shipdate.count;
        const uint16_t* sd = shipdate.data;
        const int32_t*  ok = orderkey.data;
        const int32_t*  ep = extprice.data;
        const uint8_t*  dc = discount.data;

        // Binary search: first row with shipdate > SDATE_CUT
        size_t start = 0;
        { size_t lo=0,hi=n;
          while(lo<hi){size_t m=(lo+hi)>>1; if(sd[m]<=SDATE_CUT) lo=m+1; else hi=m;}
          start=lo; }

        for (size_t i = start; i < n; i++) {
            int32_t key = ok[i];
            if (orders_map.find(key)) {
                double rev = (double)ep[i] * (100.0 - dc[i]) / 10000.0;
                revenue_map[key] += rev;
            }
        }
    }

    // Step 4: Collect results and sort
    std::vector<Result> results;
    results.reserve(revenue_map.size());
    for (auto [key, rev] : revenue_map) {
        OrderInfo* info = orders_map.find(key);
        if (info) {
            results.push_back({key, rev, info->orderdate, info->shippriority});
        }
    }

    // Sort by revenue DESC, orderdate ASC
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b){
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.orderdate < b.orderdate;
    });

    {
        GENDB_PHASE("output");
        static const int BASE_DATE = 8035;
        std::string out = rdir + "/Q3.csv";
        FILE* f = fopen(out.c_str(), "w");
        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        int limit = std::min((int)results.size(), 10);
        char datebuf[16];
        for (int i = 0; i < limit; i++) {
            auto& r = results[i];
            gendb::epoch_days_to_date_str(BASE_DATE + r.orderdate, datebuf);
            fprintf(f, "%d,%.4f,%s,%d\n",
                    r.orderkey, r.revenue, datebuf, r.shippriority);
        }
        fclose(f);
    }
    return 0;
}
