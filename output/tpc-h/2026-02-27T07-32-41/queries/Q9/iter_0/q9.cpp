// Q9: Product Type Profit Measure
// 6-table join: part(green)+supplier+lineitem+partsupp+orders+nation
// Group by (nation, year), sum profit, sort by nation asc, year desc
// Usage: ./q9 <gendb_dir> <results_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <string>
#include "mmap_utils.h"
#include "hash_utils.h"
#include "timing_utils.h"
#include "date_utils.h"

using namespace gendb;

static const int32_t BASE_DATE = 8035;

struct Name26 { char s[26]; };

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: q9 <gendb> <results>\n"); return 1; }
    GENDB_PHASE("total");

    gendb::init_date_tables();

    std::string gendb = argv[1];
    std::string rdir  = argv[2];

    // Step 1: Load part, build bitset of green partkeys
    // p_partkey: 1..2000000 for SF10
    const int MAX_PART = 2100000;
    uint8_t* is_green = (uint8_t*)calloc(MAX_PART, 1);
    {
        MmapColumn<int32_t> partkey  (gendb + "/part/p_partkey.bin");
        MmapColumn<uint8_t> has_green(gendb + "/part/p_has_green.bin");
        size_t n = partkey.count;
        for (size_t i = 0; i < n; i++)
            if (has_green.data[i]) is_green[partkey.data[i]] = 1;
    }

    // Step 2: Load partsupp, build hash map (partkey,suppkey)->supplycost
    // Only for green parts
    CompactHashMapPair<int32_t> ps_map(500000);
    {
        MmapColumn<int32_t> partkey    (gendb + "/partsupp/ps_partkey.bin");
        MmapColumn<int32_t> suppkey    (gendb + "/partsupp/ps_suppkey.bin");
        MmapColumn<int32_t> supplycost (gendb + "/partsupp/ps_supplycost.bin");
        size_t n = partkey.count;
        for (size_t i = 0; i < n; i++) {
            int32_t pk = partkey.data[i];
            if (pk < MAX_PART && is_green[pk]) {
                ps_map.insert({pk, suppkey.data[i]}, supplycost.data[i]);
            }
        }
    }

    // Step 3: Load supplier, build dense array suppkey->nationkey
    // s_suppkey: 1..100000 for SF10
    const int MAX_SUPP = 110000;
    uint8_t* supp_nat = new uint8_t[MAX_SUPP]();
    {
        MmapColumn<int32_t> suppkey  (gendb + "/supplier/s_suppkey.bin");
        MmapColumn<uint8_t> nationkey(gendb + "/supplier/s_nationkey.bin");
        size_t n = suppkey.count;
        for (size_t i = 0; i < n; i++)
            supp_nat[suppkey.data[i]] = nationkey.data[i];
    }

    // Step 4: Load nation names (25 entries, nationkey 0..24)
    char nation_names[25][26] = {};
    {
        MmapColumn<uint8_t> natkey(gendb + "/nation/n_nationkey.bin");
        MmapColumn<Name26>  natname(gendb + "/nation/n_name.bin");
        size_t n = natkey.count;
        for (size_t i = 0; i < n; i++) {
            int k = natkey.data[i];
            if (k < 25) strncpy(nation_names[k], natname.data[i].s, 25);
        }
    }

    // Step 5: Load orders, build dense array orderkey->year_offset
    // o_orderkey: sparse in [1..60000000]
    const int MAX_OKEY = 60000001;
    uint8_t* order_year = (uint8_t*)calloc(MAX_OKEY, 1); // year - 1992
    {
        MmapColumn<int32_t>  orderkey (gendb + "/orders/o_orderkey.bin");
        MmapColumn<uint16_t> orderdate(gendb + "/orders/o_orderdate.bin");
        size_t n = orderkey.count;
        for (size_t i = 0; i < n; i++) {
            int32_t ok = orderkey.data[i];
            if (ok < MAX_OKEY) {
                int32_t epoch = BASE_DATE + orderdate.data[i];
                order_year[ok] = (uint8_t)(gendb::extract_year(epoch) - 1992);
            }
        }
    }

    // Step 6: Scan lineitem, accumulate profit by (nation, year)
    // Years: 1992-1998 -> offsets 0-6 (use 10 slots to be safe)
    double profit[25][10] = {};

    {
        MmapColumn<int32_t> orderkey(gendb + "/lineitem/l_orderkey.bin");
        MmapColumn<int32_t> partkey (gendb + "/lineitem/l_partkey.bin");
        MmapColumn<int32_t> suppkey (gendb + "/lineitem/l_suppkey.bin");
        MmapColumn<uint8_t> quantity(gendb + "/lineitem/l_quantity.bin");
        MmapColumn<int32_t> extprice(gendb + "/lineitem/l_extprice.bin");
        MmapColumn<uint8_t> discount(gendb + "/lineitem/l_discount.bin");

        mmap_prefetch_all(orderkey, partkey, suppkey, quantity, extprice, discount);

        size_t n = orderkey.count;
        const int32_t* ok = orderkey.data;
        const int32_t* pk = partkey.data;
        const int32_t* sk = suppkey.data;
        const uint8_t* qt = quantity.data;
        const int32_t* ep = extprice.data;
        const uint8_t* dc = discount.data;

        for (size_t i = 0; i < n; i++) {
            int32_t p = pk[i];
            if (p >= MAX_PART || !is_green[p]) continue;

            int32_t s = sk[i];
            int32_t* sc = ps_map.find({p, s});
            if (!sc) continue;

            int32_t o = ok[i];
            if (o >= MAX_OKEY) continue;

            uint8_t nat = supp_nat[s];
            uint8_t yr  = order_year[o];
            if (nat >= 25 || yr >= 10) continue;

            // amount = extprice*(1-discount) - supplycost*quantity
            // = ep*(100-dc)/10000 - sc*qt/100
            double amount = ((double)ep[i] * (100 - dc[i]) - (double)(*sc) * qt[i] * 100.0) / 10000.0;
            profit[nat][yr] += amount;
        }
    }

    free(is_green);
    delete[] supp_nat;
    free(order_year);

    // Collect non-zero results
    struct Row { char nation[26]; int year; double sum_profit; };
    std::vector<Row> rows;
    for (int n = 0; n < 25; n++) {
        if (!nation_names[n][0]) continue;
        for (int y = 0; y < 10; y++) {
            if (profit[n][y] != 0.0) {
                Row r;
                strncpy(r.nation, nation_names[n], 25); r.nation[25] = '\0';
                r.year = 1992 + y;
                r.sum_profit = profit[n][y];
                rows.push_back(r);
            }
        }
    }

    // Sort by nation ASC, year DESC
    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
        int c = strcmp(a.nation, b.nation);
        if (c != 0) return c < 0;
        return a.year > b.year;
    });

    {
        GENDB_PHASE("output");
        std::string out = rdir + "/Q9.csv";
        FILE* f = fopen(out.c_str(), "w");
        fprintf(f, "nation,o_year,sum_profit\n");
        for (auto& r : rows)
            fprintf(f, "%s,%d,%.4f\n", r.nation, r.year, r.sum_profit);
        fclose(f);
    }
    return 0;
}
