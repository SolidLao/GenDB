// Q18 — Large Volume Customer
// SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity)
// FROM customer, orders, lineitem
// WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity)>300)
//   AND c_custkey=o_custkey AND o_orderkey=l_orderkey
// GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
// ORDER BY o_totalprice DESC, o_orderdate LIMIT 100

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "timing_utils.h"
#include "date_utils.h"

using namespace std;

template<typename T>
static T* mmap_col(const char* dir, const char* name, size_t& count) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    count = st.st_size / sizeof(T);
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    return (T*)p;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr,"Usage: q18 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        size_t Nl, No, Nc;
        size_t tmp;
        // lineitem
        int32_t* li_okey = mmap_col<int32_t>(gendb_dir, "li_orderkey.bin", Nl);
        int8_t*  li_qty  = mmap_col<int8_t> (gendb_dir, "li_qty.bin",      tmp);
        // orders
        int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",  No);
        int32_t* ord_ckey = mmap_col<int32_t>(gendb_dir, "ord_custkey.bin",   tmp);
        int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin", tmp);
        double*  ord_tp   = mmap_col<double> (gendb_dir, "ord_totalprice.bin",tmp);

        int NT = min((int)thread::hardware_concurrency(), 32);

        // Step 1: Parallel scan lineitem → sum_qty per orderkey
        // Use thread-local unordered_maps, then merge
        vector<unordered_map<int32_t,int32_t>> local_qty(NT);
        size_t chunk = (Nl + NT - 1) / NT;

        vector<thread> threads;
        for (int t = 0; t < NT; ++t) {
            size_t start = t * chunk;
            size_t end   = min(start + chunk, Nl);
            threads.emplace_back([&, t, start, end]() {
                auto& m = local_qty[t];
                m.reserve(200000);
                for (size_t i = start; i < end; ++i)
                    m[li_okey[i]] += (int32_t)li_qty[i];
            });
        }
        for (auto& th : threads) th.join();

        // Merge into single map
        unordered_map<int32_t,int32_t> qty_map;
        qty_map.reserve(No * 2);
        for (auto& m : local_qty)
            for (auto& [k,v] : m)
                qty_map[k] += v;

        // Filter: orderkeys with sum_qty > 300
        unordered_map<int32_t,int32_t> qualifying; // orderkey → sum_qty
        qualifying.reserve(200);
        for (auto& [k,v] : qty_map)
            if (v > 300) qualifying[k] = v;

        // Step 2: Join qualifying orderkeys with orders
        struct OrderRow {
            int32_t orderkey, custkey, orderdate;
            double  totalprice;
        };
        vector<OrderRow> ord_rows;
        ord_rows.reserve(qualifying.size());
        for (size_t i = 0; i < No; ++i) {
            auto it = qualifying.find(ord_okey[i]);
            if (it != qualifying.end())
                ord_rows.push_back({ord_okey[i], ord_ckey[i], ord_odat[i], ord_tp[i]});
        }

        // Build result: for each qualifying order, compute c_name from custkey
        struct Row {
            int32_t custkey, orderkey, orderdate;
            double  totalprice;
            int32_t sum_qty;
        };
        vector<Row> rows;
        rows.reserve(ord_rows.size());
        for (auto& o : ord_rows) {
            rows.push_back({o.custkey, o.orderkey, o.orderdate,
                            o.totalprice, qualifying[o.orderkey]});
        }

        // Sort: totalprice DESC, orderdate ASC
        sort(rows.begin(), rows.end(), [](const Row& a, const Row& b){
            if (a.totalprice != b.totalprice) return a.totalprice > b.totalprice;
            return a.orderdate < b.orderdate;
        });
        if ((int)rows.size() > 100) rows.resize(100);

        {
            GENDB_PHASE_MS("output", output_ms);
            char outpath[512];
            snprintf(outpath, sizeof(outpath), "%s/Q18.csv", results_dir);
            FILE* out = fopen(outpath, "w");
            fprintf(out, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
            char datebuf[16];
            for (auto& row : rows) {
                gendb::epoch_days_to_date_str(row.orderdate, datebuf);
                fprintf(out, "Customer#%09d,%d,%d,%s,%.2f,%.2f\n",
                        row.custkey, row.custkey,
                        row.orderkey, datebuf,
                        row.totalprice, (double)row.sum_qty);
            }
            fclose(out);
        }
    }
    return 0;
}
