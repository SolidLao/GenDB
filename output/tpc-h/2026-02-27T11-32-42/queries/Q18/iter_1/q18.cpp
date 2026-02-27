// Q18 iter_1 — Uses li2_orderkey.bin (sorted by orderkey) for sequential group-by.
// Find orderkeys with sum_qty > 300, then join with orders and customer.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <algorithm>
#include <unordered_map>
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
    madvise(p, st.st_size, MADV_SEQUENTIAL);
    close(fd);
    return (T*)p;
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: q18 <gendb_dir> <results_dir>\n"); return 1; }
    const char* gendb_dir   = argv[1];
    const char* results_dir = argv[2];

    gendb::init_date_tables();

    double output_ms = 0;
    {
        GENDB_PHASE("total");

        // Load lineitem secondary sort (by orderkey)
        size_t N, No, tmp;
        int32_t* li2_okey = mmap_col<int32_t>(gendb_dir, "li2_orderkey.bin", N);
        int8_t*  li2_qty  = mmap_col<int8_t> (gendb_dir, "li2_qty.bin",      tmp);

        // Load orders columns
        int32_t* ord_okey = mmap_col<int32_t>(gendb_dir, "ord_orderkey.bin",  No);
        int32_t* ord_ckey = mmap_col<int32_t>(gendb_dir, "ord_custkey.bin",   tmp);
        int32_t* ord_odat = mmap_col<int32_t>(gendb_dir, "ord_orderdate.bin", tmp);
        double*  ord_tp   = mmap_col<double> (gendb_dir, "ord_totalprice.bin",tmp);

        // Step 1: Sequential scan of li2 (sorted by orderkey), group by orderkey
        // Find groups with sum_qty > 300
        unordered_map<int32_t,int32_t> qualifying;
        qualifying.reserve(256);

        {
            size_t i = 0;
            while (i < N) {
                int32_t ok = li2_okey[i];
                int32_t sum = 0;
                while (i < N && li2_okey[i] == ok) {
                    sum += (int32_t)li2_qty[i];
                    ++i;
                }
                if (sum > 300) qualifying[ok] = sum;
            }
        }

        // Step 2: Scan orders to find qualifying order details
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

        // Build result rows
        struct Row {
            int32_t custkey, orderkey, orderdate;
            double  totalprice;
            int32_t sum_qty;
        };
        vector<Row> rows;
        rows.reserve(ord_rows.size());
        for (auto& o : ord_rows)
            rows.push_back({o.custkey, o.orderkey, o.orderdate,
                            o.totalprice, qualifying[o.orderkey]});

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
                        row.custkey, row.custkey, row.orderkey, datebuf,
                        row.totalprice, (double)row.sum_qty);
            }
            fclose(out);
        }
    }
    return 0;
}
