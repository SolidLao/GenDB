// Q1 iter_1: Parallel pre scan with thread-local aggregation
// GROUP BY stmt, rfile with COUNT, COUNT DISTINCT adsh, AVG line

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <unordered_set>

static const int NTHREADS = 32;

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<PreRec> pre;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    std::string meta_path = gendb_dir + "/metadata.json";

    // Build stmt/rfile name lookups
    // stmt_id and rfile_id are small numbers (< 16)
    // Build reverse mapping from metadata.json
    FILE* mf = fopen(meta_path.c_str(), "r");
    char meta_buf[65536]; size_t mn = 0;
    if (mf) { mn = fread(meta_buf, 1, sizeof(meta_buf)-1, mf); meta_buf[mn] = 0; fclose(mf); }

    // Parse stmt_ids and rfile_ids from metadata
    const char* stmt_names[32] = {};
    const char* rfile_names[32] = {};
    // Parse: "stmt_ids": {"BS": 1, "IS": 2, ...}
    auto parse_ids = [&](const char* section, const char* names[], int maxn) {
        const char* p = strstr(meta_buf, section);
        if (!p) return;
        p = strchr(p, '{');
        if (!p) return;
        p++;
        while (*p && *p != '}') {
            while (*p == ' ' || *p == ',') p++;
            if (*p != '"') break;
            p++; // skip "
            const char* key_start = p;
            while (*p && *p != '"') p++;
            size_t key_len = p - key_start;
            if (*p == '"') p++;
            while (*p == ' ' || *p == ':') p++;
            char* ep;
            int id = (int)strtol(p, &ep, 10);
            if (ep > p && id >= 0 && id < maxn) {
                char* name = (char*)malloc(key_len+1);
                memcpy(name, key_start, key_len);
                name[key_len] = 0;
                names[id] = name;
            }
            p = ep;
        }
    };
    parse_ids("stmt_ids", stmt_names, 32);
    parse_ids("rfile_ids", rfile_names, 32);

    // Thread-local aggregation: key = (stmt_id << 8) | rfile_id
    struct GroupVal {
        uint64_t cnt;
        uint64_t line_sum;
        std::unordered_set<uint32_t> adsh_set;
    };
    struct ThreadAgg {
        std::unordered_map<uint32_t, GroupVal> map;
        ThreadAgg() { map.reserve(64); }
    };
    std::vector<ThreadAgg> taggs(NTHREADS);

    uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
    std::vector<std::thread> threads;
    threads.reserve(NTHREADS);

    for (int t = 0; t < NTHREADS; t++) {
        uint64_t start = (uint64_t)t * chunk;
        uint64_t end = std::min(start + chunk, pre.count);
        if (start >= pre.count) break;
        threads.emplace_back([&, t, start, end] {
            auto& agg = taggs[t].map;
            for (uint64_t i = start; i < end; i++) {
                const PreRec& p = pre.data[i];
                if (p.stmt_id == 0) continue; // NULL stmt
                uint32_t key = ((uint32_t)p.stmt_id << 8) | p.rfile_id;
                auto& gv = agg[key];
                gv.cnt++;
                gv.line_sum += p.line;
                gv.adsh_set.insert(p.adsh_id);
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge thread-local results
    auto& base = taggs[0].map;
    for (int t = 1; t < NTHREADS; t++) {
        for (auto& [k, gv] : taggs[t].map) {
            auto& b = base[k];
            b.cnt += gv.cnt;
            b.line_sum += gv.line_sum;
            b.adsh_set.insert(gv.adsh_set.begin(), gv.adsh_set.end());
        }
    }

    struct Row {
        uint8_t stmt_id, rfile_id;
        uint64_t cnt, num_filings;
        double avg_line;
    };
    std::vector<Row> results;
    for (auto& [k, gv] : base) {
        uint8_t stmt_id = (k >> 8) & 0xFF;
        uint8_t rfile_id = k & 0xFF;
        double avg_line = gv.cnt > 0 ? (double)gv.line_sum / gv.cnt : 0.0;
        results.push_back({stmt_id, rfile_id, gv.cnt, (uint64_t)gv.adsh_set.size(), avg_line});
    }
    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.cnt > b.cnt;
    });

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q1.csv").c_str(), "w");
        fprintf(f, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (const auto& r : results) {
            const char* sn = (r.stmt_id < 32 && stmt_names[r.stmt_id]) ? stmt_names[r.stmt_id] : "";
            const char* rn = (r.rfile_id < 32 && rfile_names[r.rfile_id]) ? rfile_names[r.rfile_id] : "";
            fprintf(f, "%s,%s,%lu,%lu,%.6f\n", sn, rn, r.cnt, r.num_filings, r.avg_line);
        }
        fclose(f);
    }

    return 0;
}
