// Q1 iter_3: Sort-aggregate approach (replaces hash set for distinct counting)
// Key insight: extract (key, adsh_id, line) tuples → parallel sort → linear aggregation
// Avoids unordered_set entirely, uses O(N log N) sort + O(N) linear scan
// This is more cache-friendly and parallelizes better than per-group hash sets

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include <cstdio>
#include <cstring>

static const int NTHREADS = 64;

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<PreRec> pre;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    std::string meta_path = gendb_dir + "/metadata.json";

    // Parse stmt_ids and rfile_ids from metadata
    FILE* mf = fopen(meta_path.c_str(), "r");
    char meta_buf[65536]; size_t mn = 0;
    if (mf) { mn = fread(meta_buf, 1, sizeof(meta_buf)-1, mf); meta_buf[mn] = 0; fclose(mf); }

    const char* stmt_names[32] = {};
    const char* rfile_names[32] = {};
    auto parse_ids = [&](const char* section, const char* names[], int maxn) {
        const char* p = strstr(meta_buf, section);
        if (!p) return;
        p = strchr(p, '{');
        if (!p) return;
        p++;
        while (*p && *p != '}') {
            while (*p == ' ' || *p == ',') p++;
            if (*p != '"') break;
            p++;
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

    // Extract (group_key, adsh_id, line) tuples in parallel
    // group_key = (stmt_id << 8) | rfile_id
    struct Tuple { uint16_t group_key; uint32_t adsh_id; uint32_t line; };

    struct ThreadData {
        std::vector<Tuple> tuples;
        ThreadData() { tuples.reserve(200000); }
    };
    std::vector<ThreadData> tdata(NTHREADS);

    {
        uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, pre.count);
            if (start >= pre.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& tuples = tdata[t].tuples;
                for (uint64_t i = start; i < end; i++) {
                    const PreRec& p = pre.data[i];
                    if (p.stmt_id == 0) continue; // NULL stmt
                    uint16_t gk = ((uint16_t)p.stmt_id << 8) | p.rfile_id;
                    tuples.push_back({gk, p.adsh_id, (uint32_t)p.line});
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Concatenate all tuples
    size_t total = 0;
    for (auto& td : tdata) total += td.tuples.size();
    std::vector<Tuple> all_tuples;
    all_tuples.reserve(total);
    for (auto& td : tdata) for (auto& t : td.tuples) all_tuples.push_back(t);
    tdata.clear();

    // Parallel sort by (group_key, adsh_id) for linear distinct counting
    __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
        [](const Tuple& a, const Tuple& b) {
            if (a.group_key != b.group_key) return a.group_key < b.group_key;
            return a.adsh_id < b.adsh_id;
        });

    // Linear aggregation: count(*), count(DISTINCT adsh_id), sum(line)
    struct Row {
        uint16_t group_key;
        uint64_t cnt;
        uint64_t num_filings;
        uint64_t line_sum;
    };
    std::vector<Row> results;
    results.reserve(64);

    uint64_t idx = 0;
    while (idx < all_tuples.size()) {
        uint16_t gk = all_tuples[idx].group_key;
        uint64_t cnt = 0;
        uint64_t num_filings = 0;
        uint64_t line_sum = 0;
        uint32_t last_adsh = UINT32_MAX;

        while (idx < all_tuples.size() && all_tuples[idx].group_key == gk) {
            const Tuple& t = all_tuples[idx];
            cnt++;
            line_sum += t.line;
            if (t.adsh_id != last_adsh) {
                num_filings++;
                last_adsh = t.adsh_id;
            }
            idx++;
        }
        results.push_back({gk, cnt, num_filings, line_sum});
    }

    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.cnt > b.cnt;
    });

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q1.csv").c_str(), "w");
        fprintf(f, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (const auto& r : results) {
            uint8_t stmt_id = (r.group_key >> 8) & 0xFF;
            uint8_t rfile_id = r.group_key & 0xFF;
            const char* sn = (stmt_id < 32 && stmt_names[stmt_id]) ? stmt_names[stmt_id] : "";
            const char* rn = (rfile_id < 32 && rfile_names[rfile_id]) ? rfile_names[rfile_id] : "";
            double avg_line = r.cnt > 0 ? (double)r.line_sum / r.cnt : 0.0;
            fprintf(f, "%s,%s,%lu,%lu,%.6f\n", sn, rn, r.cnt, r.num_filings, avg_line);
        }
        fclose(f);
    }

    return 0;
}
