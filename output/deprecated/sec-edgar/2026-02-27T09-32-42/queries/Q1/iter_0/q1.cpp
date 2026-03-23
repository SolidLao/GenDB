// Q1: Scan pre table, group by (stmt, rfile), count/distinct adsh/avg line
// SELECT stmt, rfile, COUNT(*) AS cnt,
//        COUNT(DISTINCT adsh) AS num_filings, AVG(line) AS avg_line_num
// FROM pre WHERE stmt IS NOT NULL GROUP BY stmt, rfile ORDER BY cnt DESC

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <unordered_set>
#include <map>

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // Load pre.bin
    BinCol<PreRec> pre;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    // Load dicts
    Dict stmt_dict, rfile_dict;
    stmt_dict.load((gendb_dir + "/dict_stmt.bin").c_str());
    rfile_dict.load((gendb_dir + "/dict_rfile.bin").c_str());

    // Aggregation: key = (stmt_id, rfile_id) → {count, distinct_adsh_set, line_sum}
    // stmt_id 0 = empty/NULL → skip those
    // Use a flat map since there are only ~20 unique (stmt, rfile) pairs

    struct GroupState {
        uint64_t cnt = 0;
        uint64_t line_sum = 0;
        std::unordered_set<uint32_t> adsh_set;
    };

    // Encode group key as uint32: (stmt_id << 8) | rfile_id
    std::unordered_map<uint32_t, GroupState> groups;
    groups.reserve(64);

    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& r = pre.data[i];
        if (r.stmt_id == 0) continue; // NULL stmt
        uint32_t key = ((uint32_t)r.stmt_id << 8) | r.rfile_id;
        auto& g = groups[key];
        g.cnt++;
        g.line_sum += r.line;
        g.adsh_set.insert(r.adsh_id);
    }

    // Sort by cnt DESC
    std::vector<uint32_t> keys;
    keys.reserve(groups.size());
    for (auto& [k, v] : groups) keys.push_back(k);
    std::sort(keys.begin(), keys.end(), [&](uint32_t a, uint32_t b) {
        return groups[a].cnt > groups[b].cnt;
    });

    // Write output
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q1.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        fprintf(f, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (uint32_t key : keys) {
            uint32_t stmt_id = (key >> 8) & 0xFF;
            uint32_t rfile_id = key & 0xFF;
            auto& g = groups[key];
            double avg_line = (g.cnt > 0) ? (double)g.line_sum / g.cnt : 0.0;

            write_csv_str(f, stmt_dict, stmt_id);
            fputc(',', f);
            write_csv_str(f, rfile_dict, rfile_id);
            fprintf(f, ",%llu,%zu,%.2f\n",
                (unsigned long long)g.cnt,
                g.adsh_set.size(),
                avg_line);
        }
        fclose(f);
    }

    return 0;
}
