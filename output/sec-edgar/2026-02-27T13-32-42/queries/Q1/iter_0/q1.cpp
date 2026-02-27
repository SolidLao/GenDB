#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <unordered_set>
#include <map>
#include <cstdio>
#include "timing_utils.h"

using namespace std;

#pragma pack(push, 1)
struct PreRow {
    uint32_t adsh_id;
    uint32_t tag_id;
    uint32_t version_id;
    uint32_t plabel_id;
    uint32_t line;
    char     stmt[3];   // actual string e.g. "BS", "IS", null-terminated
    char     rfile[2];  // actual string e.g. "H", "X", null-terminated
    uint8_t  inpth;
    uint8_t  negating;
};
#pragma pack(pop)

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");

    if (argc < 3) { cerr << "Usage: " << argv[0] << " <gendb_dir> <results_dir>\n"; return 1; }
    string gendb_dir = argv[1];
    string results_dir = argv[2];

    // Load pre.bin
    vector<PreRow> pre;
    {
        FILE* f = fopen((gendb_dir + "/pre.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        pre.resize(count);
        fread(pre.data(), sizeof(PreRow), count, f);
        fclose(f);
    }

    // Group by (stmt, rfile): cnt, distinct adsh, sum of line
    // stmt is a string (up to 2 chars), rfile is a string (1 char)
    // Use string keys in a map

    struct Group {
        uint64_t cnt = 0;
        unordered_set<uint32_t> adsh_set;
        double sum_line = 0;
        string stmt_str;
        string rfile_str;
    };

    // Key: packed as stmt[0]*256+stmt[1], rfile[0]
    // Use string-based map for correctness
    map<pair<string,string>, Group> groups;

    for (const auto& row : pre) {
        if (row.stmt[0] == 0) continue; // skip NULL stmt

        string s(row.stmt);
        string r(row.rfile);

        auto key = make_pair(s, r);
        auto& g = groups[key];
        g.cnt++;
        g.adsh_set.insert(row.adsh_id);
        g.sum_line += row.line;
        g.stmt_str = s;
        g.rfile_str = r;
    }

    // Sort by cnt DESC
    struct Result {
        string stmt;
        string rfile;
        uint64_t cnt;
        uint64_t num_filings;
        double avg_line;
    };

    vector<Result> results;
    for (auto& [key, g] : groups) {
        results.push_back({g.stmt_str, g.rfile_str, g.cnt, (uint64_t)g.adsh_set.size(), g.sum_line / g.cnt});
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.cnt > b.cnt;
    });

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q1.csv").c_str(), "w");
        fprintf(f, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (auto& r : results) {
            fprintf(f, "%s,%s,%lu,%lu,%.2f\n", r.stmt.c_str(), r.rfile.c_str(), r.cnt, r.num_filings, r.avg_line);
        }
        fclose(f);
    }

    return 0;
}
