// Q1 iter_2: Bitmap-based distinct adsh counting, parallel scan
// Only 14 groups (stmt x rfile combos), use bit arrays for O(1) distinct count

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <atomic>
#include <mutex>

static const int NTHREADS = 64;
static const int MAX_GROUPS = 256; // stmt_id < 16, rfile_id < 16 → 256 combos max
static const int MAX_ADSH = 90000; // adsh_ids go up to ~86K
static const int BITMAP_WORDS_G = (MAX_ADSH + 63) / 64;

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<PreRec> pre;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    std::string meta_path = gendb_dir + "/metadata.json";

    // Thread-local: per group, count rows and sum lines
    // Groups indexed by (stmt_id << 4) | rfile_id
    struct GroupCount { uint64_t cnt, line_sum; };
    struct ThreadData {
        GroupCount groups[MAX_GROUPS] = {};
        // Bitmap for distinct adsh per group: MAX_ADSH bits per group
        std::vector<uint64_t> bitmap; // [MAX_GROUPS * BITMAP_WORDS_G]
        ThreadData() : bitmap(MAX_GROUPS * BITMAP_WORDS_G, 0) {}
    };
    std::vector<ThreadData> tdata(NTHREADS);

    uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
    std::vector<std::thread> threads;
    threads.reserve(NTHREADS);

    for (int t = 0; t < NTHREADS; t++) {
        uint64_t start = (uint64_t)t * chunk;
        uint64_t end = std::min(start + chunk, pre.count);
        if (start >= pre.count) break;
        threads.emplace_back([&, t, start, end] {
            auto& td = tdata[t];
            for (uint64_t i = start; i < end; i++) {
                const PreRec& p = pre.data[i];
                if (p.stmt_id == 0) continue;
                int grp = (p.stmt_id << 4) | p.rfile_id;
                if (grp >= MAX_GROUPS) continue;
                td.groups[grp].cnt++;
                td.groups[grp].line_sum += p.line;
                // Set bit for adsh_id in bitmap
                if (p.adsh_id < MAX_ADSH) {
                    int word_idx = p.adsh_id / 64;
                    uint64_t bit = 1ULL << (p.adsh_id % 64);
                    td.bitmap[(size_t)grp * BITMAP_WORDS_G + word_idx] |= bit;
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge thread results
    // Merge group counts
    GroupCount merged_groups[MAX_GROUPS] = {};
    std::vector<uint64_t> merged_bitmap(MAX_GROUPS * BITMAP_WORDS_G, 0);

    for (int t = 0; t < NTHREADS; t++) {
        for (int g = 0; g < MAX_GROUPS; g++) {
            merged_groups[g].cnt += tdata[t].groups[g].cnt;
            merged_groups[g].line_sum += tdata[t].groups[g].line_sum;
        }
        for (size_t j = 0; j < (size_t)(MAX_GROUPS * BITMAP_WORDS_G); j++) {
            merged_bitmap[j] |= tdata[t].bitmap[j];
        }
    }

    // Parse metadata for stmt/rfile names
    FILE* mf = fopen(meta_path.c_str(), "r");
    char meta_buf[65536]; size_t mn = 0;
    if (mf) { mn = fread(meta_buf, 1, sizeof(meta_buf)-1, mf); meta_buf[mn] = 0; fclose(mf); }

    const char* stmt_names[32] = {};
    const char* rfile_names[32] = {};
    auto parse_ids = [&](const char* section, const char* names[], int maxn) {
        const char* p = strstr(meta_buf, section);
        if (!p) return;
        p = strchr(p, '{');
        if (!p) return; p++;
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

    // Collect results
    struct Row {
        uint8_t stmt_id, rfile_id;
        uint64_t cnt, num_filings;
        double avg_line;
    };
    std::vector<Row> results;
    for (int g = 0; g < MAX_GROUPS; g++) {
        if (merged_groups[g].cnt == 0) continue;
        uint8_t stmt_id = (g >> 4) & 0xFF;
        uint8_t rfile_id = g & 0xF;
        // Count distinct adsh using bitmap popcount
        uint64_t distinct = 0;
        for (int w = 0; w < BITMAP_WORDS_G; w++) {
            distinct += __builtin_popcountll(merged_bitmap[(size_t)g * BITMAP_WORDS_G + w]);
        }
        double avg_line = (merged_groups[g].cnt > 0) ?
            (double)merged_groups[g].line_sum / merged_groups[g].cnt : 0.0;
        results.push_back({stmt_id, rfile_id, merged_groups[g].cnt, distinct, avg_line});
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
