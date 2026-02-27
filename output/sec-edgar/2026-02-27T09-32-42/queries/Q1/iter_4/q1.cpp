// Q1 iter_4: Bitmap-based distinct adsh counting (replaces unordered_set)
// Key insights:
// 1. Pre-enumerate ~40 unique (stmt_id, rfile_id) group keys
// 2. Per-thread: flat bitmap per group (1407 uint64 words = 11KB per group)
//    vs. unordered_set<uint32_t> which has O(N) nodes
// 3. Merge: OR bitmaps across 64 threads + popcount → O(N_groups × BM_WORDS)
// 4. No hash map merging overhead, cache-friendly bitmap operations

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <cstdio>
#include <cstring>
#include <bit>  // std::popcount (C++20)

static const int NTHREADS = 64;
static const uint32_t MAX_ADSH = 90000; // safe upper bound
static const int BM_WORDS = (MAX_ADSH + 63) / 64; // = 1407

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<PreRec> pre;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    std::string meta_path = gendb_dir + "/metadata.json";

    // Parse stmt_ids and rfile_ids
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

    // Step 1: Quick single-threaded pass to enumerate all unique group keys
    // group_key = (stmt_id << 8) | rfile_id, max = 65535
    static uint8_t gk_to_idx[65536];
    memset(gk_to_idx, 255, sizeof(gk_to_idx)); // 255 = unused
    uint16_t used_gks[256]; // at most 256 distinct groups
    int num_groups = 0;

    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        if (p.stmt_id == 0) continue;
        uint32_t gk = ((uint32_t)p.stmt_id << 8) | p.rfile_id;
        if (gk_to_idx[gk] == 255) {
            gk_to_idx[gk] = (uint8_t)num_groups;
            used_gks[num_groups++] = (uint16_t)gk;
            if (num_groups >= 255) break; // safety limit
        }
    }

    // Step 2: Parallel scan with per-thread bitmaps
    // Per-thread data: counts + line_sums + bitmaps
    // Memory per thread: num_groups × (8 + 8 + BM_WORDS×8) = num_groups × (16 + 11256) ≈ 450KB for 40 groups
    struct ThreadData {
        std::vector<uint64_t> cnt;       // per-group count
        std::vector<uint64_t> lsum;      // per-group line sum
        std::vector<uint64_t> bitmaps;   // per-group bitmaps: [group_idx * BM_WORDS .. (group_idx+1)*BM_WORDS)
        ThreadData() {}
        void init(int ng) {
            cnt.assign(ng, 0);
            lsum.assign(ng, 0);
            bitmaps.assign((size_t)ng * BM_WORDS, 0);
        }
    };
    std::vector<ThreadData> tdata(NTHREADS);
    for (auto& td : tdata) td.init(num_groups);

    {
        uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, pre.count);
            if (start >= pre.count) break;
            threads.emplace_back([&, t, start, end] {
                ThreadData& td = tdata[t];
                auto* cnt_p = td.cnt.data();
                auto* lsum_p = td.lsum.data();
                auto* bm_p = td.bitmaps.data();

                for (uint64_t i = start; i < end; i++) {
                    const PreRec& p = pre.data[i];
                    if (p.stmt_id == 0) continue;
                    uint32_t gk = ((uint32_t)p.stmt_id << 8) | p.rfile_id;
                    uint8_t gidx = gk_to_idx[gk];
                    if (gidx == 255) continue;

                    cnt_p[gidx]++;
                    lsum_p[gidx] += p.line;

                    // Set bitmap bit for this adsh_id
                    if (p.adsh_id < MAX_ADSH) {
                        uint64_t word_idx = (size_t)gidx * BM_WORDS + (p.adsh_id >> 6);
                        bm_p[word_idx] |= (1ULL << (p.adsh_id & 63));
                    }
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Step 3: Merge counts and bitmaps across threads
    // Sum counts and line_sums; OR bitmaps
    std::vector<uint64_t> total_cnt(num_groups, 0);
    std::vector<uint64_t> total_lsum(num_groups, 0);
    std::vector<uint64_t> merged_bm((size_t)num_groups * BM_WORDS, 0);

    for (int t = 0; t < NTHREADS; t++) {
        if (tdata[t].cnt.empty()) break;
        for (int g = 0; g < num_groups; g++) {
            total_cnt[g] += tdata[t].cnt[g];
            total_lsum[g] += tdata[t].lsum[g];
        }
        // OR bitmaps
        const uint64_t* src = tdata[t].bitmaps.data();
        uint64_t* dst = merged_bm.data();
        size_t sz = (size_t)num_groups * BM_WORDS;
        for (size_t w = 0; w < sz; w++) dst[w] |= src[w];
    }
    tdata.clear();

    // Step 4: Build result rows with popcount for distinct adsh
    struct Row {
        uint16_t group_key;
        uint64_t cnt, num_filings, lsum;
    };
    std::vector<Row> results;
    results.reserve(num_groups);

    for (int g = 0; g < num_groups; g++) {
        if (total_cnt[g] == 0) continue;
        // Popcount the bitmap for this group
        uint64_t distinct = 0;
        const uint64_t* bm = merged_bm.data() + (size_t)g * BM_WORDS;
        for (int w = 0; w < BM_WORDS; w++) {
            distinct += __builtin_popcountll(bm[w]);
        }
        results.push_back({used_gks[g], total_cnt[g], distinct, total_lsum[g]});
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
            double avg_line = r.cnt > 0 ? (double)r.lsum / r.cnt : 0.0;
            fprintf(f, "%s,%s,%lu,%lu,%.6f\n", sn, rn, r.cnt, r.num_filings, avg_line);
        }
        fclose(f);
    }

    return 0;
}
