// Q1 iter_5: Eliminate single-threaded pre-scan by parallelizing group enumeration
// Key improvements over iter_4:
// 1. Phase 1 (group enumeration): parallel instead of single-threaded
//    Each thread scans its chunk, builds local set of group keys (~40 unique)
//    Merge: collect unique gks from all threads → build gk_to_idx
// 2. Phase 2 (bitmap scan): same as iter_4
// Expected saving: ~15-20ms from parallelizing the enumeration scan

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <cstdio>
#include <cstring>
#include <bit>  // std::popcount (C++20)

static const int NTHREADS = 64;
static const uint32_t MAX_ADSH = 90000;
static const int BM_WORDS = (MAX_ADSH + 63) / 64; // = 1407

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

    // Step 1: Parallel group key enumeration (parallelized vs iter_4's single-threaded scan)
    // Each thread scans its chunk and records unique (stmt_id, rfile_id) group keys
    static uint8_t gk_to_idx[65536];
    memset(gk_to_idx, 255, sizeof(gk_to_idx));
    uint16_t used_gks[256];
    int num_groups = 0;

    {
        GENDB_PHASE("group_enum");
        // Per-thread: small bitset tracking seen group keys (gk = stmt_id<<8|rfile_id, max 65536)
        struct ThreadEnum {
            std::vector<uint16_t> gks; // unique gks found in this thread's chunk
            uint8_t seen[256]; // seen flags per high byte (stmt_id), low byte mask approach
            // Actually use a flat bool array for simplicity (256 bytes, fits in L1)
            uint8_t seen_flat[65536]; // 64KB — too large for L1, use smaller
            // Since stmt_id is small (≤32) and rfile_id is small (≤8), gk ≤ 32*256 = 8192
            // Use seen_flat[8192] = 8KB → fits in L1
            ThreadEnum() { memset(seen_flat, 0, sizeof(seen_flat)); }
        };
        // Allocate thread enum structures
        std::vector<std::unique_ptr<ThreadEnum>> tenums(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) tenums[t] = std::make_unique<ThreadEnum>();

        uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, pre.count);
            if (start >= pre.count) break;
            threads.emplace_back([&, t, start, end] {
                ThreadEnum& te = *tenums[t];
                for (uint64_t i = start; i < end; i++) {
                    const PreRec& p = pre.data[i];
                    if (p.stmt_id == 0) continue;
                    uint32_t gk = ((uint32_t)p.stmt_id << 8) | p.rfile_id;
                    if (gk < 65536 && !te.seen_flat[gk]) {
                        te.seen_flat[gk] = 1;
                        te.gks.push_back((uint16_t)gk);
                    }
                }
            });
        }
        for (auto& t : threads) t.join();

        // Merge: collect all unique gks across threads
        for (int t = 0; t < NTHREADS; t++) {
            if (!tenums[t]) continue;
            for (uint16_t gk : tenums[t]->gks) {
                if (gk_to_idx[gk] == 255) {
                    gk_to_idx[gk] = (uint8_t)num_groups;
                    used_gks[num_groups++] = gk;
                    if (num_groups >= 255) break;
                }
            }
        }
    }

    // Step 2: Parallel bitmap scan (same as iter_4)
    struct ThreadData {
        std::vector<uint64_t> cnt;
        std::vector<uint64_t> lsum;
        std::vector<uint64_t> bitmaps;
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
        GENDB_PHASE("bitmap_scan");
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
    std::vector<uint64_t> total_cnt(num_groups, 0);
    std::vector<uint64_t> total_lsum(num_groups, 0);
    std::vector<uint64_t> merged_bm((size_t)num_groups * BM_WORDS, 0);

    {
        GENDB_PHASE("merge");
        for (int t = 0; t < NTHREADS; t++) {
            if (tdata[t].cnt.empty()) break;
            for (int g = 0; g < num_groups; g++) {
                total_cnt[g] += tdata[t].cnt[g];
                total_lsum[g] += tdata[t].lsum[g];
            }
            const uint64_t* src = tdata[t].bitmaps.data();
            uint64_t* dst = merged_bm.data();
            size_t sz = (size_t)num_groups * BM_WORDS;
            for (size_t w = 0; w < sz; w++) dst[w] |= src[w];
        }
        tdata.clear();
    }

    // Step 4: Popcount for distinct adsh
    struct Row {
        uint16_t group_key;
        uint64_t cnt, num_filings, lsum;
    };
    std::vector<Row> results;
    results.reserve(num_groups);

    for (int g = 0; g < num_groups; g++) {
        if (total_cnt[g] == 0) continue;
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
