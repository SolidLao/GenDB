// Q1 iter_1: Parallel scan with bitset for distinct adsh counting
#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <bitset>
#include <thread>
#include <cstdio>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "timing_utils.h"

using namespace std;

#pragma pack(push, 1)
struct PreRow { uint32_t adsh_id, tag_id, version_id, plabel_id, line; char stmt[3], rfile[2]; uint8_t inpth, negating; };
#pragma pack(pop)

// Encode stmt → 0-8: BS,IS,CF,EQ,CI,UN,SI,CP,other
inline int enc_stmt(const char* s) {
    uint16_t v = ((uint16_t)(uint8_t)s[0] << 8) | (uint8_t)s[1];
    switch(v) {
        case 0x4253: return 0; // BS
        case 0x4953: return 1; // IS
        case 0x4346: return 2; // CF
        case 0x4551: return 3; // EQ
        case 0x4349: return 4; // CI
        case 0x554E: return 5; // UN
        case 0x5349: return 6; // SI
        case 0x4350: return 7; // CP
        default:     return 8;
    }
}
// Encode rfile → 0-3: H,R,X,other
inline int enc_rfile(const char* r) {
    switch(r[0]) {
        case 'H': return 0;
        case 'R': return 1;
        case 'X': return 2;
        default:  return 3;
    }
}

static const char* SD[] = {"BS","IS","CF","EQ","CI","UN","SI","CP","other"};
static const char* RD[] = {"H","R","X","other"};
static const int NS = 9, NR = 4, NG = 36; // 9 stmts * 4 rfiles

struct ThreadData {
    uint64_t cnt[36] = {};
    uint64_t sum_line[36] = {};
    bitset<131072> adsh_bits[36];
};

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];

    int fd = open((gendb_dir + "/pre.bin").c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    const char* mp = (const char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise((void*)mp, fsz, MADV_SEQUENTIAL); close(fd);

    uint64_t total_rows; memcpy(&total_rows, mp, 8);
    const PreRow* rows = reinterpret_cast<const PreRow*>(mp + 8);

    int NT = 32;
    vector<ThreadData> td(NT);

    auto worker = [&](int tid) {
        uint64_t s = (uint64_t)tid * total_rows / NT;
        uint64_t e = (uint64_t)(tid+1) * total_rows / NT;
        auto& t = td[tid];
        for (uint64_t i = s; i < e; i++) {
            const PreRow& r = rows[i];
            if (!r.stmt[0]) continue;
            int k = enc_stmt(r.stmt) * NR + enc_rfile(r.rfile);
            t.cnt[k]++;
            t.sum_line[k] += r.line;
            if (r.adsh_id < 131072) t.adsh_bits[k].set(r.adsh_id);
        }
    };

    vector<thread> ths;
    for (int i = 0; i < NT; i++) ths.emplace_back(worker, i);
    for (auto& t : ths) t.join();
    munmap((void*)mp, fsz);

    // Merge
    uint64_t cnt[36] = {}, sl[36] = {};
    bitset<131072> ab[36];
    for (int i = 0; i < NT; i++)
        for (int k = 0; k < NG; k++) {
            cnt[k] += td[i].cnt[k];
            sl[k] += td[i].sum_line[k];
            ab[k] |= td[i].adsh_bits[k];
        }

    struct Result { const char* stmt; const char* rfile; uint64_t cnt, nf; double avg; };
    vector<Result> results;
    for (int k = 0; k < NG; k++) {
        if (!cnt[k]) continue;
        results.push_back({SD[k/NR], RD[k%NR], cnt[k], ab[k].count(), (double)sl[k]/cnt[k]});
    }
    sort(results.begin(), results.end(), [](const Result& a, const Result& b){ return a.cnt > b.cnt; });

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q1.csv").c_str(), "w");
        fprintf(f, "stmt,rfile,cnt,num_filings,avg_line_num\n");
        for (auto& r : results)
            fprintf(f, "%s,%s,%lu,%lu,%.2f\n", r.stmt, r.rfile, r.cnt, r.nf, r.avg);
        fclose(f);
    }
    return 0;
}
