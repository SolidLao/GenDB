// Q4 Join Order Sampling Program
// Tests candidate join orders empirically to pick the best approach.
//
// Candidates:
//   A: Scan num → filter sub+tag → binary search pre_key_sorted (current)
//   B: Scan pre (EQ) → filter sub → build compact hash set → scan num → probe set → tag lookup
//   C: Eliminate string-heavy tag_join_map build: precompute FNV hash arrays from dicts
//      + probe tag_pk_hash directly during scan (no tag row iteration)
//
// Reports: cardinalities at each stage, estimated probe costs, memory footprints.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <climits>
#include <cassert>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static auto now_ms() {
    return std::chrono::steady_clock::now();
}
static double elapsed_ms(std::chrono::steady_clock::time_point t0) {
    return std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-t0).count();
}

static const void* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return ptr;
}

static std::vector<std::string> load_dict(const std::string& path) {
    size_t sz;
    const uint8_t* d = (const uint8_t*)mmap_file(path, sz);
    uint32_t n = *(const uint32_t*)d;
    std::vector<std::string> dict; dict.reserve(n);
    size_t off = 4;
    for (uint32_t i = 0; i < n; i++) {
        uint16_t len = *(const uint16_t*)(d+off); off += 2;
        dict.emplace_back((const char*)(d+off), len); off += len;
    }
    return dict;
}

// FNV-64a
static uint64_t fnv64(const char* data, size_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) { h ^= (uint8_t)data[i]; h *= 1099511628211ULL; }
    return h ? h : 1;
}

#pragma pack(push,1)
struct PreKeyEntry { int32_t adsh, tag, ver, row_id; };
struct TagHashSlot { uint64_t key_hash; int32_t row_id; int32_t _pad; };
#pragma pack(pop)

struct PreKeyCmp {
    bool operator()(const PreKeyEntry& a, const PreKeyEntry& b) const {
        if (a.adsh != b.adsh) return a.adsh < b.adsh;
        if (a.tag  != b.tag)  return a.tag  < b.tag;
        return a.ver < b.ver;
    }
};

// Compact 3-int key for pre set
struct PreKey3 {
    int32_t adsh, tag, ver;
    bool operator==(const PreKey3& o) const { return adsh==o.adsh && tag==o.tag && ver==o.ver; }
};
struct PreKey3Hash {
    size_t operator()(const PreKey3& k) const {
        uint64_t h = (uint64_t)(uint32_t)k.adsh;
        h ^= (uint64_t)(uint32_t)k.tag * 0xff51afd7ed558ccdULL;
        h ^= (uint64_t)(uint32_t)k.ver * 0xc4ceb9fe1a85ec53ULL;
        return (size_t)h;
    }
};

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    const std::string g = argv[1];

    printf("=== Q4 Join Order Sampling ===\n\n");

    // -------------------------------------------------------
    // Load shared dicts
    // -------------------------------------------------------
    auto t0 = now_ms();
    auto uom_dict  = load_dict(g + "/shared/uom.dict");
    auto stmt_dict = load_dict(g + "/shared/stmt.dict");
    auto tag_dict  = load_dict(g + "/shared/tag_numpre.dict");
    auto ver_dict  = load_dict(g + "/shared/version_numpre.dict");

    int8_t usd_code = -1, eq_code = -1;
    for (size_t i = 0; i < uom_dict.size(); i++) if (uom_dict[i]=="USD") { usd_code=(int8_t)i; break; }
    for (size_t i = 0; i < stmt_dict.size(); i++) if (stmt_dict[i]=="EQ")  { eq_code =(int8_t)i; break; }
    printf("USD code=%d  EQ code=%d\n", (int)usd_code, (int)eq_code);
    printf("tag_dict size=%zu  ver_dict size=%zu\n", tag_dict.size(), ver_dict.size());
    printf("Dict load: %.1f ms\n\n", elapsed_ms(t0));

    // -------------------------------------------------------
    // Load sub arrays
    // -------------------------------------------------------
    t0 = now_ms();
    constexpr int32_t SUB_N = 86135;
    constexpr int32_t BWORDS = (SUB_N+63)/64;
    uint64_t sub_valid[BWORDS] = {};
    int16_t  sub_sic[SUB_N]  = {};
    int32_t  sub_cik_arr[SUB_N] = {};
    {
        size_t s1,s2;
        const int16_t* sic = (const int16_t*)mmap_file(g+"/sub/sic.bin", s1);
        const int32_t* cik = (const int32_t*)mmap_file(g+"/sub/cik.bin", s2);
        int32_t nr = (int32_t)(s1/2);
        for (int32_t i = 0; i < nr; i++) {
            sub_sic[i] = sic[i];
            sub_cik_arr[i] = cik[i];
            if (sic[i]>=4000 && sic[i]<=4999) sub_valid[i>>6] |= 1ULL<<(i&63);
        }
    }
    int32_t sub_pass = 0;
    for (int32_t i = 0; i < SUB_N; i++) if (sub_valid[i>>6]&(1ULL<<(i&63))) sub_pass++;
    printf("Sub build: %.1f ms  sic-filter rows=%d (%.1f%%)\n\n",
           elapsed_ms(t0), sub_pass, 100.0*sub_pass/SUB_N);

    // -------------------------------------------------------
    // Approach C: Build FNV hash arrays from dicts (new approach)
    // -------------------------------------------------------
    t0 = now_ms();
    std::vector<uint64_t> h_tag(tag_dict.size()), h_ver(ver_dict.size());
    for (size_t i = 0; i < tag_dict.size(); i++) h_tag[i] = fnv64(tag_dict[i].data(), tag_dict[i].size());
    for (size_t j = 0; j < ver_dict.size(); j++) h_ver[j] = fnv64(ver_dict[j].data(), ver_dict[j].size());
    double fnv_build_ms = elapsed_ms(t0);
    printf("Approach C - FNV hash arrays build: %.1f ms  (tag=%zu entries, ver=%zu entries)\n",
           fnv_build_ms, tag_dict.size(), ver_dict.size());

    // Load tag_pk_hash
    t0 = now_ms();
    size_t tph_sz;
    const uint8_t* tph_raw = (const uint8_t*)mmap_file(g+"/indexes/tag_pk_hash.bin", tph_sz);
    uint32_t tph_cap = *(const uint32_t*)tph_raw;
    const TagHashSlot* tph = (const TagHashSlot*)(tph_raw + 8);
    madvise((void*)tph_raw, tph_sz, MADV_RANDOM);

    size_t abs_sz;
    const int8_t* tag_abs = (const int8_t*)mmap_file(g+"/tag/abstract.bin", abs_sz);
    printf("tag_pk_hash load: %.1f ms  cap=%u  size=%.1f MB\n",
           elapsed_ms(t0), tph_cap, tph_sz/1e6);

    // -------------------------------------------------------
    // Approach B: Build compact pre_eq_sub hash set
    // -------------------------------------------------------
    t0 = now_ms();
    size_t ps_sz, pa_sz, pt_sz, pv_sz;
    const int8_t*  pre_stmt_col = (const int8_t* )mmap_file(g+"/pre/stmt.bin",    ps_sz);
    const int32_t* pre_adsh_col = (const int32_t*)mmap_file(g+"/pre/adsh.bin",    pa_sz);
    const int32_t* pre_tag_col  = (const int32_t*)mmap_file(g+"/pre/tag.bin",     pt_sz);
    const int32_t* pre_ver_col  = (const int32_t*)mmap_file(g+"/pre/version.bin", pv_sz);
    madvise((void*)pre_stmt_col, ps_sz, MADV_SEQUENTIAL);
    madvise((void*)pre_adsh_col, pa_sz, MADV_SEQUENTIAL);
    madvise((void*)pre_tag_col,  pt_sz, MADV_SEQUENTIAL);
    madvise((void*)pre_ver_col,  pv_sz, MADV_SEQUENTIAL);
    int64_t pre_N = (int64_t)(ps_sz);

    std::unordered_set<PreKey3, PreKey3Hash> pre_eq_sub_set;
    pre_eq_sub_set.reserve(131072);

    int64_t pre_eq_cnt = 0, pre_eq_sub_cnt = 0;
    for (int64_t i = 0; i < pre_N; i++) {
        if (pre_stmt_col[i] != eq_code) continue;
        pre_eq_cnt++;
        int32_t ac = pre_adsh_col[i];
        if (ac < 0 || ac >= SUB_N) continue;
        if (!(sub_valid[ac>>6]&(1ULL<<(ac&63)))) continue;
        pre_eq_sub_cnt++;
        pre_eq_sub_set.insert({ac, pre_tag_col[i], pre_ver_col[i]});
    }
    double pre_set_build_ms = elapsed_ms(t0);
    printf("\nApproach B - pre_eq_sub_set build: %.1f ms\n", pre_set_build_ms);
    printf("  pre total=%lld  EQ rows=%lld (%.1f%%)  EQ+sub=%lld (%.1f%%)\n",
           (long long)pre_N, (long long)pre_eq_cnt, 100.0*pre_eq_cnt/pre_N,
           (long long)pre_eq_sub_cnt, 100.0*pre_eq_sub_cnt/pre_N);
    printf("  hash set unique keys=%zu  size=%.2f MB\n",
           pre_eq_sub_set.size(), pre_eq_sub_set.size()*24.0/1e6);

    // -------------------------------------------------------
    // Load num columns + zone maps
    // -------------------------------------------------------
    t0 = now_ms();
    size_t n1,n2,n3,n4,n5,zm_sz;
    const int8_t*  num_uom  = (const int8_t* )mmap_file(g+"/num/uom.bin",     n1);
    const double*  num_val  = (const double* )mmap_file(g+"/num/value.bin",   n2);
    const int32_t* num_adsh = (const int32_t*)mmap_file(g+"/num/adsh.bin",    n3);
    const int32_t* num_tag  = (const int32_t*)mmap_file(g+"/num/tag.bin",     n4);
    const int32_t* num_ver  = (const int32_t*)mmap_file(g+"/num/version.bin", n5);
    int64_t num_N = (int64_t)(n1);
    madvise((void*)num_uom,  n1, MADV_SEQUENTIAL);
    madvise((void*)num_val,  n2, MADV_SEQUENTIAL);
    madvise((void*)num_adsh, n3, MADV_SEQUENTIAL);
    madvise((void*)num_tag,  n4, MADV_SEQUENTIAL);
    madvise((void*)num_ver,  n5, MADV_SEQUENTIAL);

    #pragma pack(push,1)
    struct NumZM { int8_t uom_min, uom_max; int32_t dd_min, dd_max; };
    #pragma pack(pop)
    const uint8_t* zm_raw = (const uint8_t*)mmap_file(g+"/indexes/num_zonemaps.bin", zm_sz);
    int32_t n_blocks = *(const int32_t*)zm_raw;
    const NumZM* zm = (const NumZM*)(zm_raw+4);
    printf("num load: %.1f ms  rows=%lld  blocks=%d\n\n", elapsed_ms(t0), (long long)num_N, n_blocks);

    // Also load pre_key_sorted for approach A
    size_t pks_sz;
    const uint8_t* pks_raw = (const uint8_t*)mmap_file(g+"/indexes/pre_key_sorted.bin", pks_sz);
    uint32_t n_pks = *(const uint32_t*)pks_raw;
    const PreKeyEntry* pks = (const PreKeyEntry*)(pks_raw+4);
    madvise((void*)pks_raw, pks_sz, MADV_RANDOM);

    // -------------------------------------------------------
    // Run num scan: measure cardinalities and approach costs
    // -------------------------------------------------------
    int64_t cnt_usd=0, cnt_val=0, cnt_sub=0, cnt_tag=0, cnt_pre_A=0, cnt_pre_B=0, cnt_pre_C=0;

    // APPROACH A timing: binary search pre_key_sorted
    t0 = now_ms();
    for (int32_t b = 0; b < n_blocks; b++) {
        if (zm[b].uom_max < 0) continue;  // skip negative uom blocks
        int64_t rs = (int64_t)b*100000, re = std::min(rs+(int64_t)100000, num_N);
        bool all_usd = (zm[b].uom_min==0 && zm[b].uom_max==0);
        for (int64_t i = rs; i < re; i++) {
            if (!all_usd && num_uom[i]!=usd_code) continue; cnt_usd++;
            double v = num_val[i]; if (std::isnan(v)) continue; cnt_val++;
            int32_t ac = num_adsh[i];
            if (ac<0||ac>=SUB_N) continue;
            if (!(sub_valid[ac>>6]&(1ULL<<(ac&63)))) continue; cnt_sub++;
            // Approach A: binary search pre_key_sorted
            PreKeyEntry sk = {ac, num_tag[i], num_ver[i], 0};
            const PreKeyEntry* lb = std::lower_bound(pks, pks+n_pks, sk, PreKeyCmp{});
            if (lb!=pks+n_pks && lb->adsh==ac && lb->tag==sk.tag && lb->ver==sk.ver) {
                // Found in pre; would now check stmt
                for (auto it=lb; it!=pks+n_pks && it->adsh==ac && it->tag==sk.tag && it->ver==sk.ver; ++it) {
                    if (pre_stmt_col[it->row_id]==eq_code) { cnt_pre_A++; break; }
                }
            }
        }
    }
    double time_A = elapsed_ms(t0);

    // APPROACH B timing: probe pre_eq_sub_set
    t0 = now_ms();
    cnt_usd=0; cnt_val=0; cnt_sub=0;
    for (int32_t b = 0; b < n_blocks; b++) {
        if (zm[b].uom_max < 0) continue;
        int64_t rs = (int64_t)b*100000, re = std::min(rs+(int64_t)100000, num_N);
        bool all_usd = (zm[b].uom_min==0 && zm[b].uom_max==0);
        for (int64_t i = rs; i < re; i++) {
            if (!all_usd && num_uom[i]!=usd_code) continue; cnt_usd++;
            double v = num_val[i]; if (std::isnan(v)) continue; cnt_val++;
            int32_t ac = num_adsh[i];
            if (ac<0||ac>=SUB_N) continue;
            if (!(sub_valid[ac>>6]&(1ULL<<(ac&63)))) continue; cnt_sub++;
            // Approach B: probe pre_eq_sub_set (O(1))
            PreKey3 pk3 = {ac, num_tag[i], num_ver[i]};
            if (pre_eq_sub_set.count(pk3)) cnt_pre_B++;
        }
    }
    double time_B = elapsed_ms(t0);

    // APPROACH C timing: probe tag_pk_hash using FNV arrays
    t0 = now_ms();
    cnt_sub=0; cnt_tag=0; cnt_pre_C=0;
    for (int32_t b = 0; b < n_blocks; b++) {
        if (zm[b].uom_max < 0) continue;
        int64_t rs = (int64_t)b*100000, re = std::min(rs+(int64_t)100000, num_N);
        bool all_usd = (zm[b].uom_min==0 && zm[b].uom_max==0);
        for (int64_t i = rs; i < re; i++) {
            if (!all_usd && num_uom[i]!=usd_code) continue;
            double v = num_val[i]; if (std::isnan(v)) continue;
            int32_t ac = num_adsh[i];
            if (ac<0||ac>=SUB_N) continue;
            if (!(sub_valid[ac>>6]&(1ULL<<(ac&63)))) continue; cnt_sub++;
            // Tag probe via FNV arrays + tag_pk_hash
            int32_t tc = num_tag[i], vc = num_ver[i];
            if (tc<0||(size_t)tc>=h_tag.size()) continue;
            if (vc<0||(size_t)vc>=h_ver.size()) continue;
            uint64_t kh = h_tag[tc] ^ (h_ver[vc] * 0x9e3779b97f4a7c15ULL);
            if (!kh) kh=1;
            uint32_t pos = (uint32_t)(kh & (tph_cap-1));
            int32_t row_id = INT32_MIN;
            for (uint32_t p2 = 0; p2 < tph_cap; p2++) {
                uint32_t sl = (pos+p2)&(tph_cap-1);
                if (tph[sl].row_id == INT32_MIN) break;
                if (tph[sl].key_hash == kh) { row_id = tph[sl].row_id; break; }
            }
            if (row_id==INT32_MIN || tag_abs[row_id]!=0) continue; cnt_tag++;
            // pre lookup (approach B hash set)
            PreKey3 pk3 = {ac, tc, vc};
            if (pre_eq_sub_set.count(pk3)) cnt_pre_C++;
        }
    }
    double time_C = elapsed_ms(t0);

    printf("=== Num Scan Cardinality ===\n");
    printf("  USD pass:         %lld (%.1f%%)\n", (long long)cnt_usd,  100.0*cnt_usd/num_N);
    printf("  value IS NOT NULL:%lld (%.1f%%)\n", (long long)cnt_val,  100.0*cnt_val/num_N);
    printf("  sub sic filter:   %lld (%.1f%%)\n", (long long)cnt_sub,  100.0*cnt_sub/num_N);
    printf("  tag abstract=0:   %lld (%.1f%%)\n", (long long)cnt_tag,  100.0*cnt_tag/num_N);

    printf("\n=== Approach A (current: binary search pre_key_sorted) ===\n");
    printf("  Output rows (pre EQ match): %lld\n", (long long)cnt_pre_A);
    printf("  Scan-only time: %.1f ms\n", time_A);

    printf("\n=== Approach B (pre_eq_sub hash set probe, no tag check) ===\n");
    printf("  Output rows (pre+sub match): %lld\n", (long long)cnt_pre_B);
    printf("  Scan-only time: %.1f ms\n", time_B);

    printf("\n=== Approach C (FNV arrays + tag_pk_hash + pre_eq_sub set) ===\n");
    printf("  Output rows (pre+sub+tag match): %lld\n", (long long)cnt_pre_C);
    printf("  Scan-only time: %.1f ms\n", time_C);

    printf("\n=== Memory Footprints ===\n");
    printf("  sub_valid bitset:           %.1f KB\n", BWORDS*8.0/1024);
    printf("  h_tag array (%zu entries):   %.1f KB\n", h_tag.size(), h_tag.size()*8.0/1024);
    printf("  h_ver array (%zu entries):   %.1f KB\n", h_ver.size(), h_ver.size()*8.0/1024);
    printf("  tag_pk_hash:                %.1f MB\n", tph_sz/1e6);
    printf("  tag_abstract:               %.1f KB\n", abs_sz/1024.0);
    printf("  pre_eq_sub_set (%zu keys):   %.1f KB\n",
           pre_eq_sub_set.size(), pre_eq_sub_set.size()*24.0/1024);
    printf("  pre_key_sorted:             %.1f MB (random access)\n", pks_sz/1e6);

    printf("\n=== Summary: Recommended Approach ===\n");
    double total_A = time_A;   // build_sub + scan_A (binary search)
    double total_B = pre_set_build_ms + time_B;  // pre_set_build + scan_B
    double total_C = fnv_build_ms + pre_set_build_ms + time_C; // fnv_build + pre_set + scan_C
    printf("  Approach A (current):    scan=%.1f ms  [no separate pre build]\n", total_A);
    printf("  Approach B (pre hash):   pre_set_build=%.1f ms + scan=%.1f ms = %.1f ms\n",
           pre_set_build_ms, time_B, total_B);
    printf("  Approach C (FNV+tag_pk+pre hash):  build=%.1f ms + scan=%.1f ms = %.1f ms\n",
           fnv_build_ms+pre_set_build_ms, time_C, total_C);

    return 0;
}
