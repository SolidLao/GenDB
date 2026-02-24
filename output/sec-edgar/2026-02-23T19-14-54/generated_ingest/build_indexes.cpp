// build_indexes.cpp — Build binary hash indexes from ingested columns
//
// Reads binary columnar files from sf3.gendb/, writes:
//   indexes/sub_adsh_hash.bin   — adsh_code → sub row index
//   indexes/tag_tv_hash.bin     — (tag_code,version_code) → tag row index
//   indexes/pre_atv_hash.bin    — existence set (adsh,tag,version) in pre (Q24 anti-join + Q4/Q6)
//   indexes/pre_eq_hash.bin     — existence set (adsh,tag,version) where stmt='EQ' (Q4)
//   indexes/pre_is_hash.bin     — (adsh,tag,version) → plabel_code where stmt='IS' (Q6)
//   indexes/num_ddate_zone_map.bin — zone map on num.ddate (Q24 range filter)
//
// All hash tables: open-addressing, linear probing, bounded probe (C24).
// Capacity: next_power_of_2(count * 2) for ≤50% load factor (C9).
// Empty slot sentinel: adsh_code/tag_code = INT32_MIN (set via std::fill, NOT memset — C20).

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <climits>
#include <vector>
#include <string>
#include <algorithm>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

static const int32_t EMPTY = INT32_MIN;

// ---- next_power_of_2 --------------------------------------------------
static uint32_t next_pow2(uint64_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8;
    n |= n >> 16; n |= n >> 32;
    return (uint32_t)(n + 1);
}

// ---- mmap column file -------------------------------------------------
template<typename T>
static const T* mmap_col(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); count = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    count = st.st_size / sizeof(T);
    if (count == 0) { close(fd); return nullptr; }
    void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { fprintf(stderr, "mmap failed for %s\n", path.c_str()); count = 0; return nullptr; }
    return reinterpret_cast<const T*>(p);
}

// ---- Write binary file ------------------------------------------------
static void write_file(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); return; }
    fwrite(data, 1, bytes, f);
    fclose(f);
    printf("  Wrote %s (%.1f MB)\n", path.c_str(), bytes / 1048576.0);
}

// ---- Hash functions ---------------------------------------------------
static inline uint32_t hash1(int32_t a, uint32_t mask) {
    return ((uint32_t)a * 2654435761u) & mask;
}
static inline uint32_t hash2(int32_t a, int32_t b, uint32_t mask) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761u;
    h ^= (uint64_t)(uint32_t)b * 1234567891u;
    return (uint32_t)(h ^ (h >> 32)) & mask;
}
static inline uint32_t hash3(int32_t a, int32_t b, int32_t c, uint32_t mask) {
    uint64_t h = (uint64_t)(uint32_t)a * 2654435761u;
    h ^= (uint64_t)(uint32_t)b * 1234567891u;
    h ^= (uint64_t)(uint32_t)c * 2246822519u;
    return (uint32_t)(h ^ (h >> 32)) & mask;
}

// ---- Load dict file, return vector of strings -------------------------
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open dict %s\n", path.c_str()); return dict; }
    char buf[4096];
    while (fgets(buf, sizeof(buf), f)) {
        size_t n = strlen(buf);
        while (n > 0 && (buf[n-1] == '\n' || buf[n-1] == '\r')) buf[--n] = '\0';
        dict.push_back(std::string(buf));
    }
    fclose(f);
    return dict;
}

// ---- Find code for a string in dict -----------------------------------
static int16_t find_code16(const std::vector<std::string>& dict, const std::string& s) {
    for (size_t i = 0; i < dict.size(); i++)
        if (dict[i] == s) return (int16_t)i;
    return -1;
}

// ====================================================================
// 1. sub_adsh_hash: adsh_code → sub row index
// Format: [uint32_t cap][{int32_t adsh_code, uint32_t row_idx} × cap]
// ====================================================================
struct SubSlot { int32_t adsh_code; uint32_t row_idx; };

static void build_sub_adsh_hash(const std::string& db_dir) {
    printf("[sub_adsh_hash] Building...\n");
    size_t n;
    const int32_t* adsh = mmap_col<int32_t>(db_dir + "/sub/adsh.bin", n);
    if (!adsh) return;

    uint32_t cap = next_pow2((uint32_t)n * 2);
    uint32_t mask = cap - 1;
    printf("  sub rows=%zu, cap=%u\n", n, cap);

    std::vector<SubSlot> ht(cap);
    std::fill(ht.begin(), ht.end(), SubSlot{EMPTY, 0}); // C20: use fill not memset

    for (uint32_t i = 0; i < (uint32_t)n; i++) {
        int32_t k = adsh[i];
        uint32_t h = hash1(k, mask);
        for (uint32_t probe = 0; probe < cap; probe++) { // C24: bounded
            uint32_t slot = (h + probe) & mask;
            if (ht[slot].adsh_code == EMPTY) {
                ht[slot] = {k, i};
                break;
            }
        }
    }

    // Write: [uint32_t cap][slots]
    FILE* f = fopen((db_dir + "/indexes/sub_adsh_hash.bin").c_str(), "wb");
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(ht.data(), sizeof(SubSlot), cap, f);
    fclose(f);
    printf("  Wrote sub_adsh_hash.bin (%.1f MB)\n", (sizeof(uint32_t) + cap*sizeof(SubSlot))/1048576.0);
}

// ====================================================================
// 2. tag_tv_hash: (tag_code, version_code) → tag row index
// Format: [uint32_t cap][{int32_t tag, int32_t ver, uint32_t row_idx} × cap]
// ====================================================================
struct TagSlot { int32_t tag_code; int32_t ver_code; uint32_t row_idx; };

static void build_tag_tv_hash(const std::string& db_dir) {
    printf("[tag_tv_hash] Building...\n");
    size_t nt, nv;
    const int32_t* tags = mmap_col<int32_t>(db_dir + "/tag/tag.bin", nt);
    const int32_t* vers = mmap_col<int32_t>(db_dir + "/tag/version.bin", nv);
    if (!tags || !vers || nt != nv) { fprintf(stderr, "tag column size mismatch\n"); return; }

    uint32_t cap = next_pow2((uint32_t)nt * 2);
    uint32_t mask = cap - 1;
    printf("  tag rows=%zu, cap=%u\n", nt, cap);

    std::vector<TagSlot> ht(cap);
    std::fill(ht.begin(), ht.end(), TagSlot{EMPTY, EMPTY, 0});

    for (uint32_t i = 0; i < (uint32_t)nt; i++) {
        int32_t tk = tags[i], vk = vers[i];
        uint32_t h = hash2(tk, vk, mask);
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t slot = (h + probe) & mask;
            if (ht[slot].tag_code == EMPTY) {
                ht[slot] = {tk, vk, i};
                break;
            }
            // Skip duplicate (tag,version) pairs — keep first occurrence
            if (ht[slot].tag_code == tk && ht[slot].ver_code == vk) break;
        }
    }

    FILE* f = fopen((db_dir + "/indexes/tag_tv_hash.bin").c_str(), "wb");
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(ht.data(), sizeof(TagSlot), cap, f);
    fclose(f);
    printf("  Wrote tag_tv_hash.bin (%.1f MB)\n", (sizeof(uint32_t) + cap*sizeof(TagSlot))/1048576.0);
}

// ====================================================================
// 3. pre_atv_hash: existence set (adsh, tag, version) in pre
// Used by Q24 anti-join + Q4/Q6 semi-join
// Format: [uint32_t cap][{int32_t adsh, int32_t tag, int32_t ver} × cap]
// ====================================================================
struct AtvSlot { int32_t adsh_code; int32_t tag_code; int32_t ver_code; };

static void build_pre_atv_hash(const std::string& db_dir) {
    printf("[pre_atv_hash] Building...\n");
    size_t na, nt, nv;
    const int32_t* adsh = mmap_col<int32_t>(db_dir + "/pre/adsh.bin", na);
    const int32_t* tags = mmap_col<int32_t>(db_dir + "/pre/tag.bin",  nt);
    const int32_t* vers = mmap_col<int32_t>(db_dir + "/pre/version.bin", nv);
    if (!adsh || !tags || !vers) return;
    size_t n = na;

    uint32_t cap = next_pow2((uint32_t)n * 2);
    uint32_t mask = cap - 1;
    printf("  pre rows=%zu, cap=%u (%.0f MB)\n", n, cap, cap*sizeof(AtvSlot)/1048576.0);

    // Use mmap anonymous for large allocation to avoid page fault stall (P22)
    size_t bytes = (size_t)cap * sizeof(AtvSlot);
    void* raw = mmap(nullptr, bytes, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (raw == MAP_FAILED) { fprintf(stderr, "mmap anon failed\n"); return; }
    AtvSlot* ht = reinterpret_cast<AtvSlot*>(raw);

    // Parallel zero init (P22: distribute page faults across cores)
    #pragma omp parallel for schedule(static) num_threads(32)
    for (uint32_t i = 0; i < cap; i++) {
        ht[i] = {EMPTY, EMPTY, EMPTY};
    }

    // Sequential insert (hash insert is not easily parallelized without atomics)
    for (uint32_t i = 0; i < (uint32_t)n; i++) {
        int32_t ak = adsh[i], tk = tags[i], vk = vers[i];
        uint32_t h = hash3(ak, tk, vk, mask);
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t slot = (h + probe) & mask;
            if (ht[slot].adsh_code == EMPTY) {
                ht[slot] = {ak, tk, vk};
                break;
            }
            if (ht[slot].adsh_code == ak && ht[slot].tag_code == tk && ht[slot].ver_code == vk) break;
        }
        if (i % 2000000 == 0) printf("  [pre_atv] %u/%zu\n", i, n);
    }

    FILE* f = fopen((db_dir + "/indexes/pre_atv_hash.bin").c_str(), "wb");
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(ht, sizeof(AtvSlot), cap, f);
    fclose(f);
    munmap(raw, bytes);
    printf("  Wrote pre_atv_hash.bin (%.1f MB)\n", (sizeof(uint32_t) + (size_t)cap*sizeof(AtvSlot))/1048576.0);
}

// ====================================================================
// 4. pre_eq_hash: existence set (adsh,tag,version) where stmt='EQ'
// Format: [uint32_t cap][{int32_t adsh, int32_t tag, int32_t ver} × cap]
// ====================================================================
static void build_pre_eq_hash(const std::string& db_dir) {
    printf("[pre_eq_hash] Building (stmt='EQ' filter)...\n");

    auto stmt_dict = load_dict(db_dir + "/pre/stmt_dict.txt");
    int16_t eq_code = find_code16(stmt_dict, "EQ");
    printf("  stmt='EQ' code: %d\n", (int)eq_code);
    if (eq_code < 0) { fprintf(stderr, "  EQ not found in stmt dict!\n"); return; }

    size_t na, nt, nv, ns;
    const int32_t* adsh = mmap_col<int32_t>(db_dir + "/pre/adsh.bin", na);
    const int32_t* tags = mmap_col<int32_t>(db_dir + "/pre/tag.bin",  nt);
    const int32_t* vers = mmap_col<int32_t>(db_dir + "/pre/version.bin", nv);
    const int16_t* stmt = mmap_col<int16_t>(db_dir + "/pre/stmt.bin", ns);
    if (!adsh || !tags || !vers || !stmt) return;
    size_t n = na;

    // Count EQ rows to size hash table (P23)
    size_t eq_count = 0;
    for (size_t i = 0; i < n; i++) if (stmt[i] == eq_code) eq_count++;
    printf("  EQ rows: %zu\n", eq_count);

    uint32_t cap = next_pow2((uint32_t)(eq_count * 2 + 1));
    uint32_t mask = cap - 1;

    std::vector<AtvSlot> ht(cap);
    std::fill(ht.begin(), ht.end(), AtvSlot{EMPTY, EMPTY, EMPTY});

    for (uint32_t i = 0; i < (uint32_t)n; i++) {
        if (stmt[i] != eq_code) continue;
        int32_t ak = adsh[i], tk = tags[i], vk = vers[i];
        uint32_t h = hash3(ak, tk, vk, mask);
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t slot = (h + probe) & mask;
            if (ht[slot].adsh_code == EMPTY) { ht[slot] = {ak, tk, vk}; break; }
            if (ht[slot].adsh_code == ak && ht[slot].tag_code == tk && ht[slot].ver_code == vk) break;
        }
    }

    FILE* f = fopen((db_dir + "/indexes/pre_eq_hash.bin").c_str(), "wb");
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(ht.data(), sizeof(AtvSlot), cap, f);
    fclose(f);
    printf("  Wrote pre_eq_hash.bin (%.1f MB)\n", (sizeof(uint32_t) + cap*sizeof(AtvSlot))/1048576.0);
}

// ====================================================================
// 5. pre_is_hash: (adsh,tag,version) → plabel_code where stmt='IS'
// Format: [uint32_t cap][{int32_t adsh, int32_t tag, int32_t ver, int32_t plabel} × cap]
// ====================================================================
struct IsSlot { int32_t adsh_code; int32_t tag_code; int32_t ver_code; int32_t plabel_code; };

static void build_pre_is_hash(const std::string& db_dir) {
    printf("[pre_is_hash] Building (stmt='IS' filter)...\n");

    auto stmt_dict = load_dict(db_dir + "/pre/stmt_dict.txt");
    int16_t is_code = find_code16(stmt_dict, "IS");
    printf("  stmt='IS' code: %d\n", (int)is_code);
    if (is_code < 0) { fprintf(stderr, "  IS not found in stmt dict!\n"); return; }

    size_t na, nt, nv, ns, np;
    const int32_t* adsh   = mmap_col<int32_t>(db_dir + "/pre/adsh.bin",    na);
    const int32_t* tags   = mmap_col<int32_t>(db_dir + "/pre/tag.bin",     nt);
    const int32_t* vers   = mmap_col<int32_t>(db_dir + "/pre/version.bin", nv);
    const int16_t* stmt   = mmap_col<int16_t>(db_dir + "/pre/stmt.bin",    ns);
    const int32_t* plabel = mmap_col<int32_t>(db_dir + "/pre/plabel.bin",  np);
    if (!adsh || !tags || !vers || !stmt || !plabel) return;
    size_t n = na;

    size_t is_count = 0;
    for (size_t i = 0; i < n; i++) if (stmt[i] == is_code) is_count++;
    printf("  IS rows: %zu\n", is_count);

    uint32_t cap = next_pow2((uint32_t)(is_count * 2 + 1));
    uint32_t mask = cap - 1;

    std::vector<IsSlot> ht(cap);
    std::fill(ht.begin(), ht.end(), IsSlot{EMPTY, EMPTY, EMPTY, EMPTY});

    for (uint32_t i = 0; i < (uint32_t)n; i++) {
        if (stmt[i] != is_code) continue;
        int32_t ak = adsh[i], tk = tags[i], vk = vers[i], pk = plabel[i];
        uint32_t h = hash3(ak, tk, vk, mask);
        for (uint32_t probe = 0; probe < cap; probe++) {
            uint32_t slot = (h + probe) & mask;
            if (ht[slot].adsh_code == EMPTY) { ht[slot] = {ak, tk, vk, pk}; break; }
            if (ht[slot].adsh_code == ak && ht[slot].tag_code == tk && ht[slot].ver_code == vk) break;
        }
    }

    FILE* f = fopen((db_dir + "/indexes/pre_is_hash.bin").c_str(), "wb");
    fwrite(&cap, sizeof(uint32_t), 1, f);
    fwrite(ht.data(), sizeof(IsSlot), cap, f);
    fclose(f);
    printf("  Wrote pre_is_hash.bin (%.1f MB)\n", (sizeof(uint32_t) + cap*sizeof(IsSlot))/1048576.0);
}

// ====================================================================
// 6. num_ddate_zone_map: zone map on num.ddate
// Format: [uint32_t num_blocks][{int32_t min, int32_t max, uint32_t block_size} × num_blocks]
// ====================================================================
struct ZoneBlock { int32_t min_val; int32_t max_val; uint32_t block_size; };

static void build_num_ddate_zone_map(const std::string& db_dir) {
    printf("[num_ddate_zone_map] Building...\n");
    size_t n;
    const int32_t* ddate = mmap_col<int32_t>(db_dir + "/num/ddate.bin", n);
    if (!ddate) return;

    const uint32_t BLOCK = 65536;
    uint32_t num_blocks = (uint32_t)((n + BLOCK - 1) / BLOCK);
    std::vector<ZoneBlock> zones(num_blocks);

    for (uint32_t b = 0; b < num_blocks; b++) {
        uint32_t start = b * BLOCK;
        uint32_t end   = std::min(start + BLOCK, (uint32_t)n);
        int32_t mn = ddate[start], mx = ddate[start];
        for (uint32_t i = start + 1; i < end; i++) {
            if (ddate[i] < mn) mn = ddate[i];
            if (ddate[i] > mx) mx = ddate[i];
        }
        zones[b] = {mn, mx, end - start};
    }

    FILE* f = fopen((db_dir + "/indexes/num_ddate_zone_map.bin").c_str(), "wb");
    fwrite(&num_blocks, sizeof(uint32_t), 1, f);
    fwrite(zones.data(), sizeof(ZoneBlock), num_blocks, f);
    fclose(f);
    printf("  Wrote num_ddate_zone_map.bin (%u blocks)\n", num_blocks);
}

// ====================================================================
// Main
// ====================================================================
int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <db_dir>\n", argv[0]);
        return 1;
    }
    std::string db_dir = argv[1];

    double t0 = omp_get_wtime();

    // Build indexes in dependency order
    // sub and tag can run in parallel with pre
    #pragma omp parallel sections num_threads(3)
    {
        #pragma omp section
        {
            build_sub_adsh_hash(db_dir);
            build_tag_tv_hash(db_dir);
            build_num_ddate_zone_map(db_dir);
        }
        #pragma omp section
        {
            build_pre_atv_hash(db_dir);  // largest, do first in this section
        }
        #pragma omp section
        {
            build_pre_eq_hash(db_dir);
            build_pre_is_hash(db_dir);
        }
    }

    printf("[DONE] Total index build time: %.1fs\n", omp_get_wtime() - t0);
    return 0;
}
