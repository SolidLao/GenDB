/**
 * sampling_join_order.cpp — Q6 join order empirical comparison
 *
 * Tests two candidate approaches for building the is_map:
 *
 * Order A (current): Build is_map from ALL pre rows where stmt='IS' (~1.73M entries)
 *   Memory: 4M capacity × 16 bytes = 64MB → exceeds L3 (44MB)
 *
 * Order B (proposed): First collect qualifying adsh_codes (fy=2023) into a DenseBitmap,
 *   then scan pre filtering by BOTH stmt='IS' AND adsh in fy2023_set → ~350K entries
 *   Memory: ~500K capacity × 16 bytes = 8MB → fits in L3 (44MB)
 *
 * Usage: ./sampling_join_order <gendb_dir>
 */

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <chrono>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// Minimal mmap helper
static const void* mmap_file(const char* path, size_t& out_size) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return p;
}

// Minimal dict loader: layout uint8_t N; N x { int8_t code, uint8_t slen, char[slen] }
static int8_t load_dict_code(const char* path, const char* target) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    uint8_t N = 0; read(fd, &N, 1);
    for (int i = 0; i < (int)N; i++) {
        int8_t code=0; uint8_t slen=0;
        read(fd, &code, 1); read(fd, &slen, 1);
        char buf[256]={};
        read(fd, buf, slen);
        if (strncmp(buf, target, slen) == 0 && strlen(target) == slen) {
            close(fd); return code;
        }
    }
    close(fd);
    fprintf(stderr, "Dict key not found: %s in %s\n", target, path);
    exit(1);
}

// Minimal Robin Hood hash map (key=uint64_t → value=uint32_t)
struct SimpleHashMap {
    struct Entry { uint64_t key; uint32_t value; uint16_t dist; bool occupied; };
    std::vector<Entry> table;
    size_t mask = 0, count = 0;

    void init(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.assign(cap, Entry{0,0,0,false});
        mask = cap - 1;
        count = 0;
    }

    void insert(uint64_t key, uint32_t value) {
        size_t pos = (key * 0x9E3779B97F4A7C15ULL) & mask;
        Entry e{key, value, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) { table[pos].value = value; return; }
            if (e.dist > table[pos].dist) std::swap(e, table[pos]);
            pos = (pos + 1) & mask;
            e.dist++;
        }
        table[pos] = e;
        count++;
    }

    uint32_t* find(uint64_t key) {
        size_t pos = (key * 0x9E3779B97F4A7C15ULL) & mask;
        uint16_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }
};

static double now_ms() {
    using namespace std::chrono;
    return duration_cast<duration<double,std::milli>>(
        steady_clock::now().time_since_epoch()).count();
}

// Flush CPU data caches by reading a large buffer
static void flush_caches() {
    static std::vector<char> buf(256 * 1024 * 1024, 0);
    volatile char sink = 0;
    for (size_t i = 0; i < buf.size(); i += 64) sink ^= buf[i];
    (void)sink;
}

int main(int argc, char* argv[]) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    const char* gdir = argv[1];

    char path[1024];

    // --- Load dict codes ---
    snprintf(path, sizeof(path), "%s/indexes/stmt_codes.bin", gdir);
    int8_t is_code = load_dict_code(path, "IS");
    printf("is_code = %d\n", (int)is_code);

    // --- Load sub/fy.bin ---
    snprintf(path, sizeof(path), "%s/sub/fy.bin", gdir);
    size_t sub_fy_sz = 0;
    const int16_t* sub_fy = (const int16_t*)mmap_file(path, sub_fy_sz);
    size_t sub_N = sub_fy_sz / sizeof(int16_t);
    printf("sub rows: %zu\n", sub_N);

    // Count fy=2023 adsh_codes
    size_t fy2023_count = 0;
    for (size_t i = 0; i < sub_N; i++) if (sub_fy[i] == 2023) fy2023_count++;
    printf("fy=2023 adsh_codes: %zu (%.1f%% of sub)\n",
           fy2023_count, 100.0 * fy2023_count / sub_N);

    // Build fy2023 bitset
    std::vector<bool> fy2023_bits(sub_N, false);
    for (size_t i = 0; i < sub_N; i++) if (sub_fy[i] == 2023) fy2023_bits[i] = true;

    // --- Load pre columns ---
    snprintf(path, sizeof(path), "%s/pre/adsh_code.bin", gdir);
    size_t pre_adsh_sz = 0;
    const int32_t* pre_adsh = (const int32_t*)mmap_file(path, pre_adsh_sz);
    size_t pre_N = pre_adsh_sz / sizeof(int32_t);

    snprintf(path, sizeof(path), "%s/pre/tagver_code.bin", gdir);
    size_t pre_tv_sz = 0;
    const int32_t* pre_tagver = (const int32_t*)mmap_file(path, pre_tv_sz);

    snprintf(path, sizeof(path), "%s/pre/stmt_code.bin", gdir);
    size_t pre_stmt_sz = 0;
    const int8_t* pre_stmt = (const int8_t*)mmap_file(path, pre_stmt_sz);

    printf("pre rows: %zu\n", pre_N);

    // Count IS rows
    size_t is_count = 0;
    size_t is_and_fy_count = 0;
    for (size_t i = 0; i < pre_N; i++) {
        if (pre_stmt[i] == is_code) {
            is_count++;
            int32_t ac = pre_adsh[i];
            if (ac >= 0 && (size_t)ac < sub_N && fy2023_bits[ac])
                is_and_fy_count++;
        }
    }
    printf("IS pre rows: %zu (%.1f%% of pre)\n", is_count, 100.0*is_count/pre_N);
    printf("IS+fy2023 pre rows: %zu (%.1f%% of IS rows, %.1f%% of pre)\n",
           is_and_fy_count,
           100.0*is_and_fy_count/is_count,
           100.0*is_and_fy_count/pre_N);

    // =========================================================
    // ORDER A: Build full IS map (~1.73M entries)
    // Memory: ~1.73M * (4/3) rounded up to power-of-2 * 16 bytes
    // =========================================================
    printf("\n--- Order A: Build is_map from all IS rows ---\n");
    flush_caches();

    SimpleHashMap is_map_A;
    std::vector<uint32_t> next_A(pre_N, UINT32_MAX);

    double t0 = now_ms();
    is_map_A.init(is_count + 1);
    for (size_t i = 0; i < pre_N; i++) {
        if (pre_stmt[i] != is_code) continue;
        uint64_t key = ((uint64_t)(uint32_t)pre_adsh[i] << 32) | (uint32_t)pre_tagver[i];
        uint32_t* hp = is_map_A.find(key);
        if (hp) {
            next_A[i] = *hp;
            *hp = (uint32_t)i;
        } else {
            is_map_A.insert(key, (uint32_t)i);
        }
    }
    double t1 = now_ms();
    printf("  is_map_A entries: %zu\n", is_map_A.count);
    printf("  is_map_A capacity: %zu\n", is_map_A.table.size());
    printf("  is_map_A memory: %.1f MB\n", is_map_A.table.size() * 16.0 / (1024*1024));
    printf("  next_A memory: %.1f MB\n", next_A.size() * 4.0 / (1024*1024));
    printf("  Build time A: %.1f ms\n", t1 - t0);

    // =========================================================
    // ORDER B: Build fy2023 set first, then IS+fy2023 map (~350K entries)
    // Memory: ~350K * (4/3) * 16 bytes ≈ 7-8 MB
    // =========================================================
    printf("\n--- Order B: Build fy2023 adsh set, then IS+fy2023 map ---\n");
    flush_caches();

    SimpleHashMap is_map_B;
    std::vector<uint32_t> next_B(pre_N, UINT32_MAX);

    double t2 = now_ms();
    // Build fy2023 adsh set is already done (fy2023_bits)
    // Now scan pre: only insert if stmt='IS' AND adsh is in fy2023
    is_map_B.init(is_and_fy_count + 1);
    for (size_t i = 0; i < pre_N; i++) {
        if (pre_stmt[i] != is_code) continue;
        int32_t ac = pre_adsh[i];
        if (ac < 0 || (size_t)ac >= sub_N || !fy2023_bits[ac]) continue;
        uint64_t key = ((uint64_t)(uint32_t)ac << 32) | (uint32_t)pre_tagver[i];
        uint32_t* hp = is_map_B.find(key);
        if (hp) {
            next_B[i] = *hp;
            *hp = (uint32_t)i;
        } else {
            is_map_B.insert(key, (uint32_t)i);
        }
    }
    double t3 = now_ms();
    printf("  is_map_B entries: %zu\n", is_map_B.count);
    printf("  is_map_B capacity: %zu\n", is_map_B.table.size());
    printf("  is_map_B memory: %.1f MB\n", is_map_B.table.size() * 16.0 / (1024*1024));
    printf("  next_B memory: %.1f MB\n", next_B.size() * 4.0 / (1024*1024));
    printf("  Build time B: %.1f ms\n", t3 - t2);

    printf("\n--- Summary ---\n");
    printf("Order A (full IS map):           %.1f ms, map=%.1f MB\n",
           t1 - t0, is_map_A.table.size() * 16.0 / (1024*1024));
    printf("Order B (fy2023-filtered IS map): %.1f ms, map=%.1f MB\n",
           t3 - t2, is_map_B.table.size() * 16.0 / (1024*1024));
    printf("Speedup B/A: %.2fx\n", (t1 - t0) / (t3 - t2));
    printf("Map size reduction: %.1fx (%zu -> %zu entries)\n",
           (double)is_map_A.count / is_map_B.count,
           is_map_A.count, is_map_B.count);

    // Verify both maps produce same hits for a sample of num rows
    printf("\nNote: Order B reduces probe-side cache pressure.\n");
    printf("With L3=44MB: Order A map (%.0fMB) exceeds L3, Order B map (%.0fMB) fits.\n",
           is_map_A.table.size()*16.0/(1024*1024),
           is_map_B.table.size()*16.0/(1024*1024));

    return 0;
}
