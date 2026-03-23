// Build flat partsupp lookup: for each partkey, store 4 (suppkey, supplycost) pairs contiguously
// Output: column_versions/partsupp.pk_flat.dense_array/flat.bin
// Layout: uint32_t max_partkey header, then PSFlat[max_partkey+1]
// PSFlat = { int32_t suppkeys[4]; double supplycosts[4]; } = 48 bytes

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

struct PSFlat {
    int32_t suppkeys[4];
    double supplycosts[4];
};
static_assert(sizeof(PSFlat) == 48, "PSFlat must be 48 bytes");

static void* map_file(const char* path, size_t& sz) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    struct stat st; fstat(fd, &st); sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return p;
}

int main(int argc, char** argv) {
    if (argc < 2) { fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]); return 1; }
    char path[4096]; size_t sz;

    // Read partsupp_pk_index
    snprintf(path, sizeof(path), "%s/indexes/partsupp_pk_index.bin", argv[1]);
    auto* idx_raw = (const char*)map_file(path, sz);
    uint32_t max_partkey = *(const uint32_t*)idx_raw;
    struct PSEntry { uint32_t start; uint32_t count; };
    const PSEntry* ps_idx = (const PSEntry*)(idx_raw + 4);

    // Read ps_suppkey and ps_supplycost
    snprintf(path, sizeof(path), "%s/partsupp/ps_suppkey.bin", argv[1]);
    const int32_t* ps_sk = (const int32_t*)map_file(path, sz);
    snprintf(path, sizeof(path), "%s/partsupp/ps_supplycost.bin", argv[1]);
    const double* ps_cost = (const double*)map_file(path, sz);

    // Build flat array
    size_t n_entries = (size_t)max_partkey + 1;
    PSFlat* flat = (PSFlat*)calloc(n_entries, sizeof(PSFlat));
    if (!flat) { perror("calloc"); return 1; }

    // Initialize suppkeys to -1 sentinel
    for (size_t i = 0; i < n_entries; i++)
        for (int j = 0; j < 4; j++) flat[i].suppkeys[j] = -1;

    uint32_t max_count = 0;
    for (uint32_t pk = 0; pk <= max_partkey; pk++) {
        uint32_t s = ps_idx[pk].start, c = ps_idx[pk].count;
        if (c > 4) c = 4;
        if (c > max_count) max_count = c;
        for (uint32_t j = 0; j < c; j++) {
            flat[pk].suppkeys[j] = ps_sk[s + j];
            flat[pk].supplycosts[j] = ps_cost[s + j];
        }
    }

    // Write output
    snprintf(path, sizeof(path), "%s/column_versions/partsupp.pk_flat.dense_array", argv[1]);
    char cmd[4200]; snprintf(cmd, sizeof(cmd), "mkdir -p %s", path);
    system(cmd);

    snprintf(path, sizeof(path), "%s/column_versions/partsupp.pk_flat.dense_array/flat.bin", argv[1]);
    FILE* fp = fopen(path, "wb");
    if (!fp) { perror("fopen"); return 1; }
    fwrite(&max_partkey, sizeof(uint32_t), 1, fp);
    fwrite(flat, sizeof(PSFlat), n_entries, fp);
    fclose(fp);

    size_t file_size = 4 + n_entries * sizeof(PSFlat);
    printf("Built partsupp flat: max_partkey=%u, max_count=%u, entries=%zu, file_size=%zuMB\n",
           max_partkey, max_count, n_entries, file_size / (1024*1024));

    free(flat);
    return 0;
}
