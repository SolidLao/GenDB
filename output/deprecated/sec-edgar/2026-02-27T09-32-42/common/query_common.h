// Common utilities for SEC EDGAR query programs
#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <functional>

// ====== Binary Record Formats (must match ingest.cpp) ======
#pragma pack(push,1)
struct SubRec {
    uint32_t adsh_id;
    uint32_t cik;
    uint32_t name_id;
    int32_t  sic;
    int32_t  fy;
    uint32_t filed;
    uint8_t  wksi;
    uint8_t  _pad[3];
};

struct TagRec {
    uint32_t tag_id;
    uint32_t ver_id;
    uint32_t tlabel_id;
    uint8_t  abstract;
    uint8_t  custom;
    uint8_t  crdr;
    uint8_t  iord;
};

struct PreRec {
    uint32_t adsh_id;
    uint32_t tag_id;
    uint32_t ver_id;
    uint32_t plabel_id;
    uint32_t line;
    uint8_t  stmt_id;
    uint8_t  rfile_id;
    uint8_t  inpth;
    uint8_t  negating;
};

struct NumRec {
    double   value;
    uint32_t adsh_id;
    uint32_t tag_id;
    uint32_t ver_id;
    uint32_t ddate;
    uint8_t  uom_id;
    uint8_t  qtrs;
    uint8_t  has_value;
    uint8_t  _pad;
};
#pragma pack(pop)

// ====== MMap column loader ======
template<typename T>
struct BinCol {
    const T* data;
    uint64_t count;
    void* mmap_ptr;
    size_t mmap_size;
    int fd;

    BinCol() : data(nullptr), count(0), mmap_ptr(nullptr), mmap_size(0), fd(-1) {}
    ~BinCol() {
        if (mmap_ptr && mmap_ptr != MAP_FAILED) munmap(mmap_ptr, mmap_size);
        if (fd >= 0) close(fd);
    }

    bool load(const char* path) {
        fd = open(path, O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path); return false; }
        struct stat st; fstat(fd, &st);
        mmap_size = st.st_size;
        mmap_ptr = mmap(nullptr, mmap_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (mmap_ptr == MAP_FAILED) { mmap_ptr = nullptr; close(fd); fd=-1; return false; }
        madvise(mmap_ptr, mmap_size, MADV_SEQUENTIAL);
        count = *(const uint64_t*)mmap_ptr;
        data = (const T*)((const char*)mmap_ptr + 8);
        return true;
    }
};

// ====== Dictionary loader ======
struct Dict {
    std::vector<uint32_t> offsets;
    std::vector<char> data;
    uint32_t count;

    Dict() : count(0) {}

    bool load(const char* path) {
        FILE* f = fopen(path, "rb");
        if (!f) { fprintf(stderr, "Cannot open dict %s\n", path); return false; }
        fread(&count, 4, 1, f);
        offsets.resize(count + 1);
        fread(offsets.data(), 4, count + 1, f);
        uint32_t total = offsets[count];
        data.resize(total);
        if (total > 0) fread(data.data(), 1, total, f);
        fclose(f);
        return true;
    }

    std::string get(uint32_t id) const {
        if (id >= count) return "";
        return std::string(data.data() + offsets[id], offsets[id+1] - offsets[id]);
    }

    const char* ptr(uint32_t id) const {
        if (id >= count) return "";
        return data.data() + offsets[id];
    }

    uint32_t len(uint32_t id) const {
        if (id >= count) return 0;
        return offsets[id+1] - offsets[id];
    }

    // Find ID for a string (linear scan - for setup only)
    uint32_t find(const char* s, size_t slen) const {
        for (uint32_t i = 0; i < count; i++) {
            uint32_t l = offsets[i+1] - offsets[i];
            if (l == slen && memcmp(data.data() + offsets[i], s, slen) == 0)
                return i;
        }
        return UINT32_MAX;
    }

    uint32_t find(const std::string& s) const { return find(s.c_str(), s.size()); }
};

// ====== Metadata loader (simple key=val from JSON) ======
// Returns ID for a string value in a small dict like uom_ids, stmt_ids
static uint32_t metadata_get_id(const std::string& meta_path, const char* section, const char* key) {
    FILE* f = fopen(meta_path.c_str(), "r");
    if (!f) return UINT32_MAX;
    char buf[65536]; size_t n = fread(buf, 1, sizeof(buf)-1, f); buf[n] = 0; fclose(f);
    // find section
    const char* p = strstr(buf, section);
    if (!p) return UINT32_MAX;
    // find key within section
    std::string quoted = std::string("\"") + key + "\"";
    p = strstr(p, quoted.c_str());
    if (!p) return UINT32_MAX;
    p += quoted.size();
    while (*p == ' ' || *p == ':') p++;
    char* ep; uint32_t id = (uint32_t)strtoul(p, &ep, 10);
    if (ep == p) return UINT32_MAX;
    return id;
}

// ====== CSV output helpers ======
static inline bool needs_quoting(const char* s, size_t len) {
    for (size_t i = 0; i < len; i++)
        if (s[i] == ',' || s[i] == '"' || s[i] == '\n' || s[i] == '\r') return true;
    return false;
}

static inline void write_csv_string(FILE* f, const char* s, size_t len) {
    if (needs_quoting(s, len)) {
        fputc('"', f);
        for (size_t i = 0; i < len; i++) {
            if (s[i] == '"') fputc('"', f);
            fputc(s[i], f);
        }
        fputc('"', f);
    } else {
        fwrite(s, 1, len, f);
    }
}

static inline void write_csv_str(FILE* f, const Dict& d, uint32_t id) {
    write_csv_string(f, d.ptr(id), d.len(id));
}
