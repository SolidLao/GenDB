// Q16: Parts/Supplier Relationship — GenDB optimized (iter 1 v9)
// Key: 2-pass fused index probe with atomic scatter, no sort needed for probe,
// pre-computed sort keys for output, minimal string matching for supplier exclusion
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <omp.h>

#include "timing_utils.h"
#include "cli_params.h"
#include "mmap_utils.h"

struct Dict {
    std::vector<std::string> entries;
    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        struct stat st; fstat(fd, &st);
        size_t sz = st.st_size;
        std::vector<char> buf(sz);
        size_t off = 0;
        while (off < sz) { ssize_t r = ::read(fd, buf.data()+off, sz-off); if (r<=0) break; off+=r; }
        ::close(fd);
        const char* p = buf.data();
        uint32_t count; memcpy(&count, p, 4); p += 4;
        entries.resize(count);
        for (uint32_t i = 0; i < count; i++) {
            uint16_t len; memcpy(&len, p, 2); p += 2;
            entries[i] = std::string(p, len); p += len;
        }
    }
};

struct GroupInfo {
    int8_t brand_code;
    int16_t type_code;
    int32_t size;
};

static inline int i32_to_str(int32_t v, char* buf) {
    if (v == 0) { buf[0] = '0'; return 1; }
    char tmp[12]; int len = 0;
    while (v > 0) { tmp[len++] = '0' + (v % 10); v /= 10; }
    for (int i = len - 1; i >= 0; i--) buf[len - 1 - i] = tmp[i];
    return len;
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    std::string p_brand_neq = gendb::parse_string_arg(argc, argv, "--p_brand_neq", "Brand#45");
    std::string p_type_pattern = gendb::parse_string_arg(argc, argv, "--p_type_pattern", "MEDIUM POLISHED%");
    std::string s_comment_pattern = gendb::parse_string_arg(argc, argv, "--s_comment_pattern", "%Customer%Complaints%");

    std::vector<std::string> comment_segments;
    {
        size_t pos = 0;
        while (pos < s_comment_pattern.size() && s_comment_pattern[pos] == '%') pos++;
        while (pos < s_comment_pattern.size()) {
            size_t next = s_comment_pattern.find('%', pos);
            if (next == std::string::npos) { comment_segments.push_back(s_comment_pattern.substr(pos)); break; }
            if (next > pos) comment_segments.push_back(s_comment_pattern.substr(pos, next - pos));
            pos = next + 1;
        }
    }
    std::string type_prefix = p_type_pattern;
    while (!type_prefix.empty() && type_prefix.back() == '%') type_prefix.pop_back();

    // Mmap BEFORE timer
    gendb::MmapColumn<int32_t> s_suppkey_col;
    gendb::MmapColumn<uint32_t> s_comment_offsets_col;
    gendb::MmapColumn<char> s_comment_data_col;
    gendb::MmapColumn<int32_t> p_partkey_col;
    gendb::MmapColumn<int8_t> p_brand_col;
    gendb::MmapColumn<int16_t> p_type_col;
    gendb::MmapColumn<int32_t> p_size_col;
    gendb::MmapColumn<int32_t> ps_suppkey_col;
    gendb::MmapColumn<char> index_raw;

    {
    GENDB_PHASE("total");

    {
        GENDB_PHASE("data_loading");
        s_suppkey_col.open(gendb_dir + "/supplier/s_suppkey.bin");
        s_comment_offsets_col.open(gendb_dir + "/supplier/s_comment_offsets.bin");
        s_comment_data_col.open(gendb_dir + "/supplier/s_comment_data.bin");
        p_partkey_col.open(gendb_dir + "/part/p_partkey.bin");
        p_brand_col.open(gendb_dir + "/part/p_brand.bin");
        p_type_col.open(gendb_dir + "/part/p_type.bin");
        p_size_col.open(gendb_dir + "/part/p_size.bin");
        ps_suppkey_col.open(gendb_dir + "/partsupp/ps_suppkey.bin");
        ps_suppkey_col.advise_random();
        index_raw.open(gendb_dir + "/indexes/partsupp_ps_partkey_grouped.bin");
        madvise((void*)index_raw.data, index_raw.file_size, MADV_SEQUENTIAL);
    }

    // Build excluded suppliers
    static constexpr int SUPPKEY_DOMAIN = 100001;
    uint8_t* excluded_supp = new uint8_t[SUPPKEY_DOMAIN]();
    {
        GENDB_PHASE("build_excluded_suppliers");
        size_t n_supp = s_suppkey_col.count;
        const uint32_t* offsets = s_comment_offsets_col.data;
        const char* cdata = s_comment_data_col.data;
        const int32_t* suppkeys = s_suppkey_col.data;

        // Optimize: for the default pattern %Customer%Complaints%,
        // use memmem-style search for "Customer" then "Complaints"
        const char* seg0 = comment_segments.size() > 0 ? comment_segments[0].c_str() : nullptr;
        size_t seg0_len = comment_segments.size() > 0 ? comment_segments[0].size() : 0;
        const char* seg1 = comment_segments.size() > 1 ? comment_segments[1].c_str() : nullptr;
        size_t seg1_len = comment_segments.size() > 1 ? comment_segments[1].size() : 0;
        int n_segs = (int)comment_segments.size();

        #pragma omp parallel for schedule(static, 4096)
        for (size_t i = 0; i < n_supp; i++) {
            uint32_t start = offsets[i], end = offsets[i + 1];
            size_t len = end - start;
            const char* s = cdata + start;

            if (n_segs == 2) {
                // Fast path for 2 segments
                void* p1 = memmem(s, len, seg0, seg0_len);
                if (!p1) continue;
                size_t pos1 = (const char*)p1 - s + seg0_len;
                void* p2 = memmem(s + pos1, len - pos1, seg1, seg1_len);
                if (!p2) continue;
            } else {
                bool match = true;
                size_t pos = 0;
                for (int si = 0; si < n_segs; si++) {
                    void* p = memmem(s + pos, len - pos, comment_segments[si].c_str(),
                                     comment_segments[si].size());
                    if (!p) { match = false; break; }
                    pos = (const char*)p - s + comment_segments[si].size();
                }
                if (!match) continue;
            }
            int32_t sk = suppkeys[i];
            if (sk >= 0 && sk < SUPPKEY_DOMAIN) excluded_supp[sk] = 1;
        }
    }

    // Dict setup
    Dict brand_dict, type_dict;
    brand_dict.load(gendb_dir + "/part/p_brand_dict.bin");
    type_dict.load(gendb_dir + "/part/p_type_dict.bin");
    int n_brands = (int)brand_dict.entries.size();
    int n_types = (int)type_dict.entries.size();

    int8_t excluded_brand_code = -1;
    for (int i = 0; i < n_brands; i++)
        if (brand_dict.entries[i] == p_brand_neq) { excluded_brand_code = (int8_t)i; break; }

    uint8_t* excluded_type = new uint8_t[n_types]();
    for (int i = 0; i < n_types; i++)
        if (type_dict.entries[i].size() >= type_prefix.size() &&
            memcmp(type_dict.entries[i].c_str(), type_prefix.c_str(), type_prefix.size()) == 0)
            excluded_type[i] = 1;

    const int sizes[] = {49, 14, 23, 45, 19, 3, 36, 9};
    int8_t size_to_idx[51];
    memset(size_to_idx, -1, sizeof(size_to_idx));
    for (int i = 0; i < 8; i++) size_to_idx[sizes[i]] = (int8_t)i;

    int stride8 = n_types * 8;
    int map_size = n_brands * stride8;
    uint16_t* group_map = new uint16_t[map_size];
    memset(group_map, 0xFF, map_size * sizeof(uint16_t));
    std::vector<GroupInfo> group_infos;
    uint16_t next_gid = 0;

    for (int b = 0; b < n_brands; b++) {
        if (b == excluded_brand_code) continue;
        for (int t = 0; t < n_types; t++) {
            if (excluded_type[t]) continue;
            for (int s = 0; s < 8; s++) {
                group_map[b * stride8 + t * 8 + s] = next_gid++;
                group_infos.push_back({(int8_t)b, (int16_t)t, sizes[s]});
            }
        }
    }
    int n_groups = next_gid;

    // Pre-compute output strings for groups (avoid repeated dict lookups during sort)
    // Sort key: brand_str, type_str, size — pre-build for comparison
    // Since dict codes are sequential and brands/types are lexicographically ordered in dict,
    // we can just sort by (brand_code, type_code, size) which gives the same lex order
    // Wait — dict codes may not be in lex order. Let's build a lex rank.
    std::vector<uint8_t> brand_rank(n_brands);
    {
        std::vector<uint8_t> idx(n_brands);
        for (int i = 0; i < n_brands; i++) idx[i] = i;
        std::sort(idx.begin(), idx.end(),
                  [&](uint8_t a, uint8_t b) { return brand_dict.entries[a] < brand_dict.entries[b]; });
        for (int r = 0; r < n_brands; r++) brand_rank[idx[r]] = r;
    }
    std::vector<uint8_t> type_rank(n_types);
    {
        std::vector<uint16_t> idx(n_types);
        for (int i = 0; i < n_types; i++) idx[i] = i;
        std::sort(idx.begin(), idx.end(),
                  [&](uint16_t a, uint16_t b) { return type_dict.entries[a] < type_dict.entries[b]; });
        for (int r = 0; r < n_types; r++) type_rank[idx[r]] = r;
    }

    // Filter parts — two-pass scatter
    size_t n_parts = p_partkey_col.count;
    const int32_t* partkeys = p_partkey_col.data;
    const int8_t* brands = p_brand_col.data;
    const int16_t* types_col = p_type_col.data;
    const int32_t* psizes = p_size_col.data;

    int32_t* qual_pk = nullptr;
    uint16_t* qual_gid = nullptr;
    size_t total_qual = 0;
    {
        GENDB_PHASE("filter_parts");
        size_t morsel = 65536;
        size_t n_morsels = (n_parts + morsel - 1) / morsel;
        std::vector<size_t> counts(n_morsels);

        #pragma omp parallel for schedule(static)
        for (size_t m = 0; m < n_morsels; m++) {
            size_t start = m * morsel, end = std::min(start + morsel, n_parts);
            size_t cnt = 0;
            for (size_t i = start; i < end; i++) {
                int32_t sz = psizes[i];
                if (sz < 1 || sz > 50 || size_to_idx[sz] < 0) continue;
                if (brands[i] == excluded_brand_code) continue;
                if (excluded_type[(uint16_t)types_col[i]]) continue;
                cnt++;
            }
            counts[m] = cnt;
        }

        std::vector<size_t> psum(n_morsels + 1, 0);
        for (size_t m = 0; m < n_morsels; m++) psum[m+1] = psum[m] + counts[m];
        total_qual = psum[n_morsels];
        qual_pk = new int32_t[total_qual];
        qual_gid = new uint16_t[total_qual];

        #pragma omp parallel for schedule(static)
        for (size_t m = 0; m < n_morsels; m++) {
            size_t start = m * morsel, end = std::min(start + morsel, n_parts);
            size_t pos = psum[m];
            for (size_t i = start; i < end; i++) {
                int32_t sz = psizes[i];
                if (sz < 1 || sz > 50) continue;
                int8_t si = size_to_idx[sz];
                if (si < 0) continue;
                int8_t bc = brands[i];
                if (bc == excluded_brand_code) continue;
                int16_t tc = types_col[i];
                if (excluded_type[(uint16_t)tc]) continue;
                qual_pk[pos] = partkeys[i];
                qual_gid[pos] = group_map[bc * stride8 + tc * 8 + si];
                pos++;
            }
        }
    }

    // 2-pass fused index probe with atomic scatter
    const char* idx_ptr = index_raw.data;
    uint64_t idx_num_entries;
    memcpy(&idx_num_entries, idx_ptr, 8);
    const uint32_t* idx_body = reinterpret_cast<const uint32_t*>(idx_ptr + 8);
    const int32_t* ps_suppkeys = ps_suppkey_col.data;

    std::vector<int32_t> group_counts(n_groups, 0);
    {
        GENDB_PHASE("main_scan");

        // Pass 1: count suppkeys per group from index metadata
        std::vector<size_t> group_cnt(n_groups, 0);
        for (size_t qi = 0; qi < total_qual; qi++) {
            int32_t pk = qual_pk[qi];
            if ((uint64_t)pk < idx_num_entries)
                group_cnt[qual_gid[qi]] += idx_body[pk * 2 + 1];
        }

        // Prefix sum
        size_t total_sk = 0;
        std::vector<size_t> group_off(n_groups + 1, 0);
        for (int g = 0; g < n_groups; g++) {
            group_off[g] = total_sk;
            total_sk += group_cnt[g];
        }
        group_off[n_groups] = total_sk;

        // Allocate and scatter
        int32_t* all_sk = new int32_t[total_sk];
        std::vector<std::atomic<size_t>> group_wpos(n_groups);
        for (int g = 0; g < n_groups; g++)
            group_wpos[g].store(group_off[g], std::memory_order_relaxed);

        // Pass 2: probe + scatter
        #pragma omp parallel for schedule(dynamic, 2048)
        for (size_t qi = 0; qi < total_qual; qi++) {
            int32_t pk = qual_pk[qi];
            uint16_t gid = qual_gid[qi];
            if ((uint64_t)pk >= idx_num_entries) continue;

            uint32_t ps_start = idx_body[pk * 2];
            uint32_t ps_count = idx_body[pk * 2 + 1];

            for (uint32_t j = 0; j < ps_count; j++) {
                int32_t sk = ps_suppkeys[ps_start + j];
                if (sk >= 0 && sk < SUPPKEY_DOMAIN && !excluded_supp[sk]) {
                    size_t pos = group_wpos[gid].fetch_add(1, std::memory_order_relaxed);
                    all_sk[pos] = sk;
                }
            }
        }

        // Distinct count per group (parallel)
        {
            GENDB_PHASE("aggregation");
            #pragma omp parallel for schedule(dynamic, 64)
            for (int g = 0; g < n_groups; g++) {
                size_t start = group_off[g];
                size_t end = group_wpos[g].load(std::memory_order_relaxed);
                size_t cnt = end - start;
                if (cnt == 0) continue;
                int32_t* arr = all_sk + start;
                std::sort(arr, arr + cnt);
                int32_t distinct = 1;
                for (size_t i = 1; i < cnt; i++)
                    if (arr[i] != arr[i-1]) distinct++;
                group_counts[g] = distinct;
            }
        }
        delete[] all_sk;
    }

    delete[] qual_pk;
    delete[] qual_gid;
    delete[] excluded_supp;
    delete[] excluded_type;
    delete[] group_map;

    // Sort and output
    {
        GENDB_PHASE("output");
        struct Result {
            int32_t supplier_cnt;
            uint8_t brand_code;
            uint16_t type_code;
            int32_t size;
            // Pre-computed sort key: (-supplier_cnt, brand_rank, type_rank, size)
            uint64_t sort_key;
        };
        std::vector<Result> results;
        results.reserve(n_groups);
        for (int i = 0; i < n_groups; i++) {
            if (group_counts[i] > 0) {
                auto& gi = group_infos[i];
                // Pack sort key: invert supplier_cnt for DESC, rest ASC
                uint64_t key = ((uint64_t)(0xFFFF - (uint16_t)group_counts[i]) << 48) |
                               ((uint64_t)brand_rank[(uint8_t)gi.brand_code] << 40) |
                               ((uint64_t)type_rank[(uint16_t)gi.type_code] << 24) |
                               ((uint64_t)(uint16_t)gi.size);
                results.push_back({group_counts[i], (uint8_t)gi.brand_code,
                                   (uint16_t)gi.type_code, gi.size, key});
            }
        }

        std::sort(results.begin(), results.end(),
                  [](const Result& a, const Result& b) { return a.sort_key < b.sort_key; });

        // Build CSV in memory
        size_t est_size = results.size() * 80 + 64;
        char* csv = new char[est_size];
        size_t pos = 0;
        const char hdr[] = "p_brand,p_type,p_size,supplier_cnt\n";
        memcpy(csv + pos, hdr, sizeof(hdr) - 1); pos += sizeof(hdr) - 1;

        for (auto& r : results) {
            const auto& brand = brand_dict.entries[r.brand_code];
            csv[pos++] = '"';
            memcpy(csv + pos, brand.data(), brand.size()); pos += brand.size();
            csv[pos++] = '"';
            csv[pos++] = ',';
            const auto& type = type_dict.entries[r.type_code];
            csv[pos++] = '"';
            memcpy(csv + pos, type.data(), type.size()); pos += type.size();
            csv[pos++] = '"';
            csv[pos++] = ',';
            pos += i32_to_str(r.size, csv + pos);
            csv[pos++] = ',';
            pos += i32_to_str(r.supplier_cnt, csv + pos);
            csv[pos++] = '\n';
        }

        int fd = ::open((results_dir + "/Q16.csv").c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ssize_t w = ::write(fd, csv, pos);
        (void)w;
        ::close(fd);
        delete[] csv;
    }

    } // total timer
    return 0;
}
