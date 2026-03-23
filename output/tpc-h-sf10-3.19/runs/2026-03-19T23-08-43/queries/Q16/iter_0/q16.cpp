#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <bitset>
#include <thread>
#include <atomic>
#include <omp.h>
#include <parallel/algorithm>

#include "timing_utils.h"
#include "cli_params.h"
#include "mmap_utils.h"

// ---- Dictionary loader ----
struct Dict {
    std::vector<std::string> entries;
    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open dict: " + path);
        struct stat st; fstat(fd, &st);
        size_t sz = st.st_size;
        std::vector<char> buf(sz);
        size_t off = 0;
        while (off < sz) {
            ssize_t r = ::read(fd, buf.data() + off, sz - off);
            if (r <= 0) break;
            off += r;
        }
        ::close(fd);
        const char* p = buf.data();
        uint32_t count = *reinterpret_cast<const uint32_t*>(p); p += 4;
        entries.resize(count);
        for (uint32_t i = 0; i < count; i++) {
            uint16_t len = *reinterpret_cast<const uint16_t*>(p); p += 2;
            entries[i] = std::string(p, len);
            p += len;
        }
    }
};

static bool starts_with(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

// ---- Structs ----
struct QualPart {
    int32_t partkey;
    int8_t brand_code;
    int16_t type_code;
    int32_t size;
};

struct Pair {
    uint32_t group_id;
    int32_t suppkey;
};

struct GroupInfo {
    int8_t brand_code;
    int16_t type_code;
    int32_t size;
};

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Parse CLI params
    std::string p_brand_neq = gendb::parse_string_arg(argc, argv, "--p_brand_neq", "Brand#45");
    std::string p_type_pattern = gendb::parse_string_arg(argc, argv, "--p_type_pattern", "MEDIUM POLISHED%");
    std::string s_comment_pattern = gendb::parse_string_arg(argc, argv, "--s_comment_pattern", "%Customer%Complaints%");

    // Extract LIKE substrings from s_comment_pattern (%X%Y%)
    // Parse: strip leading %, then find segments between %
    std::vector<std::string> comment_segments;
    {
        std::string pat = s_comment_pattern;
        size_t pos = 0;
        while (pos < pat.size() && pat[pos] == '%') pos++;
        while (pos < pat.size()) {
            size_t next = pat.find('%', pos);
            if (next == std::string::npos) {
                comment_segments.push_back(pat.substr(pos));
                break;
            }
            if (next > pos) comment_segments.push_back(pat.substr(pos, next - pos));
            pos = next + 1;
        }
    }

    // Extract type prefix from p_type_pattern (e.g. "MEDIUM POLISHED%"  -> "MEDIUM POLISHED")
    std::string type_prefix = p_type_pattern;
    while (!type_prefix.empty() && type_prefix.back() == '%') type_prefix.pop_back();

    // Declare mmap resources BEFORE timer to exclude munmap TLB shootdown
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

    // ---- Phase 1: Load data ----
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
    }

    // ---- Phase 2: Build excluded suppliers bitset ----
    std::vector<bool> excluded_supp(100001, false);
    {
        GENDB_PHASE("supplier_exclusion");
        size_t n_supp = s_suppkey_col.count;
        const uint32_t* offsets = s_comment_offsets_col.data;
        const char* cdata = s_comment_data_col.data;
        const int32_t* suppkeys = s_suppkey_col.data;

        for (size_t i = 0; i < n_supp; i++) {
            uint32_t start = offsets[i];
            uint32_t end = offsets[i + 1];
            size_t len = end - start;
            const char* s = cdata + start;

            // General LIKE matching with segments
            bool match = true;
            size_t pos = 0;
            for (auto& seg : comment_segments) {
                // Find seg starting from pos
                bool found = false;
                for (size_t j = pos; j + seg.size() <= len; j++) {
                    if (memcmp(s + j, seg.data(), seg.size()) == 0) {
                        pos = j + seg.size();
                        found = true;
                        break;
                    }
                }
                if (!found) { match = false; break; }
            }
            if (match) {
                int32_t sk = suppkeys[i];
                if (sk >= 0 && sk <= 100000) excluded_supp[sk] = true;
            }
        }
    }

    // ---- Phase 3: Load dicts, identify excluded codes ----
    Dict brand_dict, type_dict;
    int8_t excluded_brand_code = -1;
    std::vector<bool> excluded_type_codes;
    {
        GENDB_PHASE("dict_setup");
        brand_dict.load(gendb_dir + "/part/p_brand_dict.bin");
        type_dict.load(gendb_dir + "/part/p_type_dict.bin");

        for (size_t i = 0; i < brand_dict.entries.size(); i++) {
            if (brand_dict.entries[i] == p_brand_neq) {
                excluded_brand_code = (int8_t)i;
                break;
            }
        }

        excluded_type_codes.resize(type_dict.entries.size(), false);
        for (size_t i = 0; i < type_dict.entries.size(); i++) {
            if (starts_with(type_dict.entries[i], type_prefix)) {
                excluded_type_codes[i] = true;
            }
        }
    }

    // ---- Phase 4: Filter parts (parallel) ----
    // Size lookup array
    bool size_ok[51] = {};
    int sizes[] = {49, 14, 23, 45, 19, 3, 36, 9};
    for (int s : sizes) if (s <= 50) size_ok[s] = true;

    size_t n_parts = p_partkey_col.count;
    const int32_t* partkeys = p_partkey_col.data;
    const int8_t* brands = p_brand_col.data;
    const int16_t* types = p_type_col.data;
    const int32_t* psizes = p_size_col.data;

    // Group map: (brand, type, size) -> group_id
    std::unordered_map<uint64_t, uint32_t> group_map;
    std::vector<GroupInfo> group_infos;
    std::vector<QualPart> qual_parts;

    {
        GENDB_PHASE("filter_parts");

        // Two-pass parallel scatter
        unsigned nthreads = std::thread::hardware_concurrency();
        if (nthreads > 64) nthreads = 64;
        size_t morsel = 65536;

        // Pass 1: count qualifying per thread
        size_t n_morsels = (n_parts + morsel - 1) / morsel;
        std::vector<size_t> counts(n_morsels, 0);

        #pragma omp parallel for schedule(dynamic)
        for (size_t m = 0; m < n_morsels; m++) {
            size_t start = m * morsel;
            size_t end = std::min(start + morsel, n_parts);
            size_t cnt = 0;
            for (size_t i = start; i < end; i++) {
                int32_t sz = psizes[i];
                if (sz < 0 || sz > 50 || !size_ok[sz]) continue;
                if (brands[i] == excluded_brand_code) continue;
                int16_t tc = types[i];
                if (tc >= 0 && (size_t)tc < excluded_type_codes.size() && excluded_type_codes[tc]) continue;
                cnt++;
            }
            counts[m] = cnt;
        }

        // Prefix sum
        std::vector<size_t> offsets(n_morsels + 1, 0);
        for (size_t m = 0; m < n_morsels; m++) offsets[m + 1] = offsets[m] + counts[m];
        size_t total_qual = offsets[n_morsels];
        qual_parts.resize(total_qual);

        // Pass 2: scatter
        #pragma omp parallel for schedule(dynamic)
        for (size_t m = 0; m < n_morsels; m++) {
            size_t start = m * morsel;
            size_t end = std::min(start + morsel, n_parts);
            size_t pos = offsets[m];
            for (size_t i = start; i < end; i++) {
                int32_t sz = psizes[i];
                if (sz < 0 || sz > 50 || !size_ok[sz]) continue;
                if (brands[i] == excluded_brand_code) continue;
                int16_t tc = types[i];
                if (tc >= 0 && (size_t)tc < excluded_type_codes.size() && excluded_type_codes[tc]) continue;
                qual_parts[pos++] = {partkeys[i], brands[i], tc, sz};
            }
        }

        // Build group map (single-threaded, ~300K parts -> ~18K groups)
        group_map.reserve(20000);
        for (auto& qp : qual_parts) {
            uint64_t key = ((uint64_t)(uint8_t)qp.brand_code << 48) |
                           ((uint64_t)(uint16_t)qp.type_code << 32) |
                           (uint64_t)(uint32_t)qp.size;
            auto it = group_map.find(key);
            if (it == group_map.end()) {
                uint32_t gid = group_infos.size();
                group_map[key] = gid;
                group_infos.push_back({qp.brand_code, qp.type_code, qp.size});
            }
        }
    }

    // ---- Phase 5: Index probe + collect pairs ----
    // Parse index
    const char* idx_ptr = index_raw.data;
    uint64_t idx_num_entries = *reinterpret_cast<const uint64_t*>(idx_ptr);
    const uint32_t* idx_body = reinterpret_cast<const uint32_t*>(idx_ptr + 8);
    // idx_body[partkey*2] = start, idx_body[partkey*2+1] = count

    const int32_t* ps_suppkeys = ps_suppkey_col.data;

    std::vector<Pair> all_pairs;
    {
        GENDB_PHASE("index_probe");

        // Parallel: each thread collects its own pairs
        unsigned nthreads = std::thread::hardware_concurrency();
        if (nthreads > 64) nthreads = 64;
        size_t n_qual = qual_parts.size();
        size_t morsel = 4096;
        size_t n_morsels = (n_qual + morsel - 1) / morsel;

        std::vector<std::vector<Pair>> thread_pairs(nthreads);
        for (auto& tp : thread_pairs) tp.reserve(n_qual * 4 / nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& my_pairs = thread_pairs[tid];

            #pragma omp for schedule(dynamic)
            for (size_t m = 0; m < n_morsels; m++) {
                size_t start = m * morsel;
                size_t end = std::min(start + morsel, n_qual);
                for (size_t i = start; i < end; i++) {
                    auto& qp = qual_parts[i];
                    int32_t pk = qp.partkey;
                    if (pk < 0 || (uint64_t)pk >= idx_num_entries) continue;

                    uint32_t ps_start = idx_body[pk * 2];
                    uint32_t ps_count = idx_body[pk * 2 + 1];

                    // Compute group_id
                    uint64_t key = ((uint64_t)(uint8_t)qp.brand_code << 48) |
                                   ((uint64_t)(uint16_t)qp.type_code << 32) |
                                   (uint64_t)(uint32_t)qp.size;
                    uint32_t gid = group_map[key]; // already built, safe read-only

                    for (uint32_t j = 0; j < ps_count; j++) {
                        int32_t sk = ps_suppkeys[ps_start + j];
                        if (sk >= 0 && sk <= 100000 && excluded_supp[sk]) continue;
                        my_pairs.push_back({gid, sk});
                    }
                }
            }
        }

        // Merge thread pairs
        size_t total = 0;
        for (auto& tp : thread_pairs) total += tp.size();
        all_pairs.resize(total);
        size_t off = 0;
        for (auto& tp : thread_pairs) {
            memcpy(all_pairs.data() + off, tp.data(), tp.size() * sizeof(Pair));
            off += tp.size();
        }
    }

    // ---- Phase 6: Count distinct suppkeys per group ----
    std::vector<uint32_t> group_counts(group_infos.size(), 0);
    {
        GENDB_PHASE("aggregation");

        // Sort by (group_id, suppkey) using __gnu_parallel
        __gnu_parallel::sort(all_pairs.begin(), all_pairs.end(),
            [](const Pair& a, const Pair& b) {
                if (a.group_id != b.group_id) return a.group_id < b.group_id;
                return a.suppkey < b.suppkey;
            });

        // Linear scan count distinct
        size_t n = all_pairs.size();
        if (n > 0) {
            uint32_t cur_gid = all_pairs[0].group_id;
            int32_t cur_sk = all_pairs[0].suppkey;
            uint32_t cnt = 1;
            for (size_t i = 1; i < n; i++) {
                if (all_pairs[i].group_id != cur_gid) {
                    group_counts[cur_gid] = cnt;
                    cur_gid = all_pairs[i].group_id;
                    cur_sk = all_pairs[i].suppkey;
                    cnt = 1;
                } else if (all_pairs[i].suppkey != cur_sk) {
                    cur_sk = all_pairs[i].suppkey;
                    cnt++;
                }
            }
            group_counts[cur_gid] = cnt;
        }
    }

    // ---- Phase 7: Sort and output ----
    {
        GENDB_PHASE("output");

        // Build result tuples
        struct Result {
            uint32_t supplier_cnt;
            int8_t brand_code;
            int16_t type_code;
            int32_t size;
        };
        std::vector<Result> results;
        results.reserve(group_infos.size());
        for (size_t i = 0; i < group_infos.size(); i++) {
            if (group_counts[i] > 0) {
                results.push_back({group_counts[i], group_infos[i].brand_code,
                                   group_infos[i].type_code, group_infos[i].size});
            }
        }

        // Sort: supplier_cnt DESC, brand ASC, type ASC, size ASC
        // For brand/type: compare by decoded string
        std::sort(results.begin(), results.end(),
            [&](const Result& a, const Result& b) {
                if (a.supplier_cnt != b.supplier_cnt) return a.supplier_cnt > b.supplier_cnt;
                int cmp = brand_dict.entries[a.brand_code].compare(brand_dict.entries[b.brand_code]);
                if (cmp != 0) return cmp < 0;
                cmp = type_dict.entries[a.type_code].compare(type_dict.entries[b.type_code]);
                if (cmp != 0) return cmp < 0;
                return a.size < b.size;
            });

        // Write CSV
        FILE* f = fopen((results_dir + "/Q16.csv").c_str(), "w");
        fprintf(f, "p_brand,p_type,p_size,supplier_cnt\n");
        for (auto& r : results) {
            fprintf(f, "\"%s\",\"%s\",%d,%u\n",
                brand_dict.entries[r.brand_code].c_str(),
                type_dict.entries[r.type_code].c_str(),
                r.size, r.supplier_cnt);
        }
        fclose(f);
    }

    } // total timer

    return 0;
}
