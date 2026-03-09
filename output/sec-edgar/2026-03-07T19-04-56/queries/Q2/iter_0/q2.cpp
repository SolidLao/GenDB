// Q2: Find rows with max value per (adsh, tag) where uom='pure', fy=2022
// Two-pass approach: pass1 builds max map, pass2 collects matches
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

// Combine sub_fk (uint32) and tag_code (uint32) into a single uint64 key
static inline uint64_t make_key(uint32_t sub_fk, uint32_t tag_code) {
    return ((uint64_t)sub_fk << 32) | (uint64_t)tag_code;
}

// Custom hash for uint64 keys
struct U64Hash {
    size_t operator()(uint64_t k) const {
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return k;
    }
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb(argv[1]);
    std::string results(argv[2]);

    GENDB_PHASE_MS("total", total_ms);

    // ===================== DATA LOADING =====================
    double load_ms = 0;
    uint8_t pureCode = 255;
    uint64_t rangeStart = 0, rangeEnd = 0;
    size_t subRows = 0;

    // Columns (mmap)
    MmapColumn<uint32_t> sub_fk_col;
    MmapColumn<uint32_t> tag_code_col;
    MmapColumn<double>   value_col;
    MmapColumn<int16_t>  fy_col;
    MmapColumn<uint64_t> name_offsets_col;
    MmapColumn<char>     name_data_col;
    MmapColumn<uint64_t> tag_dict_offsets_col;
    MmapColumn<char>     tag_dict_data_col;

    {
        GENDB_PHASE_MS("data_loading", load_ms);

        // 1. Resolve pure code from uom_dict
        {
            MmapColumn<uint64_t> dict_off(gendb + "/num/uom_dict_offsets.bin");
            MmapColumn<char> dict_data(gendb + "/num/uom_dict_data.bin");
            for (size_t i = 0; i + 1 < dict_off.count; i++) {
                size_t start = dict_off[i], end = dict_off[i+1];
                if (end - start == 4 && memcmp(dict_data.data + start, "pure", 4) == 0) {
                    pureCode = (uint8_t)i;
                    break;
                }
            }
        }
        if (pureCode == 255) {
            fprintf(stderr, "ERROR: 'pure' not found in uom_dict\n");
            return 1;
        }

        // 2. Load uom_offsets to get row range for pureCode
        {
            int fd = ::open((gendb + "/num/uom_offsets.bin").c_str(), O_RDONLY);
            uint32_t numEntries;
            ::read(fd, &numEntries, 4);
            if (pureCode < numEntries) {
                struct Range { uint64_t start, end; };
                // Seek to the right entry
                lseek(fd, 4 + pureCode * sizeof(Range), SEEK_SET);
                Range r;
                ::read(fd, &r, sizeof(Range));
                rangeStart = r.start;
                rangeEnd = r.end;
            }
            ::close(fd);
        }

        // 3. mmap all needed columns
        sub_fk_col.open(gendb + "/num/sub_fk.bin");
        tag_code_col.open(gendb + "/num/tag_code.bin");
        value_col.open(gendb + "/num/value.bin");
        fy_col.open(gendb + "/sub/fy.bin");
        name_offsets_col.open(gendb + "/sub/name_offsets.bin");
        name_data_col.open(gendb + "/sub/name_data.bin");
        tag_dict_offsets_col.open(gendb + "/dicts/tag_dict_offsets.bin");
        tag_dict_data_col.open(gendb + "/dicts/tag_dict_data.bin");

        subRows = fy_col.count;

        // Prefetch the num columns in the pure range
        // Use madvise willneed on the range we'll scan
        auto advise_range = [](const void* base, size_t elem_size, uint64_t start, uint64_t end) {
            if (start >= end) return;
            size_t byte_start = start * elem_size;
            size_t byte_end = end * elem_size;
            // Align to page
            size_t page = 4096;
            byte_start = (byte_start / page) * page;
            madvise((void*)((char*)base + byte_start), byte_end - byte_start, MADV_SEQUENTIAL);
        };
        advise_range(sub_fk_col.data, 4, rangeStart, rangeEnd);
        advise_range(tag_code_col.data, 4, rangeStart, rangeEnd);
        advise_range(value_col.data, 8, rangeStart, rangeEnd);
    }

    // ===================== BUILD FY2022 BITSET =====================
    std::vector<bool> fy2022(subRows, false);
    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < subRows; i++) {
            if (fy_col[i] == 2022) fy2022[i] = true;
        }
    }

    // ===================== PASS 1: BUILD MAX MAP =====================
    // Using parallel thread-local maps, then merge
    uint64_t scanLen = rangeEnd - rangeStart;
    int nthreads = omp_get_max_threads();

    // Global max map
    std::unordered_map<uint64_t, double, U64Hash> max_map;
    {
        GENDB_PHASE("build_joins");

        std::vector<std::unordered_map<uint64_t, double, U64Hash>> local_maps(nthreads);
        for (auto& m : local_maps) m.reserve(500000 / nthreads + 1000);

        const uint32_t* sfk = sub_fk_col.data + rangeStart;
        const uint32_t* tc  = tag_code_col.data + rangeStart;
        const double*   val = value_col.data + rangeStart;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& lm = local_maps[tid];

            #pragma omp for schedule(static)
            for (uint64_t i = 0; i < scanLen; i++) {
                double v = val[i];
                if (std::isnan(v)) continue;
                uint64_t key = make_key(sfk[i], tc[i]);
                auto it = lm.find(key);
                if (it == lm.end()) {
                    lm.emplace(key, v);
                } else if (v > it->second) {
                    it->second = v;
                }
            }
        }

        // Merge local maps into global
        max_map.reserve(600000);
        for (auto& lm : local_maps) {
            for (auto& [k, v] : lm) {
                auto it = max_map.find(k);
                if (it == max_map.end()) {
                    max_map.emplace(k, v);
                } else if (v > it->second) {
                    it->second = v;
                }
            }
        }
    }

    // ===================== PASS 2: COLLECT MATCHES =====================
    struct Match {
        uint32_t sub_fk;
        uint32_t tag_code;
        double value;
    };
    std::vector<Match> matches;
    {
        GENDB_PHASE("main_scan");

        std::vector<std::vector<Match>> local_matches(nthreads);

        const uint32_t* sfk = sub_fk_col.data + rangeStart;
        const uint32_t* tc  = tag_code_col.data + rangeStart;
        const double*   val = value_col.data + rangeStart;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& lm = local_matches[tid];

            #pragma omp for schedule(static)
            for (uint64_t i = 0; i < scanLen; i++) {
                double v = val[i];
                if (std::isnan(v)) continue;
                uint32_t sf = sfk[i];
                if (sf >= subRows || !fy2022[sf]) continue;
                uint64_t key = make_key(sf, tc[i]);
                auto it = max_map.find(key);
                if (it != max_map.end() && v == it->second) {
                    lm.push_back({sf, tc[i], v});
                }
            }
        }

        // Concatenate
        size_t total = 0;
        for (auto& lm : local_matches) total += lm.size();
        matches.reserve(total);
        for (auto& lm : local_matches) {
            matches.insert(matches.end(), lm.begin(), lm.end());
        }
    }

    // ===================== DECODE AND TOP-K =====================
    {
        GENDB_PHASE_MS("output", output_ms);

        // Decode strings for sorting
        struct ResultRow {
            std::string name;
            std::string tag;
            double value;
        };
        std::vector<ResultRow> rows;
        rows.reserve(matches.size());

        for (auto& m : matches) {
            ResultRow r;
            r.value = m.value;

            // Decode sub name
            uint64_t nstart = name_offsets_col[m.sub_fk];
            uint64_t nend = name_offsets_col[m.sub_fk + 1];
            r.name.assign(name_data_col.data + nstart, nend - nstart);

            // Decode tag
            uint64_t tstart = tag_dict_offsets_col[m.tag_code];
            uint64_t tend = tag_dict_offsets_col[m.tag_code + 1];
            r.tag.assign(tag_dict_data_col.data + tstart, tend - tstart);

            rows.push_back(std::move(r));
        }

        // Sort: value DESC, name ASC, tag ASC
        std::sort(rows.begin(), rows.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.value != b.value) return a.value > b.value;
            if (a.name != b.name) return a.name < b.name;
            return a.tag < b.tag;
        });

        // Write output
        size_t limit = std::min(rows.size(), (size_t)100);
        std::string outPath = results + "/Q2.csv";
        FILE* fp = fopen(outPath.c_str(), "w");
        fprintf(fp, "name,tag,value\n");
        for (size_t i = 0; i < limit; i++) {
            // Quote fields that contain commas
            const auto& name = rows[i].name;
            const auto& tag = rows[i].tag;
            if (name.find(',') != std::string::npos) {
                fprintf(fp, "\"%s\"", name.c_str());
            } else {
                fprintf(fp, "%s", name.c_str());
            }
            if (tag.find(',') != std::string::npos) {
                fprintf(fp, ",\"%s\",%.2f\n", tag.c_str(), rows[i].value);
            } else {
                fprintf(fp, ",%s,%.2f\n", tag.c_str(), rows[i].value);
            }
        }
        fclose(fp);
    }

    return 0;
}
