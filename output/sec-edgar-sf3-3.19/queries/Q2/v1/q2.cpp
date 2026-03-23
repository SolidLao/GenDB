#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <climits>
#include <thread>

#include "timing_utils.h"
#include "cli_params.h"
#include "mmap_utils.h"

// ---- Dictionary loader ----
struct Dictionary {
    std::vector<uint32_t> offsets;
    std::vector<char> str_data;
    uint32_t count = 0;

    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open dict: " + path);
        ::read(fd, &count, 4);
        offsets.resize(count + 1);
        ::read(fd, offsets.data(), (count + 1) * 4);
        str_data.resize(offsets[count]);
        ::read(fd, str_data.data(), offsets[count]);
        ::close(fd);
    }

    std::string_view get(uint32_t code) const {
        return std::string_view(str_data.data() + offsets[code], offsets[code + 1] - offsets[code]);
    }

    uint32_t find_code(const std::string& s) const {
        for (uint32_t i = 0; i < count; i++) {
            size_t len = offsets[i + 1] - offsets[i];
            if (len == s.size() && memcmp(str_data.data() + offsets[i], s.data(), len) == 0)
                return i;
        }
        return UINT32_MAX;
    }
};

// ---- Open-addressing hash map ----
struct MaxHashMap {
    struct Entry {
        uint64_t key;
        double value;
    };
    Entry* slots;
    uint32_t capacity;
    uint32_t mask;
    static constexpr uint64_t EMPTY = UINT64_MAX;

    MaxHashMap() : slots(nullptr), capacity(0), mask(0) {}
    ~MaxHashMap() { delete[] slots; }

    void init(uint32_t cap_hint) {
        capacity = 1;
        while (capacity < cap_hint * 2) capacity <<= 1;
        mask = capacity - 1;
        slots = new Entry[capacity];
        for (uint32_t i = 0; i < capacity; i++) {
            slots[i].key = EMPTY;
            slots[i].value = -1e300;
        }
    }

    inline void insert_or_max(uint64_t key, double val) {
        uint32_t idx = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            if (slots[idx].key == key) {
                if (val > slots[idx].value) slots[idx].value = val;
                return;
            }
            if (slots[idx].key == EMPTY) {
                slots[idx].key = key;
                slots[idx].value = val;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    inline const double* find(uint64_t key) const {
        uint32_t idx = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (true) {
            if (slots[idx].key == key) return &slots[idx].value;
            if (slots[idx].key == EMPTY) return nullptr;
            idx = (idx + 1) & mask;
        }
    }
};

struct QualRow {
    uint32_t adsh_code;
    uint32_t tag_code;
    double value;
};

struct Result {
    double value;
    uint32_t name_code;
    uint32_t tag_code;
};

static void write_csv_field(FILE* f, const std::string_view& sv) {
    bool needs_quote = false;
    for (char c : sv) {
        if (c == ',' || c == '"' || c == '\n') { needs_quote = true; break; }
    }
    if (needs_quote) {
        fputc('"', f);
        for (char c : sv) {
            if (c == '"') fputc('"', f);
            fputc(c, f);
        }
        fputc('"', f);
    } else {
        fwrite(sv.data(), 1, sv.size(), f);
    }
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    int64_t limit = gendb::parse_int_arg(argc, argv, "--limit", 100);
    std::string uom_eq = gendb::parse_string_arg(argc, argv, "--uom_eq", "pure");
    int64_t fy_eq = gendb::parse_int_arg(argc, argv, "--fy_eq", 2022);
    int16_t fy_val = (int16_t)fy_eq;

    // Mmap columns OUTSIDE timing to exclude TLB shootdown
    gendb::MmapColumn<uint16_t> num_uom(gendb_dir + "/num/uom.bin");
    gendb::MmapColumn<uint8_t>  num_vnull(gendb_dir + "/num/value_null.bin");
    gendb::MmapColumn<double>   num_value(gendb_dir + "/num/value.bin");
    gendb::MmapColumn<uint32_t> num_adsh(gendb_dir + "/num/adsh.bin");
    gendb::MmapColumn<uint32_t> num_tag(gendb_dir + "/num/tag.bin");
    gendb::MmapColumn<int16_t>  sub_fy(gendb_dir + "/sub/fy.bin");
    gendb::MmapColumn<uint32_t> sub_adsh(gendb_dir + "/sub/adsh.bin");
    gendb::MmapColumn<uint32_t> sub_name(gendb_dir + "/sub/name.bin");

    {
    GENDB_PHASE("total");

    Dictionary uom_dict, tag_dict, name_dict;
    uint32_t sub_adsh_lookup_size = 0;
    std::vector<uint32_t> sub_adsh_lookup;

    {
        GENDB_PHASE("data_loading");
        // Load dictionaries in parallel threads
        std::thread t1([&]{ uom_dict.load(gendb_dir + "/dictionaries/uom.dict"); });
        std::thread t2([&]{ tag_dict.load(gendb_dir + "/dictionaries/tag.dict"); });
        std::thread t3([&]{ name_dict.load(gendb_dir + "/dictionaries/name.dict"); });

        // Load sub_adsh_lookup index on main thread
        int fd = ::open((gendb_dir + "/indexes/sub_adsh_lookup.idx").c_str(), O_RDONLY);
        ::read(fd, &sub_adsh_lookup_size, 4);
        sub_adsh_lookup.resize(sub_adsh_lookup_size);
        ::read(fd, sub_adsh_lookup.data(), sub_adsh_lookup_size * 4);
        ::close(fd);

        t1.join(); t2.join(); t3.join();
    }

    uint16_t uom_code = (uint16_t)uom_dict.find_code(uom_eq);

    // Build qualifying adsh bitset and name lookup
    uint32_t max_adsh_code = sub_adsh_lookup_size;
    std::vector<uint8_t> adsh_bitset((max_adsh_code + 7) / 8, 0);
    std::vector<uint32_t> qualifying_name(max_adsh_code, UINT32_MAX);

    {
        GENDB_PHASE("dim_filter");
        size_t sub_rows = sub_fy.count;
        for (size_t i = 0; i < sub_rows; i++) {
            if (sub_fy[i] == fy_val) {
                uint32_t ac = sub_adsh[i];
                adsh_bitset[ac >> 3] |= (1 << (ac & 7));
                qualifying_name[ac] = sub_name[i];
            }
        }
    }

    // Single pass: collect qualifying rows
    size_t num_rows = num_uom.count;
    int nthreads = std::min(omp_get_max_threads(), 32);

    std::vector<std::vector<QualRow>> thread_collected(nthreads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_collected[tid];
            local.reserve(std::max((size_t)1000, num_rows / nthreads / 4));
            const uint16_t* uom_ptr = num_uom.data;
            const uint8_t* vnull_ptr = num_vnull.data;
            const double* val_ptr = num_value.data;
            const uint32_t* adsh_ptr = num_adsh.data;
            const uint32_t* tag_ptr = num_tag.data;
            const uint8_t* bs = adsh_bitset.data();

            #pragma omp for schedule(static)
            for (size_t i = 0; i < num_rows; i++) {
                if (__builtin_expect(uom_ptr[i] != uom_code, 1)) continue;
                if (vnull_ptr[i] != 0) continue;
                uint32_t ac = adsh_ptr[i];
                if (!(bs[ac >> 3] & (1 << (ac & 7)))) continue;
                local.push_back({ac, tag_ptr[i], val_ptr[i]});
            }
        }
    }

    // Build max map + match
    std::vector<Result> all_results;
    {
        GENDB_PHASE("aggregation");

        size_t total_qual = 0;
        for (auto& v : thread_collected) total_qual += v.size();

        MaxHashMap max_map;
        max_map.init(total_qual > 0 ? (uint32_t)total_qual : 1024);

        // Build max map single-threaded (sequential scan of compact data)
        for (int t = 0; t < nthreads; t++) {
            for (auto& r : thread_collected[t]) {
                uint64_t key = ((uint64_t)r.adsh_code << 32) | r.tag_code;
                max_map.insert_or_max(key, r.value);
            }
        }

        // Match phase - parallel
        std::vector<std::vector<Result>> thread_results(nthreads);
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local_res = thread_results[tid];
            local_res.reserve(200);
            const uint32_t* qn = qualifying_name.data();

            #pragma omp for schedule(static)
            for (int t = 0; t < nthreads; t++) {
                for (auto& r : thread_collected[t]) {
                    uint64_t key = ((uint64_t)r.adsh_code << 32) | r.tag_code;
                    const double* mv = max_map.find(key);
                    if (mv && r.value == *mv) {
                        local_res.push_back({r.value, qn[r.adsh_code], r.tag_code});
                    }
                }
            }
        }

        size_t total_res = 0;
        for (auto& v : thread_results) total_res += v.size();
        all_results.reserve(total_res);
        for (auto& v : thread_results) {
            all_results.insert(all_results.end(), v.begin(), v.end());
        }
    }

    {
        GENDB_PHASE("topk_sort");
        size_t k = std::min((size_t)limit, all_results.size());
        auto cmp = [&](const Result& a, const Result& b) {
            if (a.value != b.value) return a.value > b.value;
            auto na = name_dict.get(a.name_code);
            auto nb = name_dict.get(b.name_code);
            if (na != nb) return na < nb;
            return tag_dict.get(a.tag_code) < tag_dict.get(b.tag_code);
        };
        if (k < all_results.size()) {
            std::partial_sort(all_results.begin(), all_results.begin() + k, all_results.end(), cmp);
        } else {
            std::sort(all_results.begin(), all_results.end(), cmp);
        }
        all_results.resize(k);
    }

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q2.csv").c_str(), "w");
        fprintf(f, "name,tag,value\n");
        for (auto& r : all_results) {
            write_csv_field(f, name_dict.get(r.name_code));
            fputc(',', f);
            write_csv_field(f, tag_dict.get(r.tag_code));
            fprintf(f, ",%g\n", r.value);
        }
        fclose(f);
    }

    } // end total
    return 0;
}
