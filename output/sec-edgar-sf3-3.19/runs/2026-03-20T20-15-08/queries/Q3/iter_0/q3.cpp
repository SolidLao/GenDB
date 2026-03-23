#include "timing_utils.h"
#include "cli_params.h"
#include "mmap_utils.h"

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

struct Dictionary {
    std::vector<std::string> entries;

    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open dict: " + path);
        struct stat st;
        fstat(fd, &st);
        size_t sz = st.st_size;
        char* buf = (char*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (buf == MAP_FAILED) throw std::runtime_error("mmap dict failed");

        uint32_t num = *(uint32_t*)buf;
        const uint32_t* offsets = (const uint32_t*)(buf + 4);
        const char* str_data = buf + 4 + (num + 1) * 4;

        entries.resize(num);
        for (uint32_t i = 0; i < num; i++) {
            entries[i] = std::string(str_data + offsets[i], offsets[i+1] - offsets[i]);
        }
        munmap(buf, sz);
    }

    uint32_t find_code(const std::string& val) const {
        for (uint32_t i = 0; i < (uint32_t)entries.size(); i++) {
            if (entries[i] == val) return i;
        }
        return UINT32_MAX;
    }
};

// Open-addressing hash map for (name_code, cik) -> sum(value)
struct MainAggMap {
    struct Entry {
        uint32_t name_code;
        int32_t cik;
        int64_t sum_cents;
        bool occupied;
    };

    Entry* table;
    uint32_t mask;
    uint32_t capacity;

    MainAggMap() : table(nullptr), mask(0), capacity(0) {}

    void init(uint32_t cap_hint) {
        capacity = 1;
        while (capacity < cap_hint) capacity <<= 1;
        mask = capacity - 1;
        table = (Entry*)calloc(capacity, sizeof(Entry));
    }

    ~MainAggMap() { if (table) free(table); }

    inline void add(uint32_t name_code, int32_t cik, int64_t cents) {
        uint32_t h = (name_code * 2654435761u ^ (uint32_t)cik * 2246822519u) & mask;
        for (uint32_t probe = 0; probe < capacity; probe++) {
            Entry& e = table[h];
            if (!e.occupied) {
                e.name_code = name_code;
                e.cik = cik;
                e.sum_cents = cents;
                e.occupied = true;
                return;
            }
            if (e.name_code == name_code && e.cik == cik) {
                e.sum_cents += cents;
                return;
            }
            h = (h + 1) & mask;
        }
    }
};

// Open-addressing hash map for cik -> sum(value)
struct CikAggMap {
    struct Entry {
        int32_t cik;
        int64_t sum_cents;
        bool occupied;
    };

    Entry* table;
    uint32_t mask;
    uint32_t capacity;

    CikAggMap() : table(nullptr), mask(0), capacity(0) {}

    void init(uint32_t cap_hint) {
        capacity = 1;
        while (capacity < cap_hint) capacity <<= 1;
        mask = capacity - 1;
        table = (Entry*)calloc(capacity, sizeof(Entry));
    }

    ~CikAggMap() { if (table) free(table); }

    inline void add(int32_t cik, int64_t cents) {
        uint32_t h = ((uint32_t)cik * 2654435761u) & mask;
        for (uint32_t probe = 0; probe < capacity; probe++) {
            Entry& e = table[h];
            if (!e.occupied) {
                e.cik = cik;
                e.sum_cents = cents;
                e.occupied = true;
                return;
            }
            if (e.cik == cik) {
                e.sum_cents += cents;
                return;
            }
            h = (h + 1) & mask;
        }
    }
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]);
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();
    int64_t p_limit = gendb::parse_int_arg(argc, argv, "--limit", 100);
    std::string p_uom_eq = gendb::parse_string_arg(argc, argv, "--uom_eq", "USD");
    int64_t p_fy_eq = gendb::parse_int_arg(argc, argv, "--fy_eq", 2022);
    (void)gendb::parse_string_arg(argc, argv, "--uom_eq_2", "USD");
    (void)gendb::parse_int_arg(argc, argv, "--fy_eq_2", 2022);

    // Declare mmaps outside timing scope to exclude munmap TLB shootdown cost
    gendb::MmapColumn<int16_t> sub_fy;
    gendb::MmapColumn<uint32_t> sub_adsh;
    gendb::MmapColumn<uint32_t> sub_name;
    gendb::MmapColumn<int32_t> sub_cik;
    gendb::MmapColumn<uint32_t> num_adsh;
    gendb::MmapColumn<uint16_t> num_uom;
    gendb::MmapColumn<uint8_t> num_value_null;
    gendb::MmapColumn<double> num_value;

    {
    GENDB_PHASE("total");

    Dictionary uom_dict, name_dict;
    uint16_t uom_code;
    uint32_t sub_info_size = 0;

    {
        GENDB_PHASE("data_loading");

        uom_dict.load(gendb_dir + "/dictionaries/uom.dict");
        name_dict.load(gendb_dir + "/dictionaries/name.dict");

        uom_code = (uint16_t)uom_dict.find_code(p_uom_eq);

        // Read sub_adsh_lookup size for adsh code range
        {
            int fd = ::open((gendb_dir + "/indexes/sub_adsh_lookup.idx").c_str(), O_RDONLY);
            if (fd < 0) throw std::runtime_error("Cannot open sub_adsh_lookup.idx");
            ssize_t r = ::read(fd, &sub_info_size, 4);
            (void)r;
            ::close(fd);
        }

        sub_fy.open(gendb_dir + "/sub/fy.bin");
        sub_adsh.open(gendb_dir + "/sub/adsh.bin");
        sub_name.open(gendb_dir + "/sub/name.bin");
        sub_cik.open(gendb_dir + "/sub/cik.bin");

        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_uom.open(gendb_dir + "/num/uom.bin");
        num_value_null.open(gendb_dir + "/num/value_null.bin");
        num_value.open(gendb_dir + "/num/value.bin");
    }

    // Build sub_info_array: indexed by adsh_code -> {name_code, cik}
    struct SubInfo {
        uint32_t name_code;
        int32_t cik;
    };

    std::vector<SubInfo> sub_info(sub_info_size, {UINT32_MAX, 0});

    {
        GENDB_PHASE("prefilter_sub");
        size_t sub_rows = sub_fy.count;
        int16_t fy_val = (int16_t)p_fy_eq;
        for (size_t i = 0; i < sub_rows; i++) {
            if (sub_fy[i] == fy_val) {
                uint32_t adsh_code = sub_adsh[i];
                if (adsh_code < sub_info_size) {
                    sub_info[adsh_code].name_code = sub_name[i];
                    sub_info[adsh_code].cik = sub_cik[i];
                }
            }
        }
    }

    // Parallel scan with per-thread hash maps
    unsigned int nthreads = std::thread::hardware_concurrency();
    if (nthreads == 0) nthreads = 32;
    if (nthreads > 32) nthreads = 32;

    // Allocate per-thread maps
    std::vector<MainAggMap> main_maps(nthreads);
    std::vector<CikAggMap> cik_maps(nthreads);

    {
        GENDB_PHASE("main_scan");
        const size_t num_rows = num_uom.count;
        const uint16_t* uom_data = num_uom.data;
        const uint8_t* vnull_data = num_value_null.data;
        const uint32_t* adsh_data = num_adsh.data;
        const double* value_data = num_value.data;
        const SubInfo* si_ptr = sub_info.data();
        const uint32_t si_sz = (uint32_t)sub_info.size();

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            // Init maps inside parallel region for NUMA-local allocation
            main_maps[tid].init(32768);
            cik_maps[tid].init(32768);
            MainAggMap& my_main = main_maps[tid];
            CikAggMap& my_cik = cik_maps[tid];

            #pragma omp for schedule(dynamic, 100000)
            for (size_t i = 0; i < num_rows; i++) {
                if (uom_data[i] != uom_code) continue;
                if (vnull_data[i] != 0) continue;
                uint32_t ac = adsh_data[i];
                if (ac >= si_sz) continue;
                uint32_t nc = si_ptr[ac].name_code;
                if (nc == UINT32_MAX) continue;
                int64_t cents = llround(value_data[i] * 100.0);
                my_main.add(nc, si_ptr[ac].cik, cents);
                my_cik.add(si_ptr[ac].cik, cents);
            }
        }
    }

    // Tree-reduce merge: merge pairs of maps in parallel, log2(nthreads) rounds
    {
        GENDB_PHASE("merge_maps");
        for (unsigned stride = 1; stride < nthreads; stride <<= 1) {
            #pragma omp parallel for num_threads(nthreads) schedule(static)
            for (unsigned t = 0; t < nthreads; t += stride * 2) {
                unsigned src = t + stride;
                if (src >= nthreads || !main_maps[src].table) continue;
                MainAggMap& dst_main = main_maps[t];
                MainAggMap& src_main = main_maps[src];
                for (uint32_t i = 0; i < src_main.capacity; i++) {
                    auto& e = src_main.table[i];
                    if (e.occupied) dst_main.add(e.name_code, e.cik, e.sum_cents);
                }
                free(src_main.table); src_main.table = nullptr;

                CikAggMap& dst_cik = cik_maps[t];
                CikAggMap& src_cik = cik_maps[src];
                for (uint32_t i = 0; i < src_cik.capacity; i++) {
                    auto& e = src_cik.table[i];
                    if (e.occupied) dst_cik.add(e.cik, e.sum_cents);
                }
                free(src_cik.table); src_cik.table = nullptr;
            }
        }
    }

    // Final merged maps are in index 0
    MainAggMap& global_main = main_maps[0];
    CikAggMap& global_cik = cik_maps[0];

    // Compute HAVING threshold: AVG of cik sums (in cents)
    double having_threshold_cents;
    {
        GENDB_PHASE("having_threshold");
        __int128 total_sum = 0;
        uint64_t count = 0;
        for (uint32_t i = 0; i < global_cik.capacity; i++) {
            if (global_cik.table[i].occupied) {
                total_sum += global_cik.table[i].sum_cents;
                count++;
            }
        }
        having_threshold_cents = (count > 0) ? (double)total_sum / (double)count : 0.0;
    }

    // Filter and top-K
    struct Result {
        uint32_t name_code;
        int32_t cik;
        int64_t total_cents;
    };
    std::vector<Result> results;

    {
        GENDB_PHASE("filter_topk");
        results.reserve(16000);
        for (uint32_t i = 0; i < global_main.capacity; i++) {
            auto& e = global_main.table[i];
            if (e.occupied && (double)e.sum_cents > having_threshold_cents) {
                results.push_back({e.name_code, e.cik, e.sum_cents});
            }
        }

        auto cmp = [](const Result& a, const Result& b) {
            if (a.total_cents != b.total_cents) return a.total_cents > b.total_cents;
            if (a.cik != b.cik) return a.cik < b.cik;
            return a.name_code < b.name_code;
        };
        size_t k = std::min((size_t)p_limit, results.size());
        if (k < results.size()) {
            std::partial_sort(results.begin(), results.begin() + k, results.end(), cmp);
            results.resize(k);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }
    }

    // Output
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q3.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) throw std::runtime_error("Cannot open output: " + out_path);

        fprintf(fout, "name,cik,total_value\n");
        for (auto& r : results) {
            const std::string& name = name_dict.entries[r.name_code];
            // Always quote the name column
            fputc('"', fout);
            for (char c : name) {
                if (c == '"') { fputc('"', fout); fputc('"', fout); }
                else fputc(c, fout);
            }
            fputc('"', fout);
            // Format cents as fixed-point with 2 decimal places
            int64_t cents = r.total_cents;
            bool neg = cents < 0;
            uint64_t abs_cents = neg ? (uint64_t)(-(cents + 1)) + 1 : (uint64_t)cents;
            uint64_t whole = abs_cents / 100;
            uint64_t frac = abs_cents % 100;
            if (neg)
                fprintf(fout, ",%d,-%llu.%02llu\n", r.cik, (unsigned long long)whole, (unsigned long long)frac);
            else
                fprintf(fout, ",%d,%llu.%02llu\n", r.cik, (unsigned long long)whole, (unsigned long long)frac);
        }
        fclose(fout);
    }

    } // total timer

    return 0;
}
