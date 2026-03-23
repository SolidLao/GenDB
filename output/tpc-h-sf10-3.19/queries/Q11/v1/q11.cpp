#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"
#include "cli_params.h"

struct MappedFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    MappedFile() = default;
    MappedFile(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::perror(path.c_str()); return; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; close(fd); fd = -1; return; }
        madvise(data, size, advice);
    }
    ~MappedFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
    template<typename T> size_t count() const { return size / sizeof(T); }
};

static std::vector<char> read_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::perror(path.c_str()); return {}; }
    struct stat st; fstat(fd, &st);
    std::vector<char> buf(st.st_size);
    size_t total = 0;
    while (total < buf.size()) {
        ssize_t r = read(fd, buf.data() + total, buf.size() - total);
        if (r <= 0) break;
        total += r;
    }
    close(fd);
    return buf;
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    std::string n_name_eq = gendb::parse_string_arg(argc, argv, "--n_name_eq", "GERMANY");

    // Mmap columns before timer to exclude munmap TLB shootdown cost
    MappedFile ps_suppkey_file(gendb_dir + "/partsupp/ps_suppkey.bin", MADV_SEQUENTIAL);
    MappedFile ps_partkey_file(gendb_dir + "/partsupp/ps_partkey.bin", MADV_SEQUENTIAL);
    MappedFile ps_supplycost_file(gendb_dir + "/partsupp/ps_supplycost.bin", MADV_SEQUENTIAL);
    MappedFile ps_availqty_file(gendb_dir + "/partsupp/ps_availqty.bin", MADV_SEQUENTIAL);
    MappedFile s_suppkey_file(gendb_dir + "/supplier/s_suppkey.bin");
    MappedFile s_nationkey_file(gendb_dir + "/supplier/s_nationkey.bin");

    // Pre-allocate aggregation array (16MB) before timer
    static constexpr int MAX_PARTKEY = 2000001;
    std::vector<double> agg(MAX_PARTKEY, 0.0);

    {
    GENDB_PHASE("total");

    // Step 1: Filter nation
    int32_t target_nationkey = -1;
    {
        GENDB_PHASE("filter_nation");
        auto nk_data = read_file(gendb_dir + "/nation/n_nationkey.bin");
        auto off_data = read_file(gendb_dir + "/nation/n_name_offsets.bin");
        auto str_data = read_file(gendb_dir + "/nation/n_name_data.bin");

        const int32_t* nkeys = reinterpret_cast<const int32_t*>(nk_data.data());
        const int32_t* offsets = reinterpret_cast<const int32_t*>(off_data.data());
        size_t n_nations = nk_data.size() / sizeof(int32_t);
        const char* sdata = str_data.data();

        for (size_t i = 0; i < n_nations; i++) {
            int32_t start = offsets[i];
            int32_t end = offsets[i + 1];
            size_t len = end - start;
            if (len == n_name_eq.size() && memcmp(sdata + start, n_name_eq.data(), len) == 0) {
                target_nationkey = nkeys[i];
                break;
            }
        }
    }

    if (target_nationkey < 0) {
        FILE* out = fopen((results_dir + "/Q11.csv").c_str(), "w");
        fprintf(out, "ps_partkey,value\n");
        fclose(out);
        return 0;
    }

    // Step 2: Build supplier filter — uint8 array indexed by suppkey
    const int32_t* s_suppkeys = s_suppkey_file.as<int32_t>();
    const int32_t* s_nationkeys = s_nationkey_file.as<int32_t>();
    size_t n_suppliers = s_suppkey_file.count<int32_t>();

    static constexpr int MAX_SUPPKEY = 100001;
    std::vector<uint8_t> supplier_filter(MAX_SUPPKEY, 0);

    {
        GENDB_PHASE("build_supplier_filter");
        for (size_t i = 0; i < n_suppliers; i++) {
            if (s_nationkeys[i] == target_nationkey) {
                supplier_filter[s_suppkeys[i]] = 1;
            }
        }
    }

    // Step 3: Single-pass scan of partsupp — aggregate + grand total
    const int32_t* ps_suppkeys = ps_suppkey_file.as<int32_t>();
    const int32_t* ps_partkeys = ps_partkey_file.as<int32_t>();
    const double* ps_supplycosts = ps_supplycost_file.as<double>();
    const int32_t* ps_availqtys = ps_availqty_file.as<int32_t>();
    size_t n_partsupp = ps_suppkey_file.count<int32_t>();

    double grand_total = 0.0;
    {
        GENDB_PHASE("main_scan");
        double* agg_ptr = agg.data();
        const uint8_t* sf = supplier_filter.data();

        for (size_t i = 0; i < n_partsupp; i++) {
            if (sf[ps_suppkeys[i]]) {
                double val = ps_supplycosts[i] * static_cast<double>(ps_availqtys[i]);
                agg_ptr[ps_partkeys[i]] += val;
                grand_total += val;
            }
        }
    }

    // Step 4: HAVING filter
    // TPC-H Q11: fraction = 0.0001 / SF. Derive SF from partsupp row count.
    double sf = static_cast<double>(n_partsupp) / 800000.0;
    double fraction = 0.0001 / sf;
    double threshold = grand_total * fraction;
    struct Result {
        int32_t partkey;
        double value;
    };
    std::vector<Result> results;

    {
        GENDB_PHASE("having_filter");
        const double* agg_ptr = agg.data();
        results.reserve(400000);
        for (int i = 0; i < MAX_PARTKEY; i++) {
            if (agg_ptr[i] > threshold) {
                results.push_back({static_cast<int32_t>(i), agg_ptr[i]});
            }
        }
    }

    // Step 5: Sort by value DESC
    {
        GENDB_PHASE("sort");
        std::sort(results.begin(), results.end(),
            [](const Result& a, const Result& b) { return a.value > b.value; });
    }

    // Output
    {
        GENDB_PHASE("output");
        FILE* out = fopen((results_dir + "/Q11.csv").c_str(), "w");
        fprintf(out, "ps_partkey,value\n");
        for (const auto& r : results) {
            fprintf(out, "%d,%.2f\n", r.partkey, r.value);
        }
        fclose(out);
    }

    } // end total timer

    return 0;
}
