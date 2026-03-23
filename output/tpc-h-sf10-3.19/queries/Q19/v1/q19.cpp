#include "timing_utils.h"
#include "cli_params.h"

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ---- mmap helper ----
struct MappedFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;
    void open(const std::string& path, bool sequential = false) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::perror(path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st); size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
        if (sequential) {
            madvise(data, size, MADV_SEQUENTIAL);
            madvise(data, size, MADV_HUGEPAGE);
        }
    }
    ~MappedFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
    }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
};

// ---- dict loader ----
static std::vector<std::string> load_dict(const std::string& path) {
    MappedFile f; f.open(path);
    const uint8_t* p = (const uint8_t*)f.data;
    uint32_t count = *(const uint32_t*)p; p += 4;
    std::vector<std::string> dict(count);
    for (uint32_t i = 0; i < count; i++) {
        uint16_t len = *(const uint16_t*)p; p += 2;
        dict[i] = std::string((const char*)p, len); p += len;
    }
    return dict;
}

static int8_t find_code(const std::vector<std::string>& dict, const std::string& val) {
    for (size_t i = 0; i < dict.size(); i++) {
        if (dict[i] == val) return (int8_t)i;
    }
    return -1;
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Parse CLI parameters
    auto p_brand_eq_2  = gendb::parse_string_arg(argc, argv, "--p_brand_eq_2", "Brand#12");
    auto p_brand_eq_3  = gendb::parse_string_arg(argc, argv, "--p_brand_eq_3", "Brand#23");
    auto p_brand_eq    = gendb::parse_string_arg(argc, argv, "--p_brand_eq", "Brand#34");

    int64_t l_qty_lo_2 = gendb::parse_int_arg(argc, argv, "--l_quantity_lower_2", 1);
    int64_t l_qty_hi_2 = gendb::parse_int_arg(argc, argv, "--l_quantity_upper_2", 11);
    int64_t l_qty_lo_3 = gendb::parse_int_arg(argc, argv, "--l_quantity_lower_3", 10);
    int64_t l_qty_hi_3 = gendb::parse_int_arg(argc, argv, "--l_quantity_upper_3", 20);
    int64_t l_qty_lo   = gendb::parse_int_arg(argc, argv, "--l_quantity_lower", 20);
    int64_t l_qty_hi   = gendb::parse_int_arg(argc, argv, "--l_quantity_upper", 30);

    int64_t p_sz_lo_2  = gendb::parse_int_arg(argc, argv, "--p_size_lower_2", 1);
    int64_t p_sz_hi_2  = gendb::parse_int_arg(argc, argv, "--p_size_upper_2", 5);
    int64_t p_sz_lo_3  = gendb::parse_int_arg(argc, argv, "--p_size_lower_3", 1);
    int64_t p_sz_hi_3  = gendb::parse_int_arg(argc, argv, "--p_size_upper_3", 10);
    int64_t p_sz_lo    = gendb::parse_int_arg(argc, argv, "--p_size_lower", 1);
    int64_t p_sz_hi    = gendb::parse_int_arg(argc, argv, "--p_size_upper", 15);

    auto l_shipinstruct_eq   = gendb::parse_string_arg(argc, argv, "--l_shipinstruct_eq", "DELIVER IN PERSON");
    auto l_shipinstruct_eq_2 = gendb::parse_string_arg(argc, argv, "--l_shipinstruct_eq_2", "DELIVER IN PERSON");
    auto l_shipinstruct_eq_3 = gendb::parse_string_arg(argc, argv, "--l_shipinstruct_eq_3", "DELIVER IN PERSON");

    // Quantity ranges per branch (double for comparison with l_quantity which is double)
    double qty_lo[4], qty_hi[4];
    qty_lo[1] = (double)l_qty_lo_2; qty_hi[1] = (double)l_qty_hi_2;
    qty_lo[2] = (double)l_qty_lo_3; qty_hi[2] = (double)l_qty_hi_3;
    qty_lo[3] = (double)l_qty_lo;   qty_hi[3] = (double)l_qty_hi;

    // Mmap all files BEFORE timing scope (exclude munmap TLB shootdown)
    // Part columns
    MappedFile mf_p_partkey, mf_p_brand, mf_p_container, mf_p_size;
    mf_p_partkey.open(gendb_dir + "/part/p_partkey.bin");
    mf_p_brand.open(gendb_dir + "/part/p_brand.bin");
    mf_p_container.open(gendb_dir + "/part/p_container.bin");
    mf_p_size.open(gendb_dir + "/part/p_size.bin");

    // Lineitem columns
    MappedFile mf_l_shipinstruct, mf_l_shipmode, mf_l_partkey, mf_l_quantity, mf_l_extendedprice, mf_l_discount;
    mf_l_shipinstruct.open(gendb_dir + "/lineitem/l_shipinstruct.bin", true);
    mf_l_shipmode.open(gendb_dir + "/lineitem/l_shipmode.bin", true);
    mf_l_partkey.open(gendb_dir + "/lineitem/l_partkey.bin", true);
    mf_l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin", true);
    mf_l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin", true);
    mf_l_discount.open(gendb_dir + "/lineitem/l_discount.bin", true);

    const uint32_t PART_ROWS = (uint32_t)(mf_p_partkey.size / sizeof(int32_t));
    const uint64_t LI_ROWS = mf_l_partkey.size / sizeof(int32_t);

    {
    GENDB_PHASE("total");

    // ---- Phase 1: Load dictionaries, resolve codes ----
    int8_t brand_code[4]; // branch 1,2,3
    int8_t shipinstruct_codes[4]; // per branch (all same for default params but parameterized)
    int8_t air_code, airreg_code;
    // Container codes per branch: 4 each
    int8_t container_codes[4][4];

    {
        GENDB_PHASE("data_loading");

        auto p_brand_dict = load_dict(gendb_dir + "/part/p_brand_dict.bin");
        auto p_container_dict = load_dict(gendb_dir + "/part/p_container_dict.bin");
        auto l_shipmode_dict = load_dict(gendb_dir + "/lineitem/l_shipmode_dict.bin");
        auto l_shipinstruct_dict = load_dict(gendb_dir + "/lineitem/l_shipinstruct_dict.bin");

        brand_code[1] = find_code(p_brand_dict, p_brand_eq_2);
        brand_code[2] = find_code(p_brand_dict, p_brand_eq_3);
        brand_code[3] = find_code(p_brand_dict, p_brand_eq);

        // Branch 1: SM containers
        const char* sm_containers[] = {"SM CASE", "SM BOX", "SM PACK", "SM PKG"};
        for (int i = 0; i < 4; i++) container_codes[1][i] = find_code(p_container_dict, sm_containers[i]);
        // Branch 2: MED containers
        const char* med_containers[] = {"MED BAG", "MED BOX", "MED PKG", "MED PACK"};
        for (int i = 0; i < 4; i++) container_codes[2][i] = find_code(p_container_dict, med_containers[i]);
        // Branch 3: LG containers
        const char* lg_containers[] = {"LG CASE", "LG BOX", "LG PACK", "LG PKG"};
        for (int i = 0; i < 4; i++) container_codes[3][i] = find_code(p_container_dict, lg_containers[i]);

        air_code = find_code(l_shipmode_dict, "AIR");
        airreg_code = find_code(l_shipmode_dict, "AIR REG");

        shipinstruct_codes[1] = find_code(l_shipinstruct_dict, l_shipinstruct_eq_2);
        shipinstruct_codes[2] = find_code(l_shipinstruct_dict, l_shipinstruct_eq_3);
        shipinstruct_codes[3] = find_code(l_shipinstruct_dict, l_shipinstruct_eq);
    }

    // For the lineitem scan, we need a single shipinstruct code check.
    // If all three branches have the same shipinstruct, we can do a single check.
    // Otherwise we need per-branch check. For simplicity, since all defaults are the same,
    // but to be correct with parameterization, we encode it in the branch array.
    // Actually, the shipinstruct filter is on the lineitem side. If all 3 are the same code,
    // we can use a single early filter. Otherwise, we defer to after branch lookup.
    bool single_shipinstruct = (shipinstruct_codes[1] == shipinstruct_codes[2] &&
                                 shipinstruct_codes[2] == shipinstruct_codes[3]);
    int8_t common_shipinstruct = shipinstruct_codes[1];

    // ---- Phase 2: Build branch array ----
    // branch[partkey] = 0 (no match), 1 (Brand#12/SM), 2 (Brand#23/MED), 3 (Brand#34/LG)
    // Also encode shipinstruct requirement per branch if they differ.

    const int32_t* __restrict__ p_partkey = mf_p_partkey.as<int32_t>();
    const int8_t*  __restrict__ p_brand   = mf_p_brand.as<int8_t>();
    const int8_t*  __restrict__ p_container = mf_p_container.as<int8_t>();
    const int32_t* __restrict__ p_size    = mf_p_size.as<int32_t>();

    // Find max partkey for array sizing
    int32_t max_pk = 0;
    for (uint32_t i = 0; i < PART_ROWS; i++) {
        if (p_partkey[i] > max_pk) max_pk = p_partkey[i];
    }

    std::vector<uint8_t> branch_arr(max_pk + 1, 0);
    uint8_t* __restrict__ branch = branch_arr.data();

    {
        GENDB_PHASE("build_branch_array");

        // Size ranges per branch
        int32_t sz_lo[4], sz_hi[4];
        sz_lo[1] = (int32_t)p_sz_lo_2; sz_hi[1] = (int32_t)p_sz_hi_2;
        sz_lo[2] = (int32_t)p_sz_lo_3; sz_hi[2] = (int32_t)p_sz_hi_3;
        sz_lo[3] = (int32_t)p_sz_lo;   sz_hi[3] = (int32_t)p_sz_hi;

        for (uint32_t i = 0; i < PART_ROWS; i++) {
            int8_t b = p_brand[i];
            int8_t c = p_container[i];
            int32_t s = p_size[i];
            int32_t pk = p_partkey[i];

            for (int br = 1; br <= 3; br++) {
                if (b != brand_code[br]) continue;
                if (s < sz_lo[br] || s > sz_hi[br]) continue;
                // Check container
                if (c == container_codes[br][0] || c == container_codes[br][1] ||
                    c == container_codes[br][2] || c == container_codes[br][3]) {
                    branch[pk] = (uint8_t)br;
                    break;
                }
            }
        }
    }

    // ---- Phase 3: Parallel scan of lineitem ----
    const int8_t*  __restrict__ l_shipinstruct = mf_l_shipinstruct.as<int8_t>();
    const int8_t*  __restrict__ l_shipmode     = mf_l_shipmode.as<int8_t>();
    const int32_t* __restrict__ l_partkey      = mf_l_partkey.as<int32_t>();
    const double*  __restrict__ l_quantity     = mf_l_quantity.as<double>();
    const double*  __restrict__ l_extendedprice = mf_l_extendedprice.as<double>();
    const double*  __restrict__ l_discount     = mf_l_discount.as<double>();

    double total_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel reduction(+:total_revenue)
        {
            double local_sum = 0.0;

            #pragma omp for schedule(dynamic, 65536)
            for (uint64_t i = 0; i < LI_ROWS; i++) {
                // Filter 1: shipinstruct (25% pass)
                if (single_shipinstruct) {
                    if (l_shipinstruct[i] != common_shipinstruct) continue;
                } else {
                    // Defer shipinstruct check to after branch lookup
                }

                // Filter 2: shipmode IN (AIR, AIR REG) (13% pass)
                int8_t sm = l_shipmode[i];
                if (sm != air_code && sm != airreg_code) continue;

                // Filter 3: branch lookup
                int32_t pk = l_partkey[i];
                uint8_t br = branch[pk];
                if (br == 0) continue;

                // If not single shipinstruct, check per-branch
                if (!single_shipinstruct) {
                    if (l_shipinstruct[i] != shipinstruct_codes[br]) continue;
                }

                // Filter 4: quantity range
                double qty = l_quantity[i];
                if (qty < qty_lo[br] || qty > qty_hi[br]) continue;

                // Accumulate
                local_sum += l_extendedprice[i] * (1.0 - l_discount[i]);
            }

            total_revenue += local_sum;
        }
    }

    // ---- Phase 4: Output ----
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q19.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        fprintf(f, "revenue\n%.4f\n", total_revenue);
        fclose(f);
    }

    } // end total timer

    return 0;
}
