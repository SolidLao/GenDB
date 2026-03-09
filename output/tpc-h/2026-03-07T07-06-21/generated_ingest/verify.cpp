#include <cstdio>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <cstring>
#include <string>

template<typename T>
std::vector<T> read_col(const char* path, size_t max_rows = 0) {
    FILE* f = fopen(path, "rb");
    if (!f) { perror(path); return {}; }
    fseek(f, 0, SEEK_END);
    size_t n = ftell(f) / sizeof(T);
    if (max_rows && max_rows < n) n = max_rows;
    fseek(f, 0, SEEK_SET);
    std::vector<T> v(n);
    fread(v.data(), sizeof(T), n, f);
    fclose(f);
    return v;
}

int main() {
    const char* base = "/home/jl4492/GenDB/output/tpc-h/2026-03-07T07-06-21/gendb";

    // Spot-check l_shipdate (should be days since epoch, 1992-01-02 = 8036)
    {
        auto sd = read_col<int32_t>((std::string(base)+"/lineitem/l_shipdate.bin").c_str(), 10);
        printf("l_shipdate[0..9]: ");
        for (auto v : sd) printf("%d ", v);
        printf("\n  (1992-01-02 = 8036, 1998-12-01 = 10561)\n");
        // Check sorted
        auto full = read_col<int32_t>((std::string(base)+"/lineitem/l_shipdate.bin").c_str());
        bool sorted = true;
        for (size_t i = 1; i < full.size(); i++) if (full[i] < full[i-1]) { sorted = false; break; }
        printf("l_shipdate sorted: %s (%zu rows)\n", sorted?"YES":"NO", full.size());
        printf("l_shipdate min=%d max=%d\n", full.front(), full.back());
    }

    // Spot-check l_extendedprice (should be non-zero doubles)
    {
        auto ep = read_col<double>((std::string(base)+"/lineitem/l_extendedprice.bin").c_str(), 5);
        printf("l_extendedprice[0..4]: ");
        for (auto v : ep) printf("%.2f ", v);
        printf("\n");
    }

    // Spot-check l_discount (int8, values 0-10)
    {
        auto d = read_col<int8_t>((std::string(base)+"/lineitem/l_discount.bin").c_str(), 10);
        printf("l_discount[0..9] (int8, should be 0-10): ");
        for (auto v : d) printf("%d ", (int)v);
        printf("\n");
    }

    // Spot-check l_quantity (int8, values 1-50)
    {
        auto q = read_col<int8_t>((std::string(base)+"/lineitem/l_quantity.bin").c_str(), 10);
        printf("l_quantity[0..9] (int8, should be 1-50): ");
        for (auto v : q) printf("%d ", (int)v);
        printf("\n");
    }

    // Spot-check o_orderdate (should be days since epoch ~1992-1998)
    {
        auto od = read_col<int32_t>((std::string(base)+"/orders/o_orderdate.bin").c_str(), 5);
        printf("o_orderdate[0..4]: ");
        for (auto v : od) printf("%d ", v);
        printf("\n  (1996-01-02 = 9497)\n");
    }

    // Spot-check ps_supplycost (should be non-zero doubles)
    {
        auto sc = read_col<double>((std::string(base)+"/partsupp/ps_supplycost.bin").c_str(), 5);
        printf("ps_supplycost[0..4]: ");
        for (auto v : sc) printf("%.2f ", v);
        printf("\n");
    }

    // Spot-check n_name (fixed 26-byte strings)
    {
        FILE* f = fopen((std::string(base)+"/nation/n_name.bin").c_str(), "rb");
        char buf[26];
        printf("n_name[0..4]: ");
        for (int i = 0; i < 5; i++) {
            fread(buf, 1, 26, f);
            printf("'%s' ", buf);
        }
        printf("\n");
        fclose(f);
    }

    // Spot-check c_mktsegment (int8, values 0-4)
    {
        auto mk = read_col<int8_t>((std::string(base)+"/customer/c_mktsegment.bin").c_str(), 10);
        printf("c_mktsegment[0..9] (int8 0=AUTO,1=BLDG,2=FURN,3=HSHLD,4=MACH): ");
        for (auto v : mk) printf("%d ", (int)v);
        printf("\n");
    }

    // Check zone map
    {
        FILE* f = fopen((std::string(base)+"/lineitem/l_shipdate_zone_map.bin").c_str(), "rb");
        uint32_t num_blocks, block_size;
        fread(&num_blocks, 4, 1, f);
        fread(&block_size, 4, 1, f);
        struct ZE { int32_t mn, mx; };
        std::vector<ZE> zones(num_blocks);
        fread(zones.data(), 8, num_blocks, f);
        fclose(f);
        printf("Zone map: %u blocks of %u rows\n", num_blocks, block_size);
        printf("Block 0: min=%d max=%d\n", zones[0].mn, zones[0].mx);
        printf("Block N-1: min=%d max=%d\n", zones[num_blocks-1].mn, zones[num_blocks-1].mx);
    }

    // Check orders dense index
    {
        auto idx = read_col<int32_t>((std::string(base)+"/orders/orders_by_orderkey.bin").c_str());
        auto ok  = read_col<int32_t>((std::string(base)+"/orders/o_orderkey.bin").c_str());
        printf("orders_by_orderkey size: %zu\n", idx.size());
        // Verify: for orderkey[0], check round-trip
        int32_t key0   = ok[0];
        int32_t row0   = idx[key0];
        printf("o_orderkey[0]=%d, idx[%d]=%d (should be 0)\n", key0, key0, row0);
    }

    // Check p_name for 'green'
    {
        FILE* f = fopen((std::string(base)+"/part/p_name.bin").c_str(), "rb");
        char buf[56];
        int found = 0;
        for (int i = 0; i < 100; i++) {
            fread(buf, 1, 56, f);
            if (strstr(buf, "green")) { printf("p_name[%d]='%s' (contains 'green')\n", i, buf); found++; }
        }
        if (!found) printf("No 'green' in first 100 p_name entries\n");
        fclose(f);
    }

    printf("\nVerification complete.\n");
    return 0;
}
