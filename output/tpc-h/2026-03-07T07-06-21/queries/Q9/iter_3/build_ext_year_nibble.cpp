// Build storage extension: orders.o_year.nibble_by_orderkey
// Produces a nibble-packed array where nibble[orderkey] = (year - 1992), 0xF = invalid.
// This allows O(1) lookup from l_orderkey → year_offset with a single 30MB L3-resident access,
// eliminating the two-level 297MB (orders_by_orderkey 240MB + o_orderdate 57MB) random lookup.
//
// Usage: ./build_ext <gendb_dir>

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>

// Days-since-epoch → Gregorian year (identical to query code)
static int extract_year(int32_t days) {
    int32_t z   = days + 719468;
    int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    if (doy >= 306) y++;
    return (int)y;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb = argv[1];
    auto t0 = std::chrono::high_resolution_clock::now();

    // ── Read o_orderkey.bin (15M × int32_t = 60MB) ───────────────────────────
    const size_t ORDER_ROWS = 15000000;
    std::vector<int32_t> o_orderkey(ORDER_ROWS);
    {
        FILE* f = fopen((gendb + "/orders/o_orderkey.bin").c_str(), "rb");
        if (!f) { perror("o_orderkey.bin"); return 1; }
        size_t n = fread(o_orderkey.data(), sizeof(int32_t), ORDER_ROWS, f);
        fclose(f);
        if (n != ORDER_ROWS) { fprintf(stderr, "Short read: o_orderkey.bin got %zu/%zu\n", n, ORDER_ROWS); return 1; }
    }

    // ── Read o_orderdate.bin (15M × int32_t = 60MB) ──────────────────────────
    std::vector<int32_t> o_orderdate(ORDER_ROWS);
    {
        FILE* f = fopen((gendb + "/orders/o_orderdate.bin").c_str(), "rb");
        if (!f) { perror("o_orderdate.bin"); return 1; }
        size_t n = fread(o_orderdate.data(), sizeof(int32_t), ORDER_ROWS, f);
        fclose(f);
        if (n != ORDER_ROWS) { fprintf(stderr, "Short read: o_orderdate.bin got %zu/%zu\n", n, ORDER_ROWS); return 1; }
    }

    // ── Find max orderkey ─────────────────────────────────────────────────────
    int32_t max_key = 0;
    for (size_t i = 0; i < ORDER_ROWS; i++)
        if (o_orderkey[i] > max_key) max_key = o_orderkey[i];
    fprintf(stderr, "max_orderkey = %d\n", max_key);

    // Nibble array: entry k stored in byte k/2, nibble (k%2)*4 bits from lsb
    // Value: 0-6 = year-1992 (years 1992-1998), 0xF = invalid/missing
    // Array covers orderkeys 0 .. max_key inclusive
    size_t nibble_bytes = (size_t)(max_key / 2) + 1;
    std::vector<uint8_t> nibbles(nibble_bytes, 0xFF);  // init all to 0xFF (both nibbles = 0xF = invalid)

    // ── Fill nibble array ─────────────────────────────────────────────────────
    int year_min = 9999, year_max = 0;
    for (size_t i = 0; i < ORDER_ROWS; i++) {
        int32_t key = o_orderkey[i];
        int     yr  = extract_year(o_orderdate[i]);
        int     off = yr - 1992;
        if (off < 0 || off > 6) { fprintf(stderr, "WARN: year %d out of range at row %zu\n", yr, i); continue; }
        if (yr < year_min) year_min = yr;
        if (yr > year_max) year_max = yr;
        // Pack into nibble: lower nibble for even keys, upper nibble for odd keys
        size_t  byte_idx = (size_t)key / 2;
        int     shift    = ((size_t)key & 1u) ? 4 : 0;
        nibbles[byte_idx] = (nibbles[byte_idx] & ~(0xFu << shift)) | ((uint8_t)off << shift);
    }
    fprintf(stderr, "year range: %d - %d\n", year_min, year_max);

    // ── Write output ──────────────────────────────────────────────────────────
    const std::string out_dir  = gendb + "/column_versions/orders.o_year.nibble_by_orderkey";
    const std::string out_path = out_dir + "/year_nibbles.bin";
    {
        FILE* f = fopen(out_path.c_str(), "wb");
        if (!f) { perror(out_path.c_str()); return 1; }
        fwrite(nibbles.data(), 1, nibble_bytes, f);
        fclose(f);
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double,std::milli>(t1-t0).count();
    fprintf(stderr, "Written %zu bytes to %s in %.1f ms\n", nibble_bytes, out_path.c_str(), ms);
    fprintf(stderr, "Rows processed: %zu, max_key: %d, nibble_array_bytes: %zu\n",
            ORDER_ROWS, max_key, nibble_bytes);
    return 0;
}
