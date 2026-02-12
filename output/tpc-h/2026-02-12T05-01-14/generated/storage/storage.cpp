#include "storage.h"
#include "../utils/date_utils.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <thread>
#include <atomic>
#include <chrono>
#include <iostream>

namespace gendb {

// Helper: mmap a file for reading
static const char* mmap_file_read(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s\n", path.c_str());
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }

    out_size = sb.st_size;
    const char* data = (const char*)mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        close(fd);
        return nullptr;
    }

    madvise((void*)data, out_size, MADV_SEQUENTIAL);
    close(fd);
    return data;
}

// Helper: write binary column to file
template<typename T>
static void write_column(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        fprintf(stderr, "Failed to create %s\n", path.c_str());
        return;
    }

    // Set 4MB buffer
    char buf[4 * 1024 * 1024];
    setvbuf(f, buf, _IOFBF, sizeof(buf));

    fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
}

// Specialization for string vectors
static void write_string_column(const std::string& path, const std::vector<std::string>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        fprintf(stderr, "Failed to create %s\n", path.c_str());
        return;
    }

    char buf[4 * 1024 * 1024];
    setvbuf(f, buf, _IOFBF, sizeof(buf));

    // Write strings with length prefix
    for (const auto& str : data) {
        uint32_t len = str.size();
        fwrite(&len, sizeof(uint32_t), 1, f);
        fwrite(str.data(), 1, len, f);
    }
    fclose(f);
}

// Helper: write metadata (row count)
static void write_metadata(const std::string& gendb_dir, const std::string& table_name, size_t row_count) {
    std::string meta_path = gendb_dir + "/" + table_name + ".meta";
    FILE* f = fopen(meta_path.c_str(), "w");
    if (f) {
        fprintf(f, "{\"row_count\": %zu}\n", row_count);
        fclose(f);
    }
}

// Parse helpers
static inline const char* skip_field(const char* p) {
    while (*p != '|') p++;
    return p + 1;
}

static inline int32_t parse_int(const char* p, const char** end) {
    char* e;
    int32_t val = strtol(p, &e, 10);
    *end = e + 1;  // Skip delimiter
    return val;
}

static inline double parse_double(const char* p, const char** end) {
    char* e;
    double val = strtod(p, &e);
    *end = e + 1;  // Skip delimiter
    return val;
}

static inline char parse_char(const char* p, const char** end) {
    char c = *p;
    *end = p + 2;  // Skip char + delimiter
    return c;
}

static inline std::string parse_string(const char* p, const char** end) {
    const char* start = p;
    while (*p != '|') p++;
    *end = p + 1;
    return std::string(start, p - start);
}

static inline int32_t parse_date_field(const char* p, const char** end) {
    return parse_date(p);  // Will parse until delimiter
}

// Lineitem ingestion
void ingest_lineitem(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    // Estimate rows
    size_t estimated_rows = file_size / 150;  // ~150 bytes per line

    LineitemTable table;
    table.l_orderkey.reserve(estimated_rows);
    table.l_partkey.reserve(estimated_rows);
    table.l_suppkey.reserve(estimated_rows);
    table.l_linenumber.reserve(estimated_rows);
    table.l_quantity.reserve(estimated_rows);
    table.l_extendedprice.reserve(estimated_rows);
    table.l_discount.reserve(estimated_rows);
    table.l_tax.reserve(estimated_rows);
    table.l_returnflag.reserve(estimated_rows);
    table.l_linestatus.reserve(estimated_rows);
    table.l_shipdate.reserve(estimated_rows);
    table.l_commitdate.reserve(estimated_rows);
    table.l_receiptdate.reserve(estimated_rows);
    table.l_shipinstruct.reserve(estimated_rows);
    table.l_shipmode.reserve(estimated_rows);
    table.l_comment.reserve(estimated_rows);

    // Parse
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') {
            p++;
            continue;
        }

        const char* next;
        table.l_orderkey.push_back(parse_int(p, &next)); p = next;
        table.l_partkey.push_back(parse_int(p, &next)); p = next;
        table.l_suppkey.push_back(parse_int(p, &next)); p = next;
        table.l_linenumber.push_back(parse_int(p, &next)); p = next;
        table.l_quantity.push_back(parse_double(p, &next)); p = next;
        table.l_extendedprice.push_back(parse_double(p, &next)); p = next;
        table.l_discount.push_back(parse_double(p, &next)); p = next;
        table.l_tax.push_back(parse_double(p, &next)); p = next;
        table.l_returnflag.push_back(parse_char(p, &next)); p = next;
        table.l_linestatus.push_back(parse_char(p, &next)); p = next;
        table.l_shipdate.push_back(parse_date_field(p, &next)); p = skip_field(p);
        table.l_commitdate.push_back(parse_date_field(p, &next)); p = skip_field(p);
        table.l_receiptdate.push_back(parse_date_field(p, &next)); p = skip_field(p);
        table.l_shipinstruct.push_back(parse_string(p, &next)); p = next;
        table.l_shipmode.push_back(parse_string(p, &next)); p = next;
        table.l_comment.push_back(parse_string(p, &next)); p = next;

        // Skip to next line
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }

    munmap((void*)data, file_size);

    // Skip sorting for now (baseline version) - can add back as optimization
    // Sorting 60M rows takes too long for initial baseline

    // Write columns directly (unsorted)
    write_column(gendb_dir + "/lineitem_l_orderkey.bin", table.l_orderkey);
    write_column(gendb_dir + "/lineitem_l_partkey.bin", table.l_partkey);
    write_column(gendb_dir + "/lineitem_l_suppkey.bin", table.l_suppkey);
    write_column(gendb_dir + "/lineitem_l_linenumber.bin", table.l_linenumber);
    write_column(gendb_dir + "/lineitem_l_quantity.bin", table.l_quantity);
    write_column(gendb_dir + "/lineitem_l_extendedprice.bin", table.l_extendedprice);
    write_column(gendb_dir + "/lineitem_l_discount.bin", table.l_discount);
    write_column(gendb_dir + "/lineitem_l_tax.bin", table.l_tax);
    write_column(gendb_dir + "/lineitem_l_returnflag.bin", table.l_returnflag);
    write_column(gendb_dir + "/lineitem_l_linestatus.bin", table.l_linestatus);
    write_column(gendb_dir + "/lineitem_l_shipdate.bin", table.l_shipdate);
    write_column(gendb_dir + "/lineitem_l_commitdate.bin", table.l_commitdate);
    write_column(gendb_dir + "/lineitem_l_receiptdate.bin", table.l_receiptdate);
    write_string_column(gendb_dir + "/lineitem_l_shipinstruct.bin", table.l_shipinstruct);
    write_string_column(gendb_dir + "/lineitem_l_shipmode.bin", table.l_shipmode);
    write_string_column(gendb_dir + "/lineitem_l_comment.bin", table.l_comment);

    write_metadata(gendb_dir, "lineitem", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    printf("lineitem: %zu rows in %.2fs\n", table.size(), elapsed);
}

// Orders ingestion (similar pattern)
void ingest_orders(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    size_t estimated_rows = file_size / 120;

    OrdersTable table;
    table.o_orderkey.reserve(estimated_rows);
    table.o_custkey.reserve(estimated_rows);
    table.o_orderstatus.reserve(estimated_rows);
    table.o_totalprice.reserve(estimated_rows);
    table.o_orderdate.reserve(estimated_rows);
    table.o_orderpriority.reserve(estimated_rows);
    table.o_clerk.reserve(estimated_rows);
    table.o_shippriority.reserve(estimated_rows);
    table.o_comment.reserve(estimated_rows);

    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') {
            p++;
            continue;
        }

        const char* next;
        table.o_orderkey.push_back(parse_int(p, &next)); p = next;
        table.o_custkey.push_back(parse_int(p, &next)); p = next;
        table.o_orderstatus.push_back(parse_char(p, &next)); p = next;
        table.o_totalprice.push_back(parse_double(p, &next)); p = next;
        table.o_orderdate.push_back(parse_date_field(p, &next)); p = skip_field(p);
        table.o_orderpriority.push_back(parse_string(p, &next)); p = next;
        table.o_clerk.push_back(parse_string(p, &next)); p = next;
        table.o_shippriority.push_back(parse_int(p, &next)); p = next;
        table.o_comment.push_back(parse_string(p, &next)); p = next;

        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }

    munmap((void*)data, file_size);

    // Skip sorting for now (baseline version)
    write_column(gendb_dir + "/orders_o_orderkey.bin", table.o_orderkey);
    write_column(gendb_dir + "/orders_o_custkey.bin", table.o_custkey);
    write_column(gendb_dir + "/orders_o_orderstatus.bin", table.o_orderstatus);
    write_column(gendb_dir + "/orders_o_totalprice.bin", table.o_totalprice);
    write_column(gendb_dir + "/orders_o_orderdate.bin", table.o_orderdate);
    write_string_column(gendb_dir + "/orders_o_orderpriority.bin", table.o_orderpriority);
    write_string_column(gendb_dir + "/orders_o_clerk.bin", table.o_clerk);
    write_column(gendb_dir + "/orders_o_shippriority.bin", table.o_shippriority);
    write_string_column(gendb_dir + "/orders_o_comment.bin", table.o_comment);

    write_metadata(gendb_dir, "orders", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    printf("orders: %zu rows in %.2fs\n", table.size(), elapsed);
}

// Customer ingestion
void ingest_customer(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    size_t estimated_rows = file_size / 180;

    CustomerTable table;
    table.c_custkey.reserve(estimated_rows);
    table.c_name.reserve(estimated_rows);
    table.c_address.reserve(estimated_rows);
    table.c_nationkey.reserve(estimated_rows);
    table.c_phone.reserve(estimated_rows);
    table.c_acctbal.reserve(estimated_rows);
    table.c_mktsegment.reserve(estimated_rows);
    table.c_comment.reserve(estimated_rows);

    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') {
            p++;
            continue;
        }

        const char* next;
        table.c_custkey.push_back(parse_int(p, &next)); p = next;
        table.c_name.push_back(parse_string(p, &next)); p = next;
        table.c_address.push_back(parse_string(p, &next)); p = next;
        table.c_nationkey.push_back(parse_int(p, &next)); p = next;
        table.c_phone.push_back(parse_string(p, &next)); p = next;
        table.c_acctbal.push_back(parse_double(p, &next)); p = next;
        table.c_mktsegment.push_back(parse_string(p, &next)); p = next;
        table.c_comment.push_back(parse_string(p, &next)); p = next;

        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }

    munmap((void*)data, file_size);

    write_column(gendb_dir + "/customer_c_custkey.bin", table.c_custkey);
    write_string_column(gendb_dir + "/customer_c_name.bin", table.c_name);
    write_string_column(gendb_dir + "/customer_c_address.bin", table.c_address);
    write_column(gendb_dir + "/customer_c_nationkey.bin", table.c_nationkey);
    write_string_column(gendb_dir + "/customer_c_phone.bin", table.c_phone);
    write_column(gendb_dir + "/customer_c_acctbal.bin", table.c_acctbal);
    write_string_column(gendb_dir + "/customer_c_mktsegment.bin", table.c_mktsegment);
    write_string_column(gendb_dir + "/customer_c_comment.bin", table.c_comment);

    write_metadata(gendb_dir, "customer", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    printf("customer: %zu rows in %.2fs\n", table.size(), elapsed);
}

// Supplier, Part, Partsupp, Nation, Region (similar simplified implementations)
void ingest_supplier(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();
    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    SupplierTable table;
    table.s_suppkey.reserve(10000);
    table.s_name.reserve(10000);
    table.s_address.reserve(10000);
    table.s_nationkey.reserve(10000);
    table.s_phone.reserve(10000);
    table.s_acctbal.reserve(10000);
    table.s_comment.reserve(10000);

    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') { p++; continue; }
        const char* next;
        table.s_suppkey.push_back(parse_int(p, &next)); p = next;
        table.s_name.push_back(parse_string(p, &next)); p = next;
        table.s_address.push_back(parse_string(p, &next)); p = next;
        table.s_nationkey.push_back(parse_int(p, &next)); p = next;
        table.s_phone.push_back(parse_string(p, &next)); p = next;
        table.s_acctbal.push_back(parse_double(p, &next)); p = next;
        table.s_comment.push_back(parse_string(p, &next)); p = next;
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }
    munmap((void*)data, file_size);

    write_column(gendb_dir + "/supplier_s_suppkey.bin", table.s_suppkey);
    write_string_column(gendb_dir + "/supplier_s_name.bin", table.s_name);
    write_string_column(gendb_dir + "/supplier_s_address.bin", table.s_address);
    write_column(gendb_dir + "/supplier_s_nationkey.bin", table.s_nationkey);
    write_string_column(gendb_dir + "/supplier_s_phone.bin", table.s_phone);
    write_column(gendb_dir + "/supplier_s_acctbal.bin", table.s_acctbal);
    write_string_column(gendb_dir + "/supplier_s_comment.bin", table.s_comment);
    write_metadata(gendb_dir, "supplier", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("supplier: %zu rows in %.2fs\n", table.size(),
           std::chrono::duration<double>(end_time - start_time).count());
}

void ingest_part(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();
    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    PartTable table;
    size_t est = file_size / 150;
    table.p_partkey.reserve(est);
    table.p_name.reserve(est);
    table.p_mfgr.reserve(est);
    table.p_brand.reserve(est);
    table.p_type.reserve(est);
    table.p_size.reserve(est);
    table.p_container.reserve(est);
    table.p_retailprice.reserve(est);
    table.p_comment.reserve(est);

    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') { p++; continue; }
        const char* next;
        table.p_partkey.push_back(parse_int(p, &next)); p = next;
        table.p_name.push_back(parse_string(p, &next)); p = next;
        table.p_mfgr.push_back(parse_string(p, &next)); p = next;
        table.p_brand.push_back(parse_string(p, &next)); p = next;
        table.p_type.push_back(parse_string(p, &next)); p = next;
        table.p_size.push_back(parse_int(p, &next)); p = next;
        table.p_container.push_back(parse_string(p, &next)); p = next;
        table.p_retailprice.push_back(parse_double(p, &next)); p = next;
        table.p_comment.push_back(parse_string(p, &next)); p = next;
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }
    munmap((void*)data, file_size);

    write_column(gendb_dir + "/part_p_partkey.bin", table.p_partkey);
    write_string_column(gendb_dir + "/part_p_name.bin", table.p_name);
    write_string_column(gendb_dir + "/part_p_mfgr.bin", table.p_mfgr);
    write_string_column(gendb_dir + "/part_p_brand.bin", table.p_brand);
    write_string_column(gendb_dir + "/part_p_type.bin", table.p_type);
    write_column(gendb_dir + "/part_p_size.bin", table.p_size);
    write_string_column(gendb_dir + "/part_p_container.bin", table.p_container);
    write_column(gendb_dir + "/part_p_retailprice.bin", table.p_retailprice);
    write_string_column(gendb_dir + "/part_p_comment.bin", table.p_comment);
    write_metadata(gendb_dir, "part", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("part: %zu rows in %.2fs\n", table.size(),
           std::chrono::duration<double>(end_time - start_time).count());
}

void ingest_partsupp(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();
    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    PartsuppTable table;
    size_t est = file_size / 150;
    table.ps_partkey.reserve(est);
    table.ps_suppkey.reserve(est);
    table.ps_availqty.reserve(est);
    table.ps_supplycost.reserve(est);
    table.ps_comment.reserve(est);

    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') { p++; continue; }
        const char* next;
        table.ps_partkey.push_back(parse_int(p, &next)); p = next;
        table.ps_suppkey.push_back(parse_int(p, &next)); p = next;
        table.ps_availqty.push_back(parse_int(p, &next)); p = next;
        table.ps_supplycost.push_back(parse_double(p, &next)); p = next;
        table.ps_comment.push_back(parse_string(p, &next)); p = next;
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }
    munmap((void*)data, file_size);

    write_column(gendb_dir + "/partsupp_ps_partkey.bin", table.ps_partkey);
    write_column(gendb_dir + "/partsupp_ps_suppkey.bin", table.ps_suppkey);
    write_column(gendb_dir + "/partsupp_ps_availqty.bin", table.ps_availqty);
    write_column(gendb_dir + "/partsupp_ps_supplycost.bin", table.ps_supplycost);
    write_string_column(gendb_dir + "/partsupp_ps_comment.bin", table.ps_comment);
    write_metadata(gendb_dir, "partsupp", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("partsupp: %zu rows in %.2fs\n", table.size(),
           std::chrono::duration<double>(end_time - start_time).count());
}

void ingest_nation(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();
    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    NationTable table;
    table.n_nationkey.reserve(50);
    table.n_name.reserve(50);
    table.n_regionkey.reserve(50);
    table.n_comment.reserve(50);

    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') { p++; continue; }
        const char* next;
        table.n_nationkey.push_back(parse_int(p, &next)); p = next;
        table.n_name.push_back(parse_string(p, &next)); p = next;
        table.n_regionkey.push_back(parse_int(p, &next)); p = next;
        table.n_comment.push_back(parse_string(p, &next)); p = next;
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }
    munmap((void*)data, file_size);

    write_column(gendb_dir + "/nation_n_nationkey.bin", table.n_nationkey);
    write_string_column(gendb_dir + "/nation_n_name.bin", table.n_name);
    write_column(gendb_dir + "/nation_n_regionkey.bin", table.n_regionkey);
    write_string_column(gendb_dir + "/nation_n_comment.bin", table.n_comment);
    write_metadata(gendb_dir, "nation", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("nation: %zu rows in %.2fs\n", table.size(),
           std::chrono::duration<double>(end_time - start_time).count());
}

void ingest_region(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();
    size_t file_size;
    const char* data = mmap_file_read(tbl_path, file_size);
    if (!data) return;

    RegionTable table;
    table.r_regionkey.reserve(10);
    table.r_name.reserve(10);
    table.r_comment.reserve(10);

    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n' || *p == '\0') { p++; continue; }
        const char* next;
        table.r_regionkey.push_back(parse_int(p, &next)); p = next;
        table.r_name.push_back(parse_string(p, &next)); p = next;
        table.r_comment.push_back(parse_string(p, &next)); p = next;
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }
    munmap((void*)data, file_size);

    write_column(gendb_dir + "/region_r_regionkey.bin", table.r_regionkey);
    write_string_column(gendb_dir + "/region_r_name.bin", table.r_name);
    write_string_column(gendb_dir + "/region_r_comment.bin", table.r_comment);
    write_metadata(gendb_dir, "region", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("region: %zu rows in %.2fs\n", table.size(),
           std::chrono::duration<double>(end_time - start_time).count());
}

// Read metadata
size_t read_row_count(const std::string& gendb_dir, const std::string& table_name) {
    std::string meta_path = gendb_dir + "/" + table_name + ".meta";
    FILE* f = fopen(meta_path.c_str(), "r");
    if (!f) return 0;

    size_t count = 0;
    fscanf(f, "{\"row_count\": %zu}", &count);
    fclose(f);
    return count;
}

// Template mmap column loader
template<typename T>
const T* mmap_column(const std::string& gendb_dir, const std::string& table_name,
                     const std::string& column_name, size_t& out_count) {
    std::string path = gendb_dir + "/" + table_name + "_" + column_name + ".bin";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return nullptr;

    struct stat sb;
    fstat(fd, &sb);
    out_count = sb.st_size / sizeof(T);

    const T* data = (const T*)mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise((void*)data, sb.st_size, MADV_SEQUENTIAL);
    close(fd);

    return data;
}

// Explicit instantiations
template const int32_t* mmap_column<int32_t>(const std::string&, const std::string&, const std::string&, size_t&);
template const double* mmap_column<double>(const std::string&, const std::string&, const std::string&, size_t&);
template const char* mmap_column<char>(const std::string&, const std::string&, const std::string&, size_t&);

// Query-specific column loaders
void load_lineitem_columns_q1(const std::string& gendb_dir,
                                const int32_t** l_shipdate,
                                const double** l_quantity,
                                const double** l_extendedprice,
                                const double** l_discount,
                                const double** l_tax,
                                const char** l_returnflag,
                                const char** l_linestatus,
                                size_t& count) {
    *l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", count);
    size_t tmp;
    *l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", tmp);
    *l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", tmp);
    *l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", tmp);
    *l_tax = mmap_column<double>(gendb_dir, "lineitem", "l_tax", tmp);
    *l_returnflag = mmap_column<char>(gendb_dir, "lineitem", "l_returnflag", tmp);
    *l_linestatus = mmap_column<char>(gendb_dir, "lineitem", "l_linestatus", tmp);
}

void load_lineitem_columns_q3(const std::string& gendb_dir,
                                const int32_t** l_orderkey,
                                const int32_t** l_shipdate,
                                const double** l_extendedprice,
                                const double** l_discount,
                                size_t& count) {
    *l_orderkey = mmap_column<int32_t>(gendb_dir, "lineitem", "l_orderkey", count);
    size_t tmp;
    *l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", tmp);
    *l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", tmp);
    *l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", tmp);
}

void load_lineitem_columns_q6(const std::string& gendb_dir,
                                const int32_t** l_shipdate,
                                const double** l_quantity,
                                const double** l_extendedprice,
                                const double** l_discount,
                                size_t& count) {
    *l_shipdate = mmap_column<int32_t>(gendb_dir, "lineitem", "l_shipdate", count);
    size_t tmp;
    *l_quantity = mmap_column<double>(gendb_dir, "lineitem", "l_quantity", tmp);
    *l_extendedprice = mmap_column<double>(gendb_dir, "lineitem", "l_extendedprice", tmp);
    *l_discount = mmap_column<double>(gendb_dir, "lineitem", "l_discount", tmp);
}

void load_orders_columns_q3(const std::string& gendb_dir,
                             const int32_t** o_orderkey,
                             const int32_t** o_custkey,
                             const int32_t** o_orderdate,
                             const int32_t** o_shippriority,
                             size_t& count) {
    *o_orderkey = mmap_column<int32_t>(gendb_dir, "orders", "o_orderkey", count);
    size_t tmp;
    *o_custkey = mmap_column<int32_t>(gendb_dir, "orders", "o_custkey", tmp);
    *o_orderdate = mmap_column<int32_t>(gendb_dir, "orders", "o_orderdate", tmp);
    *o_shippriority = mmap_column<int32_t>(gendb_dir, "orders", "o_shippriority", tmp);
}

void load_customer_columns_q3(const std::string& gendb_dir,
                               const int32_t** c_custkey,
                               const std::string** c_mktsegment,
                               size_t& count) {
    *c_custkey = mmap_column<int32_t>(gendb_dir, "customer", "c_custkey", count);
    // For strings, we need a different approach - for now, simplified
    // In production, dictionary-encode or handle properly
    *c_mktsegment = nullptr;  // Simplified - will load in query directly
}

} // namespace gendb
