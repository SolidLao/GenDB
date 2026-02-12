#include "storage.h"
#include "../utils/date_utils.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <atomic>
#include <algorithm>

namespace gendb {

// Helper: mmap input file for fast parsing
struct MmapFile {
    const char* data;
    size_t size;
    int fd;

    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            fprintf(stderr, "Failed to open %s\n", path.c_str());
            exit(1);
        }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            fprintf(stderr, "Failed to mmap %s\n", path.c_str());
            exit(1);
        }
        madvise((void*)data, size, MADV_SEQUENTIAL);
    }

    ~MmapFile() {
        munmap((void*)data, size);
        close(fd);
    }
};

// Helper: write binary column
template<typename T>
void write_column(const std::string& path, const std::vector<T>& vec) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        fprintf(stderr, "Failed to write %s\n", path.c_str());
        exit(1);
    }
    char buf[1<<20]; // 1MB buffer
    setvbuf(f, buf, _IOFBF, sizeof(buf));
    fwrite(vec.data(), sizeof(T), vec.size(), f);
    fclose(f);
}

// Specialization for std::string columns (write as length-prefixed strings)
void write_string_column(const std::string& path, const std::vector<std::string>& vec) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        fprintf(stderr, "Failed to write %s\n", path.c_str());
        exit(1);
    }
    char buf[1<<20];
    setvbuf(f, buf, _IOFBF, sizeof(buf));
    for (const auto& s : vec) {
        uint32_t len = s.size();
        fwrite(&len, sizeof(len), 1, f);
        fwrite(s.data(), 1, len, f);
    }
    fclose(f);
}

// Helper: parse fields from pipe-delimited line
inline const char* parse_int32(const char* p, int32_t& out) {
    out = strtol(p, nullptr, 10);
    while (*p && *p != '|') ++p;
    return (*p == '|') ? p + 1 : p;
}

inline const char* parse_double(const char* p, double& out) {
    out = strtod(p, nullptr);
    while (*p && *p != '|') ++p;
    return (*p == '|') ? p + 1 : p;
}

inline const char* parse_string(const char* p, std::string& out) {
    const char* start = p;
    while (*p && *p != '|') ++p;
    out.assign(start, p - start);
    return (*p == '|') ? p + 1 : p;
}

inline const char* parse_char(const char* p, char& out) {
    out = *p;
    while (*p && *p != '|') ++p;
    return (*p == '|') ? p + 1 : p;
}

inline const char* parse_date(const char* p, int32_t& out) {
    char date_buf[16];
    const char* start = p;
    while (*p && *p != '|') ++p;
    size_t len = std::min<size_t>(p - start, 15);
    memcpy(date_buf, start, len);
    date_buf[len] = '\0';
    out = parse_date(date_buf);
    return (*p == '|') ? p + 1 : p;
}

// Lineitem ingestion with parallel chunk parsing
void ingest_lineitem(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    MmapFile file(tbl_path);

    // Estimate rows and pre-allocate
    const size_t avg_line_len = 150;
    size_t estimated_rows = file.size / avg_line_len;

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

    // Find line boundaries for parallel parsing
    const int num_threads = std::thread::hardware_concurrency();
    const size_t chunk_size = file.size / num_threads;

    struct Chunk {
        const char* start;
        const char* end;
    };

    std::vector<Chunk> chunks;
    const char* pos = file.data;
    for (int i = 0; i < num_threads; ++i) {
        const char* chunk_start = pos;
        const char* chunk_end = (i == num_threads - 1) ? file.data + file.size : pos + chunk_size;

        // Find next newline
        if (i < num_threads - 1) {
            while (chunk_end < file.data + file.size && *chunk_end != '\n') ++chunk_end;
            if (chunk_end < file.data + file.size) ++chunk_end; // Skip newline
        }

        chunks.push_back({chunk_start, chunk_end});
        pos = chunk_end;
    }

    // Parse chunks in parallel
    std::vector<LineitemTable> local_tables(num_threads);
    std::vector<std::thread> threads;

    for (int tid = 0; tid < num_threads; ++tid) {
        threads.emplace_back([&, tid]() {
            auto& local = local_tables[tid];
            const char* p = chunks[tid].start;
            const char* end = chunks[tid].end;

            size_t local_est = (end - p) / avg_line_len;
            local.l_orderkey.reserve(local_est);
            local.l_partkey.reserve(local_est);
            local.l_suppkey.reserve(local_est);
            local.l_linenumber.reserve(local_est);
            local.l_quantity.reserve(local_est);
            local.l_extendedprice.reserve(local_est);
            local.l_discount.reserve(local_est);
            local.l_tax.reserve(local_est);
            local.l_returnflag.reserve(local_est);
            local.l_linestatus.reserve(local_est);
            local.l_shipdate.reserve(local_est);
            local.l_commitdate.reserve(local_est);
            local.l_receiptdate.reserve(local_est);
            local.l_shipinstruct.reserve(local_est);
            local.l_shipmode.reserve(local_est);
            local.l_comment.reserve(local_est);

            while (p < end) {
                int32_t i32;
                double d;
                char c;
                std::string s;

                p = parse_int32(p, i32); local.l_orderkey.push_back(i32);
                p = parse_int32(p, i32); local.l_partkey.push_back(i32);
                p = parse_int32(p, i32); local.l_suppkey.push_back(i32);
                p = parse_int32(p, i32); local.l_linenumber.push_back(i32);
                p = parse_double(p, d); local.l_quantity.push_back(d);
                p = parse_double(p, d); local.l_extendedprice.push_back(d);
                p = parse_double(p, d); local.l_discount.push_back(d);
                p = parse_double(p, d); local.l_tax.push_back(d);
                p = parse_char(p, c); local.l_returnflag.push_back(c);
                p = parse_char(p, c); local.l_linestatus.push_back(c);
                p = parse_date(p, i32); local.l_shipdate.push_back(i32);
                p = parse_date(p, i32); local.l_commitdate.push_back(i32);
                p = parse_date(p, i32); local.l_receiptdate.push_back(i32);
                p = parse_string(p, s); local.l_shipinstruct.push_back(s);
                p = parse_string(p, s); local.l_shipmode.push_back(s);
                p = parse_string(p, s); local.l_comment.push_back(s);

                // Skip to next line
                while (p < end && *p != '\n') ++p;
                if (p < end) ++p;
            }
        });
    }

    for (auto& t : threads) t.join();

    // Merge local results
    size_t total_rows = 0;
    for (const auto& local : local_tables) {
        total_rows += local.size();
    }

    table.l_orderkey.clear(); table.l_orderkey.reserve(total_rows);
    table.l_partkey.clear(); table.l_partkey.reserve(total_rows);
    table.l_suppkey.clear(); table.l_suppkey.reserve(total_rows);
    table.l_linenumber.clear(); table.l_linenumber.reserve(total_rows);
    table.l_quantity.clear(); table.l_quantity.reserve(total_rows);
    table.l_extendedprice.clear(); table.l_extendedprice.reserve(total_rows);
    table.l_discount.clear(); table.l_discount.reserve(total_rows);
    table.l_tax.clear(); table.l_tax.reserve(total_rows);
    table.l_returnflag.clear(); table.l_returnflag.reserve(total_rows);
    table.l_linestatus.clear(); table.l_linestatus.reserve(total_rows);
    table.l_shipdate.clear(); table.l_shipdate.reserve(total_rows);
    table.l_commitdate.clear(); table.l_commitdate.reserve(total_rows);
    table.l_receiptdate.clear(); table.l_receiptdate.reserve(total_rows);
    table.l_shipinstruct.clear(); table.l_shipinstruct.reserve(total_rows);
    table.l_shipmode.clear(); table.l_shipmode.reserve(total_rows);
    table.l_comment.clear(); table.l_comment.reserve(total_rows);

    for (const auto& local : local_tables) {
        table.l_orderkey.insert(table.l_orderkey.end(), local.l_orderkey.begin(), local.l_orderkey.end());
        table.l_partkey.insert(table.l_partkey.end(), local.l_partkey.begin(), local.l_partkey.end());
        table.l_suppkey.insert(table.l_suppkey.end(), local.l_suppkey.begin(), local.l_suppkey.end());
        table.l_linenumber.insert(table.l_linenumber.end(), local.l_linenumber.begin(), local.l_linenumber.end());
        table.l_quantity.insert(table.l_quantity.end(), local.l_quantity.begin(), local.l_quantity.end());
        table.l_extendedprice.insert(table.l_extendedprice.end(), local.l_extendedprice.begin(), local.l_extendedprice.end());
        table.l_discount.insert(table.l_discount.end(), local.l_discount.begin(), local.l_discount.end());
        table.l_tax.insert(table.l_tax.end(), local.l_tax.begin(), local.l_tax.end());
        table.l_returnflag.insert(table.l_returnflag.end(), local.l_returnflag.begin(), local.l_returnflag.end());
        table.l_linestatus.insert(table.l_linestatus.end(), local.l_linestatus.begin(), local.l_linestatus.end());
        table.l_shipdate.insert(table.l_shipdate.end(), local.l_shipdate.begin(), local.l_shipdate.end());
        table.l_commitdate.insert(table.l_commitdate.end(), local.l_commitdate.begin(), local.l_commitdate.end());
        table.l_receiptdate.insert(table.l_receiptdate.end(), local.l_receiptdate.begin(), local.l_receiptdate.end());
        table.l_shipinstruct.insert(table.l_shipinstruct.end(), local.l_shipinstruct.begin(), local.l_shipinstruct.end());
        table.l_shipmode.insert(table.l_shipmode.end(), local.l_shipmode.begin(), local.l_shipmode.end());
        table.l_comment.insert(table.l_comment.end(), local.l_comment.begin(), local.l_comment.end());
    }

    // Write binary columns
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

    // Write metadata
    FILE* meta = fopen((gendb_dir + "/lineitem_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();
    printf("lineitem: %zu rows in %.2fs\n", table.size(), elapsed);
}

// Orders ingestion (similar pattern, simpler schema)
void ingest_orders(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    MmapFile file(tbl_path);
    OrdersTable table;

    const size_t avg_line_len = 120;
    size_t estimated_rows = file.size / avg_line_len;
    table.o_orderkey.reserve(estimated_rows);
    table.o_custkey.reserve(estimated_rows);
    table.o_orderstatus.reserve(estimated_rows);
    table.o_totalprice.reserve(estimated_rows);
    table.o_orderdate.reserve(estimated_rows);
    table.o_orderpriority.reserve(estimated_rows);
    table.o_clerk.reserve(estimated_rows);
    table.o_shippriority.reserve(estimated_rows);
    table.o_comment.reserve(estimated_rows);

    const char* p = file.data;
    const char* end = file.data + file.size;

    while (p < end) {
        int32_t i32;
        double d;
        std::string s;

        p = parse_int32(p, i32); table.o_orderkey.push_back(i32);
        p = parse_int32(p, i32); table.o_custkey.push_back(i32);
        p = parse_string(p, s); table.o_orderstatus.push_back(s);
        p = parse_double(p, d); table.o_totalprice.push_back(d);
        p = parse_date(p, i32); table.o_orderdate.push_back(i32);
        p = parse_string(p, s); table.o_orderpriority.push_back(s);
        p = parse_string(p, s); table.o_clerk.push_back(s);
        p = parse_int32(p, i32); table.o_shippriority.push_back(i32);
        p = parse_string(p, s); table.o_comment.push_back(s);

        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }

    write_column(gendb_dir + "/orders_o_orderkey.bin", table.o_orderkey);
    write_column(gendb_dir + "/orders_o_custkey.bin", table.o_custkey);
    write_string_column(gendb_dir + "/orders_o_orderstatus.bin", table.o_orderstatus);
    write_column(gendb_dir + "/orders_o_totalprice.bin", table.o_totalprice);
    write_column(gendb_dir + "/orders_o_orderdate.bin", table.o_orderdate);
    write_string_column(gendb_dir + "/orders_o_orderpriority.bin", table.o_orderpriority);
    write_string_column(gendb_dir + "/orders_o_clerk.bin", table.o_clerk);
    write_column(gendb_dir + "/orders_o_shippriority.bin", table.o_shippriority);
    write_string_column(gendb_dir + "/orders_o_comment.bin", table.o_comment);

    FILE* meta = fopen((gendb_dir + "/orders_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    printf("orders: %zu rows in %.2fs\n", table.size(), elapsed);
}

// Customer ingestion
void ingest_customer(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    MmapFile file(tbl_path);
    CustomerTable table;

    const size_t avg_line_len = 160;
    size_t estimated_rows = file.size / avg_line_len;
    table.c_custkey.reserve(estimated_rows);
    table.c_name.reserve(estimated_rows);
    table.c_address.reserve(estimated_rows);
    table.c_nationkey.reserve(estimated_rows);
    table.c_phone.reserve(estimated_rows);
    table.c_acctbal.reserve(estimated_rows);
    table.c_mktsegment.reserve(estimated_rows);
    table.c_comment.reserve(estimated_rows);

    const char* p = file.data;
    const char* end = file.data + file.size;

    while (p < end) {
        int32_t i32;
        double d;
        std::string s;

        p = parse_int32(p, i32); table.c_custkey.push_back(i32);
        p = parse_string(p, s); table.c_name.push_back(s);
        p = parse_string(p, s); table.c_address.push_back(s);
        p = parse_int32(p, i32); table.c_nationkey.push_back(i32);
        p = parse_string(p, s); table.c_phone.push_back(s);
        p = parse_double(p, d); table.c_acctbal.push_back(d);
        p = parse_string(p, s); table.c_mktsegment.push_back(s);
        p = parse_string(p, s); table.c_comment.push_back(s);

        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }

    write_column(gendb_dir + "/customer_c_custkey.bin", table.c_custkey);
    write_string_column(gendb_dir + "/customer_c_name.bin", table.c_name);
    write_string_column(gendb_dir + "/customer_c_address.bin", table.c_address);
    write_column(gendb_dir + "/customer_c_nationkey.bin", table.c_nationkey);
    write_string_column(gendb_dir + "/customer_c_phone.bin", table.c_phone);
    write_column(gendb_dir + "/customer_c_acctbal.bin", table.c_acctbal);
    write_string_column(gendb_dir + "/customer_c_mktsegment.bin", table.c_mktsegment);
    write_string_column(gendb_dir + "/customer_c_comment.bin", table.c_comment);

    FILE* meta = fopen((gendb_dir + "/customer_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    printf("customer: %zu rows in %.2fs\n", table.size(), elapsed);
}

// Nation, Region, Supplier, Part, Partsupp (small tables, serial ingestion)
void ingest_nation(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    MmapFile file(tbl_path);
    NationTable table;

    const char* p = file.data;
    const char* end = file.data + file.size;

    while (p < end) {
        int32_t i32;
        std::string s;
        p = parse_int32(p, i32); table.n_nationkey.push_back(i32);
        p = parse_string(p, s); table.n_name.push_back(s);
        p = parse_int32(p, i32); table.n_regionkey.push_back(i32);
        p = parse_string(p, s); table.n_comment.push_back(s);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }

    write_column(gendb_dir + "/nation_n_nationkey.bin", table.n_nationkey);
    write_string_column(gendb_dir + "/nation_n_name.bin", table.n_name);
    write_column(gendb_dir + "/nation_n_regionkey.bin", table.n_regionkey);
    write_string_column(gendb_dir + "/nation_n_comment.bin", table.n_comment);

    FILE* meta = fopen((gendb_dir + "/nation_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("nation: %zu rows in %.2fs\n", table.size(), std::chrono::duration<double>(end_time - start).count());
}

void ingest_region(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    MmapFile file(tbl_path);
    RegionTable table;

    const char* p = file.data;
    const char* end = file.data + file.size;

    while (p < end) {
        int32_t i32;
        std::string s;
        p = parse_int32(p, i32); table.r_regionkey.push_back(i32);
        p = parse_string(p, s); table.r_name.push_back(s);
        p = parse_string(p, s); table.r_comment.push_back(s);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }

    write_column(gendb_dir + "/region_r_regionkey.bin", table.r_regionkey);
    write_string_column(gendb_dir + "/region_r_name.bin", table.r_name);
    write_string_column(gendb_dir + "/region_r_comment.bin", table.r_comment);

    FILE* meta = fopen((gendb_dir + "/region_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("region: %zu rows in %.2fs\n", table.size(), std::chrono::duration<double>(end_time - start).count());
}

void ingest_supplier(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    MmapFile file(tbl_path);
    SupplierTable table;

    const char* p = file.data;
    const char* end = file.data + file.size;

    while (p < end) {
        int32_t i32;
        double d;
        std::string s;
        p = parse_int32(p, i32); table.s_suppkey.push_back(i32);
        p = parse_string(p, s); table.s_name.push_back(s);
        p = parse_string(p, s); table.s_address.push_back(s);
        p = parse_int32(p, i32); table.s_nationkey.push_back(i32);
        p = parse_string(p, s); table.s_phone.push_back(s);
        p = parse_double(p, d); table.s_acctbal.push_back(d);
        p = parse_string(p, s); table.s_comment.push_back(s);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }

    write_column(gendb_dir + "/supplier_s_suppkey.bin", table.s_suppkey);
    write_string_column(gendb_dir + "/supplier_s_name.bin", table.s_name);
    write_string_column(gendb_dir + "/supplier_s_address.bin", table.s_address);
    write_column(gendb_dir + "/supplier_s_nationkey.bin", table.s_nationkey);
    write_string_column(gendb_dir + "/supplier_s_phone.bin", table.s_phone);
    write_column(gendb_dir + "/supplier_s_acctbal.bin", table.s_acctbal);
    write_string_column(gendb_dir + "/supplier_s_comment.bin", table.s_comment);

    FILE* meta = fopen((gendb_dir + "/supplier_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("supplier: %zu rows in %.2fs\n", table.size(), std::chrono::duration<double>(end_time - start).count());
}

void ingest_part(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    MmapFile file(tbl_path);
    PartTable table;

    const char* p = file.data;
    const char* end = file.data + file.size;

    while (p < end) {
        int32_t i32;
        double d;
        std::string s;
        p = parse_int32(p, i32); table.p_partkey.push_back(i32);
        p = parse_string(p, s); table.p_name.push_back(s);
        p = parse_string(p, s); table.p_mfgr.push_back(s);
        p = parse_string(p, s); table.p_brand.push_back(s);
        p = parse_string(p, s); table.p_type.push_back(s);
        p = parse_int32(p, i32); table.p_size.push_back(i32);
        p = parse_string(p, s); table.p_container.push_back(s);
        p = parse_double(p, d); table.p_retailprice.push_back(d);
        p = parse_string(p, s); table.p_comment.push_back(s);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }

    write_column(gendb_dir + "/part_p_partkey.bin", table.p_partkey);
    write_string_column(gendb_dir + "/part_p_name.bin", table.p_name);
    write_string_column(gendb_dir + "/part_p_mfgr.bin", table.p_mfgr);
    write_string_column(gendb_dir + "/part_p_brand.bin", table.p_brand);
    write_string_column(gendb_dir + "/part_p_type.bin", table.p_type);
    write_column(gendb_dir + "/part_p_size.bin", table.p_size);
    write_string_column(gendb_dir + "/part_p_container.bin", table.p_container);
    write_column(gendb_dir + "/part_p_retailprice.bin", table.p_retailprice);
    write_string_column(gendb_dir + "/part_p_comment.bin", table.p_comment);

    FILE* meta = fopen((gendb_dir + "/part_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("part: %zu rows in %.2fs\n", table.size(), std::chrono::duration<double>(end_time - start).count());
}

void ingest_partsupp(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    MmapFile file(tbl_path);
    PartsuppTable table;

    const char* p = file.data;
    const char* end = file.data + file.size;

    while (p < end) {
        int32_t i32;
        double d;
        std::string s;
        p = parse_int32(p, i32); table.ps_partkey.push_back(i32);
        p = parse_int32(p, i32); table.ps_suppkey.push_back(i32);
        p = parse_int32(p, i32); table.ps_availqty.push_back(i32);
        p = parse_double(p, d); table.ps_supplycost.push_back(d);
        p = parse_string(p, s); table.ps_comment.push_back(s);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }

    write_column(gendb_dir + "/partsupp_ps_partkey.bin", table.ps_partkey);
    write_column(gendb_dir + "/partsupp_ps_suppkey.bin", table.ps_suppkey);
    write_column(gendb_dir + "/partsupp_ps_availqty.bin", table.ps_availqty);
    write_column(gendb_dir + "/partsupp_ps_supplycost.bin", table.ps_supplycost);
    write_string_column(gendb_dir + "/partsupp_ps_comment.bin", table.ps_comment);

    FILE* meta = fopen((gendb_dir + "/partsupp_meta.txt").c_str(), "w");
    fprintf(meta, "%zu\n", table.size());
    fclose(meta);

    auto end_time = std::chrono::high_resolution_clock::now();
    printf("partsupp: %zu rows in %.2fs\n", table.size(), std::chrono::duration<double>(end_time - start).count());
}

// Mmap column reading
template<typename T>
MmapColumn<T>::~MmapColumn() {
    if (mapping) {
        munmap(mapping, mapping_size);
    }
}

template<typename T>
MmapColumn<T> mmap_column(const std::string& gendb_dir, const std::string& table_name,
                           const std::string& column_name, size_t row_count) {
    std::string path = gendb_dir + "/" + table_name + "_" + column_name + ".bin";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s\n", path.c_str());
        exit(1);
    }

    size_t file_size = row_count * sizeof(T);
    void* mapping = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (mapping == MAP_FAILED) {
        fprintf(stderr, "Failed to mmap %s\n", path.c_str());
        exit(1);
    }

    madvise(mapping, file_size, MADV_SEQUENTIAL);

    MmapColumn<T> col;
    col.data = static_cast<T*>(mapping);
    col.count = row_count;
    col.mapping = mapping;
    col.mapping_size = file_size;
    return col;
}

// Explicit instantiations for destructor and function
template struct MmapColumn<int32_t>;
template struct MmapColumn<double>;
template struct MmapColumn<char>;

template MmapColumn<int32_t> mmap_column<int32_t>(const std::string&, const std::string&, const std::string&, size_t);
template MmapColumn<double> mmap_column<double>(const std::string&, const std::string&, const std::string&, size_t);
template MmapColumn<char> mmap_column<char>(const std::string&, const std::string&, const std::string&, size_t);

size_t read_row_count(const std::string& gendb_dir, const std::string& table_name) {
    std::string path = gendb_dir + "/" + table_name + "_meta.txt";
    FILE* f = fopen(path.c_str(), "r");
    if (!f) {
        fprintf(stderr, "Failed to read %s\n", path.c_str());
        exit(1);
    }
    size_t count;
    fscanf(f, "%zu", &count);
    fclose(f);
    return count;
}

} // namespace gendb
