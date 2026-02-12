#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <thread>
#include <algorithm>

// Helper: Parse pipe-delimited line
std::vector<std::string> split_line(const char* start, const char* end) {
    std::vector<std::string> fields;
    const char* p = start;
    const char* field_start = p;

    while (p < end) {
        if (*p == '|') {
            fields.emplace_back(field_start, p - field_start);
            field_start = p + 1;
        }
        p++;
    }
    return fields;
}

// Helper: Fast numeric parsing
inline int32_t parse_int(const char* str) {
    return static_cast<int32_t>(strtol(str, nullptr, 10));
}

inline double parse_double(const char* str) {
    return strtod(str, nullptr);
}

// Helper: mmap a file
const char* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        return nullptr;
    }

    out_size = sb.st_size;
    const char* data = static_cast<const char*>(mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (data == MAP_FAILED) {
        return nullptr;
    }

    madvise(const_cast<char*>(data), out_size, MADV_SEQUENTIAL);
    return data;
}

// Helper: Write binary column
template<typename T>
void write_column_binary(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        std::cerr << "Failed to open " << path << " for writing" << std::endl;
        return;
    }

    // 1MB write buffer
    char buffer[1 << 20];
    setvbuf(f, buffer, _IOFBF, sizeof(buffer));

    fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
}

// Helper: Write string column (length-prefixed)
void write_string_column(const std::string& path, const std::vector<std::string>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        std::cerr << "Failed to open " << path << " for writing" << std::endl;
        return;
    }

    char buffer[1 << 20];
    setvbuf(f, buffer, _IOFBF, sizeof(buffer));

    for (const auto& s : data) {
        uint32_t len = static_cast<uint32_t>(s.size());
        fwrite(&len, sizeof(uint32_t), 1, f);
        fwrite(s.data(), 1, len, f);
    }
    fclose(f);
}

// Helper: Write metadata
void write_metadata(const std::string& gendb_dir, const std::string& table, size_t row_count) {
    std::string meta_path = gendb_dir + "/" + table + ".meta";
    std::ofstream f(meta_path);
    f << row_count << std::endl;
    f.close();
}

void ingest_lineitem(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    // Estimate rows and pre-allocate
    size_t estimated_rows = file_size / 150; // avg ~150 bytes per row
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

    // Parse line by line
    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 16) {
                    table.l_orderkey.push_back(parse_int(fields[0].c_str()));
                    table.l_partkey.push_back(parse_int(fields[1].c_str()));
                    table.l_suppkey.push_back(parse_int(fields[2].c_str()));
                    table.l_linenumber.push_back(parse_int(fields[3].c_str()));
                    table.l_quantity.push_back(parse_double(fields[4].c_str()));
                    table.l_extendedprice.push_back(parse_double(fields[5].c_str()));
                    table.l_discount.push_back(parse_double(fields[6].c_str()));
                    table.l_tax.push_back(parse_double(fields[7].c_str()));
                    table.l_returnflag.push_back(static_cast<uint8_t>(fields[8][0]));
                    table.l_linestatus.push_back(static_cast<uint8_t>(fields[9][0]));
                    table.l_shipdate.push_back(date_utils::parse_date(fields[10]));
                    table.l_commitdate.push_back(date_utils::parse_date(fields[11]));
                    table.l_receiptdate.push_back(date_utils::parse_date(fields[12]));
                    table.l_shipinstruct.push_back(fields[13]);
                    table.l_shipmode.push_back(fields[14]);
                    table.l_comment.push_back(fields[15]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    // Sort by l_shipdate for zone map effectiveness
    std::vector<size_t> indices(table.size());
    for (size_t i = 0; i < indices.size(); i++) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.l_shipdate[a] < table.l_shipdate[b];
    });

    // Reorder all columns by sorted indices
    LineitemTable sorted;
    sorted.l_orderkey.resize(table.size());
    sorted.l_partkey.resize(table.size());
    sorted.l_suppkey.resize(table.size());
    sorted.l_linenumber.resize(table.size());
    sorted.l_quantity.resize(table.size());
    sorted.l_extendedprice.resize(table.size());
    sorted.l_discount.resize(table.size());
    sorted.l_tax.resize(table.size());
    sorted.l_returnflag.resize(table.size());
    sorted.l_linestatus.resize(table.size());
    sorted.l_shipdate.resize(table.size());
    sorted.l_commitdate.resize(table.size());
    sorted.l_receiptdate.resize(table.size());
    sorted.l_shipinstruct.resize(table.size());
    sorted.l_shipmode.resize(table.size());
    sorted.l_comment.resize(table.size());

    for (size_t i = 0; i < indices.size(); i++) {
        size_t idx = indices[i];
        sorted.l_orderkey[i] = table.l_orderkey[idx];
        sorted.l_partkey[i] = table.l_partkey[idx];
        sorted.l_suppkey[i] = table.l_suppkey[idx];
        sorted.l_linenumber[i] = table.l_linenumber[idx];
        sorted.l_quantity[i] = table.l_quantity[idx];
        sorted.l_extendedprice[i] = table.l_extendedprice[idx];
        sorted.l_discount[i] = table.l_discount[idx];
        sorted.l_tax[i] = table.l_tax[idx];
        sorted.l_returnflag[i] = table.l_returnflag[idx];
        sorted.l_linestatus[i] = table.l_linestatus[idx];
        sorted.l_shipdate[i] = table.l_shipdate[idx];
        sorted.l_commitdate[i] = table.l_commitdate[idx];
        sorted.l_receiptdate[i] = table.l_receiptdate[idx];
        sorted.l_shipinstruct[i] = table.l_shipinstruct[idx];
        sorted.l_shipmode[i] = table.l_shipmode[idx];
        sorted.l_comment[i] = table.l_comment[idx];
    }

    // Write binary columns
    write_column_binary(gendb_dir + "/lineitem.l_orderkey", sorted.l_orderkey);
    write_column_binary(gendb_dir + "/lineitem.l_partkey", sorted.l_partkey);
    write_column_binary(gendb_dir + "/lineitem.l_suppkey", sorted.l_suppkey);
    write_column_binary(gendb_dir + "/lineitem.l_linenumber", sorted.l_linenumber);
    write_column_binary(gendb_dir + "/lineitem.l_quantity", sorted.l_quantity);
    write_column_binary(gendb_dir + "/lineitem.l_extendedprice", sorted.l_extendedprice);
    write_column_binary(gendb_dir + "/lineitem.l_discount", sorted.l_discount);
    write_column_binary(gendb_dir + "/lineitem.l_tax", sorted.l_tax);
    write_column_binary(gendb_dir + "/lineitem.l_returnflag", sorted.l_returnflag);
    write_column_binary(gendb_dir + "/lineitem.l_linestatus", sorted.l_linestatus);
    write_column_binary(gendb_dir + "/lineitem.l_shipdate", sorted.l_shipdate);
    write_column_binary(gendb_dir + "/lineitem.l_commitdate", sorted.l_commitdate);
    write_column_binary(gendb_dir + "/lineitem.l_receiptdate", sorted.l_receiptdate);
    write_string_column(gendb_dir + "/lineitem.l_shipinstruct", sorted.l_shipinstruct);
    write_string_column(gendb_dir + "/lineitem.l_shipmode", sorted.l_shipmode);
    write_string_column(gendb_dir + "/lineitem.l_comment", sorted.l_comment);

    write_metadata(gendb_dir, "lineitem", sorted.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "lineitem: " << sorted.size() << " rows in " << elapsed << "s" << std::endl;
}

void ingest_orders(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    size_t estimated_rows = file_size / 120;
    OrdersTable table;
    table.o_orderkey.reserve(estimated_rows);
    table.o_custkey.reserve(estimated_rows);
    table.o_orderstatus.reserve(estimated_rows);
    table.o_totalprice.reserve(estimated_rows);
    table.o_orderdate.reserve(estimated_rows);
    table.o_shippriority.reserve(estimated_rows);

    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 9) {
                    table.o_orderkey.push_back(parse_int(fields[0].c_str()));
                    table.o_custkey.push_back(parse_int(fields[1].c_str()));
                    table.o_orderstatus.push_back(static_cast<uint8_t>(fields[2][0]));
                    table.o_totalprice.push_back(parse_double(fields[3].c_str()));
                    table.o_orderdate.push_back(date_utils::parse_date(fields[4]));
                    table.o_orderpriority.push_back(fields[5]);
                    table.o_clerk.push_back(fields[6]);
                    table.o_shippriority.push_back(parse_int(fields[7].c_str()));
                    table.o_comment.push_back(fields[8]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    // Sort by o_orderdate
    std::vector<size_t> indices(table.size());
    for (size_t i = 0; i < indices.size(); i++) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.o_orderdate[a] < table.o_orderdate[b];
    });

    OrdersTable sorted;
    sorted.o_orderkey.resize(table.size());
    sorted.o_custkey.resize(table.size());
    sorted.o_orderstatus.resize(table.size());
    sorted.o_totalprice.resize(table.size());
    sorted.o_orderdate.resize(table.size());
    sorted.o_orderpriority.resize(table.size());
    sorted.o_clerk.resize(table.size());
    sorted.o_shippriority.resize(table.size());
    sorted.o_comment.resize(table.size());

    for (size_t i = 0; i < indices.size(); i++) {
        size_t idx = indices[i];
        sorted.o_orderkey[i] = table.o_orderkey[idx];
        sorted.o_custkey[i] = table.o_custkey[idx];
        sorted.o_orderstatus[i] = table.o_orderstatus[idx];
        sorted.o_totalprice[i] = table.o_totalprice[idx];
        sorted.o_orderdate[i] = table.o_orderdate[idx];
        sorted.o_orderpriority[i] = table.o_orderpriority[idx];
        sorted.o_clerk[i] = table.o_clerk[idx];
        sorted.o_shippriority[i] = table.o_shippriority[idx];
        sorted.o_comment[i] = table.o_comment[idx];
    }

    write_column_binary(gendb_dir + "/orders.o_orderkey", sorted.o_orderkey);
    write_column_binary(gendb_dir + "/orders.o_custkey", sorted.o_custkey);
    write_column_binary(gendb_dir + "/orders.o_orderstatus", sorted.o_orderstatus);
    write_column_binary(gendb_dir + "/orders.o_totalprice", sorted.o_totalprice);
    write_column_binary(gendb_dir + "/orders.o_orderdate", sorted.o_orderdate);
    write_column_binary(gendb_dir + "/orders.o_shippriority", sorted.o_shippriority);
    write_string_column(gendb_dir + "/orders.o_orderpriority", sorted.o_orderpriority);
    write_string_column(gendb_dir + "/orders.o_clerk", sorted.o_clerk);
    write_string_column(gendb_dir + "/orders.o_comment", sorted.o_comment);

    write_metadata(gendb_dir, "orders", sorted.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "orders: " << sorted.size() << " rows in " << elapsed << "s" << std::endl;
}

void ingest_customer(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    size_t estimated_rows = file_size / 180;
    CustomerTable table;
    table.c_custkey.reserve(estimated_rows);
    table.c_nationkey.reserve(estimated_rows);
    table.c_acctbal.reserve(estimated_rows);
    table.c_mktsegment.reserve(estimated_rows);

    DictionaryEncoder seg_encoder;

    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 8) {
                    table.c_custkey.push_back(parse_int(fields[0].c_str()));
                    table.c_name.push_back(fields[1]);
                    table.c_address.push_back(fields[2]);
                    table.c_nationkey.push_back(parse_int(fields[3].c_str()));
                    table.c_phone.push_back(fields[4]);
                    table.c_acctbal.push_back(parse_double(fields[5].c_str()));
                    table.c_mktsegment.push_back(seg_encoder.encode(fields[6]));
                    table.c_comment.push_back(fields[7]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    write_column_binary(gendb_dir + "/customer.c_custkey", table.c_custkey);
    write_column_binary(gendb_dir + "/customer.c_nationkey", table.c_nationkey);
    write_column_binary(gendb_dir + "/customer.c_acctbal", table.c_acctbal);
    write_column_binary(gendb_dir + "/customer.c_mktsegment", table.c_mktsegment);
    write_string_column(gendb_dir + "/customer.c_name", table.c_name);
    write_string_column(gendb_dir + "/customer.c_address", table.c_address);
    write_string_column(gendb_dir + "/customer.c_phone", table.c_phone);
    write_string_column(gendb_dir + "/customer.c_comment", table.c_comment);

    // Write dictionary
    write_string_column(gendb_dir + "/customer.c_mktsegment.dict", seg_encoder.id_to_str);

    write_metadata(gendb_dir, "customer", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "customer: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

void ingest_nation(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    NationTable table;

    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 4) {
                    table.n_nationkey.push_back(parse_int(fields[0].c_str()));
                    table.n_name.push_back(fields[1]);
                    table.n_regionkey.push_back(parse_int(fields[2].c_str()));
                    table.n_comment.push_back(fields[3]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    write_column_binary(gendb_dir + "/nation.n_nationkey", table.n_nationkey);
    write_column_binary(gendb_dir + "/nation.n_regionkey", table.n_regionkey);
    write_string_column(gendb_dir + "/nation.n_name", table.n_name);
    write_string_column(gendb_dir + "/nation.n_comment", table.n_comment);

    write_metadata(gendb_dir, "nation", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "nation: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

void ingest_region(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    RegionTable table;

    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 3) {
                    table.r_regionkey.push_back(parse_int(fields[0].c_str()));
                    table.r_name.push_back(fields[1]);
                    table.r_comment.push_back(fields[2]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    write_column_binary(gendb_dir + "/region.r_regionkey", table.r_regionkey);
    write_string_column(gendb_dir + "/region.r_name", table.r_name);
    write_string_column(gendb_dir + "/region.r_comment", table.r_comment);

    write_metadata(gendb_dir, "region", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "region: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

void ingest_supplier(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    SupplierTable table;

    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 7) {
                    table.s_suppkey.push_back(parse_int(fields[0].c_str()));
                    table.s_name.push_back(fields[1]);
                    table.s_address.push_back(fields[2]);
                    table.s_nationkey.push_back(parse_int(fields[3].c_str()));
                    table.s_phone.push_back(fields[4]);
                    table.s_acctbal.push_back(parse_double(fields[5].c_str()));
                    table.s_comment.push_back(fields[6]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    write_column_binary(gendb_dir + "/supplier.s_suppkey", table.s_suppkey);
    write_column_binary(gendb_dir + "/supplier.s_nationkey", table.s_nationkey);
    write_column_binary(gendb_dir + "/supplier.s_acctbal", table.s_acctbal);
    write_string_column(gendb_dir + "/supplier.s_name", table.s_name);
    write_string_column(gendb_dir + "/supplier.s_address", table.s_address);
    write_string_column(gendb_dir + "/supplier.s_phone", table.s_phone);
    write_string_column(gendb_dir + "/supplier.s_comment", table.s_comment);

    write_metadata(gendb_dir, "supplier", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "supplier: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

void ingest_part(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    PartTable table;

    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 9) {
                    table.p_partkey.push_back(parse_int(fields[0].c_str()));
                    table.p_name.push_back(fields[1]);
                    table.p_mfgr.push_back(fields[2]);
                    table.p_brand.push_back(fields[3]);
                    table.p_type.push_back(fields[4]);
                    table.p_size.push_back(parse_int(fields[5].c_str()));
                    table.p_container.push_back(fields[6]);
                    table.p_retailprice.push_back(parse_double(fields[7].c_str()));
                    table.p_comment.push_back(fields[8]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    write_column_binary(gendb_dir + "/part.p_partkey", table.p_partkey);
    write_column_binary(gendb_dir + "/part.p_size", table.p_size);
    write_column_binary(gendb_dir + "/part.p_retailprice", table.p_retailprice);
    write_string_column(gendb_dir + "/part.p_name", table.p_name);
    write_string_column(gendb_dir + "/part.p_mfgr", table.p_mfgr);
    write_string_column(gendb_dir + "/part.p_brand", table.p_brand);
    write_string_column(gendb_dir + "/part.p_type", table.p_type);
    write_string_column(gendb_dir + "/part.p_container", table.p_container);
    write_string_column(gendb_dir + "/part.p_comment", table.p_comment);

    write_metadata(gendb_dir, "part", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "part: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

void ingest_partsupp(const std::string& tbl_path, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    size_t file_size;
    const char* data = mmap_file(tbl_path, file_size);
    if (!data) return;

    PartsuppTable table;

    const char* p = data;
    const char* end = data + file_size;
    const char* line_start = p;

    while (p < end) {
        if (*p == '\n') {
            if (p > line_start) {
                auto fields = split_line(line_start, p);
                if (fields.size() >= 5) {
                    table.ps_partkey.push_back(parse_int(fields[0].c_str()));
                    table.ps_suppkey.push_back(parse_int(fields[1].c_str()));
                    table.ps_availqty.push_back(parse_int(fields[2].c_str()));
                    table.ps_supplycost.push_back(parse_double(fields[3].c_str()));
                    table.ps_comment.push_back(fields[4]);
                }
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap(const_cast<char*>(data), file_size);

    write_column_binary(gendb_dir + "/partsupp.ps_partkey", table.ps_partkey);
    write_column_binary(gendb_dir + "/partsupp.ps_suppkey", table.ps_suppkey);
    write_column_binary(gendb_dir + "/partsupp.ps_availqty", table.ps_availqty);
    write_column_binary(gendb_dir + "/partsupp.ps_supplycost", table.ps_supplycost);
    write_string_column(gendb_dir + "/partsupp.ps_comment", table.ps_comment);

    write_metadata(gendb_dir, "partsupp", table.size());

    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "partsupp: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// mmap column loading
template<typename T>
T* mmap_column(const std::string& gendb_dir, const std::string& table, const std::string& column, size_t& out_size) {
    std::string path = gendb_dir + "/" + table + "." + column;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;
    out_size = file_size / sizeof(T);

    T* data = static_cast<T*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (data == MAP_FAILED) {
        return nullptr;
    }

    madvise(data, file_size, MADV_SEQUENTIAL);
    return data;
}

// Explicit instantiations
template int32_t* mmap_column<int32_t>(const std::string&, const std::string&, const std::string&, size_t&);
template double* mmap_column<double>(const std::string&, const std::string&, const std::string&, size_t&);
template uint8_t* mmap_column<uint8_t>(const std::string&, const std::string&, const std::string&, size_t&);

std::vector<std::string> load_string_column(const std::string& gendb_dir, const std::string& table, const std::string& column) {
    std::string path = gendb_dir + "/" + table + "." + column;
    std::ifstream f(path, std::ios::binary);
    std::vector<std::string> result;

    while (f) {
        uint32_t len;
        if (!f.read(reinterpret_cast<char*>(&len), sizeof(uint32_t))) break;
        std::string s(len, '\0');
        f.read(&s[0], len);
        result.push_back(s);
    }

    return result;
}

size_t get_row_count(const std::string& gendb_dir, const std::string& table) {
    std::string meta_path = gendb_dir + "/" + table + ".meta";
    std::ifstream f(meta_path);
    size_t count;
    f >> count;
    return count;
}
