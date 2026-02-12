#include "storage/storage.h"
#include "utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstring>
#include <chrono>
#include <thread>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

using namespace gendb;

// Helper: parse a pipe-delimited line (handles trailing pipe)
static std::vector<std::string> split_line(const char* start, const char* end) {
    std::vector<std::string> fields;
    const char* p = start;
    const char* field_start = p;

    while (p < end) {
        if (*p == '|') {
            fields.emplace_back(field_start, p);
            field_start = p + 1;
        }
        p++;
    }

    // Handle trailing field (if line doesn't end with |)
    if (field_start < end && *field_start != '\n' && *field_start != '\r') {
        fields.emplace_back(field_start, end);
    }

    return fields;
}

// Helper: fast integer parsing
static int32_t parse_int(const char* str) {
    return static_cast<int32_t>(strtol(str, nullptr, 10));
}

// Helper: fast double parsing
static double parse_double(const char* str) {
    return strtod(str, nullptr);
}

// Helper: trim whitespace
static std::string trim(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(s[start])) start++;
    size_t end = s.size();
    while (end > start && std::isspace(s[end - 1])) end--;
    return s.substr(start, end - start);
}

// Load lineitem table
static void load_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string path = data_dir + "/lineitem.tbl";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return;
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = static_cast<const char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        close(fd);
        std::cerr << "mmap failed for " << path << std::endl;
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Estimate row count and reserve
    size_t estimated_rows = file_size / 150;  // Average ~150 bytes per line
    LineItemTable table;
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

    // Parse line by line
    const char* line_start = data;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n') {
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
                table.l_shipdate.push_back(parse_date(fields[10].c_str()));
                table.l_commitdate.push_back(parse_date(fields[11].c_str()));
                table.l_receiptdate.push_back(parse_date(fields[12].c_str()));
                table.l_shipinstruct.push_back(trim(fields[13]));
                table.l_shipmode.push_back(trim(fields[14]));
                table.l_comment.push_back(trim(fields[15]));
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap((void*)data, file_size);
    close(fd);

    // Write binary columns
    write_lineitem(gendb_dir, table);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "lineitem: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Load orders table
static void load_orders(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string path = data_dir + "/orders.tbl";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return;
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = static_cast<const char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        close(fd);
        std::cerr << "mmap failed for " << path << std::endl;
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

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

    const char* line_start = data;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n') {
            auto fields = split_line(line_start, p);
            if (fields.size() >= 9) {
                table.o_orderkey.push_back(parse_int(fields[0].c_str()));
                table.o_custkey.push_back(parse_int(fields[1].c_str()));
                table.o_orderstatus.push_back(static_cast<uint8_t>(fields[2][0]));
                table.o_totalprice.push_back(parse_double(fields[3].c_str()));
                table.o_orderdate.push_back(parse_date(fields[4].c_str()));
                table.o_orderpriority.push_back(trim(fields[5]));
                table.o_clerk.push_back(trim(fields[6]));
                table.o_shippriority.push_back(parse_int(fields[7].c_str()));
                table.o_comment.push_back(trim(fields[8]));
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap((void*)data, file_size);
    close(fd);

    write_orders(gendb_dir, table);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "orders: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Load customer table
static void load_customer(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    std::string path = data_dir + "/customer.tbl";
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << path << std::endl;
        return;
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;

    const char* data = static_cast<const char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        close(fd);
        std::cerr << "mmap failed for " << path << std::endl;
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

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

    // Dictionary for c_mktsegment
    Dictionary mkt_dict;

    const char* line_start = data;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        if (*p == '\n') {
            auto fields = split_line(line_start, p);
            if (fields.size() >= 8) {
                table.c_custkey.push_back(parse_int(fields[0].c_str()));
                table.c_name.push_back(trim(fields[1]));
                table.c_address.push_back(trim(fields[2]));
                table.c_nationkey.push_back(parse_int(fields[3].c_str()));
                table.c_phone.push_back(trim(fields[4]));
                table.c_acctbal.push_back(parse_double(fields[5].c_str()));
                table.c_mktsegment.push_back(mkt_dict.encode(trim(fields[6])));
                table.c_comment.push_back(trim(fields[7]));
            }
            line_start = p + 1;
        }
        p++;
    }

    munmap((void*)data, file_size);
    close(fd);

    write_customer(gendb_dir, table);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "customer: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

// Simplified loaders for other tables (nation, region, supplier, part, partsupp)
static void load_nation(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ifstream in(data_dir + "/nation.tbl");
    if (!in) return;

    NationTable table;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;
        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }
        if (fields.size() >= 4) {
            table.n_nationkey.push_back(parse_int(fields[0].c_str()));
            table.n_name.push_back(trim(fields[1]));
            table.n_regionkey.push_back(parse_int(fields[2].c_str()));
            table.n_comment.push_back(trim(fields[3]));
        }
    }

    write_nation(gendb_dir, table);
    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "nation: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

static void load_region(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ifstream in(data_dir + "/region.tbl");
    if (!in) return;

    RegionTable table;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;
        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }
        if (fields.size() >= 3) {
            table.r_regionkey.push_back(parse_int(fields[0].c_str()));
            table.r_name.push_back(trim(fields[1]));
            table.r_comment.push_back(trim(fields[2]));
        }
    }

    write_region(gendb_dir, table);
    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "region: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

static void load_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ifstream in(data_dir + "/supplier.tbl");
    if (!in) return;

    SupplierTable table;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;
        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }
        if (fields.size() >= 7) {
            table.s_suppkey.push_back(parse_int(fields[0].c_str()));
            table.s_name.push_back(trim(fields[1]));
            table.s_address.push_back(trim(fields[2]));
            table.s_nationkey.push_back(parse_int(fields[3].c_str()));
            table.s_phone.push_back(trim(fields[4]));
            table.s_acctbal.push_back(parse_double(fields[5].c_str()));
            table.s_comment.push_back(trim(fields[6]));
        }
    }

    write_supplier(gendb_dir, table);
    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "supplier: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

static void load_part(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ifstream in(data_dir + "/part.tbl");
    if (!in) return;

    PartTable table;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;
        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }
        if (fields.size() >= 9) {
            table.p_partkey.push_back(parse_int(fields[0].c_str()));
            table.p_name.push_back(trim(fields[1]));
            table.p_mfgr.push_back(trim(fields[2]));
            table.p_brand.push_back(trim(fields[3]));
            table.p_type.push_back(trim(fields[4]));
            table.p_size.push_back(parse_int(fields[5].c_str()));
            table.p_container.push_back(trim(fields[6]));
            table.p_retailprice.push_back(parse_double(fields[7].c_str()));
            table.p_comment.push_back(trim(fields[8]));
        }
    }

    write_part(gendb_dir, table);
    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "part: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

static void load_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::ifstream in(data_dir + "/partsupp.tbl");
    if (!in) return;

    PartSuppTable table;
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::vector<std::string> fields;
        while (std::getline(ss, field, '|')) {
            fields.push_back(field);
        }
        if (fields.size() >= 5) {
            table.ps_partkey.push_back(parse_int(fields[0].c_str()));
            table.ps_suppkey.push_back(parse_int(fields[1].c_str()));
            table.ps_availqty.push_back(parse_int(fields[2].c_str()));
            table.ps_supplycost.push_back(parse_double(fields[3].c_str()));
            table.ps_comment.push_back(trim(fields[4]));
        }
    }

    write_partsupp(gendb_dir, table);
    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "partsupp: " << table.size() << " rows in " << elapsed << "s" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create gendb directory if it doesn't exist
    system(("mkdir -p " + gendb_dir).c_str());

    auto total_start = std::chrono::high_resolution_clock::now();

    // Ingest tables in parallel
    std::vector<std::thread> threads;
    threads.emplace_back(load_nation, data_dir, gendb_dir);
    threads.emplace_back(load_region, data_dir, gendb_dir);
    threads.emplace_back(load_supplier, data_dir, gendb_dir);
    threads.emplace_back(load_part, data_dir, gendb_dir);
    threads.emplace_back(load_partsupp, data_dir, gendb_dir);
    threads.emplace_back(load_customer, data_dir, gendb_dir);
    threads.emplace_back(load_orders, data_dir, gendb_dir);
    threads.emplace_back(load_lineitem, data_dir, gendb_dir);

    for (auto& t : threads) t.join();

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::cout << "\nTotal ingestion time: " << total_elapsed << "s" << std::endl;

    return 0;
}
