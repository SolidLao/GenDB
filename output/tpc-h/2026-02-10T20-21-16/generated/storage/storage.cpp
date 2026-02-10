#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace gendb {

// Helper: Parse pipe-delimited line
static std::vector<std::string> split_pipe(const std::string& line) {
    std::vector<std::string> fields;
    std::string field;
    std::istringstream ss(line);
    while (std::getline(ss, field, '|')) {
        fields.push_back(field);
    }
    return fields;
}

// Helper: Write binary column to file
template<typename T>
static void write_column_binary(const std::string& path, const std::vector<T>& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
    out.close();
}

// Helper: mmap read binary column
template<typename T>
static const T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    fstat(fd, &st);
    count = st.st_size / sizeof(T);

    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }

    // Advise sequential access for scans
    madvise(ptr, st.st_size, MADV_SEQUENTIAL);

    return static_cast<const T*>(ptr);
}

// Lineitem ingestion
void ingest_lineitem(const std::string& tbl_file, const std::string& gendb_dir) {
    std::cout << "Ingesting lineitem from " << tbl_file << std::endl;

    LineitemTable table;
    std::ifstream in(tbl_file);
    std::string line;

    // Reserve space for efficiency (SF10 has ~60M rows)
    table.l_orderkey.reserve(60000000);
    table.l_partkey.reserve(60000000);
    table.l_suppkey.reserve(60000000);
    table.l_linenumber.reserve(60000000);
    table.l_quantity.reserve(60000000);
    table.l_extendedprice.reserve(60000000);
    table.l_discount.reserve(60000000);
    table.l_tax.reserve(60000000);
    table.l_returnflag.reserve(60000000);
    table.l_linestatus.reserve(60000000);
    table.l_shipdate.reserve(60000000);
    table.l_commitdate.reserve(60000000);
    table.l_receiptdate.reserve(60000000);

    size_t row_count = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        auto fields = split_pipe(line);
        if (fields.size() < 16) continue;

        table.l_orderkey.push_back(std::stoi(fields[0]));
        table.l_partkey.push_back(std::stoi(fields[1]));
        table.l_suppkey.push_back(std::stoi(fields[2]));
        table.l_linenumber.push_back(std::stoi(fields[3]));
        table.l_quantity.push_back(std::stod(fields[4]));
        table.l_extendedprice.push_back(std::stod(fields[5]));
        table.l_discount.push_back(std::stod(fields[6]));
        table.l_tax.push_back(std::stod(fields[7]));

        // Dictionary encoding for returnflag
        std::string rf = fields[8];
        if (table.returnflag_lookup.find(rf) == table.returnflag_lookup.end()) {
            uint8_t code = table.returnflag_dict.size();
            table.returnflag_dict.push_back(rf);
            table.returnflag_lookup[rf] = code;
        }
        table.l_returnflag.push_back(table.returnflag_lookup[rf]);

        // Dictionary encoding for linestatus
        std::string ls = fields[9];
        if (table.linestatus_lookup.find(ls) == table.linestatus_lookup.end()) {
            uint8_t code = table.linestatus_dict.size();
            table.linestatus_dict.push_back(ls);
            table.linestatus_lookup[ls] = code;
        }
        table.l_linestatus.push_back(table.linestatus_lookup[ls]);

        table.l_shipdate.push_back(parse_date(fields[10]));
        table.l_commitdate.push_back(parse_date(fields[11]));
        table.l_receiptdate.push_back(parse_date(fields[12]));
        table.l_shipinstruct.push_back(fields[13]);
        table.l_shipmode.push_back(fields[14]);
        table.l_comment.push_back(fields[15]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Loaded " << row_count / 1000000 << "M rows..." << std::endl;
        }
    }
    in.close();

    std::cout << "  Total rows: " << row_count << std::endl;
    std::cout << "  Sorting by l_shipdate..." << std::endl;

    // Sort by l_shipdate (critical for zone maps)
    std::vector<size_t> indices(row_count);
    for (size_t i = 0; i < row_count; i++) indices[i] = i;

    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return table.l_shipdate[a] < table.l_shipdate[b];
    });

    // Reorder all columns based on sorted indices
    auto reorder = [&](auto& vec) {
        using T = typename std::decay<decltype(vec[0])>::type;
        std::vector<T> temp(row_count);
        for (size_t i = 0; i < row_count; i++) {
            temp[i] = vec[indices[i]];
        }
        vec = std::move(temp);
    };

    reorder(table.l_orderkey);
    reorder(table.l_partkey);
    reorder(table.l_suppkey);
    reorder(table.l_linenumber);
    reorder(table.l_quantity);
    reorder(table.l_extendedprice);
    reorder(table.l_discount);
    reorder(table.l_tax);
    reorder(table.l_returnflag);
    reorder(table.l_linestatus);
    reorder(table.l_shipdate);
    reorder(table.l_commitdate);
    reorder(table.l_receiptdate);
    reorder(table.l_shipinstruct);
    reorder(table.l_shipmode);
    reorder(table.l_comment);

    std::cout << "  Writing binary columns..." << std::endl;

    // Write binary column files
    std::string dir = gendb_dir + "/lineitem";
    system(("mkdir -p " + dir).c_str());

    write_column_binary(dir + "/l_orderkey.col", table.l_orderkey);
    write_column_binary(dir + "/l_partkey.col", table.l_partkey);
    write_column_binary(dir + "/l_suppkey.col", table.l_suppkey);
    write_column_binary(dir + "/l_linenumber.col", table.l_linenumber);
    write_column_binary(dir + "/l_quantity.col", table.l_quantity);
    write_column_binary(dir + "/l_extendedprice.col", table.l_extendedprice);
    write_column_binary(dir + "/l_discount.col", table.l_discount);
    write_column_binary(dir + "/l_tax.col", table.l_tax);
    write_column_binary(dir + "/l_returnflag.col", table.l_returnflag);
    write_column_binary(dir + "/l_linestatus.col", table.l_linestatus);
    write_column_binary(dir + "/l_shipdate.col", table.l_shipdate);
    write_column_binary(dir + "/l_commitdate.col", table.l_commitdate);
    write_column_binary(dir + "/l_receiptdate.col", table.l_receiptdate);

    // Write metadata (row count and dictionaries)
    std::ofstream meta(dir + "/metadata.txt");
    meta << "row_count=" << row_count << "\n";
    meta << "returnflag_dict=";
    for (const auto& s : table.returnflag_dict) meta << s << ",";
    meta << "\n";
    meta << "linestatus_dict=";
    for (const auto& s : table.linestatus_dict) meta << s << ",";
    meta << "\n";
    meta.close();

    std::cout << "  Lineitem ingestion complete!" << std::endl;
}

// Orders ingestion
void ingest_orders(const std::string& tbl_file, const std::string& gendb_dir) {
    std::cout << "Ingesting orders from " << tbl_file << std::endl;

    OrdersTable table;
    std::ifstream in(tbl_file);
    std::string line;

    table.o_orderkey.reserve(15000000);
    table.o_custkey.reserve(15000000);
    table.o_orderdate.reserve(15000000);
    table.o_shippriority.reserve(15000000);

    size_t row_count = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        auto fields = split_pipe(line);
        if (fields.size() < 9) continue;

        table.o_orderkey.push_back(std::stoi(fields[0]));
        table.o_custkey.push_back(std::stoi(fields[1]));
        table.o_orderstatus.push_back(fields[2]);
        table.o_totalprice.push_back(std::stod(fields[3]));
        table.o_orderdate.push_back(parse_date(fields[4]));
        table.o_orderpriority.push_back(fields[5]);
        table.o_clerk.push_back(fields[6]);
        table.o_shippriority.push_back(std::stoi(fields[7]));
        table.o_comment.push_back(fields[8]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  Loaded " << row_count / 1000000 << "M rows..." << std::endl;
        }
    }
    in.close();

    std::cout << "  Total rows: " << row_count << std::endl;
    std::cout << "  Writing binary columns..." << std::endl;

    std::string dir = gendb_dir + "/orders";
    system(("mkdir -p " + dir).c_str());

    write_column_binary(dir + "/o_orderkey.col", table.o_orderkey);
    write_column_binary(dir + "/o_custkey.col", table.o_custkey);
    write_column_binary(dir + "/o_orderdate.col", table.o_orderdate);
    write_column_binary(dir + "/o_shippriority.col", table.o_shippriority);

    std::ofstream meta(dir + "/metadata.txt");
    meta << "row_count=" << row_count << "\n";
    meta.close();

    std::cout << "  Orders ingestion complete!" << std::endl;
}

// Customer ingestion
void ingest_customer(const std::string& tbl_file, const std::string& gendb_dir) {
    std::cout << "Ingesting customer from " << tbl_file << std::endl;

    CustomerTable table;
    std::ifstream in(tbl_file);
    std::string line;

    table.c_custkey.reserve(1500000);
    table.c_mktsegment.reserve(1500000);

    size_t row_count = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        auto fields = split_pipe(line);
        if (fields.size() < 8) continue;

        table.c_custkey.push_back(std::stoi(fields[0]));
        table.c_name.push_back(fields[1]);
        table.c_address.push_back(fields[2]);
        table.c_nationkey.push_back(std::stoi(fields[3]));
        table.c_phone.push_back(fields[4]);
        table.c_acctbal.push_back(std::stod(fields[5]));

        // Dictionary encoding for mktsegment
        std::string ms = fields[6];
        if (table.mktsegment_lookup.find(ms) == table.mktsegment_lookup.end()) {
            uint8_t code = table.mktsegment_dict.size();
            table.mktsegment_dict.push_back(ms);
            table.mktsegment_lookup[ms] = code;
        }
        table.c_mktsegment.push_back(table.mktsegment_lookup[ms]);

        table.c_comment.push_back(fields[7]);

        row_count++;
    }
    in.close();

    std::cout << "  Total rows: " << row_count << std::endl;
    std::cout << "  Writing binary columns..." << std::endl;

    std::string dir = gendb_dir + "/customer";
    system(("mkdir -p " + dir).c_str());

    write_column_binary(dir + "/c_custkey.col", table.c_custkey);
    write_column_binary(dir + "/c_mktsegment.col", table.c_mktsegment);

    std::ofstream meta(dir + "/metadata.txt");
    meta << "row_count=" << row_count << "\n";
    meta << "mktsegment_dict=";
    for (const auto& s : table.mktsegment_dict) meta << s << ",";
    meta << "\n";
    meta.close();

    std::cout << "  Customer ingestion complete!" << std::endl;
}

// Load lineitem (only specified columns)
void load_lineitem(const std::string& gendb_dir, LineitemTable& table,
                   const std::vector<std::string>& columns_needed) {
    std::string dir = gendb_dir + "/lineitem";

    // Read metadata
    std::ifstream meta(dir + "/metadata.txt");
    std::string line;
    size_t row_count = 0;
    while (std::getline(meta, line)) {
        if (line.find("row_count=") == 0) {
            row_count = std::stoull(line.substr(10));
        } else if (line.find("returnflag_dict=") == 0) {
            std::string dict_str = line.substr(16);
            std::istringstream ss(dict_str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                if (!token.empty()) {
                    uint8_t code = table.returnflag_dict.size();
                    table.returnflag_dict.push_back(token);
                    table.returnflag_lookup[token] = code;
                }
            }
        } else if (line.find("linestatus_dict=") == 0) {
            std::string dict_str = line.substr(16);
            std::istringstream ss(dict_str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                if (!token.empty()) {
                    uint8_t code = table.linestatus_dict.size();
                    table.linestatus_dict.push_back(token);
                    table.linestatus_lookup[token] = code;
                }
            }
        }
    }
    meta.close();

    // mmap only needed columns
    for (const auto& col : columns_needed) {
        size_t count = 0;
        if (col == "l_orderkey") {
            const int32_t* data = mmap_column<int32_t>(dir + "/l_orderkey.col", count);
            table.l_orderkey.assign(data, data + count);
        } else if (col == "l_quantity") {
            const double* data = mmap_column<double>(dir + "/l_quantity.col", count);
            table.l_quantity.assign(data, data + count);
        } else if (col == "l_extendedprice") {
            const double* data = mmap_column<double>(dir + "/l_extendedprice.col", count);
            table.l_extendedprice.assign(data, data + count);
        } else if (col == "l_discount") {
            const double* data = mmap_column<double>(dir + "/l_discount.col", count);
            table.l_discount.assign(data, data + count);
        } else if (col == "l_tax") {
            const double* data = mmap_column<double>(dir + "/l_tax.col", count);
            table.l_tax.assign(data, data + count);
        } else if (col == "l_returnflag") {
            const uint8_t* data = mmap_column<uint8_t>(dir + "/l_returnflag.col", count);
            table.l_returnflag.assign(data, data + count);
        } else if (col == "l_linestatus") {
            const uint8_t* data = mmap_column<uint8_t>(dir + "/l_linestatus.col", count);
            table.l_linestatus.assign(data, data + count);
        } else if (col == "l_shipdate") {
            const int32_t* data = mmap_column<int32_t>(dir + "/l_shipdate.col", count);
            table.l_shipdate.assign(data, data + count);
        }
    }
}

// Load orders (only specified columns)
void load_orders(const std::string& gendb_dir, OrdersTable& table,
                 const std::vector<std::string>& columns_needed) {
    std::string dir = gendb_dir + "/orders";

    std::ifstream meta(dir + "/metadata.txt");
    std::string line;
    size_t row_count = 0;
    while (std::getline(meta, line)) {
        if (line.find("row_count=") == 0) {
            row_count = std::stoull(line.substr(10));
        }
    }
    meta.close();

    for (const auto& col : columns_needed) {
        size_t count = 0;
        if (col == "o_orderkey") {
            const int32_t* data = mmap_column<int32_t>(dir + "/o_orderkey.col", count);
            table.o_orderkey.assign(data, data + count);
        } else if (col == "o_custkey") {
            const int32_t* data = mmap_column<int32_t>(dir + "/o_custkey.col", count);
            table.o_custkey.assign(data, data + count);
        } else if (col == "o_orderdate") {
            const int32_t* data = mmap_column<int32_t>(dir + "/o_orderdate.col", count);
            table.o_orderdate.assign(data, data + count);
        } else if (col == "o_shippriority") {
            const int32_t* data = mmap_column<int32_t>(dir + "/o_shippriority.col", count);
            table.o_shippriority.assign(data, data + count);
        }
    }
}

// Load customer (only specified columns)
void load_customer(const std::string& gendb_dir, CustomerTable& table,
                   const std::vector<std::string>& columns_needed) {
    std::string dir = gendb_dir + "/customer";

    std::ifstream meta(dir + "/metadata.txt");
    std::string line;
    size_t row_count = 0;
    while (std::getline(meta, line)) {
        if (line.find("row_count=") == 0) {
            row_count = std::stoull(line.substr(10));
        } else if (line.find("mktsegment_dict=") == 0) {
            std::string dict_str = line.substr(16);
            std::istringstream ss(dict_str);
            std::string token;
            while (std::getline(ss, token, ',')) {
                if (!token.empty()) {
                    uint8_t code = table.mktsegment_dict.size();
                    table.mktsegment_dict.push_back(token);
                    table.mktsegment_lookup[token] = code;
                }
            }
        }
    }
    meta.close();

    for (const auto& col : columns_needed) {
        size_t count = 0;
        if (col == "c_custkey") {
            const int32_t* data = mmap_column<int32_t>(dir + "/c_custkey.col", count);
            table.c_custkey.assign(data, data + count);
        } else if (col == "c_mktsegment") {
            const uint8_t* data = mmap_column<uint8_t>(dir + "/c_mktsegment.col", count);
            table.c_mktsegment.assign(data, data + count);
        }
    }
}

} // namespace gendb
