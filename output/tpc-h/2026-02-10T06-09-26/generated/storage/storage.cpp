#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstring>

namespace gendb {

// Helper to split pipe-delimited line
static std::vector<std::string> split_line(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;

    while (std::getline(ss, field, '|')) {
        fields.push_back(field);
    }

    return fields;
}

void load_lineitem(const std::string& filepath, LineitemTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    std::string line;
    size_t count = 0;

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
    table.l_shipinstruct.reserve(60000000);
    table.l_shipmode.reserve(60000000);
    table.l_comment.reserve(60000000);

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split_line(line);
        if (fields.size() < 16) continue;

        table.l_orderkey.push_back(std::stoi(fields[0]));
        table.l_partkey.push_back(std::stoi(fields[1]));
        table.l_suppkey.push_back(std::stoi(fields[2]));
        table.l_linenumber.push_back(std::stoi(fields[3]));
        table.l_quantity.push_back(std::stod(fields[4]));
        table.l_extendedprice.push_back(std::stod(fields[5]));
        table.l_discount.push_back(std::stod(fields[6]));
        table.l_tax.push_back(std::stod(fields[7]));
        table.l_returnflag.push_back(fields[8]);
        table.l_linestatus.push_back(fields[9]);
        table.l_shipdate.push_back(parse_date(fields[10]));
        table.l_commitdate.push_back(parse_date(fields[11]));
        table.l_receiptdate.push_back(parse_date(fields[12]));
        table.l_shipinstruct.push_back(fields[13]);
        table.l_shipmode.push_back(fields[14]);
        table.l_comment.push_back(fields[15]);

        count++;
        if (count % 1000000 == 0) {
            std::cerr << "Loaded " << count << " lineitem rows...\r" << std::flush;
        }
    }

    std::cerr << std::endl;
}

void load_orders(const std::string& filepath, OrdersTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    std::string line;
    size_t count = 0;

    // Reserve space (SF10 has ~15M orders)
    table.o_orderkey.reserve(15000000);
    table.o_custkey.reserve(15000000);
    table.o_orderstatus.reserve(15000000);
    table.o_totalprice.reserve(15000000);
    table.o_orderdate.reserve(15000000);
    table.o_orderpriority.reserve(15000000);
    table.o_clerk.reserve(15000000);
    table.o_shippriority.reserve(15000000);
    table.o_comment.reserve(15000000);

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split_line(line);
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

        count++;
        if (count % 1000000 == 0) {
            std::cerr << "Loaded " << count << " orders rows...\r" << std::flush;
        }
    }

    std::cerr << std::endl;
}

void load_customer(const std::string& filepath, CustomerTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    std::string line;
    size_t count = 0;

    // Reserve space (SF10 has ~1.5M customers)
    table.c_custkey.reserve(1500000);
    table.c_name.reserve(1500000);
    table.c_address.reserve(1500000);
    table.c_nationkey.reserve(1500000);
    table.c_phone.reserve(1500000);
    table.c_acctbal.reserve(1500000);
    table.c_mktsegment.reserve(1500000);
    table.c_comment.reserve(1500000);

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split_line(line);
        if (fields.size() < 8) continue;

        table.c_custkey.push_back(std::stoi(fields[0]));
        table.c_name.push_back(fields[1]);
        table.c_address.push_back(fields[2]);
        table.c_nationkey.push_back(std::stoi(fields[3]));
        table.c_phone.push_back(fields[4]);
        table.c_acctbal.push_back(std::stod(fields[5]));
        table.c_mktsegment.push_back(fields[6]);
        table.c_comment.push_back(fields[7]);

        count++;
        if (count % 100000 == 0) {
            std::cerr << "Loaded " << count << " customer rows...\r" << std::flush;
        }
    }

    std::cerr << std::endl;
}

} // namespace gendb
