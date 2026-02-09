#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstdlib>

// Parse a pipe-delimited field
inline std::string parse_field(std::istringstream& iss) {
    std::string field;
    std::getline(iss, field, '|');
    return field;
}

// Load lineitem.tbl
void load_lineitem(const std::string& filepath, LineitemTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file " << filepath << std::endl;
        std::exit(1);
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);

        table.l_orderkey.push_back(std::stoi(parse_field(iss)));
        table.l_partkey.push_back(std::stoi(parse_field(iss)));
        table.l_suppkey.push_back(std::stoi(parse_field(iss)));
        table.l_linenumber.push_back(std::stoi(parse_field(iss)));
        table.l_quantity.push_back(std::stod(parse_field(iss)));
        table.l_extendedprice.push_back(std::stod(parse_field(iss)));
        table.l_discount.push_back(std::stod(parse_field(iss)));
        table.l_tax.push_back(std::stod(parse_field(iss)));
        table.l_returnflag.push_back(parse_field(iss));
        table.l_linestatus.push_back(parse_field(iss));
        table.l_shipdate.push_back(parse_date(parse_field(iss)));
        table.l_commitdate.push_back(parse_date(parse_field(iss)));
        table.l_receiptdate.push_back(parse_date(parse_field(iss)));
        table.l_shipinstruct.push_back(parse_field(iss));
        table.l_shipmode.push_back(parse_field(iss));
        table.l_comment.push_back(parse_field(iss));
        // Trailing pipe is ignored by getline
    }

    file.close();
}

// Load customer.tbl
void load_customer(const std::string& filepath, CustomerTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file " << filepath << std::endl;
        std::exit(1);
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);

        table.c_custkey.push_back(std::stoi(parse_field(iss)));
        table.c_name.push_back(parse_field(iss));
        table.c_address.push_back(parse_field(iss));
        table.c_nationkey.push_back(std::stoi(parse_field(iss)));
        table.c_phone.push_back(parse_field(iss));
        table.c_acctbal.push_back(std::stod(parse_field(iss)));
        table.c_mktsegment.push_back(parse_field(iss));
        table.c_comment.push_back(parse_field(iss));
        // Trailing pipe is ignored by getline
    }

    file.close();
}

// Load orders.tbl
void load_orders(const std::string& filepath, OrdersTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file " << filepath << std::endl;
        std::exit(1);
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);

        table.o_orderkey.push_back(std::stoi(parse_field(iss)));
        table.o_custkey.push_back(std::stoi(parse_field(iss)));
        table.o_orderstatus.push_back(parse_field(iss));
        table.o_totalprice.push_back(std::stod(parse_field(iss)));
        table.o_orderdate.push_back(parse_date(parse_field(iss)));
        table.o_orderpriority.push_back(parse_field(iss));
        table.o_clerk.push_back(parse_field(iss));
        table.o_shippriority.push_back(std::stoi(parse_field(iss)));
        table.o_comment.push_back(parse_field(iss));
        // Trailing pipe is ignored by getline
    }

    file.close();
}
