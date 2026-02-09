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

// Load lineitem.tbl with lazy column loading (skip unused columns)
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
        parse_field(iss); // SKIP l_partkey
        parse_field(iss); // SKIP l_suppkey
        parse_field(iss); // SKIP l_linenumber
        table.l_quantity.push_back(std::stod(parse_field(iss)));
        table.l_extendedprice.push_back(std::stod(parse_field(iss)));
        table.l_discount.push_back(std::stod(parse_field(iss)));
        table.l_tax.push_back(std::stod(parse_field(iss)));
        table.l_returnflag.push_back(parse_field(iss));
        table.l_linestatus.push_back(parse_field(iss));
        table.l_shipdate.push_back(parse_date(parse_field(iss)));
        parse_field(iss); // SKIP l_commitdate
        parse_field(iss); // SKIP l_receiptdate
        parse_field(iss); // SKIP l_shipinstruct
        parse_field(iss); // SKIP l_shipmode
        parse_field(iss); // SKIP l_comment
        // Trailing pipe is ignored by getline
    }

    file.close();
}

// Load customer.tbl with lazy column loading (skip unused columns)
// Build pre-built hash index on c_custkey during loading
void load_customer(const std::string& filepath, CustomerTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file " << filepath << std::endl;
        std::exit(1);
    }

    std::string line;
    size_t row_idx = 0;
    while (std::getline(file, line)) {
        std::istringstream iss(line);

        int32_t custkey = std::stoi(parse_field(iss));
        table.c_custkey.push_back(custkey);
        parse_field(iss); // SKIP c_name
        parse_field(iss); // SKIP c_address
        parse_field(iss); // SKIP c_nationkey
        parse_field(iss); // SKIP c_phone
        parse_field(iss); // SKIP c_acctbal
        table.c_mktsegment.push_back(parse_field(iss));
        parse_field(iss); // SKIP c_comment

        // Build pre-built hash index
        table.c_custkey_index[custkey] = row_idx;
        row_idx++;
        // Trailing pipe is ignored by getline
    }

    file.close();
}

// Load orders.tbl with lazy column loading (skip unused columns)
// Build pre-built hash indexes on o_orderkey and o_custkey during loading
void load_orders(const std::string& filepath, OrdersTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file " << filepath << std::endl;
        std::exit(1);
    }

    std::string line;
    size_t row_idx = 0;
    while (std::getline(file, line)) {
        std::istringstream iss(line);

        int32_t orderkey = std::stoi(parse_field(iss));
        int32_t custkey = std::stoi(parse_field(iss));
        table.o_orderkey.push_back(orderkey);
        table.o_custkey.push_back(custkey);
        parse_field(iss); // SKIP o_orderstatus
        parse_field(iss); // SKIP o_totalprice
        table.o_orderdate.push_back(parse_date(parse_field(iss)));
        parse_field(iss); // SKIP o_orderpriority
        parse_field(iss); // SKIP o_clerk
        table.o_shippriority.push_back(std::stoi(parse_field(iss)));
        parse_field(iss); // SKIP o_comment

        // Build pre-built hash indexes
        table.o_orderkey_index[orderkey] = row_idx;
        table.o_custkey_index.insert({custkey, row_idx});
        row_idx++;
        // Trailing pipe is ignored by getline
    }

    file.close();
}
