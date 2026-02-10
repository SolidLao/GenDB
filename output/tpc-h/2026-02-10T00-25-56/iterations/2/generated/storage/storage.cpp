#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>

// Helper: Parse pipe-delimited line
static std::vector<std::string> split(const std::string& line, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(line);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

void load_lineitem(const std::string& filepath, LineitemTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open " << filepath << std::endl;
        return;
    }

    // Pre-allocate vectors for ~6M rows (SF=1)
    constexpr size_t ESTIMATED_ROWS = 6000000;
    table.l_orderkey.reserve(ESTIMATED_ROWS);
    table.l_partkey.reserve(ESTIMATED_ROWS);
    table.l_suppkey.reserve(ESTIMATED_ROWS);
    table.l_linenumber.reserve(ESTIMATED_ROWS);
    table.l_quantity.reserve(ESTIMATED_ROWS);
    table.l_extendedprice.reserve(ESTIMATED_ROWS);
    table.l_discount.reserve(ESTIMATED_ROWS);
    table.l_tax.reserve(ESTIMATED_ROWS);
    table.l_returnflag.reserve(ESTIMATED_ROWS);
    table.l_linestatus.reserve(ESTIMATED_ROWS);
    table.l_returnflag_code.reserve(ESTIMATED_ROWS);
    table.l_linestatus_code.reserve(ESTIMATED_ROWS);
    table.l_shipdate.reserve(ESTIMATED_ROWS);
    table.l_commitdate.reserve(ESTIMATED_ROWS);
    table.l_receiptdate.reserve(ESTIMATED_ROWS);
    table.l_shipinstruct.reserve(ESTIMATED_ROWS);
    table.l_shipmode.reserve(ESTIMATED_ROWS);
    table.l_comment.reserve(ESTIMATED_ROWS);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 16) continue;  // Skip incomplete rows

        table.l_orderkey.push_back(std::stoi(fields[0]));
        table.l_partkey.push_back(std::stoi(fields[1]));
        table.l_suppkey.push_back(std::stoi(fields[2]));
        table.l_linenumber.push_back(std::stoi(fields[3]));
        table.l_quantity.push_back(std::stod(fields[4]));
        table.l_extendedprice.push_back(std::stod(fields[5]));
        table.l_discount.push_back(std::stod(fields[6]));
        table.l_tax.push_back(std::stod(fields[7]));

        char returnflag = fields[8].empty() ? ' ' : fields[8][0];
        char linestatus = fields[9].empty() ? ' ' : fields[9][0];
        table.l_returnflag.push_back(returnflag);
        table.l_linestatus.push_back(linestatus);

        // Dictionary encoding: returnflag {N:0, R:1, A:2}, linestatus {O:0, F:1}
        uint8_t returnflag_code = (returnflag == 'N') ? 0 : (returnflag == 'R') ? 1 : 2;
        uint8_t linestatus_code = (linestatus == 'O') ? 0 : 1;
        table.l_returnflag_code.push_back(returnflag_code);
        table.l_linestatus_code.push_back(linestatus_code);

        table.l_shipdate.push_back(date_utils::parse_date(fields[10]));
        table.l_commitdate.push_back(date_utils::parse_date(fields[11]));
        table.l_receiptdate.push_back(date_utils::parse_date(fields[12]));
        table.l_shipinstruct.push_back(fields[13]);
        table.l_shipmode.push_back(fields[14]);
        table.l_comment.push_back(fields[15]);
    }

    file.close();
}

void load_orders(const std::string& filepath, OrdersTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open " << filepath << std::endl;
        return;
    }

    // Pre-allocate vectors for ~1.5M rows (SF=1)
    constexpr size_t ESTIMATED_ROWS = 1500000;
    table.o_orderkey.reserve(ESTIMATED_ROWS);
    table.o_custkey.reserve(ESTIMATED_ROWS);
    table.o_orderstatus.reserve(ESTIMATED_ROWS);
    table.o_totalprice.reserve(ESTIMATED_ROWS);
    table.o_orderdate.reserve(ESTIMATED_ROWS);
    table.o_orderpriority.reserve(ESTIMATED_ROWS);
    table.o_clerk.reserve(ESTIMATED_ROWS);
    table.o_shippriority.reserve(ESTIMATED_ROWS);
    table.o_comment.reserve(ESTIMATED_ROWS);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 9) continue;

        table.o_orderkey.push_back(std::stoi(fields[0]));
        table.o_custkey.push_back(std::stoi(fields[1]));
        table.o_orderstatus.push_back(fields[2].empty() ? ' ' : fields[2][0]);
        table.o_totalprice.push_back(std::stod(fields[3]));
        table.o_orderdate.push_back(date_utils::parse_date(fields[4]));
        table.o_orderpriority.push_back(fields[5]);
        table.o_clerk.push_back(fields[6]);
        table.o_shippriority.push_back(std::stoi(fields[7]));
        table.o_comment.push_back(fields[8]);
    }

    file.close();
}

void load_customer(const std::string& filepath, CustomerTable& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open " << filepath << std::endl;
        return;
    }

    // Pre-allocate vectors for ~150K rows (SF=1)
    constexpr size_t ESTIMATED_ROWS = 150000;
    table.c_custkey.reserve(ESTIMATED_ROWS);
    table.c_name.reserve(ESTIMATED_ROWS);
    table.c_address.reserve(ESTIMATED_ROWS);
    table.c_nationkey.reserve(ESTIMATED_ROWS);
    table.c_phone.reserve(ESTIMATED_ROWS);
    table.c_acctbal.reserve(ESTIMATED_ROWS);
    table.c_mktsegment.reserve(ESTIMATED_ROWS);
    table.c_mktsegment_code.reserve(ESTIMATED_ROWS);
    table.c_comment.reserve(ESTIMATED_ROWS);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = split(line, '|');
        if (fields.size() < 8) continue;

        table.c_custkey.push_back(std::stoi(fields[0]));
        table.c_name.push_back(fields[1]);
        table.c_address.push_back(fields[2]);
        table.c_nationkey.push_back(std::stoi(fields[3]));
        table.c_phone.push_back(fields[4]);
        table.c_acctbal.push_back(std::stod(fields[5]));

        const std::string& mktsegment = fields[6];
        table.c_mktsegment.push_back(mktsegment);

        // Dictionary encoding: {AUTOMOBILE:0, BUILDING:1, FURNITURE:2, HOUSEHOLD:3, MACHINERY:4}
        uint8_t mktsegment_code = 0;
        if (mktsegment == "BUILDING") mktsegment_code = 1;
        else if (mktsegment == "FURNITURE") mktsegment_code = 2;
        else if (mktsegment == "HOUSEHOLD") mktsegment_code = 3;
        else if (mktsegment == "MACHINERY") mktsegment_code = 4;
        table.c_mktsegment_code.push_back(mktsegment_code);

        table.c_comment.push_back(fields[7]);
    }

    file.close();
}
