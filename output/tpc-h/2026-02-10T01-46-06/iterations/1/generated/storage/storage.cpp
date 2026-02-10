#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstdlib>

// Helper function to parse pipe-delimited line
std::vector<std::string> parse_line(const std::string& line) {
    std::vector<std::string> fields;
    std::string field;
    size_t start = 0;
    size_t pos = 0;

    while ((pos = line.find('|', start)) != std::string::npos) {
        field = line.substr(start, pos - start);
        fields.push_back(field);
        start = pos + 1;
    }

    // Handle trailing pipe (TPC-H .tbl files have a trailing '|')
    // The last field after the final '|' is empty, so we're done

    return fields;
}

void load_lineitem(const std::string& filepath, LineItem& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 60M rows)
    table.reserve(60000000);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 16) continue;  // Skip malformed lines

        table.l_orderkey.push_back(std::atoi(fields[0].c_str()));
        table.l_partkey.push_back(std::atoi(fields[1].c_str()));
        table.l_suppkey.push_back(std::atoi(fields[2].c_str()));
        table.l_linenumber.push_back(std::atoi(fields[3].c_str()));
        table.l_quantity.push_back(std::atof(fields[4].c_str()));
        table.l_extendedprice.push_back(std::atof(fields[5].c_str()));
        table.l_discount.push_back(std::atof(fields[6].c_str()));
        table.l_tax.push_back(std::atof(fields[7].c_str()));
        table.l_returnflag.push_back(fields[8].empty() ? ' ' : fields[8][0]);
        table.l_linestatus.push_back(fields[9].empty() ? ' ' : fields[9][0]);
        table.l_shipdate.push_back(date_utils::parse_date(fields[10]));
        table.l_commitdate.push_back(date_utils::parse_date(fields[11]));
        table.l_receiptdate.push_back(date_utils::parse_date(fields[12]));
        table.l_shipinstruct.push_back(fields[13]);
        table.l_shipmode.push_back(fields[14]);
        table.l_comment.push_back(fields[15]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " lineitem rows from " << filepath << std::endl;
}

void load_orders(const std::string& filepath, Orders& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 15M rows)
    table.reserve(15000000);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 9) continue;  // Skip malformed lines

        table.o_orderkey.push_back(std::atoi(fields[0].c_str()));
        table.o_custkey.push_back(std::atoi(fields[1].c_str()));
        table.o_orderstatus.push_back(fields[2].empty() ? ' ' : fields[2][0]);
        table.o_totalprice.push_back(std::atof(fields[3].c_str()));
        table.o_orderdate.push_back(date_utils::parse_date(fields[4]));
        table.o_orderpriority.push_back(fields[5]);
        table.o_clerk.push_back(fields[6]);
        table.o_shippriority.push_back(std::atoi(fields[7].c_str()));
        table.o_comment.push_back(fields[8]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " orders rows from " << filepath << std::endl;
}

void load_customer(const std::string& filepath, Customer& table) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 1.5M rows)
    table.reserve(1500000);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 8) continue;  // Skip malformed lines

        table.c_custkey.push_back(std::atoi(fields[0].c_str()));
        table.c_name.push_back(fields[1]);
        table.c_address.push_back(fields[2]);
        table.c_nationkey.push_back(std::atoi(fields[3].c_str()));
        table.c_phone.push_back(fields[4]);
        table.c_acctbal.push_back(std::atof(fields[5].c_str()));
        table.c_mktsegment.push_back(fields[6]);
        table.c_comment.push_back(fields[7]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " customer rows from " << filepath << std::endl;
}
