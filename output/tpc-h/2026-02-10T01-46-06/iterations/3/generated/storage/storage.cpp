#include "storage.h"
#include "../utils/date_utils.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstdlib>
#include <unordered_set>

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

void load_lineitem(const std::string& filepath, LineItem& table,
                   const std::unordered_set<std::string>& columns_to_load) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 60M rows)
    // Only allocate for columns we're actually loading
    if (columns_to_load.count("l_orderkey")) table.l_orderkey.reserve(60000000);
    if (columns_to_load.count("l_partkey")) table.l_partkey.reserve(60000000);
    if (columns_to_load.count("l_suppkey")) table.l_suppkey.reserve(60000000);
    if (columns_to_load.count("l_linenumber")) table.l_linenumber.reserve(60000000);
    if (columns_to_load.count("l_quantity")) table.l_quantity.reserve(60000000);
    if (columns_to_load.count("l_extendedprice")) table.l_extendedprice.reserve(60000000);
    if (columns_to_load.count("l_discount")) table.l_discount.reserve(60000000);
    if (columns_to_load.count("l_tax")) table.l_tax.reserve(60000000);
    if (columns_to_load.count("l_returnflag")) table.l_returnflag.reserve(60000000);
    if (columns_to_load.count("l_linestatus")) table.l_linestatus.reserve(60000000);
    if (columns_to_load.count("l_shipdate")) table.l_shipdate.reserve(60000000);
    if (columns_to_load.count("l_commitdate")) table.l_commitdate.reserve(60000000);
    if (columns_to_load.count("l_receiptdate")) table.l_receiptdate.reserve(60000000);
    if (columns_to_load.count("l_shipinstruct")) table.l_shipinstruct.reserve(60000000);
    if (columns_to_load.count("l_shipmode")) table.l_shipmode.reserve(60000000);
    if (columns_to_load.count("l_comment")) table.l_comment.reserve(60000000);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 16) continue;  // Skip malformed lines

        // Only load columns that are requested
        if (columns_to_load.count("l_orderkey")) table.l_orderkey.push_back(std::atoi(fields[0].c_str()));
        if (columns_to_load.count("l_partkey")) table.l_partkey.push_back(std::atoi(fields[1].c_str()));
        if (columns_to_load.count("l_suppkey")) table.l_suppkey.push_back(std::atoi(fields[2].c_str()));
        if (columns_to_load.count("l_linenumber")) table.l_linenumber.push_back(std::atoi(fields[3].c_str()));
        if (columns_to_load.count("l_quantity")) table.l_quantity.push_back(std::atof(fields[4].c_str()));
        if (columns_to_load.count("l_extendedprice")) table.l_extendedprice.push_back(std::atof(fields[5].c_str()));
        if (columns_to_load.count("l_discount")) table.l_discount.push_back(std::atof(fields[6].c_str()));
        if (columns_to_load.count("l_tax")) table.l_tax.push_back(std::atof(fields[7].c_str()));
        if (columns_to_load.count("l_returnflag")) table.l_returnflag.push_back(fields[8].empty() ? ' ' : fields[8][0]);
        if (columns_to_load.count("l_linestatus")) table.l_linestatus.push_back(fields[9].empty() ? ' ' : fields[9][0]);
        if (columns_to_load.count("l_shipdate")) table.l_shipdate.push_back(date_utils::parse_date(fields[10]));
        if (columns_to_load.count("l_commitdate")) table.l_commitdate.push_back(date_utils::parse_date(fields[11]));
        if (columns_to_load.count("l_receiptdate")) table.l_receiptdate.push_back(date_utils::parse_date(fields[12]));
        if (columns_to_load.count("l_shipinstruct")) table.l_shipinstruct.push_back(fields[13]);
        if (columns_to_load.count("l_shipmode")) table.l_shipmode.push_back(fields[14]);
        if (columns_to_load.count("l_comment")) table.l_comment.push_back(fields[15]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " lineitem rows from " << filepath << std::endl;
}

void load_orders(const std::string& filepath, Orders& table,
                 const std::unordered_set<std::string>& columns_to_load) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 15M rows)
    // Only allocate for columns we're actually loading
    if (columns_to_load.count("o_orderkey")) table.o_orderkey.reserve(15000000);
    if (columns_to_load.count("o_custkey")) table.o_custkey.reserve(15000000);
    if (columns_to_load.count("o_orderstatus")) table.o_orderstatus.reserve(15000000);
    if (columns_to_load.count("o_totalprice")) table.o_totalprice.reserve(15000000);
    if (columns_to_load.count("o_orderdate")) table.o_orderdate.reserve(15000000);
    if (columns_to_load.count("o_orderpriority")) table.o_orderpriority.reserve(15000000);
    if (columns_to_load.count("o_clerk")) table.o_clerk.reserve(15000000);
    if (columns_to_load.count("o_shippriority")) table.o_shippriority.reserve(15000000);
    if (columns_to_load.count("o_comment")) table.o_comment.reserve(15000000);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 9) continue;  // Skip malformed lines

        // Only load columns that are requested
        if (columns_to_load.count("o_orderkey")) table.o_orderkey.push_back(std::atoi(fields[0].c_str()));
        if (columns_to_load.count("o_custkey")) table.o_custkey.push_back(std::atoi(fields[1].c_str()));
        if (columns_to_load.count("o_orderstatus")) table.o_orderstatus.push_back(fields[2].empty() ? ' ' : fields[2][0]);
        if (columns_to_load.count("o_totalprice")) table.o_totalprice.push_back(std::atof(fields[3].c_str()));
        if (columns_to_load.count("o_orderdate")) table.o_orderdate.push_back(date_utils::parse_date(fields[4]));
        if (columns_to_load.count("o_orderpriority")) table.o_orderpriority.push_back(fields[5]);
        if (columns_to_load.count("o_clerk")) table.o_clerk.push_back(fields[6]);
        if (columns_to_load.count("o_shippriority")) table.o_shippriority.push_back(std::atoi(fields[7].c_str()));
        if (columns_to_load.count("o_comment")) table.o_comment.push_back(fields[8]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " orders rows from " << filepath << std::endl;
}

void load_customer(const std::string& filepath, Customer& table,
                   const std::unordered_set<std::string>& columns_to_load,
                   StringPool* string_pool) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 1.5M rows)
    // Only allocate for columns we're actually loading
    if (columns_to_load.count("c_custkey")) table.c_custkey.reserve(1500000);
    if (columns_to_load.count("c_name")) table.c_name.reserve(1500000);
    if (columns_to_load.count("c_address")) table.c_address.reserve(1500000);
    if (columns_to_load.count("c_nationkey")) table.c_nationkey.reserve(1500000);
    if (columns_to_load.count("c_phone")) table.c_phone.reserve(1500000);
    if (columns_to_load.count("c_acctbal")) table.c_acctbal.reserve(1500000);
    if (columns_to_load.count("c_mktsegment")) table.c_mktsegment.reserve(1500000);
    if (columns_to_load.count("c_comment")) table.c_comment.reserve(1500000);

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 8) continue;  // Skip malformed lines

        // Only load columns that are requested
        if (columns_to_load.count("c_custkey")) table.c_custkey.push_back(std::atoi(fields[0].c_str()));
        if (columns_to_load.count("c_name")) table.c_name.push_back(fields[1]);
        if (columns_to_load.count("c_address")) table.c_address.push_back(fields[2]);
        if (columns_to_load.count("c_nationkey")) table.c_nationkey.push_back(std::atoi(fields[3].c_str()));
        if (columns_to_load.count("c_phone")) table.c_phone.push_back(fields[4]);
        if (columns_to_load.count("c_acctbal")) table.c_acctbal.push_back(std::atof(fields[5].c_str()));

        // Use string interning for c_mktsegment if string_pool is provided
        if (columns_to_load.count("c_mktsegment")) {
            if (string_pool) {
                // Intern the string and store pointer (using std::string to hold pointer address)
                const char* interned = string_pool->intern(fields[6]);
                table.c_mktsegment.push_back(std::string(interned));
            } else {
                table.c_mktsegment.push_back(fields[6]);
            }
        }

        if (columns_to_load.count("c_comment")) table.c_comment.push_back(fields[7]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " customer rows from " << filepath << std::endl;
}
