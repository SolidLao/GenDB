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

void load_lineitem(const std::string& filepath, LineItem& table,
                   const std::unordered_set<std::string>& columns) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 60M rows)
    table.reserve(60000000);

    // Determine which columns to load (empty = load all)
    bool load_all = columns.empty();
    bool load_orderkey = load_all || columns.count("l_orderkey");
    bool load_partkey = load_all || columns.count("l_partkey");
    bool load_suppkey = load_all || columns.count("l_suppkey");
    bool load_linenumber = load_all || columns.count("l_linenumber");
    bool load_quantity = load_all || columns.count("l_quantity");
    bool load_extendedprice = load_all || columns.count("l_extendedprice");
    bool load_discount = load_all || columns.count("l_discount");
    bool load_tax = load_all || columns.count("l_tax");
    bool load_returnflag = load_all || columns.count("l_returnflag");
    bool load_linestatus = load_all || columns.count("l_linestatus");
    bool load_shipdate = load_all || columns.count("l_shipdate");
    bool load_commitdate = load_all || columns.count("l_commitdate");
    bool load_receiptdate = load_all || columns.count("l_receiptdate");
    bool load_shipinstruct = load_all || columns.count("l_shipinstruct");
    bool load_shipmode = load_all || columns.count("l_shipmode");
    bool load_comment = load_all || columns.count("l_comment");

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 16) continue;  // Skip malformed lines

        // Load only requested columns
        if (load_orderkey) table.l_orderkey.push_back(std::atoi(fields[0].c_str()));
        if (load_partkey) table.l_partkey.push_back(std::atoi(fields[1].c_str()));
        if (load_suppkey) table.l_suppkey.push_back(std::atoi(fields[2].c_str()));
        if (load_linenumber) table.l_linenumber.push_back(std::atoi(fields[3].c_str()));
        if (load_quantity) table.l_quantity.push_back(std::atof(fields[4].c_str()));
        if (load_extendedprice) table.l_extendedprice.push_back(std::atof(fields[5].c_str()));
        if (load_discount) table.l_discount.push_back(std::atof(fields[6].c_str()));
        if (load_tax) table.l_tax.push_back(std::atof(fields[7].c_str()));
        if (load_returnflag) table.l_returnflag.push_back(fields[8].empty() ? ' ' : fields[8][0]);
        if (load_linestatus) table.l_linestatus.push_back(fields[9].empty() ? ' ' : fields[9][0]);
        if (load_shipdate) table.l_shipdate.push_back(date_utils::parse_date(fields[10]));
        if (load_commitdate) table.l_commitdate.push_back(date_utils::parse_date(fields[11]));
        if (load_receiptdate) table.l_receiptdate.push_back(date_utils::parse_date(fields[12]));
        if (load_shipinstruct) table.l_shipinstruct.push_back(fields[13]);
        if (load_shipmode) table.l_shipmode.push_back(fields[14]);
        if (load_comment) table.l_comment.push_back(fields[15]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " lineitem rows";
    if (!load_all) {
        std::cout << " (" << columns.size() << " columns)";
    }
    std::cout << " from " << filepath << std::endl;
}

void load_orders(const std::string& filepath, Orders& table,
                 const std::unordered_set<std::string>& columns) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 15M rows)
    table.reserve(15000000);

    // Determine which columns to load (empty = load all)
    bool load_all = columns.empty();
    bool load_orderkey = load_all || columns.count("o_orderkey");
    bool load_custkey = load_all || columns.count("o_custkey");
    bool load_orderstatus = load_all || columns.count("o_orderstatus");
    bool load_totalprice = load_all || columns.count("o_totalprice");
    bool load_orderdate = load_all || columns.count("o_orderdate");
    bool load_orderpriority = load_all || columns.count("o_orderpriority");
    bool load_clerk = load_all || columns.count("o_clerk");
    bool load_shippriority = load_all || columns.count("o_shippriority");
    bool load_comment = load_all || columns.count("o_comment");

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 9) continue;  // Skip malformed lines

        if (load_orderkey) table.o_orderkey.push_back(std::atoi(fields[0].c_str()));
        if (load_custkey) table.o_custkey.push_back(std::atoi(fields[1].c_str()));
        if (load_orderstatus) table.o_orderstatus.push_back(fields[2].empty() ? ' ' : fields[2][0]);
        if (load_totalprice) table.o_totalprice.push_back(std::atof(fields[3].c_str()));
        if (load_orderdate) table.o_orderdate.push_back(date_utils::parse_date(fields[4]));
        if (load_orderpriority) table.o_orderpriority.push_back(fields[5]);
        if (load_clerk) table.o_clerk.push_back(fields[6]);
        if (load_shippriority) table.o_shippriority.push_back(std::atoi(fields[7].c_str()));
        if (load_comment) table.o_comment.push_back(fields[8]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " orders rows";
    if (!load_all) {
        std::cout << " (" << columns.size() << " columns)";
    }
    std::cout << " from " << filepath << std::endl;
}

void load_customer(const std::string& filepath, Customer& table,
                   const std::unordered_set<std::string>& columns) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        exit(1);
    }

    // Pre-allocate for SF=10 (approximately 1.5M rows)
    table.reserve(1500000);

    // Determine which columns to load (empty = load all)
    bool load_all = columns.empty();
    bool load_custkey = load_all || columns.count("c_custkey");
    bool load_name = load_all || columns.count("c_name");
    bool load_address = load_all || columns.count("c_address");
    bool load_nationkey = load_all || columns.count("c_nationkey");
    bool load_phone = load_all || columns.count("c_phone");
    bool load_acctbal = load_all || columns.count("c_acctbal");
    bool load_mktsegment = load_all || columns.count("c_mktsegment");
    bool load_comment = load_all || columns.count("c_comment");

    // Reserve space for dictionary (5 unique market segments)
    if (load_mktsegment) {
        table.c_mktsegment_dict.reserve(5);
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        auto fields = parse_line(line);
        if (fields.size() < 8) continue;  // Skip malformed lines

        if (load_custkey) table.c_custkey.push_back(std::atoi(fields[0].c_str()));
        if (load_name) table.c_name.push_back(fields[1]);
        if (load_address) table.c_address.push_back(fields[2]);
        if (load_nationkey) table.c_nationkey.push_back(std::atoi(fields[3].c_str()));
        if (load_phone) table.c_phone.push_back(fields[4]);
        if (load_acctbal) table.c_acctbal.push_back(std::atof(fields[5].c_str()));
        if (load_mktsegment) {
            uint32_t code = table.c_mktsegment_dict.encode(fields[6]);
            table.c_mktsegment_code.push_back(static_cast<uint8_t>(code));
        }
        if (load_comment) table.c_comment.push_back(fields[7]);
    }

    file.close();
    std::cout << "Loaded " << table.size() << " customer rows";
    if (!load_all) {
        std::cout << " (" << columns.size() << " columns)";
    }
    if (load_mktsegment) {
        std::cout << " [c_mktsegment: " << table.c_mktsegment_dict.size() << " unique values]";
    }
    std::cout << " from " << filepath << std::endl;
}
