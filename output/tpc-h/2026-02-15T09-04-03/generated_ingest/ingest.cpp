#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <filesystem>
#include <thread>
#include <mutex>
#include <charconv>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <ctime>

namespace fs = std::filesystem;

// Date parsing: YYYY-MM-DD to days since epoch (1970-01-01)
int32_t parseDate(const std::string& dateStr) {
    if (dateStr.empty() || dateStr.length() < 10) return 0;

    int year = std::stoi(dateStr.substr(0, 4));
    int month = std::stoi(dateStr.substr(5, 2));
    int day = std::stoi(dateStr.substr(8, 2));

    // Days per month (non-leap year)
    int daysInMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    // Adjust for leap years
    auto isLeapYear = [](int y) { return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0); };

    int32_t totalDays = 0;

    // Days for years 1970 to year-1
    for (int y = 1970; y < year; y++) {
        totalDays += isLeapYear(y) ? 366 : 365;
    }

    // Days for months in current year
    for (int m = 1; m < month; m++) {
        totalDays += daysInMonth[m - 1];
        if (m == 2 && isLeapYear(year)) totalDays++;
    }

    // Days in current month
    totalDays += day - 1;  // -1 because Jan 1 is day 0

    return totalDays;
}

// Decimal parsing: string to int64_t with scale_factor
int64_t parseDecimal(const std::string& decimalStr, int scale) {
    if (decimalStr.empty()) return 0;

    double value = std::stod(decimalStr);
    return static_cast<int64_t>(std::round(value * scale));
}

// Split line by delimiter
std::vector<std::string> splitLine(const std::string& line, char delim) {
    std::vector<std::string> parts;
    std::istringstream iss(line);
    std::string part;
    while (std::getline(iss, part, delim)) {
        parts.push_back(part);
    }
    return parts;
}

// Global dictionary builders for low-cardinality columns
std::mutex dictMutex;
std::unordered_map<std::string, uint8_t> returnflagDict;
std::unordered_map<std::string, uint8_t> linestatusDict;
std::unordered_map<std::string, uint8_t> orderstatus_dict;
std::unordered_map<std::string, uint8_t> mktsegment_dict;

uint8_t getDictCode(const std::string& value, std::unordered_map<std::string, uint8_t>& dict, const std::string& dictName) {
    std::lock_guard<std::mutex> lock(dictMutex);
    if (dict.find(value) == dict.end()) {
        dict[value] = (uint8_t)dict.size();
    }
    return dict[value];
}

struct LineitemRow {
    int32_t l_orderkey;
    int32_t l_partkey;
    int32_t l_suppkey;
    int32_t l_linenumber;
    int64_t l_quantity;
    int64_t l_extendedprice;
    int64_t l_discount;
    int64_t l_tax;
    uint8_t l_returnflag;
    uint8_t l_linestatus;
    int32_t l_shipdate;
    int32_t l_commitdate;
    int32_t l_receiptdate;
    std::string l_shipinstruct;
    std::string l_comment;
};

struct OrdersRow {
    int32_t o_orderkey;
    int32_t o_custkey;
    uint8_t o_orderstatus;
    int64_t o_totalprice;
    int32_t o_orderdate;
    std::string o_orderpriority;
    std::string o_clerk;
    int32_t o_shippriority;
    std::string o_comment;
};

struct CustomerRow {
    int32_t c_custkey;
    std::string c_name;
    std::string c_address;
    int32_t c_nationkey;
    std::string c_phone;
    int64_t c_acctbal;
    uint8_t c_mktsegment;
    std::string c_comment;
};

struct PartRow {
    int32_t p_partkey;
    std::string p_name;
    std::string p_mfgr;
    std::string p_brand;
    std::string p_type;
    int32_t p_size;
    std::string p_container;
    int64_t p_retailprice;
    std::string p_comment;
};

struct PartsuppRow {
    int32_t ps_partkey;
    int32_t ps_suppkey;
    int32_t ps_availqty;
    int64_t ps_supplycost;
    std::string ps_comment;
};

struct SupplierRow {
    int32_t s_suppkey;
    std::string s_name;
    std::string s_address;
    int32_t s_nationkey;
    std::string s_phone;
    int64_t s_acctbal;
    std::string s_comment;
};

struct NationRow {
    int32_t n_nationkey;
    std::string n_name;
    int32_t n_regionkey;
    std::string n_comment;
};

struct RegionRow {
    int32_t r_regionkey;
    std::string r_name;
    std::string r_comment;
};

// Write binary column files
void writeIntColumn(const std::string& path, const std::vector<int32_t>& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int32_t));
    out.close();
}

void writeInt64Column(const std::string& path, const std::vector<int64_t>& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(int64_t));
    out.close();
}

void writeUint8Column(const std::string& path, const std::vector<uint8_t>& data) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(uint8_t));
    out.close();
}

void writeStringColumn(const std::string& path, const std::vector<std::string>& data) {
    std::ofstream out(path, std::ios::binary);
    uint32_t count = data.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));

    for (const auto& str : data) {
        uint32_t len = str.length();
        out.write(reinterpret_cast<const char*>(&len), sizeof(uint32_t));
        out.write(str.data(), len);
    }
    out.close();
}

void writeDictionary(const std::string& path, const std::unordered_map<std::string, uint8_t>& dict) {
    std::ofstream out(path);
    for (const auto& [value, code] : dict) {
        out << (int)code << "=" << value << "\n";
    }
    out.close();
}

void ingestLineitem(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    std::string filePath = dataDir + "/lineitem.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_shipinstruct, l_comment;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 15) continue;

        l_orderkey.push_back(std::stoi(parts[0]));
        l_partkey.push_back(std::stoi(parts[1]));
        l_suppkey.push_back(std::stoi(parts[2]));
        l_linenumber.push_back(std::stoi(parts[3]));
        l_quantity.push_back(parseDecimal(parts[4], 100));
        l_extendedprice.push_back(parseDecimal(parts[5], 100));
        l_discount.push_back(parseDecimal(parts[6], 100));
        l_tax.push_back(parseDecimal(parts[7], 100));
        l_returnflag.push_back(getDictCode(parts[8], returnflagDict, "returnflag"));
        l_linestatus.push_back(getDictCode(parts[9], linestatusDict, "linestatus"));
        l_shipdate.push_back(parseDate(parts[10]));
        l_commitdate.push_back(parseDate(parts[11]));
        l_receiptdate.push_back(parseDate(parts[12]));
        l_shipinstruct.push_back(parts[13]);
        l_comment.push_back(parts[14]);

        lineCount++;
        if (lineCount % 5000000 == 0) {
            std::cout << "  Parsed " << lineCount << " lineitem rows" << std::endl;
        }
    }
    inFile.close();

    std::cout << "  Total lineitem rows: " << lineCount << std::endl;

    // Sort by l_shipdate (permutation-based)
    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return l_shipdate[i] < l_shipdate[j];
    });

    // Create sorted columns
    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(l_orderkey);
    sort_col(l_partkey);
    sort_col(l_suppkey);
    sort_col(l_linenumber);
    sort_col(l_quantity);
    sort_col(l_extendedprice);
    sort_col(l_discount);
    sort_col(l_tax);
    sort_col(l_returnflag);
    sort_col(l_linestatus);
    sort_col(l_shipdate);
    sort_col(l_commitdate);
    sort_col(l_receiptdate);
    sort_col(l_shipinstruct);
    sort_col(l_comment);

    // Write columns
    std::string tableDir = outDir + "/lineitem";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/l_orderkey.bin", l_orderkey);
    writeIntColumn(tableDir + "/l_partkey.bin", l_partkey);
    writeIntColumn(tableDir + "/l_suppkey.bin", l_suppkey);
    writeIntColumn(tableDir + "/l_linenumber.bin", l_linenumber);
    writeInt64Column(tableDir + "/l_quantity.bin", l_quantity);
    writeInt64Column(tableDir + "/l_extendedprice.bin", l_extendedprice);
    writeInt64Column(tableDir + "/l_discount.bin", l_discount);
    writeInt64Column(tableDir + "/l_tax.bin", l_tax);
    writeUint8Column(tableDir + "/l_returnflag.bin", l_returnflag);
    writeUint8Column(tableDir + "/l_linestatus.bin", l_linestatus);
    writeIntColumn(tableDir + "/l_shipdate.bin", l_shipdate);
    writeIntColumn(tableDir + "/l_commitdate.bin", l_commitdate);
    writeIntColumn(tableDir + "/l_receiptdate.bin", l_receiptdate);
    writeStringColumn(tableDir + "/l_shipinstruct.bin", l_shipinstruct);
    writeStringColumn(tableDir + "/l_comment.bin", l_comment);

    std::cout << "Lineitem ingestion complete." << std::endl;
}

void ingestOrders(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting orders..." << std::endl;

    std::string filePath = dataDir + "/orders.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> o_orderkey, o_custkey;
    std::vector<uint8_t> o_orderstatus;
    std::vector<int64_t> o_totalprice;
    std::vector<int32_t> o_orderdate;
    std::vector<std::string> o_orderpriority, o_clerk, o_comment;
    std::vector<int32_t> o_shippriority;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 9) continue;

        o_orderkey.push_back(std::stoi(parts[0]));
        o_custkey.push_back(std::stoi(parts[1]));
        o_orderstatus.push_back(getDictCode(parts[2], orderstatus_dict, "orderstatus"));
        o_totalprice.push_back(parseDecimal(parts[3], 100));
        o_orderdate.push_back(parseDate(parts[4]));
        o_orderpriority.push_back(parts[5]);
        o_clerk.push_back(parts[6]);
        o_shippriority.push_back(std::stoi(parts[7]));
        o_comment.push_back(parts[8]);

        lineCount++;
        if (lineCount % 1000000 == 0) {
            std::cout << "  Parsed " << lineCount << " orders rows" << std::endl;
        }
    }
    inFile.close();

    std::cout << "  Total orders rows: " << lineCount << std::endl;

    // Sort by o_orderkey
    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return o_orderkey[i] < o_orderkey[j];
    });

    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(o_orderkey);
    sort_col(o_custkey);
    sort_col(o_orderstatus);
    sort_col(o_totalprice);
    sort_col(o_orderdate);
    sort_col(o_orderpriority);
    sort_col(o_clerk);
    sort_col(o_shippriority);
    sort_col(o_comment);

    std::string tableDir = outDir + "/orders";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/o_orderkey.bin", o_orderkey);
    writeIntColumn(tableDir + "/o_custkey.bin", o_custkey);
    writeUint8Column(tableDir + "/o_orderstatus.bin", o_orderstatus);
    writeInt64Column(tableDir + "/o_totalprice.bin", o_totalprice);
    writeIntColumn(tableDir + "/o_orderdate.bin", o_orderdate);
    writeStringColumn(tableDir + "/o_orderpriority.bin", o_orderpriority);
    writeStringColumn(tableDir + "/o_clerk.bin", o_clerk);
    writeIntColumn(tableDir + "/o_shippriority.bin", o_shippriority);
    writeStringColumn(tableDir + "/o_comment.bin", o_comment);

    std::cout << "Orders ingestion complete." << std::endl;
}

void ingestCustomer(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting customer..." << std::endl;

    std::string filePath = dataDir + "/customer.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;
    std::vector<int64_t> c_acctbal;
    std::vector<uint8_t> c_mktsegment;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 8) continue;

        c_custkey.push_back(std::stoi(parts[0]));
        c_name.push_back(parts[1]);
        c_address.push_back(parts[2]);
        c_nationkey.push_back(std::stoi(parts[3]));
        c_phone.push_back(parts[4]);
        c_acctbal.push_back(parseDecimal(parts[5], 100));
        c_mktsegment.push_back(getDictCode(parts[6], mktsegment_dict, "mktsegment"));
        c_comment.push_back(parts[7]);

        lineCount++;
        if (lineCount % 100000 == 0) {
            std::cout << "  Parsed " << lineCount << " customer rows" << std::endl;
        }
    }
    inFile.close();

    std::cout << "  Total customer rows: " << lineCount << std::endl;

    // Sort by c_custkey
    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return c_custkey[i] < c_custkey[j];
    });

    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(c_custkey);
    sort_col(c_name);
    sort_col(c_address);
    sort_col(c_nationkey);
    sort_col(c_phone);
    sort_col(c_acctbal);
    sort_col(c_mktsegment);
    sort_col(c_comment);

    std::string tableDir = outDir + "/customer";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/c_custkey.bin", c_custkey);
    writeStringColumn(tableDir + "/c_name.bin", c_name);
    writeStringColumn(tableDir + "/c_address.bin", c_address);
    writeIntColumn(tableDir + "/c_nationkey.bin", c_nationkey);
    writeStringColumn(tableDir + "/c_phone.bin", c_phone);
    writeInt64Column(tableDir + "/c_acctbal.bin", c_acctbal);
    writeUint8Column(tableDir + "/c_mktsegment.bin", c_mktsegment);
    writeStringColumn(tableDir + "/c_comment.bin", c_comment);

    std::cout << "Customer ingestion complete." << std::endl;
}

void ingestPart(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting part..." << std::endl;

    std::string filePath = dataDir + "/part.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> p_partkey, p_size;
    std::vector<std::string> p_name, p_mfgr, p_brand, p_type, p_container, p_comment;
    std::vector<int64_t> p_retailprice;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 9) continue;

        p_partkey.push_back(std::stoi(parts[0]));
        p_name.push_back(parts[1]);
        p_mfgr.push_back(parts[2]);
        p_brand.push_back(parts[3]);
        p_type.push_back(parts[4]);
        p_size.push_back(std::stoi(parts[5]));
        p_container.push_back(parts[6]);
        p_retailprice.push_back(parseDecimal(parts[7], 100));
        p_comment.push_back(parts[8]);

        lineCount++;
        if (lineCount % 100000 == 0) {
            std::cout << "  Parsed " << lineCount << " part rows" << std::endl;
        }
    }
    inFile.close();

    std::cout << "  Total part rows: " << lineCount << std::endl;

    // Sort by p_partkey
    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return p_partkey[i] < p_partkey[j];
    });

    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(p_partkey);
    sort_col(p_name);
    sort_col(p_mfgr);
    sort_col(p_brand);
    sort_col(p_type);
    sort_col(p_size);
    sort_col(p_container);
    sort_col(p_retailprice);
    sort_col(p_comment);

    std::string tableDir = outDir + "/part";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/p_partkey.bin", p_partkey);
    writeStringColumn(tableDir + "/p_name.bin", p_name);
    writeStringColumn(tableDir + "/p_mfgr.bin", p_mfgr);
    writeStringColumn(tableDir + "/p_brand.bin", p_brand);
    writeStringColumn(tableDir + "/p_type.bin", p_type);
    writeIntColumn(tableDir + "/p_size.bin", p_size);
    writeStringColumn(tableDir + "/p_container.bin", p_container);
    writeInt64Column(tableDir + "/p_retailprice.bin", p_retailprice);
    writeStringColumn(tableDir + "/p_comment.bin", p_comment);

    std::cout << "Part ingestion complete." << std::endl;
}

void ingestPartsupp(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting partsupp..." << std::endl;

    std::string filePath = dataDir + "/partsupp.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<int64_t> ps_supplycost;
    std::vector<std::string> ps_comment;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 5) continue;

        ps_partkey.push_back(std::stoi(parts[0]));
        ps_suppkey.push_back(std::stoi(parts[1]));
        ps_availqty.push_back(std::stoi(parts[2]));
        ps_supplycost.push_back(parseDecimal(parts[3], 100));
        ps_comment.push_back(parts[4]);

        lineCount++;
        if (lineCount % 500000 == 0) {
            std::cout << "  Parsed " << lineCount << " partsupp rows" << std::endl;
        }
    }
    inFile.close();

    std::cout << "  Total partsupp rows: " << lineCount << std::endl;

    // Sort by ps_partkey, ps_suppkey
    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        if (ps_partkey[i] != ps_partkey[j]) return ps_partkey[i] < ps_partkey[j];
        return ps_suppkey[i] < ps_suppkey[j];
    });

    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(ps_partkey);
    sort_col(ps_suppkey);
    sort_col(ps_availqty);
    sort_col(ps_supplycost);
    sort_col(ps_comment);

    std::string tableDir = outDir + "/partsupp";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/ps_partkey.bin", ps_partkey);
    writeIntColumn(tableDir + "/ps_suppkey.bin", ps_suppkey);
    writeIntColumn(tableDir + "/ps_availqty.bin", ps_availqty);
    writeInt64Column(tableDir + "/ps_supplycost.bin", ps_supplycost);
    writeStringColumn(tableDir + "/ps_comment.bin", ps_comment);

    std::cout << "Partsupp ingestion complete." << std::endl;
}

void ingestSupplier(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting supplier..." << std::endl;

    std::string filePath = dataDir + "/supplier.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> s_suppkey, s_nationkey;
    std::vector<std::string> s_name, s_address, s_phone, s_comment;
    std::vector<int64_t> s_acctbal;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 7) continue;

        s_suppkey.push_back(std::stoi(parts[0]));
        s_name.push_back(parts[1]);
        s_address.push_back(parts[2]);
        s_nationkey.push_back(std::stoi(parts[3]));
        s_phone.push_back(parts[4]);
        s_acctbal.push_back(parseDecimal(parts[5], 100));
        s_comment.push_back(parts[6]);

        lineCount++;
        if (lineCount % 10000 == 0) {
            std::cout << "  Parsed " << lineCount << " supplier rows" << std::endl;
        }
    }
    inFile.close();

    std::cout << "  Total supplier rows: " << lineCount << std::endl;

    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return s_suppkey[i] < s_suppkey[j];
    });

    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(s_suppkey);
    sort_col(s_name);
    sort_col(s_address);
    sort_col(s_nationkey);
    sort_col(s_phone);
    sort_col(s_acctbal);
    sort_col(s_comment);

    std::string tableDir = outDir + "/supplier";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/s_suppkey.bin", s_suppkey);
    writeStringColumn(tableDir + "/s_name.bin", s_name);
    writeStringColumn(tableDir + "/s_address.bin", s_address);
    writeIntColumn(tableDir + "/s_nationkey.bin", s_nationkey);
    writeStringColumn(tableDir + "/s_phone.bin", s_phone);
    writeInt64Column(tableDir + "/s_acctbal.bin", s_acctbal);
    writeStringColumn(tableDir + "/s_comment.bin", s_comment);

    std::cout << "Supplier ingestion complete." << std::endl;
}

void ingestNation(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting nation..." << std::endl;

    std::string filePath = dataDir + "/nation.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 4) continue;

        n_nationkey.push_back(std::stoi(parts[0]));
        n_name.push_back(parts[1]);
        n_regionkey.push_back(std::stoi(parts[2]));
        n_comment.push_back(parts[3]);

        lineCount++;
    }
    inFile.close();

    std::cout << "  Total nation rows: " << lineCount << std::endl;

    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return n_nationkey[i] < n_nationkey[j];
    });

    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(n_nationkey);
    sort_col(n_name);
    sort_col(n_regionkey);
    sort_col(n_comment);

    std::string tableDir = outDir + "/nation";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/n_nationkey.bin", n_nationkey);
    writeStringColumn(tableDir + "/n_name.bin", n_name);
    writeIntColumn(tableDir + "/n_regionkey.bin", n_regionkey);
    writeStringColumn(tableDir + "/n_comment.bin", n_comment);

    std::cout << "Nation ingestion complete." << std::endl;
}

void ingestRegion(const std::string& dataDir, const std::string& outDir) {
    std::cout << "Ingesting region..." << std::endl;

    std::string filePath = dataDir + "/region.tbl";
    std::ifstream inFile(filePath);
    if (!inFile.is_open()) {
        std::cerr << "Error: Cannot open " << filePath << std::endl;
        return;
    }

    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;

    std::string line;
    size_t lineCount = 0;
    while (std::getline(inFile, line)) {
        auto parts = splitLine(line, '|');
        if (parts.size() < 3) continue;

        r_regionkey.push_back(std::stoi(parts[0]));
        r_name.push_back(parts[1]);
        r_comment.push_back(parts[2]);

        lineCount++;
    }
    inFile.close();

    std::cout << "  Total region rows: " << lineCount << std::endl;

    std::vector<size_t> perm(lineCount);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](size_t i, size_t j) {
        return r_regionkey[i] < r_regionkey[j];
    });

    auto sort_col = [&](auto& col) {
        auto temp = col;
        for (size_t i = 0; i < lineCount; i++) {
            col[i] = temp[perm[i]];
        }
    };

    sort_col(r_regionkey);
    sort_col(r_name);
    sort_col(r_comment);

    std::string tableDir = outDir + "/region";
    fs::create_directories(tableDir);

    writeIntColumn(tableDir + "/r_regionkey.bin", r_regionkey);
    writeStringColumn(tableDir + "/r_name.bin", r_name);
    writeStringColumn(tableDir + "/r_comment.bin", r_comment);

    std::cout << "Region ingestion complete." << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_gendb_dir>" << std::endl;
        return 1;
    }

    std::string dataDir = argv[1];
    std::string outDir = argv[2];

    fs::create_directories(outDir);

    // Ingest tables (can be parallelized in real implementation)
    ingestLineitem(dataDir, outDir);
    ingestOrders(dataDir, outDir);
    ingestCustomer(dataDir, outDir);
    ingestPart(dataDir, outDir);
    ingestPartsupp(dataDir, outDir);
    ingestSupplier(dataDir, outDir);
    ingestNation(dataDir, outDir);
    ingestRegion(dataDir, outDir);

    // Write dictionaries
    std::string indexDir = outDir + "/lineitem";
    writeDictionary(indexDir + "/l_returnflag_dict.txt", returnflagDict);
    writeDictionary(indexDir + "/l_linestatus_dict.txt", linestatusDict);

    indexDir = outDir + "/orders";
    writeDictionary(indexDir + "/o_orderstatus_dict.txt", orderstatus_dict);

    indexDir = outDir + "/customer";
    writeDictionary(indexDir + "/c_mktsegment_dict.txt", mktsegment_dict);

    std::cout << "\nIngestion complete!" << std::endl;
    std::cout << "Data written to: " << outDir << std::endl;

    return 0;
}
