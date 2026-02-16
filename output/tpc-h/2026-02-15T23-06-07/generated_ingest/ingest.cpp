#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <algorithm>
#include <charconv>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

// ============================================================================
// Date Parsing: Manual YYYY-MM-DD → days since epoch (1970-01-01)
// ============================================================================

static inline bool is_leap_year(int year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

static inline int days_in_month(int month, int year) {
    static const int days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month == 2 && is_leap_year(year)) return 29;
    return days[month - 1];
}

static int32_t parse_date(const char* str) {
    // Parse YYYY-MM-DD manually
    int year = (str[0] - '0') * 1000 + (str[1] - '0') * 100 + (str[2] - '0') * 10 + (str[3] - '0');
    int month = (str[5] - '0') * 10 + (str[6] - '0');
    int day = (str[8] - '0') * 10 + (str[9] - '0');

    // Calculate days since 1970-01-01
    int days = 0;

    // Add days for complete years (1970 to year-1)
    for (int y = 1970; y < year; ++y) {
        days += is_leap_year(y) ? 366 : 365;
    }

    // Add days for complete months in current year (1 to month-1)
    for (int m = 1; m < month; ++m) {
        days += days_in_month(m, year);
    }

    // Add remaining days (day is 1-indexed, so subtract 1)
    days += (day - 1);

    return days;
}

// Self-test: verify epoch day 0 is 1970-01-01
static void test_date_parser() {
    int32_t epoch_zero = parse_date("1970-01-01");
    if (epoch_zero != 0) {
        std::cerr << "FATAL: Date parser failed self-test. parse_date(\"1970-01-01\") = "
                  << epoch_zero << ", expected 0. ABORTING.\n";
        std::exit(1);
    }
    std::cout << "Date parser self-test passed: epoch day 0 = 1970-01-01\n";
}

// ============================================================================
// Dictionary Encoding
// ============================================================================

struct Dictionary {
    std::unordered_map<std::string, int32_t> str_to_code;
    std::vector<std::string> code_to_str;
    std::mutex mutex;

    int32_t encode(const std::string& str) {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = str_to_code.find(str);
        if (it != str_to_code.end()) {
            return it->second;
        }
        int32_t code = (int32_t)code_to_str.size();
        str_to_code[str] = code;
        code_to_str.push_back(str);
        return code;
    }

    void save(const std::string& path) {
        std::ofstream out(path);
        for (const auto& s : code_to_str) {
            out << s << "\n";
        }
    }
};

// ============================================================================
// Table Structures (SoA - Structure of Arrays)
// ============================================================================

struct NationTable {
    std::vector<int32_t> n_nationkey;
    std::vector<int32_t> n_name;  // dictionary-encoded
    std::vector<int32_t> n_regionkey;
    std::vector<std::string> n_comment;
    Dictionary n_name_dict;
};

struct RegionTable {
    std::vector<int32_t> r_regionkey;
    std::vector<int32_t> r_name;  // dictionary-encoded
    std::vector<std::string> r_comment;
    Dictionary r_name_dict;
};

struct SupplierTable {
    std::vector<int32_t> s_suppkey;
    std::vector<std::string> s_name;
    std::vector<std::string> s_address;
    std::vector<int32_t> s_nationkey;
    std::vector<std::string> s_phone;
    std::vector<int64_t> s_acctbal;  // scaled by 100
    std::vector<std::string> s_comment;
};

struct PartTable {
    std::vector<int32_t> p_partkey;
    std::vector<std::string> p_name;
    std::vector<std::string> p_mfgr;
    std::vector<std::string> p_brand;
    std::vector<std::string> p_type;
    std::vector<int32_t> p_size;
    std::vector<std::string> p_container;
    std::vector<int64_t> p_retailprice;  // scaled by 100
    std::vector<std::string> p_comment;
};

struct PartsuppTable {
    std::vector<int32_t> ps_partkey;
    std::vector<int32_t> ps_suppkey;
    std::vector<int32_t> ps_availqty;
    std::vector<int64_t> ps_supplycost;  // scaled by 100
    std::vector<std::string> ps_comment;
};

struct CustomerTable {
    std::vector<int32_t> c_custkey;
    std::vector<std::string> c_name;
    std::vector<std::string> c_address;
    std::vector<int32_t> c_nationkey;
    std::vector<std::string> c_phone;
    std::vector<int64_t> c_acctbal;  // scaled by 100
    std::vector<int32_t> c_mktsegment;  // dictionary-encoded
    std::vector<std::string> c_comment;
    Dictionary c_mktsegment_dict;
};

struct OrdersTable {
    std::vector<int32_t> o_orderkey;
    std::vector<int32_t> o_custkey;
    std::vector<int32_t> o_orderstatus;  // dictionary-encoded
    std::vector<int64_t> o_totalprice;  // scaled by 100
    std::vector<int32_t> o_orderdate;  // days since epoch
    std::vector<std::string> o_orderpriority;
    std::vector<std::string> o_clerk;
    std::vector<int32_t> o_shippriority;
    std::vector<std::string> o_comment;
    Dictionary o_orderstatus_dict;
};

struct LineitemTable {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<int64_t> l_quantity;  // scaled by 100
    std::vector<int64_t> l_extendedprice;  // scaled by 100
    std::vector<int64_t> l_discount;  // scaled by 100
    std::vector<int64_t> l_tax;  // scaled by 100
    std::vector<int32_t> l_returnflag;  // dictionary-encoded
    std::vector<int32_t> l_linestatus;  // dictionary-encoded
    std::vector<int32_t> l_shipdate;  // days since epoch
    std::vector<int32_t> l_commitdate;  // days since epoch
    std::vector<int32_t> l_receiptdate;  // days since epoch
    std::vector<std::string> l_shipinstruct;
    std::vector<std::string> l_shipmode;
    std::vector<std::string> l_comment;
    Dictionary l_returnflag_dict;
    Dictionary l_linestatus_dict;
};

// ============================================================================
// Parsing Utilities
// ============================================================================

static inline int32_t parse_int32(const char* str, const char* end) {
    int32_t value;
    std::from_chars(str, end, value);
    return value;
}

static inline int64_t parse_decimal_scaled(const char* str, const char* end, int scale_factor) {
    // Parse as double, multiply by scale_factor, round to int64_t
    double value;
    std::from_chars(str, end, value);
    return (int64_t)(value * scale_factor + 0.5);
}

static inline std::string parse_string(const char* str, const char* end) {
    return std::string(str, end);
}

// ============================================================================
// Parallel Ingestion - Nation
// ============================================================================

void ingest_nation(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting nation...\n";

    std::string input_path = data_dir + "/nation.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    NationTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        // n_nationkey
        sep = std::find(p, end, '|');
        table.n_nationkey.push_back(parse_int32(p, sep));
        p = sep + 1;

        // n_name (dictionary-encoded)
        sep = std::find(p, end, '|');
        std::string n_name = parse_string(p, sep);
        table.n_name.push_back(table.n_name_dict.encode(n_name));
        p = sep + 1;

        // n_regionkey
        sep = std::find(p, end, '|');
        table.n_regionkey.push_back(parse_int32(p, sep));
        p = sep + 1;

        // n_comment
        sep = std::find(p, end, '|');
        table.n_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.n_nationkey.size() << " rows\n";

    // Write binary columns
    std::string table_dir = output_dir + "/nation";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/n_nationkey.bin", std::ios::binary)
        .write((char*)table.n_nationkey.data(), table.n_nationkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/n_name.bin", std::ios::binary)
        .write((char*)table.n_name.data(), table.n_name.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/n_regionkey.bin", std::ios::binary)
        .write((char*)table.n_regionkey.data(), table.n_regionkey.size() * sizeof(int32_t));

    table.n_name_dict.save(table_dir + "/n_name_dict.txt");

    std::cout << "  Wrote nation table\n";
}

// ============================================================================
// Parallel Ingestion - Region
// ============================================================================

void ingest_region(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting region...\n";

    std::string input_path = data_dir + "/region.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    RegionTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        // r_regionkey
        sep = std::find(p, end, '|');
        table.r_regionkey.push_back(parse_int32(p, sep));
        p = sep + 1;

        // r_name (dictionary-encoded)
        sep = std::find(p, end, '|');
        std::string r_name = parse_string(p, sep);
        table.r_name.push_back(table.r_name_dict.encode(r_name));
        p = sep + 1;

        // r_comment
        sep = std::find(p, end, '|');
        table.r_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.r_regionkey.size() << " rows\n";

    // Write binary columns
    std::string table_dir = output_dir + "/region";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/r_regionkey.bin", std::ios::binary)
        .write((char*)table.r_regionkey.data(), table.r_regionkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/r_name.bin", std::ios::binary)
        .write((char*)table.r_name.data(), table.r_name.size() * sizeof(int32_t));

    table.r_name_dict.save(table_dir + "/r_name_dict.txt");

    std::cout << "  Wrote region table\n";
}

// ============================================================================
// Parallel Ingestion - Supplier
// ============================================================================

void ingest_supplier(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting supplier...\n";

    std::string input_path = data_dir + "/supplier.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    SupplierTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        sep = std::find(p, end, '|'); table.s_suppkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.s_name.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.s_address.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.s_nationkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.s_phone.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.s_acctbal.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|'); table.s_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.s_suppkey.size() << " rows\n";

    std::string table_dir = output_dir + "/supplier";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/s_suppkey.bin", std::ios::binary).write((char*)table.s_suppkey.data(), table.s_suppkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/s_nationkey.bin", std::ios::binary).write((char*)table.s_nationkey.data(), table.s_nationkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/s_acctbal.bin", std::ios::binary).write((char*)table.s_acctbal.data(), table.s_acctbal.size() * sizeof(int64_t));

    std::cout << "  Wrote supplier table\n";
}

// ============================================================================
// Parallel Ingestion - Part
// ============================================================================

void ingest_part(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting part...\n";

    std::string input_path = data_dir + "/part.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    PartTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        sep = std::find(p, end, '|'); table.p_partkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_name.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_mfgr.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_brand.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_type.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_size.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_container.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_retailprice.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|'); table.p_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.p_partkey.size() << " rows\n";

    std::string table_dir = output_dir + "/part";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/p_partkey.bin", std::ios::binary).write((char*)table.p_partkey.data(), table.p_partkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/p_size.bin", std::ios::binary).write((char*)table.p_size.data(), table.p_size.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/p_retailprice.bin", std::ios::binary).write((char*)table.p_retailprice.data(), table.p_retailprice.size() * sizeof(int64_t));

    std::cout << "  Wrote part table\n";
}

// ============================================================================
// Parallel Ingestion - Partsupp
// ============================================================================

void ingest_partsupp(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting partsupp...\n";

    std::string input_path = data_dir + "/partsupp.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    PartsuppTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        sep = std::find(p, end, '|'); table.ps_partkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.ps_suppkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.ps_availqty.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.ps_supplycost.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|'); table.ps_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.ps_partkey.size() << " rows\n";

    std::string table_dir = output_dir + "/partsupp";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/ps_partkey.bin", std::ios::binary).write((char*)table.ps_partkey.data(), table.ps_partkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/ps_suppkey.bin", std::ios::binary).write((char*)table.ps_suppkey.data(), table.ps_suppkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/ps_availqty.bin", std::ios::binary).write((char*)table.ps_availqty.data(), table.ps_availqty.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/ps_supplycost.bin", std::ios::binary).write((char*)table.ps_supplycost.data(), table.ps_supplycost.size() * sizeof(int64_t));

    std::cout << "  Wrote partsupp table\n";
}

// ============================================================================
// Parallel Ingestion - Customer
// ============================================================================

void ingest_customer(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting customer...\n";

    std::string input_path = data_dir + "/customer.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    CustomerTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        sep = std::find(p, end, '|'); table.c_custkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.c_name.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.c_address.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.c_nationkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.c_phone.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.c_acctbal.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|');
        std::string mktseg = parse_string(p, sep);
        table.c_mktsegment.push_back(table.c_mktsegment_dict.encode(mktseg));
        p = sep + 1;
        sep = std::find(p, end, '|'); table.c_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.c_custkey.size() << " rows\n";

    std::string table_dir = output_dir + "/customer";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/c_custkey.bin", std::ios::binary).write((char*)table.c_custkey.data(), table.c_custkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/c_nationkey.bin", std::ios::binary).write((char*)table.c_nationkey.data(), table.c_nationkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/c_acctbal.bin", std::ios::binary).write((char*)table.c_acctbal.data(), table.c_acctbal.size() * sizeof(int64_t));
    std::ofstream(table_dir + "/c_mktsegment.bin", std::ios::binary).write((char*)table.c_mktsegment.data(), table.c_mktsegment.size() * sizeof(int32_t));

    table.c_mktsegment_dict.save(table_dir + "/c_mktsegment_dict.txt");

    std::cout << "  Wrote customer table (" << table.c_mktsegment_dict.code_to_str.size() << " distinct c_mktsegment values)\n";
}

// ============================================================================
// Parallel Ingestion - Orders
// ============================================================================

void ingest_orders(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting orders...\n";

    std::string input_path = data_dir + "/orders.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    OrdersTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        sep = std::find(p, end, '|'); table.o_orderkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.o_custkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|');
        std::string status = parse_string(p, sep);
        table.o_orderstatus.push_back(table.o_orderstatus_dict.encode(status));
        p = sep + 1;
        sep = std::find(p, end, '|'); table.o_totalprice.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|'); table.o_orderdate.push_back(parse_date(p)); p = sep + 1;
        sep = std::find(p, end, '|'); table.o_orderpriority.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.o_clerk.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.o_shippriority.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.o_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.o_orderkey.size() << " rows\n";

    // Post-ingestion check: date values should be reasonable
    if (!table.o_orderdate.empty()) {
        int32_t sample_date = table.o_orderdate[table.o_orderdate.size() / 2];
        if (sample_date > 30000 || sample_date < 0) {
            std::cerr << "WARNING: o_orderdate sample value out of range: " << sample_date << "\n";
        }
    }

    std::string table_dir = output_dir + "/orders";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/o_orderkey.bin", std::ios::binary).write((char*)table.o_orderkey.data(), table.o_orderkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/o_custkey.bin", std::ios::binary).write((char*)table.o_custkey.data(), table.o_custkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/o_orderstatus.bin", std::ios::binary).write((char*)table.o_orderstatus.data(), table.o_orderstatus.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/o_totalprice.bin", std::ios::binary).write((char*)table.o_totalprice.data(), table.o_totalprice.size() * sizeof(int64_t));
    std::ofstream(table_dir + "/o_orderdate.bin", std::ios::binary).write((char*)table.o_orderdate.data(), table.o_orderdate.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/o_shippriority.bin", std::ios::binary).write((char*)table.o_shippriority.data(), table.o_shippriority.size() * sizeof(int32_t));

    table.o_orderstatus_dict.save(table_dir + "/o_orderstatus_dict.txt");

    std::cout << "  Wrote orders table\n";
}

// ============================================================================
// Parallel Ingestion - Lineitem (sorted by l_shipdate)
// ============================================================================

void ingest_lineitem(const std::string& data_dir, const std::string& output_dir) {
    std::cout << "Ingesting lineitem...\n";

    std::string input_path = data_dir + "/lineitem.tbl";
    std::ifstream in(input_path);
    if (!in) {
        std::cerr << "Failed to open " << input_path << "\n";
        return;
    }

    LineitemTable table;
    std::string line;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        const char* p = line.c_str();
        const char* end = p + line.size();
        const char* sep;

        sep = std::find(p, end, '|'); table.l_orderkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_partkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_suppkey.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_linenumber.push_back(parse_int32(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_quantity.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_extendedprice.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_discount.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_tax.push_back(parse_decimal_scaled(p, sep, 100)); p = sep + 1;
        sep = std::find(p, end, '|');
        std::string rflag = parse_string(p, sep);
        table.l_returnflag.push_back(table.l_returnflag_dict.encode(rflag));
        p = sep + 1;
        sep = std::find(p, end, '|');
        std::string lstatus = parse_string(p, sep);
        table.l_linestatus.push_back(table.l_linestatus_dict.encode(lstatus));
        p = sep + 1;
        sep = std::find(p, end, '|'); table.l_shipdate.push_back(parse_date(p)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_commitdate.push_back(parse_date(p)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_receiptdate.push_back(parse_date(p)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_shipinstruct.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_shipmode.push_back(parse_string(p, sep)); p = sep + 1;
        sep = std::find(p, end, '|'); table.l_comment.push_back(parse_string(p, sep));
    }

    std::cout << "  Loaded " << table.l_orderkey.size() << " rows\n";

    // Post-ingestion checks
    if (!table.l_shipdate.empty()) {
        int32_t sample_date = table.l_shipdate[table.l_shipdate.size() / 2];
        if (sample_date > 30000 || sample_date < 0) {
            std::cerr << "ERROR: l_shipdate sample value out of range: " << sample_date << ". ABORTING.\n";
            std::exit(1);
        }
    }
    if (!table.l_discount.empty()) {
        int64_t sample_discount = table.l_discount[table.l_discount.size() / 2];
        if (sample_discount == 0) {
            std::cerr << "WARNING: l_discount sample value is zero (possible parsing error)\n";
        }
    }

    // Sort by l_shipdate (permutation-based)
    std::cout << "  Sorting by l_shipdate...\n";
    size_t n = table.l_orderkey.size();
    std::vector<size_t> perm(n);
    for (size_t i = 0; i < n; ++i) perm[i] = i;

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
        return table.l_shipdate[a] < table.l_shipdate[b];
    });

    // Apply permutation to all columns
    auto apply_perm_int32 = [&](std::vector<int32_t>& v) {
        std::vector<int32_t> tmp(n);
        for (size_t i = 0; i < n; ++i) tmp[i] = v[perm[i]];
        v = std::move(tmp);
    };
    auto apply_perm_int64 = [&](std::vector<int64_t>& v) {
        std::vector<int64_t> tmp(n);
        for (size_t i = 0; i < n; ++i) tmp[i] = v[perm[i]];
        v = std::move(tmp);
    };

    apply_perm_int32(table.l_orderkey);
    apply_perm_int32(table.l_partkey);
    apply_perm_int32(table.l_suppkey);
    apply_perm_int32(table.l_linenumber);
    apply_perm_int64(table.l_quantity);
    apply_perm_int64(table.l_extendedprice);
    apply_perm_int64(table.l_discount);
    apply_perm_int64(table.l_tax);
    apply_perm_int32(table.l_returnflag);
    apply_perm_int32(table.l_linestatus);
    apply_perm_int32(table.l_shipdate);
    apply_perm_int32(table.l_commitdate);
    apply_perm_int32(table.l_receiptdate);

    std::cout << "  Sorted\n";

    std::string table_dir = output_dir + "/lineitem";
    mkdir(table_dir.c_str(), 0755);

    std::ofstream(table_dir + "/l_orderkey.bin", std::ios::binary).write((char*)table.l_orderkey.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_partkey.bin", std::ios::binary).write((char*)table.l_partkey.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_suppkey.bin", std::ios::binary).write((char*)table.l_suppkey.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_linenumber.bin", std::ios::binary).write((char*)table.l_linenumber.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_quantity.bin", std::ios::binary).write((char*)table.l_quantity.data(), n * sizeof(int64_t));
    std::ofstream(table_dir + "/l_extendedprice.bin", std::ios::binary).write((char*)table.l_extendedprice.data(), n * sizeof(int64_t));
    std::ofstream(table_dir + "/l_discount.bin", std::ios::binary).write((char*)table.l_discount.data(), n * sizeof(int64_t));
    std::ofstream(table_dir + "/l_tax.bin", std::ios::binary).write((char*)table.l_tax.data(), n * sizeof(int64_t));
    std::ofstream(table_dir + "/l_returnflag.bin", std::ios::binary).write((char*)table.l_returnflag.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_linestatus.bin", std::ios::binary).write((char*)table.l_linestatus.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_shipdate.bin", std::ios::binary).write((char*)table.l_shipdate.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_commitdate.bin", std::ios::binary).write((char*)table.l_commitdate.data(), n * sizeof(int32_t));
    std::ofstream(table_dir + "/l_receiptdate.bin", std::ios::binary).write((char*)table.l_receiptdate.data(), n * sizeof(int32_t));

    table.l_returnflag_dict.save(table_dir + "/l_returnflag_dict.txt");
    table.l_linestatus_dict.save(table_dir + "/l_linestatus_dict.txt");

    std::cout << "  Wrote lineitem table (" << table.l_returnflag_dict.code_to_str.size()
              << " returnflag, " << table.l_linestatus_dict.code_to_str.size() << " linestatus values)\n";
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directory
    mkdir(output_dir.c_str(), 0755);

    // Self-test date parser
    test_date_parser();

    // Ingest all tables in parallel
    std::vector<std::thread> threads;
    threads.emplace_back(ingest_nation, data_dir, output_dir);
    threads.emplace_back(ingest_region, data_dir, output_dir);
    threads.emplace_back(ingest_supplier, data_dir, output_dir);
    threads.emplace_back(ingest_part, data_dir, output_dir);
    threads.emplace_back(ingest_partsupp, data_dir, output_dir);
    threads.emplace_back(ingest_customer, data_dir, output_dir);
    threads.emplace_back(ingest_orders, data_dir, output_dir);
    threads.emplace_back(ingest_lineitem, data_dir, output_dir);

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "\nIngestion complete!\n";
    return 0;
}
