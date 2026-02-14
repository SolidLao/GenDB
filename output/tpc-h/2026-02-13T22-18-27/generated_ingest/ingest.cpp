#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <charconv>
#include <chrono>
#include <unordered_map>
#include <sstream>

// Dictionary for low-cardinality string columns
struct Dictionary {
    std::unordered_map<std::string, uint8_t> str_to_code;
    std::vector<std::string> code_to_str;

    uint8_t encode(const std::string& s) {
        auto it = str_to_code.find(s);
        if (it != str_to_code.end()) return it->second;
        uint8_t code = code_to_str.size();
        code_to_str.push_back(s);
        str_to_code[s] = code;
        return code;
    }
};

// Parse YYYY-MM-DD to days since epoch (1970-01-01)
inline int32_t parse_date(const char* s) {
    int year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
    int month = (s[5] - '0') * 10 + (s[6] - '0');
    int day = (s[8] - '0') * 10 + (s[9] - '0');

    // Days since 1970-01-01
    int days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400;

    // Add days for months
    static const int month_days[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    days += month_days[month - 1];

    // Add leap day if needed
    if (month > 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
        days++;
    }

    days += day - 1;
    return days;
}

// Parse decimal to fixed-point int64 (cents, 2 decimal places)
inline int64_t parse_decimal(const char* start, const char* end) {
    int64_t result = 0;
    bool negative = false;
    const char* p = start;

    if (*p == '-') {
        negative = true;
        p++;
    }

    // Parse integer part
    while (p < end && *p != '.') {
        result = result * 10 + (*p - '0');
        p++;
    }
    result *= 100;

    // Parse decimal part
    if (p < end && *p == '.') {
        p++;
        if (p < end) result += (*p - '0') * 10;
        p++;
        if (p < end) result += (*p - '0');
    }

    return negative ? -result : result;
}

// Parse integer using std::from_chars
inline int32_t parse_int32(const char* start, const char* end) {
    int32_t value;
    std::from_chars(start, end, value);
    return value;
}

// Track row counts for metadata
struct RowCounts {
    size_t lineitem = 0;
    size_t orders = 0;
    size_t customer = 0;
    size_t part = 0;
    size_t partsupp = 0;
    size_t supplier = 0;
    size_t nation = 0;
    size_t region = 0;
};
RowCounts g_row_counts;

struct LineitemRow {
    int32_t l_orderkey;
    int32_t l_partkey;
    int32_t l_suppkey;
    int32_t l_linenumber;
    int32_t l_quantity;
    int64_t l_extendedprice;
    int32_t l_discount;
    int32_t l_tax;
    uint8_t l_returnflag;
    uint8_t l_linestatus;
    int32_t l_shipdate;
    int32_t l_commitdate;
    int32_t l_receiptdate;
    uint8_t l_shipinstruct;
    uint8_t l_shipmode;
    std::string l_comment;
};

void parse_lineitem(const char* data, size_t size, std::vector<LineitemRow>& rows,
                   Dictionary& dict_returnflag, Dictionary& dict_linestatus,
                   Dictionary& dict_shipinstruct, Dictionary& dict_shipmode) {
    const char* p = data;
    const char* end = data + size;

    while (p < end) {
        LineitemRow row;

        // Find field boundaries
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') {
                fields.push_back(p + 1);
            }
            p++;
        }

        if (fields.size() < 16) {
            if (p < end) p++;
            continue;
        }

        // Parse fields (0-indexed)
        row.l_orderkey = parse_int32(fields[0], fields[1] - 1);
        row.l_partkey = parse_int32(fields[1], fields[2] - 1);
        row.l_suppkey = parse_int32(fields[2], fields[3] - 1);
        row.l_linenumber = parse_int32(fields[3], fields[4] - 1);
        row.l_quantity = parse_decimal(fields[4], fields[5] - 1);  // Store as cents
        row.l_extendedprice = parse_decimal(fields[5], fields[6] - 1);
        row.l_discount = parse_decimal(fields[6], fields[7] - 1);
        row.l_tax = parse_decimal(fields[7], fields[8] - 1);
        row.l_returnflag = dict_returnflag.encode(std::string(fields[8], fields[9] - 1));
        row.l_linestatus = dict_linestatus.encode(std::string(fields[9], fields[10] - 1));
        row.l_shipdate = parse_date(fields[10]);
        row.l_commitdate = parse_date(fields[11]);
        row.l_receiptdate = parse_date(fields[12]);
        row.l_shipinstruct = dict_shipinstruct.encode(std::string(fields[13], fields[14] - 1));
        row.l_shipmode = dict_shipmode.encode(std::string(fields[14], fields[15] - 1));
        row.l_comment = std::string(fields[15], p - fields[15] - 1);  // Skip trailing |

        rows.push_back(row);
        if (p < end) p++;
    }
}

void ingest_lineitem_parallel(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    // Open and mmap the file
    std::string filename = data_dir + "/lineitem.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Split into chunks by newlines
    int num_threads = std::thread::hardware_concurrency();
    size_t chunk_size = file_size / num_threads;
    std::vector<std::pair<size_t, size_t>> chunks;

    size_t offset = 0;
    for (int i = 0; i < num_threads; i++) {
        size_t start = offset;
        size_t end = (i == num_threads - 1) ? file_size : std::min(start + chunk_size, file_size);

        // Align to newline
        if (end < file_size) {
            while (end < file_size && data[end] != '\n') end++;
            if (end < file_size) end++;
        }

        chunks.push_back({start, end});
        offset = end;
    }

    // Thread-local dictionaries (will merge later)
    std::vector<Dictionary> dicts_returnflag(num_threads);
    std::vector<Dictionary> dicts_linestatus(num_threads);
    std::vector<Dictionary> dicts_shipinstruct(num_threads);
    std::vector<Dictionary> dicts_shipmode(num_threads);
    std::vector<std::vector<LineitemRow>> thread_rows(num_threads);

    // Parse in parallel
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            parse_lineitem(data + chunks[i].first, chunks[i].second - chunks[i].first,
                          thread_rows[i], dicts_returnflag[i], dicts_linestatus[i],
                          dicts_shipinstruct[i], dicts_shipmode[i]);
        });
    }

    for (auto& t : threads) t.join();

    // Merge dictionaries
    Dictionary dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode;
    for (int i = 0; i < num_threads; i++) {
        for (const auto& s : dicts_returnflag[i].code_to_str) dict_returnflag.encode(s);
        for (const auto& s : dicts_linestatus[i].code_to_str) dict_linestatus.encode(s);
        for (const auto& s : dicts_shipinstruct[i].code_to_str) dict_shipinstruct.encode(s);
        for (const auto& s : dicts_shipmode[i].code_to_str) dict_shipmode.encode(s);
    }

    // Remap codes
    for (int i = 0; i < num_threads; i++) {
        for (auto& row : thread_rows[i]) {
            row.l_returnflag = dict_returnflag.str_to_code[dicts_returnflag[i].code_to_str[row.l_returnflag]];
            row.l_linestatus = dict_linestatus.str_to_code[dicts_linestatus[i].code_to_str[row.l_linestatus]];
            row.l_shipinstruct = dict_shipinstruct.str_to_code[dicts_shipinstruct[i].code_to_str[row.l_shipinstruct]];
            row.l_shipmode = dict_shipmode.str_to_code[dicts_shipmode[i].code_to_str[row.l_shipmode]];
        }
    }

    // Merge rows
    std::vector<LineitemRow> all_rows;
    size_t total = 0;
    for (const auto& tr : thread_rows) total += tr.size();
    all_rows.reserve(total);
    for (auto& tr : thread_rows) {
        all_rows.insert(all_rows.end(), tr.begin(), tr.end());
    }

    std::cout << "Parsed " << all_rows.size() << " rows" << std::endl;

    // Sort by l_shipdate (for zone maps)
    std::cout << "Sorting by l_shipdate..." << std::endl;
    std::sort(all_rows.begin(), all_rows.end(),
              [](const LineitemRow& a, const LineitemRow& b) { return a.l_shipdate < b.l_shipdate; });

    // Write column files
    std::cout << "Writing column files..." << std::endl;

    std::vector<std::thread> write_threads;

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_orderkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_orderkey, sizeof(r.l_orderkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_partkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_partkey, sizeof(r.l_partkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_suppkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_suppkey, sizeof(r.l_suppkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_linenumber.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_linenumber, sizeof(r.l_linenumber));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_quantity.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_quantity, sizeof(r.l_quantity));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_extendedprice.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_extendedprice, sizeof(r.l_extendedprice));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_discount.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_discount, sizeof(r.l_discount));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_tax.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_tax, sizeof(r.l_tax));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_returnflag.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_returnflag, sizeof(r.l_returnflag));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_linestatus.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_linestatus, sizeof(r.l_linestatus));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_shipdate.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_shipdate, sizeof(r.l_shipdate));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_commitdate.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_commitdate, sizeof(r.l_commitdate));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_receiptdate.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_receiptdate, sizeof(r.l_receiptdate));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_shipinstruct.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_shipinstruct, sizeof(r.l_shipinstruct));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_shipmode.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) f.write((char*)&r.l_shipmode, sizeof(r.l_shipmode));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/lineitem.l_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : all_rows) {
            uint32_t len = r.l_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.l_comment.data(), len);
        }
    });

    for (auto& t : write_threads) t.join();

    // Write dictionaries
    {
        std::ofstream f(gendb_dir + "/lineitem.l_returnflag.dict");
        for (const auto& s : dict_returnflag.code_to_str) f << s << "\n";
    }
    {
        std::ofstream f(gendb_dir + "/lineitem.l_linestatus.dict");
        for (const auto& s : dict_linestatus.code_to_str) f << s << "\n";
    }
    {
        std::ofstream f(gendb_dir + "/lineitem.l_shipinstruct.dict");
        for (const auto& s : dict_shipinstruct.code_to_str) f << s << "\n";
    }
    {
        std::ofstream f(gendb_dir + "/lineitem.l_shipmode.dict");
        for (const auto& s : dict_shipmode.code_to_str) f << s << "\n";
    }

    munmap((void*)data, file_size);
    close(fd);

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();
    std::cout << "Lineitem ingestion completed in " << elapsed << " seconds" << std::endl;

    // Spot check: print min/max shipdate
    int32_t min_shipdate = all_rows.front().l_shipdate;
    int32_t max_shipdate = all_rows.back().l_shipdate;
    std::cout << "Shipdate range: " << min_shipdate << " - " << max_shipdate << " (days since epoch)" << std::endl;

    g_row_counts.lineitem = all_rows.size();
}

// Orders table
struct OrdersRow {
    int32_t o_orderkey;
    int32_t o_custkey;
    uint8_t o_orderstatus;
    int64_t o_totalprice;
    int32_t o_orderdate;
    uint8_t o_orderpriority;
    std::string o_clerk;
    int32_t o_shippriority;
    std::string o_comment;
};

void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting orders..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::string filename = data_dir + "/orders.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse
    Dictionary dict_status, dict_priority;
    std::vector<OrdersRow> rows;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        OrdersRow row;
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') fields.push_back(p + 1);
            p++;
        }

        if (fields.size() < 9) {
            if (p < end) p++;
            continue;
        }

        row.o_orderkey = parse_int32(fields[0], fields[1] - 1);
        row.o_custkey = parse_int32(fields[1], fields[2] - 1);
        row.o_orderstatus = dict_status.encode(std::string(fields[2], fields[3] - 1));
        row.o_totalprice = parse_decimal(fields[3], fields[4] - 1);
        row.o_orderdate = parse_date(fields[4]);
        row.o_orderpriority = dict_priority.encode(std::string(fields[5], fields[6] - 1));
        row.o_clerk = std::string(fields[6], fields[7] - 1);
        row.o_shippriority = parse_int32(fields[7], fields[8] - 1);
        row.o_comment = std::string(fields[8], p - fields[8] - 1);

        rows.push_back(row);
        if (p < end) p++;
    }

    std::cout << "Parsed " << rows.size() << " rows" << std::endl;

    // Sort by o_orderdate
    std::cout << "Sorting by o_orderdate..." << std::endl;
    std::sort(rows.begin(), rows.end(),
              [](const OrdersRow& a, const OrdersRow& b) { return a.o_orderdate < b.o_orderdate; });

    // Write columns in parallel
    std::cout << "Writing column files..." << std::endl;
    std::vector<std::thread> write_threads;

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_orderkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.o_orderkey, sizeof(r.o_orderkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_custkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.o_custkey, sizeof(r.o_custkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_orderstatus.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.o_orderstatus, sizeof(r.o_orderstatus));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_totalprice.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.o_totalprice, sizeof(r.o_totalprice));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_orderdate.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.o_orderdate, sizeof(r.o_orderdate));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_orderpriority.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.o_orderpriority, sizeof(r.o_orderpriority));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_clerk.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.o_clerk.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.o_clerk.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_shippriority.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.o_shippriority, sizeof(r.o_shippriority));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/orders.o_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.o_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.o_comment.data(), len);
        }
    });

    for (auto& t : write_threads) t.join();

    // Write dictionaries
    {
        std::ofstream f(gendb_dir + "/orders.o_orderstatus.dict");
        for (const auto& s : dict_status.code_to_str) f << s << "\n";
    }
    {
        std::ofstream f(gendb_dir + "/orders.o_orderpriority.dict");
        for (const auto& s : dict_priority.code_to_str) f << s << "\n";
    }

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "Orders ingestion completed in " << elapsed << " seconds" << std::endl;

    g_row_counts.orders = rows.size();
}

// Customer table
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

void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting customer..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::string filename = data_dir + "/customer.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse
    Dictionary dict_segment;
    std::vector<CustomerRow> rows;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        CustomerRow row;
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') fields.push_back(p + 1);
            p++;
        }

        if (fields.size() < 8) {
            if (p < end) p++;
            continue;
        }

        row.c_custkey = parse_int32(fields[0], fields[1] - 1);
        row.c_name = std::string(fields[1], fields[2] - 1);
        row.c_address = std::string(fields[2], fields[3] - 1);
        row.c_nationkey = parse_int32(fields[3], fields[4] - 1);
        row.c_phone = std::string(fields[4], fields[5] - 1);
        row.c_acctbal = parse_decimal(fields[5], fields[6] - 1);
        row.c_mktsegment = dict_segment.encode(std::string(fields[6], fields[7] - 1));
        row.c_comment = std::string(fields[7], p - fields[7] - 1);

        rows.push_back(row);
        if (p < end) p++;
    }

    std::cout << "Parsed " << rows.size() << " rows" << std::endl;

    // Sort by c_custkey
    std::cout << "Sorting by c_custkey..." << std::endl;
    std::sort(rows.begin(), rows.end(),
              [](const CustomerRow& a, const CustomerRow& b) { return a.c_custkey < b.c_custkey; });

    // Write columns in parallel
    std::cout << "Writing column files..." << std::endl;
    std::vector<std::thread> write_threads;

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_custkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.c_custkey, sizeof(r.c_custkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_name.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.c_name.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.c_name.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_address.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.c_address.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.c_address.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_nationkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.c_nationkey, sizeof(r.c_nationkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_phone.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.c_phone.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.c_phone.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_acctbal.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.c_acctbal, sizeof(r.c_acctbal));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_mktsegment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.c_mktsegment, sizeof(r.c_mktsegment));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/customer.c_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.c_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.c_comment.data(), len);
        }
    });

    for (auto& t : write_threads) t.join();

    // Write dictionary
    {
        std::ofstream f(gendb_dir + "/customer.c_mktsegment.dict");
        for (const auto& s : dict_segment.code_to_str) f << s << "\n";
    }

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "Customer ingestion completed in " << elapsed << " seconds" << std::endl;

    g_row_counts.customer = rows.size();
}

// Part table
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

void ingest_part(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting part..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::string filename = data_dir + "/part.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse
    std::vector<PartRow> rows;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        PartRow row;
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') fields.push_back(p + 1);
            p++;
        }

        if (fields.size() < 9) {
            if (p < end) p++;
            continue;
        }

        row.p_partkey = parse_int32(fields[0], fields[1] - 1);
        row.p_name = std::string(fields[1], fields[2] - 1);
        row.p_mfgr = std::string(fields[2], fields[3] - 1);
        row.p_brand = std::string(fields[3], fields[4] - 1);
        row.p_type = std::string(fields[4], fields[5] - 1);
        row.p_size = parse_int32(fields[5], fields[6] - 1);
        row.p_container = std::string(fields[6], fields[7] - 1);
        row.p_retailprice = parse_decimal(fields[7], fields[8] - 1);
        row.p_comment = std::string(fields[8], p - fields[8] - 1);

        rows.push_back(row);
        if (p < end) p++;
    }

    std::cout << "Parsed " << rows.size() << " rows, writing columns..." << std::endl;

    // Write columns in parallel
    std::vector<std::thread> write_threads;

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_partkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.p_partkey, sizeof(r.p_partkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_name.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.p_name.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.p_name.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_mfgr.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.p_mfgr.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.p_mfgr.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_brand.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.p_brand.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.p_brand.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_type.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.p_type.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.p_type.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_size.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.p_size, sizeof(r.p_size));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_container.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.p_container.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.p_container.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_retailprice.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.p_retailprice, sizeof(r.p_retailprice));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/part.p_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.p_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.p_comment.data(), len);
        }
    });

    for (auto& t : write_threads) t.join();

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "Part ingestion completed in " << elapsed << " seconds" << std::endl;

    g_row_counts.part = rows.size();
}

// Partsupp table
struct PartsuppRow {
    int32_t ps_partkey;
    int32_t ps_suppkey;
    int32_t ps_availqty;
    int64_t ps_supplycost;
    std::string ps_comment;
};

void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::string filename = data_dir + "/partsupp.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse
    std::vector<PartsuppRow> rows;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        PartsuppRow row;
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') fields.push_back(p + 1);
            p++;
        }

        if (fields.size() < 5) {
            if (p < end) p++;
            continue;
        }

        row.ps_partkey = parse_int32(fields[0], fields[1] - 1);
        row.ps_suppkey = parse_int32(fields[1], fields[2] - 1);
        row.ps_availqty = parse_int32(fields[2], fields[3] - 1);
        row.ps_supplycost = parse_decimal(fields[3], fields[4] - 1);
        row.ps_comment = std::string(fields[4], p - fields[4] - 1);

        rows.push_back(row);
        if (p < end) p++;
    }

    std::cout << "Parsed " << rows.size() << " rows, writing columns..." << std::endl;

    // Write columns in parallel
    std::vector<std::thread> write_threads;

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/partsupp.ps_partkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.ps_partkey, sizeof(r.ps_partkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/partsupp.ps_suppkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.ps_suppkey, sizeof(r.ps_suppkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/partsupp.ps_availqty.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.ps_availqty, sizeof(r.ps_availqty));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/partsupp.ps_supplycost.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.ps_supplycost, sizeof(r.ps_supplycost));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/partsupp.ps_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.ps_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.ps_comment.data(), len);
        }
    });

    for (auto& t : write_threads) t.join();

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "Partsupp ingestion completed in " << elapsed << " seconds" << std::endl;

    g_row_counts.partsupp = rows.size();
}

// Supplier table
struct SupplierRow {
    int32_t s_suppkey;
    std::string s_name;
    std::string s_address;
    int32_t s_nationkey;
    std::string s_phone;
    int64_t s_acctbal;
    std::string s_comment;
};

void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting supplier..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::string filename = data_dir + "/supplier.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse
    std::vector<SupplierRow> rows;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        SupplierRow row;
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') fields.push_back(p + 1);
            p++;
        }

        if (fields.size() < 7) {
            if (p < end) p++;
            continue;
        }

        row.s_suppkey = parse_int32(fields[0], fields[1] - 1);
        row.s_name = std::string(fields[1], fields[2] - 1);
        row.s_address = std::string(fields[2], fields[3] - 1);
        row.s_nationkey = parse_int32(fields[3], fields[4] - 1);
        row.s_phone = std::string(fields[4], fields[5] - 1);
        row.s_acctbal = parse_decimal(fields[5], fields[6] - 1);
        row.s_comment = std::string(fields[6], p - fields[6] - 1);

        rows.push_back(row);
        if (p < end) p++;
    }

    std::cout << "Parsed " << rows.size() << " rows, writing columns..." << std::endl;

    // Write columns
    std::vector<std::thread> write_threads;

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/supplier.s_suppkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.s_suppkey, sizeof(r.s_suppkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/supplier.s_name.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.s_name.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.s_name.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/supplier.s_address.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.s_address.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.s_address.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/supplier.s_nationkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.s_nationkey, sizeof(r.s_nationkey));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/supplier.s_phone.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.s_phone.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.s_phone.data(), len);
        }
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/supplier.s_acctbal.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.s_acctbal, sizeof(r.s_acctbal));
    });

    write_threads.emplace_back([&]() {
        std::ofstream f(gendb_dir + "/supplier.s_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.s_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.s_comment.data(), len);
        }
    });

    for (auto& t : write_threads) t.join();

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "Supplier ingestion completed in " << elapsed << " seconds" << std::endl;

    g_row_counts.supplier = rows.size();
}

// Nation table
struct NationRow {
    int32_t n_nationkey;
    std::string n_name;
    int32_t n_regionkey;
    std::string n_comment;
};

void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting nation..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::string filename = data_dir + "/nation.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse
    std::vector<NationRow> rows;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        NationRow row;
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') fields.push_back(p + 1);
            p++;
        }

        if (fields.size() < 4) {
            if (p < end) p++;
            continue;
        }

        row.n_nationkey = parse_int32(fields[0], fields[1] - 1);
        row.n_name = std::string(fields[1], fields[2] - 1);
        row.n_regionkey = parse_int32(fields[2], fields[3] - 1);
        row.n_comment = std::string(fields[3], p - fields[3] - 1);

        rows.push_back(row);
        if (p < end) p++;
    }

    std::cout << "Parsed " << rows.size() << " rows, writing columns..." << std::endl;

    // Write columns
    {
        std::ofstream f(gendb_dir + "/nation.n_nationkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.n_nationkey, sizeof(r.n_nationkey));
    }

    {
        std::ofstream f(gendb_dir + "/nation.n_name.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.n_name.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.n_name.data(), len);
        }
    }

    {
        std::ofstream f(gendb_dir + "/nation.n_regionkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.n_regionkey, sizeof(r.n_regionkey));
    }

    {
        std::ofstream f(gendb_dir + "/nation.n_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.n_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.n_comment.data(), len);
        }
    }

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "Nation ingestion completed in " << elapsed << " seconds" << std::endl;

    g_row_counts.nation = rows.size();
}

// Region table
struct RegionRow {
    int32_t r_regionkey;
    std::string r_name;
    std::string r_comment;
};

void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting region..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::string filename = data_dir + "/region.tbl";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Cannot open " << filename << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    const char* data = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed" << std::endl;
        close(fd);
        return;
    }
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    // Parse
    std::vector<RegionRow> rows;
    const char* p = data;
    const char* end = data + file_size;

    while (p < end) {
        RegionRow row;
        std::vector<const char*> fields;
        fields.push_back(p);

        while (p < end && *p != '\n') {
            if (*p == '|') fields.push_back(p + 1);
            p++;
        }

        if (fields.size() < 3) {
            if (p < end) p++;
            continue;
        }

        row.r_regionkey = parse_int32(fields[0], fields[1] - 1);
        row.r_name = std::string(fields[1], fields[2] - 1);
        row.r_comment = std::string(fields[2], p - fields[2] - 1);

        rows.push_back(row);
        if (p < end) p++;
    }

    std::cout << "Parsed " << rows.size() << " rows, writing columns..." << std::endl;

    // Write columns
    {
        std::ofstream f(gendb_dir + "/region.r_regionkey.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) f.write((char*)&r.r_regionkey, sizeof(r.r_regionkey));
    }

    {
        std::ofstream f(gendb_dir + "/region.r_name.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.r_name.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.r_name.data(), len);
        }
    }

    {
        std::ofstream f(gendb_dir + "/region.r_comment.bin", std::ios::binary);
        std::vector<char> buf(1024 * 1024);
        f.rdbuf()->pubsetbuf(buf.data(), buf.size());
        for (const auto& r : rows) {
            uint32_t len = r.r_comment.size();
            f.write((char*)&len, sizeof(len));
            f.write(r.r_comment.data(), len);
        }
    }

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start).count();
    std::cout << "Region ingestion completed in " << elapsed << " seconds" << std::endl;

    g_row_counts.region = rows.size();
}

// Helper to write metadata.json
void write_metadata(const std::string& gendb_dir,
                    size_t lineitem_rows,
                    size_t orders_rows,
                    size_t customer_rows,
                    size_t part_rows,
                    size_t partsupp_rows,
                    size_t supplier_rows,
                    size_t nation_rows,
                    size_t region_rows) {
    std::ofstream f(gendb_dir + "/metadata.json");
    f << "{\n";
    f << "  \"tables\": {\n";
    f << "    \"lineitem\": { \"row_count\": " << lineitem_rows << " },\n";
    f << "    \"orders\": { \"row_count\": " << orders_rows << " },\n";
    f << "    \"customer\": { \"row_count\": " << customer_rows << " },\n";
    f << "    \"part\": { \"row_count\": " << part_rows << " },\n";
    f << "    \"partsupp\": { \"row_count\": " << partsupp_rows << " },\n";
    f << "    \"supplier\": { \"row_count\": " << supplier_rows << " },\n";
    f << "    \"nation\": { \"row_count\": " << nation_rows << " },\n";
    f << "    \"region\": { \"row_count\": " << region_rows << " }\n";
    f << "  },\n";
    f << "  \"format\": \"binary_columnar\",\n";
    f << "  \"date_encoding\": \"days_since_epoch_1970\",\n";
    f << "  \"decimal_encoding\": \"fixed_point_int64_cents\"\n";
    f << "}\n";
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create gendb directory
    if (system(("mkdir -p " + gendb_dir).c_str()) != 0) {
        std::cerr << "Failed to create directory: " << gendb_dir << std::endl;
        return 1;
    }

    std::cout << "GenDB Ingestion Tool" << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    std::cout << "CPU cores: " << std::thread::hardware_concurrency() << std::endl;
    std::cout << std::endl;

    auto total_start = std::chrono::high_resolution_clock::now();

    // Ingest all tables
    ingest_lineitem_parallel(data_dir, gendb_dir);
    std::cout << std::endl;

    ingest_orders(data_dir, gendb_dir);
    std::cout << std::endl;

    ingest_customer(data_dir, gendb_dir);
    std::cout << std::endl;

    ingest_part(data_dir, gendb_dir);
    std::cout << std::endl;

    ingest_partsupp(data_dir, gendb_dir);
    std::cout << std::endl;

    ingest_supplier(data_dir, gendb_dir);
    std::cout << std::endl;

    ingest_nation(data_dir, gendb_dir);
    std::cout << std::endl;

    ingest_region(data_dir, gendb_dir);
    std::cout << std::endl;

    // Write metadata
    std::cout << "Writing metadata.json..." << std::endl;
    write_metadata(gendb_dir,
                   g_row_counts.lineitem,
                   g_row_counts.orders,
                   g_row_counts.customer,
                   g_row_counts.part,
                   g_row_counts.partsupp,
                   g_row_counts.supplier,
                   g_row_counts.nation,
                   g_row_counts.region);

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_elapsed = std::chrono::duration<double>(total_end - total_start).count();

    std::cout << "\n=== Ingestion Summary ===" << std::endl;
    std::cout << "Lineitem:  " << g_row_counts.lineitem << " rows" << std::endl;
    std::cout << "Orders:    " << g_row_counts.orders << " rows" << std::endl;
    std::cout << "Customer:  " << g_row_counts.customer << " rows" << std::endl;
    std::cout << "Part:      " << g_row_counts.part << " rows" << std::endl;
    std::cout << "Partsupp:  " << g_row_counts.partsupp << " rows" << std::endl;
    std::cout << "Supplier:  " << g_row_counts.supplier << " rows" << std::endl;
    std::cout << "Nation:    " << g_row_counts.nation << " rows" << std::endl;
    std::cout << "Region:    " << g_row_counts.region << " rows" << std::endl;
    std::cout << "\nTotal ingestion time: " << total_elapsed << " seconds" << std::endl;

    return 0;
}
