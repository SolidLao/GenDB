#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <map>

// Date parsing: convert YYYY-MM-DD to days since 1970-01-01
int32_t parse_date(const char* str) {
    int year, month, day;
    sscanf(str, "%d-%d-%d", &year, &month, &day);

    // Simple days since epoch calculation
    struct tm t = {0};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    time_t epoch = mktime(&t);
    return static_cast<int32_t>(epoch / 86400);
}

// Fast in-place numeric parsing
int32_t parse_int32(const char* str, const char* end) {
    return static_cast<int32_t>(strtol(str, nullptr, 10));
}

double parse_double(const char* str, const char* end) {
    return strtod(str, nullptr);
}

// String trimming
std::string trim(const char* start, const char* end) {
    return std::string(start, end);
}

// Table-specific parsers using mmap and parallel chunking
namespace lineitem_parser {
    struct Row {
        int32_t l_orderkey;
        int32_t l_partkey;
        int32_t l_suppkey;
        int32_t l_linenumber;
        double l_quantity;
        double l_extendedprice;
        double l_discount;
        double l_tax;
        std::string l_returnflag;
        std::string l_linestatus;
        int32_t l_shipdate;
        int32_t l_commitdate;
        int32_t l_receiptdate;
        std::string l_shipinstruct;
        std::string l_shipmode;
        std::string l_comment;
    };

    struct ColumnData {
        std::vector<int32_t> l_orderkey;
        std::vector<int32_t> l_partkey;
        std::vector<int32_t> l_suppkey;
        std::vector<int32_t> l_linenumber;
        std::vector<double> l_quantity;
        std::vector<double> l_extendedprice;
        std::vector<double> l_discount;
        std::vector<double> l_tax;
        std::vector<std::string> l_returnflag;
        std::vector<std::string> l_linestatus;
        std::vector<int32_t> l_shipdate;
        std::vector<int32_t> l_commitdate;
        std::vector<int32_t> l_receiptdate;
        std::vector<std::string> l_shipinstruct;
        std::vector<std::string> l_shipmode;
        std::vector<std::string> l_comment;
    };

    void parse_chunk(const char* data, size_t start_offset, size_t end_offset,
                     std::vector<Row>& local_rows) {
        const char* ptr = data + start_offset;
        const char* end_ptr = data + end_offset;

        local_rows.reserve(100000);

        while (ptr < end_ptr) {
            const char* line_start = ptr;
            const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end_ptr - ptr));
            if (!line_end) break;

            Row row;
            const char* field_start = line_start;
            int field_idx = 0;

            for (const char* p = line_start; p <= line_end; ++p) {
                if (*p == '|' || p == line_end) {
                    const char* field_end = p;

                    switch(field_idx) {
                        case 0: row.l_orderkey = parse_int32(field_start, field_end); break;
                        case 1: row.l_partkey = parse_int32(field_start, field_end); break;
                        case 2: row.l_suppkey = parse_int32(field_start, field_end); break;
                        case 3: row.l_linenumber = parse_int32(field_start, field_end); break;
                        case 4: row.l_quantity = parse_double(field_start, field_end); break;
                        case 5: row.l_extendedprice = parse_double(field_start, field_end); break;
                        case 6: row.l_discount = parse_double(field_start, field_end); break;
                        case 7: row.l_tax = parse_double(field_start, field_end); break;
                        case 8: row.l_returnflag = trim(field_start, field_end); break;
                        case 9: row.l_linestatus = trim(field_start, field_end); break;
                        case 10: row.l_shipdate = parse_date(field_start); break;
                        case 11: row.l_commitdate = parse_date(field_start); break;
                        case 12: row.l_receiptdate = parse_date(field_start); break;
                        case 13: row.l_shipinstruct = trim(field_start, field_end); break;
                        case 14: row.l_shipmode = trim(field_start, field_end); break;
                        case 15: row.l_comment = trim(field_start, field_end); break;
                    }
                    field_start = p + 1;
                    field_idx++;
                }
            }

            if (field_idx >= 16) {
                local_rows.push_back(std::move(row));
            }

            ptr = line_end + 1;
        }
    }
}

namespace orders_parser {
    struct Row {
        int32_t o_orderkey;
        int32_t o_custkey;
        std::string o_orderstatus;
        double o_totalprice;
        int32_t o_orderdate;
        std::string o_orderpriority;
        std::string o_clerk;
        int32_t o_shippriority;
        std::string o_comment;
    };

    void parse_chunk(const char* data, size_t start_offset, size_t end_offset,
                     std::vector<Row>& local_rows) {
        const char* ptr = data + start_offset;
        const char* end_ptr = data + end_offset;

        local_rows.reserve(50000);

        while (ptr < end_ptr) {
            const char* line_start = ptr;
            const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end_ptr - ptr));
            if (!line_end) break;

            Row row;
            const char* field_start = line_start;
            int field_idx = 0;

            for (const char* p = line_start; p <= line_end; ++p) {
                if (*p == '|' || p == line_end) {
                    const char* field_end = p;

                    switch(field_idx) {
                        case 0: row.o_orderkey = parse_int32(field_start, field_end); break;
                        case 1: row.o_custkey = parse_int32(field_start, field_end); break;
                        case 2: row.o_orderstatus = trim(field_start, field_end); break;
                        case 3: row.o_totalprice = parse_double(field_start, field_end); break;
                        case 4: row.o_orderdate = parse_date(field_start); break;
                        case 5: row.o_orderpriority = trim(field_start, field_end); break;
                        case 6: row.o_clerk = trim(field_start, field_end); break;
                        case 7: row.o_shippriority = parse_int32(field_start, field_end); break;
                        case 8: row.o_comment = trim(field_start, field_end); break;
                    }
                    field_start = p + 1;
                    field_idx++;
                }
            }

            if (field_idx >= 9) {
                local_rows.push_back(std::move(row));
            }

            ptr = line_end + 1;
        }
    }
}

namespace customer_parser {
    struct Row {
        int32_t c_custkey;
        std::string c_name;
        std::string c_address;
        int32_t c_nationkey;
        std::string c_phone;
        double c_acctbal;
        std::string c_mktsegment;
        std::string c_comment;
    };

    void parse_chunk(const char* data, size_t start_offset, size_t end_offset,
                     std::vector<Row>& local_rows) {
        const char* ptr = data + start_offset;
        const char* end_ptr = data + end_offset;

        local_rows.reserve(50000);

        while (ptr < end_ptr) {
            const char* line_start = ptr;
            const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end_ptr - ptr));
            if (!line_end) break;

            Row row;
            const char* field_start = line_start;
            int field_idx = 0;

            for (const char* p = line_start; p <= line_end; ++p) {
                if (*p == '|' || p == line_end) {
                    const char* field_end = p;

                    switch(field_idx) {
                        case 0: row.c_custkey = parse_int32(field_start, field_end); break;
                        case 1: row.c_name = trim(field_start, field_end); break;
                        case 2: row.c_address = trim(field_start, field_end); break;
                        case 3: row.c_nationkey = parse_int32(field_start, field_end); break;
                        case 4: row.c_phone = trim(field_start, field_end); break;
                        case 5: row.c_acctbal = parse_double(field_start, field_end); break;
                        case 6: row.c_mktsegment = trim(field_start, field_end); break;
                        case 7: row.c_comment = trim(field_start, field_end); break;
                    }
                    field_start = p + 1;
                    field_idx++;
                }
            }

            if (field_idx >= 8) {
                local_rows.push_back(std::move(row));
            }

            ptr = line_end + 1;
        }
    }
}

// Generic simple table parser (for smaller tables)
template<typename RowParser>
void ingest_small_table(const std::string& tbl_path, const std::string& output_dir,
                       RowParser parser) {
    // For small tables, just use single-threaded ifstream
    std::ifstream infile(tbl_path);
    if (!infile.is_open()) {
        std::cerr << "Failed to open " << tbl_path << std::endl;
        return;
    }

    parser(infile, output_dir);
    infile.close();
}

// Parallel ingestion for large tables
template<typename Row, typename ParseChunk>
void ingest_large_table_parallel(const std::string& tbl_path, const std::string& output_dir,
                                 const std::string& table_name, size_t estimated_rows,
                                 ParseChunk parse_chunk_fn,
                                 std::function<void(std::vector<Row>&, const std::string&)> write_columns_fn) {

    auto start_time = std::chrono::high_resolution_clock::now();

    // Open file with mmap
    int fd = open(tbl_path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << tbl_path << std::endl;
        return;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        std::cerr << "Failed to stat " << tbl_path << std::endl;
        return;
    }

    size_t file_size = sb.st_size;
    const char* data = static_cast<const char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        close(fd);
        std::cerr << "Failed to mmap " << tbl_path << std::endl;
        return;
    }

    // Advise sequential access
    madvise((void*)data, file_size, MADV_SEQUENTIAL);

    std::cout << "Ingesting " << table_name << " (" << file_size / (1024*1024) << " MB)..." << std::endl;

    // Determine number of threads
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 8;

    // Find line boundaries for chunking
    std::vector<size_t> chunk_offsets;
    chunk_offsets.push_back(0);

    size_t chunk_size = file_size / num_threads;
    for (unsigned int i = 1; i < num_threads; ++i) {
        size_t target_offset = i * chunk_size;
        // Find next newline
        const char* newline = static_cast<const char*>(memchr(data + target_offset, '\n', file_size - target_offset));
        if (newline) {
            chunk_offsets.push_back(newline - data + 1);
        }
    }
    chunk_offsets.push_back(file_size);

    // Parse chunks in parallel
    std::vector<std::thread> threads;
    std::vector<std::vector<Row>> thread_results(chunk_offsets.size() - 1);

    for (size_t i = 0; i < chunk_offsets.size() - 1; ++i) {
        threads.emplace_back([&, i]() {
            parse_chunk_fn(data, chunk_offsets[i], chunk_offsets[i+1], thread_results[i]);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge results
    std::vector<Row> all_rows;
    size_t total_rows = 0;
    for (const auto& chunk : thread_results) {
        total_rows += chunk.size();
    }
    all_rows.reserve(total_rows);

    for (auto& chunk : thread_results) {
        all_rows.insert(all_rows.end(),
                       std::make_move_iterator(chunk.begin()),
                       std::make_move_iterator(chunk.end()));
    }

    auto parse_time = std::chrono::high_resolution_clock::now();
    double parse_seconds = std::chrono::duration<double>(parse_time - start_time).count();

    std::cout << "  Parsed " << all_rows.size() << " rows in " << parse_seconds << " seconds" << std::endl;

    // Write columns
    write_columns_fn(all_rows, output_dir);

    munmap((void*)data, file_size);
    close(fd);

    auto end_time = std::chrono::high_resolution_clock::now();
    double total_seconds = std::chrono::duration<double>(end_time - start_time).count();

    std::cout << "  Total ingestion time: " << total_seconds << " seconds" << std::endl;
}

// Write lineitem columns (sorted by l_shipdate)
void write_lineitem_columns(std::vector<lineitem_parser::Row>& rows, const std::string& output_dir) {
    std::cout << "  Sorting by l_shipdate..." << std::endl;
    auto sort_start = std::chrono::high_resolution_clock::now();

    std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) {
        return a.l_shipdate < b.l_shipdate;
    });

    auto sort_end = std::chrono::high_resolution_clock::now();
    double sort_seconds = std::chrono::duration<double>(sort_end - sort_start).count();
    std::cout << "  Sorted in " << sort_seconds << " seconds" << std::endl;

    std::cout << "  Writing column files..." << std::endl;

    size_t n = rows.size();
    std::string table_dir = output_dir + "/lineitem";
    system(("mkdir -p " + table_dir).c_str());

    // Write each column in parallel
    std::vector<std::thread> write_threads;

    auto write_int32_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        for (const auto& row : rows) {
            int32_t val = extractor(row);
            out.write(reinterpret_cast<const char*>(&val), sizeof(int32_t));
        }
    };

    auto write_double_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        for (const auto& row : rows) {
            double val = extractor(row);
            out.write(reinterpret_cast<const char*>(&val), sizeof(double));
        }
    };

    auto write_string_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        std::ofstream idx(table_dir + "/" + name + ".idx", std::ios::binary);
        size_t offset = 0;
        for (const auto& row : rows) {
            const std::string& val = extractor(row);
            idx.write(reinterpret_cast<const char*>(&offset), sizeof(size_t));
            out.write(val.data(), val.size());
            offset += val.size();
        }
    };

    write_threads.emplace_back([&]() { write_int32_col("l_orderkey", [](const auto& r) { return r.l_orderkey; }); });
    write_threads.emplace_back([&]() { write_int32_col("l_partkey", [](const auto& r) { return r.l_partkey; }); });
    write_threads.emplace_back([&]() { write_int32_col("l_suppkey", [](const auto& r) { return r.l_suppkey; }); });
    write_threads.emplace_back([&]() { write_int32_col("l_linenumber", [](const auto& r) { return r.l_linenumber; }); });
    write_threads.emplace_back([&]() { write_double_col("l_quantity", [](const auto& r) { return r.l_quantity; }); });
    write_threads.emplace_back([&]() { write_double_col("l_extendedprice", [](const auto& r) { return r.l_extendedprice; }); });
    write_threads.emplace_back([&]() { write_double_col("l_discount", [](const auto& r) { return r.l_discount; }); });
    write_threads.emplace_back([&]() { write_double_col("l_tax", [](const auto& r) { return r.l_tax; }); });
    write_threads.emplace_back([&]() { write_string_col("l_returnflag", [](const auto& r) -> const std::string& { return r.l_returnflag; }); });
    write_threads.emplace_back([&]() { write_string_col("l_linestatus", [](const auto& r) -> const std::string& { return r.l_linestatus; }); });
    write_threads.emplace_back([&]() { write_int32_col("l_shipdate", [](const auto& r) { return r.l_shipdate; }); });
    write_threads.emplace_back([&]() { write_int32_col("l_commitdate", [](const auto& r) { return r.l_commitdate; }); });
    write_threads.emplace_back([&]() { write_int32_col("l_receiptdate", [](const auto& r) { return r.l_receiptdate; }); });
    write_threads.emplace_back([&]() { write_string_col("l_shipinstruct", [](const auto& r) -> const std::string& { return r.l_shipinstruct; }); });
    write_threads.emplace_back([&]() { write_string_col("l_shipmode", [](const auto& r) -> const std::string& { return r.l_shipmode; }); });
    write_threads.emplace_back([&]() { write_string_col("l_comment", [](const auto& r) -> const std::string& { return r.l_comment; }); });

    for (auto& t : write_threads) {
        t.join();
    }

    // Write metadata
    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\n";
    meta << "  \"row_count\": " << n << ",\n";
    meta << "  \"sort_key\": \"l_shipdate\",\n";
    meta << "  \"block_size\": 100000\n";
    meta << "}\n";
    meta.close();
}

void write_orders_columns(std::vector<orders_parser::Row>& rows, const std::string& output_dir) {
    std::cout << "  Sorting by o_orderkey..." << std::endl;
    std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) {
        return a.o_orderkey < b.o_orderkey;
    });

    std::cout << "  Writing column files..." << std::endl;

    size_t n = rows.size();
    std::string table_dir = output_dir + "/orders";
    system(("mkdir -p " + table_dir).c_str());

    std::vector<std::thread> write_threads;

    auto write_int32_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        for (const auto& row : rows) {
            int32_t val = extractor(row);
            out.write(reinterpret_cast<const char*>(&val), sizeof(int32_t));
        }
    };

    auto write_double_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        for (const auto& row : rows) {
            double val = extractor(row);
            out.write(reinterpret_cast<const char*>(&val), sizeof(double));
        }
    };

    auto write_string_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        std::ofstream idx(table_dir + "/" + name + ".idx", std::ios::binary);
        size_t offset = 0;
        for (const auto& row : rows) {
            const std::string& val = extractor(row);
            idx.write(reinterpret_cast<const char*>(&offset), sizeof(size_t));
            out.write(val.data(), val.size());
            offset += val.size();
        }
    };

    write_threads.emplace_back([&]() { write_int32_col("o_orderkey", [](const auto& r) { return r.o_orderkey; }); });
    write_threads.emplace_back([&]() { write_int32_col("o_custkey", [](const auto& r) { return r.o_custkey; }); });
    write_threads.emplace_back([&]() { write_string_col("o_orderstatus", [](const auto& r) -> const std::string& { return r.o_orderstatus; }); });
    write_threads.emplace_back([&]() { write_double_col("o_totalprice", [](const auto& r) { return r.o_totalprice; }); });
    write_threads.emplace_back([&]() { write_int32_col("o_orderdate", [](const auto& r) { return r.o_orderdate; }); });
    write_threads.emplace_back([&]() { write_string_col("o_orderpriority", [](const auto& r) -> const std::string& { return r.o_orderpriority; }); });
    write_threads.emplace_back([&]() { write_string_col("o_clerk", [](const auto& r) -> const std::string& { return r.o_clerk; }); });
    write_threads.emplace_back([&]() { write_int32_col("o_shippriority", [](const auto& r) { return r.o_shippriority; }); });
    write_threads.emplace_back([&]() { write_string_col("o_comment", [](const auto& r) -> const std::string& { return r.o_comment; }); });

    for (auto& t : write_threads) {
        t.join();
    }

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\n";
    meta << "  \"row_count\": " << n << ",\n";
    meta << "  \"sort_key\": \"o_orderkey\",\n";
    meta << "  \"block_size\": 100000\n";
    meta << "}\n";
    meta.close();
}

void write_customer_columns(std::vector<customer_parser::Row>& rows, const std::string& output_dir) {
    std::cout << "  Sorting by c_custkey..." << std::endl;
    std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) {
        return a.c_custkey < b.c_custkey;
    });

    std::cout << "  Writing column files..." << std::endl;

    size_t n = rows.size();
    std::string table_dir = output_dir + "/customer";
    system(("mkdir -p " + table_dir).c_str());

    std::vector<std::thread> write_threads;

    auto write_int32_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        for (const auto& row : rows) {
            int32_t val = extractor(row);
            out.write(reinterpret_cast<const char*>(&val), sizeof(int32_t));
        }
    };

    auto write_double_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        for (const auto& row : rows) {
            double val = extractor(row);
            out.write(reinterpret_cast<const char*>(&val), sizeof(double));
        }
    };

    auto write_string_col = [&](const std::string& name, auto extractor) {
        std::ofstream out(table_dir + "/" + name + ".bin", std::ios::binary);
        std::ofstream idx(table_dir + "/" + name + ".idx", std::ios::binary);
        size_t offset = 0;
        for (const auto& row : rows) {
            const std::string& val = extractor(row);
            idx.write(reinterpret_cast<const char*>(&offset), sizeof(size_t));
            out.write(val.data(), val.size());
            offset += val.size();
        }
    };

    write_threads.emplace_back([&]() { write_int32_col("c_custkey", [](const auto& r) { return r.c_custkey; }); });
    write_threads.emplace_back([&]() { write_string_col("c_name", [](const auto& r) -> const std::string& { return r.c_name; }); });
    write_threads.emplace_back([&]() { write_string_col("c_address", [](const auto& r) -> const std::string& { return r.c_address; }); });
    write_threads.emplace_back([&]() { write_int32_col("c_nationkey", [](const auto& r) { return r.c_nationkey; }); });
    write_threads.emplace_back([&]() { write_string_col("c_phone", [](const auto& r) -> const std::string& { return r.c_phone; }); });
    write_threads.emplace_back([&]() { write_double_col("c_acctbal", [](const auto& r) { return r.c_acctbal; }); });
    write_threads.emplace_back([&]() { write_string_col("c_mktsegment", [](const auto& r) -> const std::string& { return r.c_mktsegment; }); });
    write_threads.emplace_back([&]() { write_string_col("c_comment", [](const auto& r) -> const std::string& { return r.c_comment; }); });

    for (auto& t : write_threads) {
        t.join();
    }

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\n";
    meta << "  \"row_count\": " << n << ",\n";
    meta << "  \"sort_key\": \"c_custkey\",\n";
    meta << "  \"block_size\": null\n";
    meta << "}\n";
    meta.close();
}

// Simple parsers for smaller tables
void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting supplier..." << std::endl;
    std::string table_dir = gendb_dir + "/supplier";
    system(("mkdir -p " + table_dir).c_str());

    std::ifstream in(data_dir + "/supplier.tbl");
    std::vector<int32_t> s_suppkey;
    std::vector<std::string> s_name, s_address, s_phone, s_comment;
    std::vector<int32_t> s_nationkey;
    std::vector<double> s_acctbal;

    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int idx = 0;
        int32_t suppkey, nationkey;
        double acctbal;
        std::string name, address, phone, comment;

        while (std::getline(ss, field, '|')) {
            switch(idx++) {
                case 0: suppkey = std::stoi(field); break;
                case 1: name = field; break;
                case 2: address = field; break;
                case 3: nationkey = std::stoi(field); break;
                case 4: phone = field; break;
                case 5: acctbal = std::stod(field); break;
                case 6: comment = field; break;
            }
        }
        s_suppkey.push_back(suppkey);
        s_name.push_back(name);
        s_address.push_back(address);
        s_nationkey.push_back(nationkey);
        s_phone.push_back(phone);
        s_acctbal.push_back(acctbal);
        s_comment.push_back(comment);
    }

    // Write binary columns
    std::ofstream(table_dir + "/s_suppkey.bin", std::ios::binary).write(
        reinterpret_cast<const char*>(s_suppkey.data()), s_suppkey.size() * sizeof(int32_t));

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\"row_count\": " << s_suppkey.size() << "}\n";

    std::cout << "  Wrote " << s_suppkey.size() << " rows" << std::endl;
}

void ingest_part(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting part..." << std::endl;
    std::string table_dir = gendb_dir + "/part";
    system(("mkdir -p " + table_dir).c_str());

    std::ifstream in(data_dir + "/part.tbl");
    std::vector<int32_t> p_partkey, p_size;
    std::vector<double> p_retailprice;

    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int idx = 0;
        int32_t partkey, size;
        double retailprice;

        while (std::getline(ss, field, '|')) {
            if (idx == 0) partkey = std::stoi(field);
            else if (idx == 5) size = std::stoi(field);
            else if (idx == 7) retailprice = std::stod(field);
            idx++;
        }
        p_partkey.push_back(partkey);
        p_size.push_back(size);
        p_retailprice.push_back(retailprice);
    }

    std::ofstream(table_dir + "/p_partkey.bin", std::ios::binary).write(
        reinterpret_cast<const char*>(p_partkey.data()), p_partkey.size() * sizeof(int32_t));

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\"row_count\": " << p_partkey.size() << "}\n";

    std::cout << "  Wrote " << p_partkey.size() << " rows" << std::endl;
}

void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;
    std::string table_dir = gendb_dir + "/partsupp";
    system(("mkdir -p " + table_dir).c_str());

    std::ifstream in(data_dir + "/partsupp.tbl");
    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<double> ps_supplycost;

    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int idx = 0;
        int32_t partkey, suppkey, availqty;
        double supplycost;

        while (std::getline(ss, field, '|')) {
            if (idx == 0) partkey = std::stoi(field);
            else if (idx == 1) suppkey = std::stoi(field);
            else if (idx == 2) availqty = std::stoi(field);
            else if (idx == 3) supplycost = std::stod(field);
            idx++;
        }
        ps_partkey.push_back(partkey);
        ps_suppkey.push_back(suppkey);
        ps_availqty.push_back(availqty);
        ps_supplycost.push_back(supplycost);
    }

    std::ofstream(table_dir + "/ps_partkey.bin", std::ios::binary).write(
        reinterpret_cast<const char*>(ps_partkey.data()), ps_partkey.size() * sizeof(int32_t));
    std::ofstream(table_dir + "/ps_suppkey.bin", std::ios::binary).write(
        reinterpret_cast<const char*>(ps_suppkey.data()), ps_suppkey.size() * sizeof(int32_t));

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\"row_count\": " << ps_partkey.size() << "}\n";

    std::cout << "  Wrote " << ps_partkey.size() << " rows" << std::endl;
}

void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting nation..." << std::endl;
    std::string table_dir = gendb_dir + "/nation";
    system(("mkdir -p " + table_dir).c_str());

    std::ifstream in(data_dir + "/nation.tbl");
    std::vector<int32_t> n_nationkey, n_regionkey;

    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        int idx = 0;
        int32_t nationkey, regionkey;

        while (std::getline(ss, field, '|')) {
            if (idx == 0) nationkey = std::stoi(field);
            else if (idx == 2) regionkey = std::stoi(field);
            idx++;
        }
        n_nationkey.push_back(nationkey);
        n_regionkey.push_back(regionkey);
    }

    std::ofstream(table_dir + "/n_nationkey.bin", std::ios::binary).write(
        reinterpret_cast<const char*>(n_nationkey.data()), n_nationkey.size() * sizeof(int32_t));

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\"row_count\": " << n_nationkey.size() << "}\n";

    std::cout << "  Wrote " << n_nationkey.size() << " rows" << std::endl;
}

void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    std::cout << "Ingesting region..." << std::endl;
    std::string table_dir = gendb_dir + "/region";
    system(("mkdir -p " + table_dir).c_str());

    std::ifstream in(data_dir + "/region.tbl");
    std::vector<int32_t> r_regionkey;

    std::string line;
    while (std::getline(in, line)) {
        std::istringstream ss(line);
        std::string field;
        std::getline(ss, field, '|');
        r_regionkey.push_back(std::stoi(field));
    }

    std::ofstream(table_dir + "/r_regionkey.bin", std::ios::binary).write(
        reinterpret_cast<const char*>(r_regionkey.data()), r_regionkey.size() * sizeof(int32_t));

    std::ofstream meta(table_dir + "/metadata.json");
    meta << "{\"row_count\": " << r_regionkey.size() << "}\n";

    std::cout << "  Wrote " << r_regionkey.size() << " rows" << std::endl;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    std::cout << "GenDB Data Ingestion Tool" << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << "Data directory: " << data_dir << std::endl;
    std::cout << "GenDB directory: " << gendb_dir << std::endl;
    std::cout << std::endl;

    // Create base directory
    system(("mkdir -p " + gendb_dir).c_str());

    auto total_start = std::chrono::high_resolution_clock::now();

    // Ingest large tables in parallel (separate threads per table)
    std::vector<std::thread> table_threads;

    table_threads.emplace_back([&]() {
        ingest_large_table_parallel<lineitem_parser::Row>(
            data_dir + "/lineitem.tbl", gendb_dir, "lineitem", 59986052,
            lineitem_parser::parse_chunk, write_lineitem_columns);
    });

    table_threads.emplace_back([&]() {
        ingest_large_table_parallel<orders_parser::Row>(
            data_dir + "/orders.tbl", gendb_dir, "orders", 15000000,
            orders_parser::parse_chunk, write_orders_columns);
    });

    table_threads.emplace_back([&]() {
        ingest_large_table_parallel<customer_parser::Row>(
            data_dir + "/customer.tbl", gendb_dir, "customer", 1500000,
            customer_parser::parse_chunk, write_customer_columns);
    });

    // Small tables
    table_threads.emplace_back([&]() { ingest_supplier(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_part(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_partsupp(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_nation(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_region(data_dir, gendb_dir); });

    for (auto& t : table_threads) {
        t.join();
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    double total_seconds = std::chrono::duration<double>(total_end - total_start).count();

    std::cout << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << "Total ingestion time: " << total_seconds << " seconds" << std::endl;
    std::cout << "Data ingestion complete!" << std::endl;

    return 0;
}
