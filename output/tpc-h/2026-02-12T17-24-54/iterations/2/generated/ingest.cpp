#include "storage/storage.h"
#include "utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <mutex>

using namespace gendb;

// Helper: mmap a file and split into chunks at newline boundaries
struct MappedFile {
    char* data;
    size_t size;
    int fd;

    MappedFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);

        struct stat sb;
        if (fstat(fd, &sb) < 0) {
            close(fd);
            throw std::runtime_error("Cannot stat: " + path);
        }

        size = sb.st_size;
        data = static_cast<char*>(mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("mmap failed: " + path);
        }
        madvise(data, size, MADV_SEQUENTIAL);
    }

    ~MappedFile() {
        munmap(data, size);
        close(fd);
    }
};

// Helper: Find chunk boundaries (at newlines)
std::vector<std::pair<size_t, size_t>> split_chunks(const char* data, size_t size, size_t num_chunks) {
    std::vector<std::pair<size_t, size_t>> chunks;
    size_t chunk_size = size / num_chunks;

    size_t start = 0;
    for (size_t i = 0; i < num_chunks; ++i) {
        size_t end = (i == num_chunks - 1) ? size : std::min(start + chunk_size, size);

        // Adjust to next newline
        while (end < size && data[end] != '\n') {
            ++end;
        }
        if (end < size) ++end; // Include the newline

        chunks.emplace_back(start, end);
        start = end;
    }

    return chunks;
}

// Helper: Parse a line into tokens (pipe-delimited, trailing pipe)
std::vector<std::string> parse_line(const char* line_start, const char* line_end) {
    std::vector<std::string> tokens;
    const char* p = line_start;

    while (p < line_end) {
        const char* delim = std::find(p, line_end, '|');
        if (delim == line_end || *delim == '\n') break;

        tokens.emplace_back(p, delim);
        p = delim + 1;
    }

    return tokens;
}

// Ingest lineitem (large table, parallel)
void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Ingesting lineitem..." << std::flush;

    MappedFile file(data_dir + "/lineitem.tbl");
    const size_t num_threads = std::thread::hardware_concurrency();

    auto chunks = split_chunks(file.data, file.size, num_threads);

    // Thread-local storage
    struct LocalData {
        std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
        std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;
        std::vector<uint8_t> l_returnflag, l_linestatus;
        std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
        std::vector<std::string> l_shipinstruct, l_shipmode, l_comment;
        Dictionary returnflag_dict, linestatus_dict, shipmode_dict, shipinstruct_dict;
    };

    std::vector<LocalData> local_data(num_threads);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& ld = local_data[t];
            size_t est_rows = (chunks[t].second - chunks[t].first) / 150;
            ld.l_orderkey.reserve(est_rows);

            const char* p = file.data + chunks[t].first;
            const char* end = file.data + chunks[t].second;

            while (p < end) {
                const char* line_end = std::find(p, end, '\n');
                if (line_end == end) break;

                auto tokens = parse_line(p, line_end);
                if (tokens.size() >= 16) {
                    ld.l_orderkey.push_back(std::stoi(tokens[0]));
                    ld.l_partkey.push_back(std::stoi(tokens[1]));
                    ld.l_suppkey.push_back(std::stoi(tokens[2]));
                    ld.l_linenumber.push_back(std::stoi(tokens[3]));
                    ld.l_quantity.push_back(static_cast<int64_t>(std::stod(tokens[4]) * 100));
                    ld.l_extendedprice.push_back(static_cast<int64_t>(std::stod(tokens[5]) * 100));
                    ld.l_discount.push_back(static_cast<int64_t>(std::stod(tokens[6]) * 100));
                    ld.l_tax.push_back(static_cast<int64_t>(std::stod(tokens[7]) * 100));
                    ld.l_returnflag.push_back(ld.returnflag_dict.encode(tokens[8]));
                    ld.l_linestatus.push_back(ld.linestatus_dict.encode(tokens[9]));
                    ld.l_shipdate.push_back(date_to_days(tokens[10]));
                    ld.l_commitdate.push_back(date_to_days(tokens[11]));
                    ld.l_receiptdate.push_back(date_to_days(tokens[12]));
                    ld.l_shipinstruct.push_back(tokens[13]);
                    ld.l_shipmode.push_back(tokens[14]);
                    ld.l_comment.push_back(tokens[15]);
                }

                p = line_end + 1;
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge thread-local data
    size_t total_rows = 0;
    for (const auto& ld : local_data) {
        total_rows += ld.l_orderkey.size();
    }

    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber;
    std::vector<int64_t> l_quantity, l_extendedprice, l_discount, l_tax;
    std::vector<uint8_t> l_returnflag, l_linestatus;
    std::vector<int32_t> l_shipdate, l_commitdate, l_receiptdate;
    std::vector<std::string> l_shipinstruct, l_shipmode, l_comment;

    l_orderkey.reserve(total_rows);
    l_shipdate.reserve(total_rows);

    // Merge with indices for later sorting
    struct RowData {
        size_t orig_idx;
        int32_t shipdate;
    };
    std::vector<RowData> row_indices;
    row_indices.reserve(total_rows);

    size_t offset = 0;
    for (size_t t = 0; t < num_threads; ++t) {
        const auto& ld = local_data[t];
        for (size_t i = 0; i < ld.l_orderkey.size(); ++i) {
            row_indices.push_back({offset + i, ld.l_shipdate[i]});
        }
        offset += ld.l_orderkey.size();
    }

    // Merge dictionaries
    Dictionary returnflag_dict, linestatus_dict, shipmode_dict, shipinstruct_dict;
    std::vector<std::vector<uint8_t>> returnflag_remaps(num_threads);
    std::vector<std::vector<uint8_t>> linestatus_remaps(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        returnflag_remaps[t].resize(local_data[t].returnflag_dict.size());
        for (size_t i = 0; i < local_data[t].returnflag_dict.size(); ++i) {
            returnflag_remaps[t][i] = returnflag_dict.encode(local_data[t].returnflag_dict.decode(i));
        }

        linestatus_remaps[t].resize(local_data[t].linestatus_dict.size());
        for (size_t i = 0; i < local_data[t].linestatus_dict.size(); ++i) {
            linestatus_remaps[t][i] = linestatus_dict.encode(local_data[t].linestatus_dict.decode(i));
        }
    }

    // Concatenate all thread-local data
    offset = 0;
    for (size_t t = 0; t < num_threads; ++t) {
        const auto& ld = local_data[t];
        l_orderkey.insert(l_orderkey.end(), ld.l_orderkey.begin(), ld.l_orderkey.end());
        l_partkey.insert(l_partkey.end(), ld.l_partkey.begin(), ld.l_partkey.end());
        l_suppkey.insert(l_suppkey.end(), ld.l_suppkey.begin(), ld.l_suppkey.end());
        l_linenumber.insert(l_linenumber.end(), ld.l_linenumber.begin(), ld.l_linenumber.end());
        l_quantity.insert(l_quantity.end(), ld.l_quantity.begin(), ld.l_quantity.end());
        l_extendedprice.insert(l_extendedprice.end(), ld.l_extendedprice.begin(), ld.l_extendedprice.end());
        l_discount.insert(l_discount.end(), ld.l_discount.begin(), ld.l_discount.end());
        l_tax.insert(l_tax.end(), ld.l_tax.begin(), ld.l_tax.end());

        // Remap dictionary codes
        std::vector<uint8_t> remapped_rf, remapped_ls;
        remapped_rf.reserve(ld.l_returnflag.size());
        remapped_ls.reserve(ld.l_linestatus.size());
        for (uint8_t code : ld.l_returnflag) {
            remapped_rf.push_back(returnflag_remaps[t][code]);
        }
        for (uint8_t code : ld.l_linestatus) {
            remapped_ls.push_back(linestatus_remaps[t][code]);
        }
        l_returnflag.insert(l_returnflag.end(), remapped_rf.begin(), remapped_rf.end());
        l_linestatus.insert(l_linestatus.end(), remapped_ls.begin(), remapped_ls.end());

        l_shipdate.insert(l_shipdate.end(), ld.l_shipdate.begin(), ld.l_shipdate.end());
        l_commitdate.insert(l_commitdate.end(), ld.l_commitdate.begin(), ld.l_commitdate.end());
        l_receiptdate.insert(l_receiptdate.end(), ld.l_receiptdate.begin(), ld.l_receiptdate.end());
        l_shipinstruct.insert(l_shipinstruct.end(), ld.l_shipinstruct.begin(), ld.l_shipinstruct.end());
        l_shipmode.insert(l_shipmode.end(), ld.l_shipmode.begin(), ld.l_shipmode.end());
        l_comment.insert(l_comment.end(), ld.l_comment.begin(), ld.l_comment.end());
    }

    // Sort by l_shipdate
    std::sort(row_indices.begin(), row_indices.end(),
              [](const RowData& a, const RowData& b) { return a.shipdate < b.shipdate; });

    // Reorder all columns according to sorted indices
    auto reorder = [&row_indices](auto& vec) {
        auto copy = vec;
        for (size_t i = 0; i < row_indices.size(); ++i) {
            vec[i] = copy[row_indices[i].orig_idx];
        }
    };

    reorder(l_orderkey);
    reorder(l_partkey);
    reorder(l_suppkey);
    reorder(l_linenumber);
    reorder(l_quantity);
    reorder(l_extendedprice);
    reorder(l_discount);
    reorder(l_tax);
    reorder(l_returnflag);
    reorder(l_linestatus);
    reorder(l_shipdate);
    reorder(l_commitdate);
    reorder(l_receiptdate);
    reorder(l_shipinstruct);
    reorder(l_shipmode);
    reorder(l_comment);

    // Write to disk
    ColumnWriter::write_int32(gendb_dir + "/lineitem.l_orderkey.bin", l_orderkey);
    ColumnWriter::write_int32(gendb_dir + "/lineitem.l_partkey.bin", l_partkey);
    ColumnWriter::write_int32(gendb_dir + "/lineitem.l_suppkey.bin", l_suppkey);
    ColumnWriter::write_int32(gendb_dir + "/lineitem.l_linenumber.bin", l_linenumber);
    ColumnWriter::write_int64(gendb_dir + "/lineitem.l_quantity.bin", l_quantity);
    ColumnWriter::write_int64(gendb_dir + "/lineitem.l_extendedprice.bin", l_extendedprice);
    ColumnWriter::write_int64(gendb_dir + "/lineitem.l_discount.bin", l_discount);
    ColumnWriter::write_int64(gendb_dir + "/lineitem.l_tax.bin", l_tax);
    ColumnWriter::write_uint8(gendb_dir + "/lineitem.l_returnflag.bin", l_returnflag);
    ColumnWriter::write_uint8(gendb_dir + "/lineitem.l_linestatus.bin", l_linestatus);
    ColumnWriter::write_int32(gendb_dir + "/lineitem.l_shipdate.bin", l_shipdate);
    ColumnWriter::write_int32(gendb_dir + "/lineitem.l_commitdate.bin", l_commitdate);
    ColumnWriter::write_int32(gendb_dir + "/lineitem.l_receiptdate.bin", l_receiptdate);
    ColumnWriter::write_string(gendb_dir + "/lineitem.l_shipinstruct.bin", l_shipinstruct);
    ColumnWriter::write_string(gendb_dir + "/lineitem.l_shipmode.bin", l_shipmode);
    ColumnWriter::write_string(gendb_dir + "/lineitem.l_comment.bin", l_comment);
    ColumnWriter::write_dictionary(gendb_dir + "/lineitem.l_returnflag.dict", returnflag_dict);
    ColumnWriter::write_dictionary(gendb_dir + "/lineitem.l_linestatus.dict", linestatus_dict);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << " " << total_rows << " rows in " << duration << "s\n";
}

// Ingest orders (large table, parallel)
void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Ingesting orders..." << std::flush;

    MappedFile file(data_dir + "/orders.tbl");
    const size_t num_threads = std::thread::hardware_concurrency();
    auto chunks = split_chunks(file.data, file.size, num_threads);

    struct LocalData {
        std::vector<int32_t> o_orderkey, o_custkey, o_orderdate, o_shippriority;
        std::vector<uint8_t> o_orderstatus, o_orderpriority;
        std::vector<int64_t> o_totalprice;
        std::vector<std::string> o_clerk, o_comment;
    };

    std::vector<LocalData> local_data(num_threads);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& ld = local_data[t];
            const char* p = file.data + chunks[t].first;
            const char* end = file.data + chunks[t].second;

            while (p < end) {
                const char* line_end = std::find(p, end, '\n');
                if (line_end == end) break;

                auto tokens = parse_line(p, line_end);
                if (tokens.size() >= 9) {
                    ld.o_orderkey.push_back(std::stoi(tokens[0]));
                    ld.o_custkey.push_back(std::stoi(tokens[1]));
                    ld.o_orderstatus.push_back(tokens[2][0]);
                    ld.o_totalprice.push_back(static_cast<int64_t>(std::stod(tokens[3]) * 100));
                    ld.o_orderdate.push_back(date_to_days(tokens[4]));
                    ld.o_orderpriority.push_back(0); // Placeholder
                    ld.o_clerk.push_back(tokens[6]);
                    ld.o_shippriority.push_back(std::stoi(tokens[7]));
                    ld.o_comment.push_back(tokens[8]);
                }
                p = line_end + 1;
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge and sort by o_orderdate
    size_t total_rows = 0;
    for (const auto& ld : local_data) {
        total_rows += ld.o_orderkey.size();
    }

    std::vector<int32_t> o_orderkey, o_custkey, o_orderdate, o_shippriority;
    std::vector<uint8_t> o_orderstatus, o_orderpriority;
    std::vector<int64_t> o_totalprice;
    std::vector<std::string> o_clerk, o_comment;

    o_orderkey.reserve(total_rows);

    struct RowData {
        size_t orig_idx;
        int32_t orderdate;
    };
    std::vector<RowData> row_indices;
    row_indices.reserve(total_rows);

    size_t offset = 0;
    for (size_t t = 0; t < num_threads; ++t) {
        const auto& ld = local_data[t];
        for (size_t i = 0; i < ld.o_orderkey.size(); ++i) {
            row_indices.push_back({offset + i, ld.o_orderdate[i]});
        }
        offset += ld.o_orderkey.size();
    }

    for (size_t t = 0; t < num_threads; ++t) {
        const auto& ld = local_data[t];
        o_orderkey.insert(o_orderkey.end(), ld.o_orderkey.begin(), ld.o_orderkey.end());
        o_custkey.insert(o_custkey.end(), ld.o_custkey.begin(), ld.o_custkey.end());
        o_orderstatus.insert(o_orderstatus.end(), ld.o_orderstatus.begin(), ld.o_orderstatus.end());
        o_totalprice.insert(o_totalprice.end(), ld.o_totalprice.begin(), ld.o_totalprice.end());
        o_orderdate.insert(o_orderdate.end(), ld.o_orderdate.begin(), ld.o_orderdate.end());
        o_orderpriority.insert(o_orderpriority.end(), ld.o_orderpriority.begin(), ld.o_orderpriority.end());
        o_clerk.insert(o_clerk.end(), ld.o_clerk.begin(), ld.o_clerk.end());
        o_shippriority.insert(o_shippriority.end(), ld.o_shippriority.begin(), ld.o_shippriority.end());
        o_comment.insert(o_comment.end(), ld.o_comment.begin(), ld.o_comment.end());
    }

    std::sort(row_indices.begin(), row_indices.end(),
              [](const RowData& a, const RowData& b) { return a.orderdate < b.orderdate; });

    auto reorder = [&row_indices](auto& vec) {
        auto copy = vec;
        for (size_t i = 0; i < row_indices.size(); ++i) {
            vec[i] = copy[row_indices[i].orig_idx];
        }
    };

    reorder(o_orderkey);
    reorder(o_custkey);
    reorder(o_orderstatus);
    reorder(o_totalprice);
    reorder(o_orderdate);
    reorder(o_orderpriority);
    reorder(o_clerk);
    reorder(o_shippriority);
    reorder(o_comment);

    ColumnWriter::write_int32(gendb_dir + "/orders.o_orderkey.bin", o_orderkey);
    ColumnWriter::write_int32(gendb_dir + "/orders.o_custkey.bin", o_custkey);
    ColumnWriter::write_uint8(gendb_dir + "/orders.o_orderstatus.bin", o_orderstatus);
    ColumnWriter::write_int64(gendb_dir + "/orders.o_totalprice.bin", o_totalprice);
    ColumnWriter::write_int32(gendb_dir + "/orders.o_orderdate.bin", o_orderdate);
    ColumnWriter::write_uint8(gendb_dir + "/orders.o_orderpriority.bin", o_orderpriority);
    ColumnWriter::write_string(gendb_dir + "/orders.o_clerk.bin", o_clerk);
    ColumnWriter::write_int32(gendb_dir + "/orders.o_shippriority.bin", o_shippriority);
    ColumnWriter::write_string(gendb_dir + "/orders.o_comment.bin", o_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << " " << total_rows << " rows in " << duration << "s\n";
}

// Ingest customer (medium table, parallel)
void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Ingesting customer..." << std::flush;

    MappedFile file(data_dir + "/customer.tbl");
    const size_t num_threads = 32; // Medium parallelism
    auto chunks = split_chunks(file.data, file.size, num_threads);

    struct LocalData {
        std::vector<int32_t> c_custkey, c_nationkey;
        std::vector<uint8_t> c_mktsegment;
        std::vector<int64_t> c_acctbal;
        std::vector<std::string> c_name, c_address, c_phone, c_comment;
        Dictionary mktsegment_dict;
    };

    std::vector<LocalData> local_data(num_threads);
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& ld = local_data[t];
            const char* p = file.data + chunks[t].first;
            const char* end = file.data + chunks[t].second;

            while (p < end) {
                const char* line_end = std::find(p, end, '\n');
                if (line_end == end) break;

                auto tokens = parse_line(p, line_end);
                if (tokens.size() >= 8) {
                    ld.c_custkey.push_back(std::stoi(tokens[0]));
                    ld.c_name.push_back(tokens[1]);
                    ld.c_address.push_back(tokens[2]);
                    ld.c_nationkey.push_back(std::stoi(tokens[3]));
                    ld.c_phone.push_back(tokens[4]);
                    ld.c_acctbal.push_back(static_cast<int64_t>(std::stod(tokens[5]) * 100));
                    ld.c_mktsegment.push_back(ld.mktsegment_dict.encode(tokens[6]));
                    ld.c_comment.push_back(tokens[7]);
                }
                p = line_end + 1;
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge dictionaries and data
    Dictionary mktsegment_dict;
    std::vector<std::vector<uint8_t>> mktsegment_remaps(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        mktsegment_remaps[t].resize(local_data[t].mktsegment_dict.size());
        for (size_t i = 0; i < local_data[t].mktsegment_dict.size(); ++i) {
            mktsegment_remaps[t][i] = mktsegment_dict.encode(local_data[t].mktsegment_dict.decode(i));
        }
    }

    size_t total_rows = 0;
    for (const auto& ld : local_data) {
        total_rows += ld.c_custkey.size();
    }

    std::vector<int32_t> c_custkey, c_nationkey;
    std::vector<uint8_t> c_mktsegment;
    std::vector<int64_t> c_acctbal;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;

    for (size_t t = 0; t < num_threads; ++t) {
        const auto& ld = local_data[t];
        c_custkey.insert(c_custkey.end(), ld.c_custkey.begin(), ld.c_custkey.end());
        c_nationkey.insert(c_nationkey.end(), ld.c_nationkey.begin(), ld.c_nationkey.end());

        std::vector<uint8_t> remapped;
        for (uint8_t code : ld.c_mktsegment) {
            remapped.push_back(mktsegment_remaps[t][code]);
        }
        c_mktsegment.insert(c_mktsegment.end(), remapped.begin(), remapped.end());

        c_acctbal.insert(c_acctbal.end(), ld.c_acctbal.begin(), ld.c_acctbal.end());
        c_name.insert(c_name.end(), ld.c_name.begin(), ld.c_name.end());
        c_address.insert(c_address.end(), ld.c_address.begin(), ld.c_address.end());
        c_phone.insert(c_phone.end(), ld.c_phone.begin(), ld.c_phone.end());
        c_comment.insert(c_comment.end(), ld.c_comment.begin(), ld.c_comment.end());
    }

    ColumnWriter::write_int32(gendb_dir + "/customer.c_custkey.bin", c_custkey);
    ColumnWriter::write_string(gendb_dir + "/customer.c_name.bin", c_name);
    ColumnWriter::write_string(gendb_dir + "/customer.c_address.bin", c_address);
    ColumnWriter::write_int32(gendb_dir + "/customer.c_nationkey.bin", c_nationkey);
    ColumnWriter::write_string(gendb_dir + "/customer.c_phone.bin", c_phone);
    ColumnWriter::write_int64(gendb_dir + "/customer.c_acctbal.bin", c_acctbal);
    ColumnWriter::write_uint8(gendb_dir + "/customer.c_mktsegment.bin", c_mktsegment);
    ColumnWriter::write_string(gendb_dir + "/customer.c_comment.bin", c_comment);
    ColumnWriter::write_dictionary(gendb_dir + "/customer.c_mktsegment.dict", mktsegment_dict);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << " " << total_rows << " rows in " << duration << "s\n";
}

// Simple sequential ingestion for small tables
void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Ingesting nation..." << std::flush;

    std::ifstream file(data_dir + "/nation.tbl");
    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream ss(line);
        std::string token;
        std::vector<std::string> tokens;

        while (std::getline(ss, token, '|')) {
            tokens.push_back(token);
        }

        if (tokens.size() >= 4) {
            n_nationkey.push_back(std::stoi(tokens[0]));
            n_name.push_back(tokens[1]);
            n_regionkey.push_back(std::stoi(tokens[2]));
            n_comment.push_back(tokens[3]);
        }
    }

    ColumnWriter::write_int32(gendb_dir + "/nation.n_nationkey.bin", n_nationkey);
    ColumnWriter::write_string(gendb_dir + "/nation.n_name.bin", n_name);
    ColumnWriter::write_int32(gendb_dir + "/nation.n_regionkey.bin", n_regionkey);
    ColumnWriter::write_string(gendb_dir + "/nation.n_comment.bin", n_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << " " << n_nationkey.size() << " rows in " << duration << "ms\n";
}

void ingest_region(const std::string& data_dir, const std::string& gendb_dir) {
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Ingesting region..." << std::flush;

    std::ifstream file(data_dir + "/region.tbl");
    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream ss(line);
        std::string token;
        std::vector<std::string> tokens;

        while (std::getline(ss, token, '|')) {
            tokens.push_back(token);
        }

        if (tokens.size() >= 3) {
            r_regionkey.push_back(std::stoi(tokens[0]));
            r_name.push_back(tokens[1]);
            r_comment.push_back(tokens[2]);
        }
    }

    ColumnWriter::write_int32(gendb_dir + "/region.r_regionkey.bin", r_regionkey);
    ColumnWriter::write_string(gendb_dir + "/region.r_name.bin", r_name);
    ColumnWriter::write_string(gendb_dir + "/region.r_comment.bin", r_comment);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << " " << r_regionkey.size() << " rows in " << duration << "ms\n";
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>\n";
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create gendb directory
    system(("mkdir -p " + gendb_dir).c_str());

    // Parallel table ingestion (all tables concurrently)
    std::vector<std::thread> table_threads;

    table_threads.emplace_back([&]() { ingest_lineitem(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_orders(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_customer(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_nation(data_dir, gendb_dir); });
    table_threads.emplace_back([&]() { ingest_region(data_dir, gendb_dir); });

    for (auto& th : table_threads) {
        th.join();
    }

    std::cout << "Ingestion complete.\n";
    return 0;
}
