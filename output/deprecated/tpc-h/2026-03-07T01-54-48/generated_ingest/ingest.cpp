#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

enum class ColKind { Int32, Double, Date, VarlenString, DictString };

struct ColumnSpec {
    std::string name;
    ColKind kind;
};

struct TableSpec {
    std::string name;
    std::string input_file;
    std::vector<ColumnSpec> columns;
};

static int32_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int>(doe) - 719468;
}

static int32_t parse_date_days(const std::string& s) {
    int y = std::stoi(s.substr(0, 4));
    unsigned m = static_cast<unsigned>(std::stoi(s.substr(5, 2)));
    unsigned d = static_cast<unsigned>(std::stoi(s.substr(8, 2)));
    return days_from_civil(y, m, d);
}

static std::vector<std::string> split_tpch_line(const std::string& line) {
    std::vector<std::string> out;
    out.reserve(20);
    size_t start = 0;
    for (size_t i = 0; i < line.size(); ++i) {
        if (line[i] == '|') {
            out.emplace_back(line.substr(start, i - start));
            start = i + 1;
        }
    }
    return out;
}

struct VarlenWriter {
    std::ofstream off;
    std::ofstream dat;
    uint64_t cursor = 0;

    VarlenWriter(const fs::path& dir, const std::string& col) {
        off.open(dir / (col + ".off"), std::ios::binary);
        dat.open(dir / (col + ".dat"), std::ios::binary);
        if (!off || !dat) {
            throw std::runtime_error("failed opening varlen files for " + col);
        }
        off.write(reinterpret_cast<const char*>(&cursor), sizeof(cursor));
    }

    void append(const std::string& s) {
        dat.write(s.data(), static_cast<std::streamsize>(s.size()));
        cursor += static_cast<uint64_t>(s.size());
        off.write(reinterpret_cast<const char*>(&cursor), sizeof(cursor));
    }
};

static void write_dict(const fs::path& dir, const std::string& col, const std::vector<std::string>& values) {
    std::ofstream dictf(dir / (col + ".dict"), std::ios::binary);
    if (!dictf) {
        throw std::runtime_error("failed opening dict for " + col);
    }
    uint32_t n = static_cast<uint32_t>(values.size());
    dictf.write(reinterpret_cast<const char*>(&n), sizeof(n));
    for (const auto& v : values) {
        uint32_t len = static_cast<uint32_t>(v.size());
        dictf.write(reinterpret_cast<const char*>(&len), sizeof(len));
        dictf.write(v.data(), static_cast<std::streamsize>(v.size()));
    }
}

static void ingest_table(const TableSpec& spec, const fs::path& src_dir, const fs::path& dst_dir) {
    const auto begin = std::chrono::steady_clock::now();

    fs::create_directories(dst_dir / spec.name);
    fs::path out_dir = dst_dir / spec.name;

    std::ifstream in(src_dir / spec.input_file);
    if (!in) {
        throw std::runtime_error("failed opening input file: " + (src_dir / spec.input_file).string());
    }

    std::vector<std::ofstream> fixed_out(spec.columns.size());
    std::vector<std::unique_ptr<VarlenWriter>> varlen_out(spec.columns.size());
    std::vector<std::unordered_map<std::string, uint32_t>> dict_map(spec.columns.size());
    std::vector<std::vector<std::string>> dict_vals(spec.columns.size());

    for (size_t i = 0; i < spec.columns.size(); ++i) {
        const auto& c = spec.columns[i];
        if (c.kind == ColKind::Int32 || c.kind == ColKind::Date) {
            fixed_out[i].open(out_dir / (c.name + ".bin"), std::ios::binary);
            if (!fixed_out[i]) throw std::runtime_error("failed opening " + c.name);
        } else if (c.kind == ColKind::Double) {
            fixed_out[i].open(out_dir / (c.name + ".bin"), std::ios::binary);
            if (!fixed_out[i]) throw std::runtime_error("failed opening " + c.name);
        } else if (c.kind == ColKind::VarlenString) {
            varlen_out[i] = std::make_unique<VarlenWriter>(out_dir, c.name);
        } else if (c.kind == ColKind::DictString) {
            fixed_out[i].open(out_dir / (c.name + ".bin"), std::ios::binary);
            if (!fixed_out[i]) throw std::runtime_error("failed opening " + c.name);
        }
    }

    std::string line;
    uint64_t rows = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        auto fields = split_tpch_line(line);
        if (fields.size() < spec.columns.size()) {
            throw std::runtime_error("field count mismatch in table " + spec.name);
        }

        for (size_t i = 0; i < spec.columns.size(); ++i) {
            const auto& c = spec.columns[i];
            const std::string& f = fields[i];
            if (c.kind == ColKind::Int32) {
                int32_t v = std::stoi(f);
                fixed_out[i].write(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (c.kind == ColKind::Double) {
                double v = std::stod(f);
                fixed_out[i].write(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (c.kind == ColKind::Date) {
                int32_t v = parse_date_days(f);
                fixed_out[i].write(reinterpret_cast<const char*>(&v), sizeof(v));
            } else if (c.kind == ColKind::VarlenString) {
                varlen_out[i]->append(f);
            } else if (c.kind == ColKind::DictString) {
                auto it = dict_map[i].find(f);
                uint32_t code;
                if (it == dict_map[i].end()) {
                    code = static_cast<uint32_t>(dict_vals[i].size());
                    dict_vals[i].push_back(f);
                    dict_map[i].emplace(f, code);
                } else {
                    code = it->second;
                }
                fixed_out[i].write(reinterpret_cast<const char*>(&code), sizeof(code));
            }
        }
        ++rows;
    }

    for (size_t i = 0; i < spec.columns.size(); ++i) {
        if (spec.columns[i].kind == ColKind::DictString) {
            write_dict(out_dir, spec.columns[i].name, dict_vals[i]);
        }
    }

    std::ofstream meta(out_dir / "rows.txt");
    meta << rows << "\n";

    const auto end = std::chrono::steady_clock::now();
    const double sec = std::chrono::duration<double>(end - begin).count();
    std::cerr << "ingested " << std::setw(10) << spec.name << " rows=" << rows << " in "
              << std::fixed << std::setprecision(2) << sec << "s\n";
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: ingest <tpch_data_dir> <gendb_out_dir>\n";
        return 1;
    }

    fs::path src_dir = argv[1];
    fs::path dst_dir = argv[2];
    fs::create_directories(dst_dir);

    const std::vector<TableSpec> specs = {
        {"nation", "nation.tbl", {
            {"n_nationkey", ColKind::Int32}, {"n_name", ColKind::DictString}, {"n_regionkey", ColKind::Int32}, {"n_comment", ColKind::VarlenString}
        }},
        {"region", "region.tbl", {
            {"r_regionkey", ColKind::Int32}, {"r_name", ColKind::VarlenString}, {"r_comment", ColKind::VarlenString}
        }},
        {"supplier", "supplier.tbl", {
            {"s_suppkey", ColKind::Int32}, {"s_name", ColKind::VarlenString}, {"s_address", ColKind::VarlenString}, {"s_nationkey", ColKind::Int32},
            {"s_phone", ColKind::VarlenString}, {"s_acctbal", ColKind::Double}, {"s_comment", ColKind::VarlenString}
        }},
        {"part", "part.tbl", {
            {"p_partkey", ColKind::Int32}, {"p_name", ColKind::VarlenString}, {"p_mfgr", ColKind::VarlenString}, {"p_brand", ColKind::VarlenString},
            {"p_type", ColKind::VarlenString}, {"p_size", ColKind::Int32}, {"p_container", ColKind::VarlenString}, {"p_retailprice", ColKind::Double},
            {"p_comment", ColKind::VarlenString}
        }},
        {"partsupp", "partsupp.tbl", {
            {"ps_partkey", ColKind::Int32}, {"ps_suppkey", ColKind::Int32}, {"ps_availqty", ColKind::Int32}, {"ps_supplycost", ColKind::Double},
            {"ps_comment", ColKind::VarlenString}
        }},
        {"customer", "customer.tbl", {
            {"c_custkey", ColKind::Int32}, {"c_name", ColKind::VarlenString}, {"c_address", ColKind::VarlenString}, {"c_nationkey", ColKind::Int32},
            {"c_phone", ColKind::VarlenString}, {"c_acctbal", ColKind::Double}, {"c_mktsegment", ColKind::DictString}, {"c_comment", ColKind::VarlenString}
        }},
        {"orders", "orders.tbl", {
            {"o_orderkey", ColKind::Int32}, {"o_custkey", ColKind::Int32}, {"o_orderstatus", ColKind::VarlenString}, {"o_totalprice", ColKind::Double},
            {"o_orderdate", ColKind::Date}, {"o_orderpriority", ColKind::VarlenString}, {"o_clerk", ColKind::VarlenString}, {"o_shippriority", ColKind::Int32},
            {"o_comment", ColKind::VarlenString}
        }},
        {"lineitem", "lineitem.tbl", {
            {"l_orderkey", ColKind::Int32}, {"l_partkey", ColKind::Int32}, {"l_suppkey", ColKind::Int32}, {"l_linenumber", ColKind::Int32},
            {"l_quantity", ColKind::Double}, {"l_extendedprice", ColKind::Double}, {"l_discount", ColKind::Double}, {"l_tax", ColKind::Double},
            {"l_returnflag", ColKind::DictString}, {"l_linestatus", ColKind::DictString}, {"l_shipdate", ColKind::Date}, {"l_commitdate", ColKind::Date},
            {"l_receiptdate", ColKind::Date}, {"l_shipinstruct", ColKind::VarlenString}, {"l_shipmode", ColKind::VarlenString}, {"l_comment", ColKind::VarlenString}
        }}
    };

    const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
    const size_t max_parallel = std::min<size_t>(specs.size(), std::max<size_t>(2, hw / 8));

    std::vector<std::future<void>> inflight;
    for (const auto& spec : specs) {
        if (inflight.size() >= max_parallel) {
            inflight.front().get();
            inflight.erase(inflight.begin());
        }
        inflight.push_back(std::async(std::launch::async, [&, spec] {
            ingest_table(spec, src_dir, dst_dir);
        }));
    }
    for (auto& f : inflight) f.get();

    std::cerr << "ingestion complete\n";
    return 0;
}
