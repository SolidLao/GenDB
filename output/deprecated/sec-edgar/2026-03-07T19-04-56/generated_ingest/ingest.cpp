#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <numeric>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <chrono>
#include <thread>
#include <mutex>
#include <sys/stat.h>
#include <filesystem>

namespace fs = std::filesystem;

// ============================================================
// Utility: CSV parsing
// ============================================================
// Parse a CSV line handling quoted fields. Returns vector of string_views into buf.
static void parseCsvLine(const std::string& line, std::vector<std::string>& fields) {
    fields.clear();
    size_t i = 0, n = line.size();
    while (i <= n) {
        if (i < n && line[i] == '"') {
            // Quoted field
            std::string field;
            i++; // skip opening quote
            while (i < n) {
                if (line[i] == '"') {
                    if (i + 1 < n && line[i+1] == '"') {
                        field += '"';
                        i += 2;
                    } else {
                        i++; // skip closing quote
                        break;
                    }
                } else {
                    field += line[i++];
                }
            }
            if (i < n && line[i] == ',') i++; // skip delimiter
            fields.push_back(std::move(field));
        } else {
            // Unquoted field
            size_t start = i;
            while (i < n && line[i] != ',') i++;
            fields.push_back(line.substr(start, i - start));
            if (i < n) i++; // skip comma
            else { break; }
        }
    }
}

// ============================================================
// Dictionary: string -> uint32 code
// ============================================================
class StringDict {
public:
    std::unordered_map<std::string, uint32_t> map_;
    std::vector<std::string> strings_;
    std::mutex mtx_;

    uint32_t encode(const std::string& s) {
        auto it = map_.find(s);
        if (it != map_.end()) return it->second;
        uint32_t code = (uint32_t)strings_.size();
        map_[s] = code;
        strings_.push_back(s);
        return code;
    }

    uint32_t lookup(const std::string& s) const {
        auto it = map_.find(s);
        if (it != map_.end()) return it->second;
        return UINT32_MAX;
    }

    uint32_t encodeThreadSafe(const std::string& s) {
        std::lock_guard<std::mutex> lock(mtx_);
        return encode(s);
    }

    size_t size() const { return strings_.size(); }

    void save(const std::string& dir, const std::string& prefix) const {
        // Write offsets (uint64) and data (raw bytes)
        std::string offPath = dir + "/" + prefix + "_offsets.bin";
        std::string datPath = dir + "/" + prefix + "_data.bin";
        // Compute total data size
        size_t totalBytes = 0;
        for (auto& s : strings_) totalBytes += s.size();
        // Write offsets
        {
            std::ofstream f(offPath, std::ios::binary);
            std::vector<uint64_t> offsets(strings_.size() + 1);
            uint64_t off = 0;
            for (size_t i = 0; i < strings_.size(); i++) {
                offsets[i] = off;
                off += strings_[i].size();
            }
            offsets[strings_.size()] = off;
            f.write((const char*)offsets.data(), offsets.size() * sizeof(uint64_t));
        }
        // Write data
        {
            std::ofstream f(datPath, std::ios::binary);
            for (auto& s : strings_) {
                f.write(s.data(), s.size());
            }
        }
    }
};

// ============================================================
// Binary column writers
// ============================================================
template<typename T>
void writeColumn(const std::string& path, const std::vector<T>& data) {
    std::ofstream f(path, std::ios::binary);
    if (!f) { std::cerr << "Failed to open: " << path << "\n"; exit(1); }
    f.write((const char*)data.data(), data.size() * sizeof(T));
}

void writeVarlenColumn(const std::string& dir, const std::string& name,
                       const std::vector<std::string>& data) {
    std::string offPath = dir + "/" + name + "_offsets.bin";
    std::string datPath = dir + "/" + name + "_data.bin";
    std::vector<uint64_t> offsets(data.size() + 1);
    uint64_t off = 0;
    for (size_t i = 0; i < data.size(); i++) {
        offsets[i] = off;
        off += data[i].size();
    }
    offsets[data.size()] = off;
    {
        std::ofstream f(offPath, std::ios::binary);
        f.write((const char*)offsets.data(), offsets.size() * sizeof(uint64_t));
    }
    {
        std::ofstream f(datPath, std::ios::binary);
        for (auto& s : data) f.write(s.data(), s.size());
    }
}

void writeRowCount(const std::string& dir, uint64_t count) {
    std::ofstream f(dir + "/_row_count.bin", std::ios::binary);
    f.write((const char*)&count, sizeof(uint64_t));
}

// ============================================================
// Read all lines from file (fast: read entire file, split by newline)
// ============================================================
static std::vector<std::string> readAllLines(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "Cannot open: " << path << "\n"; exit(1); }
    size_t sz = f.tellg();
    f.seekg(0);
    std::string buf(sz, '\0');
    f.read(&buf[0], sz);

    std::vector<std::string> lines;
    lines.reserve(sz / 80); // rough estimate
    size_t start = 0;
    for (size_t i = 0; i < sz; i++) {
        if (buf[i] == '\n') {
            // Strip trailing \r
            size_t end = i;
            if (end > start && buf[end-1] == '\r') end--;
            lines.push_back(buf.substr(start, end - start));
            start = i + 1;
        }
    }
    if (start < sz) {
        size_t end = sz;
        if (end > start && buf[end-1] == '\r') end--;
        lines.push_back(buf.substr(start, end - start));
    }
    return lines;
}

// ============================================================
// Sort by permutation
// ============================================================
template<typename T>
std::vector<T> applyPermutation(const std::vector<T>& data, const std::vector<uint32_t>& perm) {
    std::vector<T> out(data.size());
    for (size_t i = 0; i < perm.size(); i++) {
        out[i] = data[perm[i]];
    }
    return out;
}

// Specialization for strings (use move)
static std::vector<std::string> applyPermutationStr(std::vector<std::string>& data,
                                                     const std::vector<uint32_t>& perm) {
    std::vector<std::string> out(data.size());
    for (size_t i = 0; i < perm.size(); i++) {
        out[i] = std::move(data[perm[i]]);
    }
    return out;
}

// ============================================================
// Main ingestion
// ============================================================
int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ingest <data_dir> <output_dir>\n";
        return 1;
    }
    std::string dataDir = argv[1];
    std::string outDir = argv[2];

    auto t0 = std::chrono::steady_clock::now();

    // Create directories
    fs::create_directories(outDir + "/sub");
    fs::create_directories(outDir + "/num");
    fs::create_directories(outDir + "/tag");
    fs::create_directories(outDir + "/pre");
    fs::create_directories(outDir + "/dicts");
    fs::create_directories(outDir + "/indexes");

    // Shared dictionaries
    StringDict tagDict, versionDict;

    // ============================================================
    // Phase 1: Ingest TAG table (builds tag/version dictionaries)
    // ============================================================
    std::cerr << "Ingesting tag table...\n";
    auto tTag0 = std::chrono::steady_clock::now();
    {
        auto lines = readAllLines(dataDir + "/tag.csv");
        size_t N = lines.size() - 1; // skip header
        std::cerr << "  tag rows: " << N << "\n";

        std::vector<uint32_t> tag_code(N), version_code(N);
        std::vector<int8_t> abstract_col(N);
        std::vector<std::string> tlabel_col(N);

        std::vector<std::string> fields;
        for (size_t i = 0; i < N; i++) {
            parseCsvLine(lines[i + 1], fields);
            // columns: tag(0), version(1), custom(2), abstract(3), datatype(4), iord(5), crdr(6), tlabel(7), doc(8)
            tag_code[i] = tagDict.encode(fields[0]);
            version_code[i] = versionDict.encode(fields[1]);
            abstract_col[i] = fields.size() > 3 && !fields[3].empty() ? (int8_t)std::stoi(fields[3]) : 0;
            tlabel_col[i] = fields.size() > 7 ? fields[7] : "";
        }

        writeColumn(outDir + "/tag/tag_code.bin", tag_code);
        writeColumn(outDir + "/tag/version_code.bin", version_code);
        writeColumn(outDir + "/tag/abstract.bin", abstract_col);
        writeVarlenColumn(outDir + "/tag", "tlabel", tlabel_col);
        writeRowCount(outDir + "/tag", N);
    }
    auto tTag1 = std::chrono::steady_clock::now();
    std::cerr << "  tag done in " << std::chrono::duration_cast<std::chrono::milliseconds>(tTag1 - tTag0).count() << " ms\n";
    std::cerr << "  tag_dict size: " << tagDict.size() << ", version_dict size: " << versionDict.size() << "\n";

    // ============================================================
    // Phase 2: Ingest SUB table (builds adsh -> row index map)
    // ============================================================
    std::cerr << "Ingesting sub table...\n";
    auto tSub0 = std::chrono::steady_clock::now();
    std::unordered_map<std::string, uint32_t> adshMap;
    {
        auto lines = readAllLines(dataDir + "/sub.csv");
        size_t N = lines.size() - 1;
        std::cerr << "  sub rows: " << N << "\n";

        // All sub columns
        std::vector<std::string> adsh_col(N), name_col(N), countryba_col(N), stprba_col(N);
        std::vector<std::string> cityba_col(N), countryinc_col(N), form_col(N), fp_col(N);
        std::vector<std::string> accepted_col(N), afs_col(N), fye_col(N), instance_col(N);
        std::vector<int32_t> cik_col(N), period_col(N), filed_col(N);
        std::vector<int16_t> sic_col(N), fy_col(N), nciks_col(N);
        std::vector<int8_t> prevrpt_col(N), wksi_col(N);

        adshMap.reserve(N);
        std::vector<std::string> fields;

        for (size_t i = 0; i < N; i++) {
            parseCsvLine(lines[i + 1], fields);
            // adsh(0),cik(1),name(2),sic(3),countryba(4),stprba(5),cityba(6),countryinc(7),
            // form(8),period(9),fy(10),fp(11),filed(12),accepted(13),prevrpt(14),nciks(15),
            // afs(16),wksi(17),fye(18),instance(19)
            adsh_col[i] = fields[0];
            adshMap[fields[0]] = (uint32_t)i;
            cik_col[i] = !fields[1].empty() ? std::stoi(fields[1]) : 0;
            name_col[i] = fields[2];
            sic_col[i] = !fields[3].empty() ? (int16_t)std::stoi(fields[3]) : 0;
            countryba_col[i] = fields.size() > 4 ? fields[4] : "";
            stprba_col[i] = fields.size() > 5 ? fields[5] : "";
            cityba_col[i] = fields.size() > 6 ? fields[6] : "";
            countryinc_col[i] = fields.size() > 7 ? fields[7] : "";
            form_col[i] = fields.size() > 8 ? fields[8] : "";
            period_col[i] = fields.size() > 9 && !fields[9].empty() ? std::stoi(fields[9]) : 0;
            fy_col[i] = fields.size() > 10 && !fields[10].empty() ? (int16_t)std::stoi(fields[10]) : 0;
            fp_col[i] = fields.size() > 11 ? fields[11] : "";
            filed_col[i] = fields.size() > 12 && !fields[12].empty() ? std::stoi(fields[12]) : 0;
            accepted_col[i] = fields.size() > 13 ? fields[13] : "";
            prevrpt_col[i] = fields.size() > 14 && !fields[14].empty() ? (int8_t)std::stoi(fields[14]) : 0;
            nciks_col[i] = fields.size() > 15 && !fields[15].empty() ? (int16_t)std::stoi(fields[15]) : 0;
            afs_col[i] = fields.size() > 16 ? fields[16] : "";
            wksi_col[i] = fields.size() > 17 && !fields[17].empty() ? (int8_t)std::stoi(fields[17]) : 0;
            fye_col[i] = fields.size() > 18 ? fields[18] : "";
            instance_col[i] = fields.size() > 19 ? fields[19] : "";
        }

        writeVarlenColumn(outDir + "/sub", "adsh", adsh_col);
        writeColumn(outDir + "/sub/cik.bin", cik_col);
        writeVarlenColumn(outDir + "/sub", "name", name_col);
        writeColumn(outDir + "/sub/sic.bin", sic_col);
        writeVarlenColumn(outDir + "/sub", "countryba", countryba_col);
        writeVarlenColumn(outDir + "/sub", "stprba", stprba_col);
        writeVarlenColumn(outDir + "/sub", "cityba", cityba_col);
        writeVarlenColumn(outDir + "/sub", "countryinc", countryinc_col);
        writeVarlenColumn(outDir + "/sub", "form", form_col);
        writeColumn(outDir + "/sub/period.bin", period_col);
        writeColumn(outDir + "/sub/fy.bin", fy_col);
        writeVarlenColumn(outDir + "/sub", "fp", fp_col);
        writeColumn(outDir + "/sub/filed.bin", filed_col);
        writeVarlenColumn(outDir + "/sub", "accepted", accepted_col);
        writeColumn(outDir + "/sub/prevrpt.bin", prevrpt_col);
        writeColumn(outDir + "/sub/nciks.bin", nciks_col);
        writeVarlenColumn(outDir + "/sub", "afs", afs_col);
        writeColumn(outDir + "/sub/wksi.bin", wksi_col);
        writeVarlenColumn(outDir + "/sub", "fye", fye_col);
        writeVarlenColumn(outDir + "/sub", "instance", instance_col);
        writeRowCount(outDir + "/sub", N);
    }
    auto tSub1 = std::chrono::steady_clock::now();
    std::cerr << "  sub done in " << std::chrono::duration_cast<std::chrono::milliseconds>(tSub1 - tSub0).count() << " ms\n";

    // ============================================================
    // Phase 3: Ingest NUM table (parallel parse, sort by uom_code, sub_fk)
    // ============================================================
    std::cerr << "Ingesting num table...\n";
    auto tNum0 = std::chrono::steady_clock::now();

    StringDict uomDict;
    {
        // Read all lines
        std::cerr << "  Reading num.csv...\n";
        auto lines = readAllLines(dataDir + "/num.csv");
        size_t N = lines.size() - 1;
        std::cerr << "  num rows: " << N << "\n";

        std::vector<uint32_t> sub_fk(N), tag_code(N), version_code(N);
        std::vector<uint8_t> uom_code(N);
        std::vector<int32_t> ddate(N);
        std::vector<double> value(N);

        // Parse in parallel using chunks
        unsigned nThreads = std::min((unsigned)std::thread::hardware_concurrency(), 64u);
        size_t chunkSize = (N + nThreads - 1) / nThreads;

        std::cerr << "  Parsing with " << nThreads << " threads...\n";
        // Pre-encode all strings in single-threaded pass for dictionaries
        // Actually, use thread-safe encoding
        std::vector<std::thread> threads;
        for (unsigned t = 0; t < nThreads; t++) {
            size_t start = t * chunkSize;
            size_t end = std::min(start + chunkSize, N);
            if (start >= N) break;

            threads.emplace_back([&, start, end]() {
                std::vector<std::string> fields;
                for (size_t i = start; i < end; i++) {
                    parseCsvLine(lines[i + 1], fields);
                    // adsh(0), tag(1), version(2), ddate(3), qtrs(4), uom(5), coreg(6), value(7), footnote(8)

                    // Resolve adsh -> sub FK
                    auto it = adshMap.find(fields[0]);
                    sub_fk[i] = (it != adshMap.end()) ? it->second : UINT32_MAX;

                    tag_code[i] = tagDict.encodeThreadSafe(fields[1]);
                    version_code[i] = versionDict.encodeThreadSafe(fields[2]);
                    ddate[i] = !fields[3].empty() ? std::stoi(fields[3]) : 0;
                    uom_code[i] = (uint8_t)uomDict.encodeThreadSafe(fields[5]);

                    if (fields.size() > 7 && !fields[7].empty()) {
                        try { value[i] = std::stod(fields[7]); }
                        catch (...) { value[i] = std::nan(""); }
                    } else {
                        value[i] = std::nan("");
                    }
                }
            });
        }
        for (auto& t : threads) t.join();

        // Free lines memory
        lines.clear();
        lines.shrink_to_fit();

        std::cerr << "  Sorting num by (uom_code, sub_fk)...\n";
        // Create permutation and sort
        std::vector<uint32_t> perm(N);
        std::iota(perm.begin(), perm.end(), 0);
        std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
            if (uom_code[a] != uom_code[b]) return uom_code[a] < uom_code[b];
            return sub_fk[a] < sub_fk[b];
        });

        // Apply permutation and write
        std::cerr << "  Writing num columns...\n";
        {
            auto sorted = applyPermutation(sub_fk, perm);
            writeColumn(outDir + "/num/sub_fk.bin", sorted);
        }
        {
            auto sorted = applyPermutation(tag_code, perm);
            writeColumn(outDir + "/num/tag_code.bin", sorted);
        }
        {
            auto sorted = applyPermutation(version_code, perm);
            writeColumn(outDir + "/num/version_code.bin", sorted);
        }
        {
            auto sorted = applyPermutation(uom_code, perm);
            writeColumn(outDir + "/num/uom_code.bin", sorted);
        }
        {
            auto sorted = applyPermutation(ddate, perm);
            writeColumn(outDir + "/num/ddate.bin", sorted);
        }
        {
            auto sorted = applyPermutation(value, perm);
            writeColumn(outDir + "/num/value.bin", sorted);
        }

        writeRowCount(outDir + "/num", N);

        // Write uom dictionary
        uomDict.save(outDir + "/num", "uom_dict");
    }
    auto tNum1 = std::chrono::steady_clock::now();
    std::cerr << "  num done in " << std::chrono::duration_cast<std::chrono::milliseconds>(tNum1 - tNum0).count() << " ms\n";
    std::cerr << "  uom_dict size: " << uomDict.size() << "\n";

    // ============================================================
    // Phase 4: Ingest PRE table (parallel parse, sort by stmt_code, sub_fk)
    // ============================================================
    std::cerr << "Ingesting pre table...\n";
    auto tPre0 = std::chrono::steady_clock::now();

    StringDict stmtDict, rfileDict;
    {
        auto lines = readAllLines(dataDir + "/pre.csv");
        size_t N = lines.size() - 1;
        std::cerr << "  pre rows: " << N << "\n";

        std::vector<uint32_t> sub_fk(N), tag_code(N), version_code(N);
        std::vector<uint8_t> stmt_code(N), rfile_code(N);
        std::vector<int16_t> line_col(N);
        std::vector<std::string> plabel_col(N);

        unsigned nThreads = std::min((unsigned)std::thread::hardware_concurrency(), 64u);
        size_t chunkSize = (N + nThreads - 1) / nThreads;

        std::cerr << "  Parsing with " << nThreads << " threads...\n";
        std::vector<std::thread> threads;
        for (unsigned t = 0; t < nThreads; t++) {
            size_t start = t * chunkSize;
            size_t end = std::min(start + chunkSize, N);
            if (start >= N) break;

            threads.emplace_back([&, start, end]() {
                std::vector<std::string> fields;
                for (size_t i = start; i < end; i++) {
                    parseCsvLine(lines[i + 1], fields);
                    // adsh(0), report(1), line(2), stmt(3), inpth(4), rfile(5), tag(6), version(7), plabel(8), negating(9)

                    auto it = adshMap.find(fields[0]);
                    sub_fk[i] = (it != adshMap.end()) ? it->second : UINT32_MAX;

                    tag_code[i] = tagDict.encodeThreadSafe(fields[6]);
                    version_code[i] = versionDict.encodeThreadSafe(fields[7]);
                    stmt_code[i] = (uint8_t)stmtDict.encodeThreadSafe(fields[3]);
                    rfile_code[i] = (uint8_t)rfileDict.encodeThreadSafe(fields[5]);
                    line_col[i] = !fields[2].empty() ? (int16_t)std::stoi(fields[2]) : 0;
                    plabel_col[i] = fields.size() > 8 ? fields[8] : "";
                }
            });
        }
        for (auto& t : threads) t.join();

        lines.clear();
        lines.shrink_to_fit();

        std::cerr << "  Sorting pre by (stmt_code, sub_fk)...\n";
        std::vector<uint32_t> perm(N);
        std::iota(perm.begin(), perm.end(), 0);
        std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
            if (stmt_code[a] != stmt_code[b]) return stmt_code[a] < stmt_code[b];
            return sub_fk[a] < sub_fk[b];
        });

        std::cerr << "  Writing pre columns...\n";
        {
            auto sorted = applyPermutation(sub_fk, perm);
            writeColumn(outDir + "/pre/sub_fk.bin", sorted);
        }
        {
            auto sorted = applyPermutation(tag_code, perm);
            writeColumn(outDir + "/pre/tag_code.bin", sorted);
        }
        {
            auto sorted = applyPermutation(version_code, perm);
            writeColumn(outDir + "/pre/version_code.bin", sorted);
        }
        {
            auto sorted = applyPermutation(stmt_code, perm);
            writeColumn(outDir + "/pre/stmt_code.bin", sorted);
        }
        {
            auto sorted = applyPermutation(rfile_code, perm);
            writeColumn(outDir + "/pre/rfile_code.bin", sorted);
        }
        {
            auto sorted = applyPermutation(line_col, perm);
            writeColumn(outDir + "/pre/line.bin", sorted);
        }
        {
            auto sorted = applyPermutationStr(plabel_col, perm);
            writeVarlenColumn(outDir + "/pre", "plabel", sorted);
        }

        writeRowCount(outDir + "/pre", N);
        stmtDict.save(outDir + "/pre", "stmt_dict");
        rfileDict.save(outDir + "/pre", "rfile_dict");
    }
    auto tPre1 = std::chrono::steady_clock::now();
    std::cerr << "  pre done in " << std::chrono::duration_cast<std::chrono::milliseconds>(tPre1 - tPre0).count() << " ms\n";
    std::cerr << "  stmt_dict size: " << stmtDict.size() << ", rfile_dict size: " << rfileDict.size() << "\n";

    // ============================================================
    // Phase 5: Write shared dictionaries
    // ============================================================
    std::cerr << "Writing shared dictionaries...\n";
    tagDict.save(outDir + "/dicts", "tag_dict");
    versionDict.save(outDir + "/dicts", "version_dict");

    std::cerr << "Final tag_dict size: " << tagDict.size() << "\n";
    std::cerr << "Final version_dict size: " << versionDict.size() << "\n";

    auto t1 = std::chrono::steady_clock::now();
    std::cerr << "Total ingestion time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << " ms\n";
    return 0;
}
