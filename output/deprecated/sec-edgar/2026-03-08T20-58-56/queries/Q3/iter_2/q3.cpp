#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include <omp.h>

#include "hash_utils.h"
#include "mmap_utils.h"
#include "timing_utils.h"

using namespace gendb;

namespace {

[[noreturn]] void fail(const std::string& msg) {
    std::fprintf(stderr, "q3 error: %s\n", msg.c_str());
    std::exit(1);
}

uint32_t read_u32_le(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

uint64_t read_u64_le(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

std::vector<std::string> load_dict(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) fail("cannot open dict: " + path);

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) fail("cannot read dict header: " + path);

    std::vector<std::string> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) fail("cannot read dict len: " + path);
        std::string s;
        s.resize(len);
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) fail("cannot read dict payload: " + path);
        }
        out.push_back(std::move(s));
    }
    return out;
}

std::string csv_escape(const std::string& s) {
    bool need_quote = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            need_quote = true;
            break;
        }
    }
    if (!need_quote) return s;

    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (char c : s) {
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

struct PostingEntry {
    uint32_t key;
    uint64_t start;
    uint32_t count;
};

struct PostingIndexView {
    MmapColumn<uint8_t> file;
    std::vector<PostingEntry> entries;
    const uint32_t* rowids = nullptr;
    uint64_t rowid_count = 0;

    explicit PostingIndexView(const std::string& path) : file(path) {
        if (file.count < 16) fail("posting index too small: " + path);
        const uint8_t* p = file.data;
        const uint64_t entry_count = read_u64_le(p);
        rowid_count = read_u64_le(p + 8);

        const size_t entries_bytes = static_cast<size_t>(entry_count) * 16ULL;
        const size_t rowids_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
        const size_t expected = 16ULL + entries_bytes + rowids_bytes;
        if (expected != file.count) fail("posting index size mismatch: " + path);

        entries.resize(static_cast<size_t>(entry_count));
        const uint8_t* ep = p + 16;
        for (size_t i = 0; i < entries.size(); ++i) {
            entries[i].key = read_u32_le(ep);
            entries[i].start = read_u64_le(ep + 4);
            entries[i].count = read_u32_le(ep + 12);
            ep += 16;
        }
        rowids = reinterpret_cast<const uint32_t*>(p + 16 + entries_bytes);
    }

    const PostingEntry* find_key(uint32_t key) const {
        size_t lo = 0;
        size_t hi = entries.size();
        while (lo < hi) {
            const size_t mid = lo + ((hi - lo) >> 1);
            if (entries[mid].key < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < entries.size() && entries[lo].key == key) return &entries[lo];
        return nullptr;
    }
};

struct FilteredSubRow {
    int32_t cik;
    uint32_t name_code;
};

struct OutputRow {
    uint32_t name_code;
    int32_t cik;
    long double total_value;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) fail("usage: q3 <gendb_dir> <results_dir>");

    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE("total");

    MmapColumn<uint32_t> sub_adsh;
    MmapColumn<int32_t> sub_fy;
    MmapColumn<int32_t> sub_cik;
    MmapColumn<uint32_t> sub_name;
    MmapColumn<uint32_t> num_adsh;
    MmapColumn<uint16_t> num_uom;
    MmapColumn<double> num_value;
    MmapColumn<uint8_t> sub_adsh_pk_file;

    std::vector<std::string> sub_name_dict;
    uint32_t usd_code = std::numeric_limits<uint32_t>::max();

    PostingIndexView* num_uom_posting = nullptr;

    {
        GENDB_PHASE("data_loading");

        sub_adsh.open(gendb_dir + "/sub/adsh.bin");
        sub_fy.open(gendb_dir + "/sub/fy.bin");
        sub_cik.open(gendb_dir + "/sub/cik.bin");
        sub_name.open(gendb_dir + "/sub/name.bin");
        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_uom.open(gendb_dir + "/num/uom.bin");
        num_value.open(gendb_dir + "/num/value.bin");
        sub_adsh_pk_file.open(gendb_dir + "/sub/indexes/sub_adsh_pk_hash.bin");

        mmap_prefetch_all(sub_adsh, sub_fy, sub_cik, sub_name, num_adsh, num_uom, num_value, sub_adsh_pk_file);

        sub_name_dict = load_dict(gendb_dir + "/sub/name.dict");
        const std::vector<std::string> uom_dict = load_dict(gendb_dir + "/dicts/uom.dict");
        for (uint32_t i = 0; i < static_cast<uint32_t>(uom_dict.size()); ++i) {
            if (uom_dict[i] == "USD") {
                usd_code = i;
                break;
            }
        }
        if (usd_code == std::numeric_limits<uint32_t>::max()) fail("USD not found in dicts/uom.dict");

        num_uom_posting = new PostingIndexView(gendb_dir + "/num/indexes/num_uom_hash.bin");
    }

    const size_t sub_rows = sub_adsh.count;
    if (sub_fy.count != sub_rows || sub_cik.count != sub_rows || sub_name.count != sub_rows) {
        fail("sub column size mismatch");
    }
    if (num_adsh.count != num_value.count || num_adsh.count != num_uom.count) fail("num column size mismatch");

    if (sub_adsh_pk_file.count < 8) fail("sub_adsh_pk_hash too small");
    const uint8_t* pk_bytes = sub_adsh_pk_file.data;
    const uint64_t lut_size_u64 = read_u64_le(pk_bytes);
    if (lut_size_u64 > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) fail("lut too large");
    const size_t lut_size = static_cast<size_t>(lut_size_u64);
    if (8ULL + lut_size * sizeof(uint32_t) != sub_adsh_pk_file.count) fail("sub_adsh_pk_hash size mismatch");
    const uint32_t* sub_adsh_lut = reinterpret_cast<const uint32_t*>(pk_bytes + 8);
    const uint32_t kMissing = std::numeric_limits<uint32_t>::max();

    std::vector<int32_t> adsh_to_filtered_idx(lut_size, -1);
    std::vector<FilteredSubRow> filtered_sub_rows;

    {
        GENDB_PHASE("dim_filter");

        std::ifstream zm(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin", std::ios::binary);
        if (!zm) fail("cannot open sub_fy_zonemap.bin");

        uint64_t block_size = 0;
        uint64_t blocks = 0;
        zm.read(reinterpret_cast<char*>(&block_size), sizeof(block_size));
        zm.read(reinterpret_cast<char*>(&blocks), sizeof(blocks));
        if (!zm || block_size == 0) fail("bad sub_fy_zonemap header");

        filtered_sub_rows.reserve(sub_rows / 4);
        for (uint64_t b = 0; b < blocks; ++b) {
            int32_t mn = 0;
            int32_t mx = 0;
            zm.read(reinterpret_cast<char*>(&mn), sizeof(mn));
            zm.read(reinterpret_cast<char*>(&mx), sizeof(mx));
            if (!zm) fail("bad sub_fy_zonemap payload");
            if (mn > 2022 || mx < 2022) continue;

            const size_t start = static_cast<size_t>(b * block_size);
            size_t end = start + static_cast<size_t>(block_size);
            if (end > sub_rows) end = sub_rows;

            for (size_t row = start; row < end; ++row) {
                if (sub_fy[row] != 2022) continue;

                const uint32_t adsh_code = sub_adsh[row];
                if (adsh_code >= lut_size) continue;
                const uint32_t lut_row = sub_adsh_lut[adsh_code];
                if (lut_row == kMissing || lut_row >= sub_rows) continue;
                if (adsh_to_filtered_idx[adsh_code] != -1) continue;

                const int32_t idx = static_cast<int32_t>(filtered_sub_rows.size());
                adsh_to_filtered_idx[adsh_code] = idx;
                filtered_sub_rows.push_back(FilteredSubRow{sub_cik[row], sub_name[row]});
            }
        }
    }

    const uint32_t* usd_rowids = nullptr;
    std::vector<uint32_t> usd_rowids_materialized;
    uint64_t usd_row_count = 0;

    {
        GENDB_PHASE("build_joins");

        const PostingEntry* usd_posting = num_uom_posting->find_key(usd_code);
        if (usd_posting == nullptr) fail("USD key not found in num_uom_hash");
        if (usd_code > static_cast<uint32_t>(std::numeric_limits<uint16_t>::max())) {
            fail("USD code out of uint16 range");
        }

        const uint16_t usd16 = static_cast<uint16_t>(usd_code);
        const uint32_t* all_rowids = num_uom_posting->rowids;
        usd_rowids_materialized.reserve(num_uom_posting->rowid_count * 9 / 10);
        for (uint64_t i = 0; i < num_uom_posting->rowid_count; ++i) {
            const uint32_t rowid = all_rowids[i];
            if (num_uom[rowid] == usd16) {
                usd_rowids_materialized.push_back(rowid);
            }
        }

        usd_rowids = usd_rowids_materialized.data();
        usd_row_count = static_cast<uint64_t>(usd_rowids_materialized.size());
    }

    const size_t filtered_count = filtered_sub_rows.size();
    const int threads = omp_get_max_threads();
    std::vector<std::vector<long double>> tl_totals;
    std::vector<std::vector<uint8_t>> tl_hits;
    tl_totals.reserve(static_cast<size_t>(threads));
    tl_hits.reserve(static_cast<size_t>(threads));
    for (int t = 0; t < threads; ++t) {
        tl_totals.emplace_back(filtered_count, 0.0);
        tl_hits.emplace_back(filtered_count, 0);
    }

    {
        GENDB_PHASE("main_scan");

        const uint32_t* __restrict__ num_adsh_data = num_adsh.data;
        const double* __restrict__ num_value_data = num_value.data;
        const int32_t* __restrict__ adsh_to_fidx = adsh_to_filtered_idx.data();

        std::atomic<uint64_t> next{0};
        constexpr uint64_t MORSEL = 1ULL << 16;

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            long double* __restrict__ local_totals = tl_totals[static_cast<size_t>(tid)].data();
            uint8_t* __restrict__ local_hits = tl_hits[static_cast<size_t>(tid)].data();

            for (;;) {
                const uint64_t begin = next.fetch_add(MORSEL, std::memory_order_relaxed);
                if (begin >= usd_row_count) break;
                const uint64_t end = std::min(begin + MORSEL, usd_row_count);

                for (uint64_t i = begin; i < end; ++i) {
                    const uint32_t rowid = usd_rowids[i];
                    const uint32_t adsh_code = num_adsh_data[rowid];
                    if (adsh_code >= lut_size) continue;

                    const int32_t fidx = adsh_to_fidx[adsh_code];
                    if (fidx < 0) continue;

                    const double v = num_value_data[rowid];
                    if (v != v) continue;

                    local_totals[fidx] += static_cast<long double>(v);
                    local_hits[fidx] = 1;
                }
            }
        }
    }

    {
        GENDB_PHASE("output");

        std::vector<long double> filtered_totals(filtered_count, 0.0L);
        std::vector<uint8_t> filtered_present(filtered_count, 0);

        for (int t = 0; t < threads; ++t) {
            const long double* t_totals = tl_totals[static_cast<size_t>(t)].data();
            const uint8_t* t_hits = tl_hits[static_cast<size_t>(t)].data();
            for (size_t i = 0; i < filtered_count; ++i) {
                filtered_totals[i] += t_totals[i];
                filtered_present[i] |= t_hits[i];
            }
        }

        CompactHashMap<uint64_t, long double> name_cik_totals(filtered_count * 2 + 16);
        CompactHashMap<int32_t, long double> cik_totals(filtered_count + 16);

        for (size_t i = 0; i < filtered_count; ++i) {
            if (filtered_present[i] == 0) continue;
            const FilteredSubRow& fr = filtered_sub_rows[i];
            const uint64_t name_cik_key =
                (static_cast<uint64_t>(static_cast<uint32_t>(fr.cik)) << 32) |
                static_cast<uint64_t>(fr.name_code);
            name_cik_totals[name_cik_key] += filtered_totals[i];
            cik_totals[fr.cik] += filtered_totals[i];
        }

        long double threshold = std::numeric_limits<long double>::infinity();
        if (cik_totals.size() > 0) {
            long double sum_subtotals = 0.0L;
            size_t groups = 0;
            for (const auto& kv : cik_totals) {
                sum_subtotals += static_cast<long double>(kv.second);
                ++groups;
            }
            threshold = sum_subtotals / static_cast<long double>(groups);
        }

        std::vector<OutputRow> output_rows;
        output_rows.reserve(name_cik_totals.size());
        for (const auto& kv : name_cik_totals) {
            const long double total = kv.second;
            if (total <= threshold) continue;
            const uint64_t packed = kv.first;
            output_rows.push_back(OutputRow{
                static_cast<uint32_t>(packed & 0xFFFFFFFFULL),
                static_cast<int32_t>(packed >> 32),
                total
            });
        }

        auto row_less = [](const OutputRow& a, const OutputRow& b) {
            if (a.total_value != b.total_value) return a.total_value > b.total_value;
            if (a.cik != b.cik) return a.cik < b.cik;
            return a.name_code < b.name_code;
        };

        if (output_rows.size() > 100) {
            std::nth_element(output_rows.begin(), output_rows.begin() + 100, output_rows.end(), row_less);
            output_rows.resize(100);
        }
        std::sort(output_rows.begin(), output_rows.end(), row_less);

        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) fail("cannot open output csv: " + out_path);

        std::fprintf(out, "name,cik,total_value\n");
        for (const OutputRow& row : output_rows) {
            if (row.name_code >= sub_name_dict.size()) {
                std::fclose(out);
                fail("name code out of dictionary range");
            }
            const std::string esc_name = csv_escape(sub_name_dict[row.name_code]);
            std::fprintf(out, "%s,%d,%.2Lf\n", esc_name.c_str(), row.cik, row.total_value);
        }
        std::fclose(out);
    }

    delete num_uom_posting;
    return 0;
}
