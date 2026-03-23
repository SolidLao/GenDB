#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
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

    std::vector<std::string> values;
    values.reserve(n);
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
        values.push_back(std::move(s));
    }
    return values;
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
        if (c == '"') {
            out.push_back('"');
            out.push_back('"');
        } else {
            out.push_back(c);
        }
    }
    out.push_back('"');
    return out;
}

uint16_t resolve_usd_code(const std::vector<std::string>& uom_dict) {
    for (uint32_t i = 0; i < uom_dict.size(); ++i) {
        if (uom_dict[i] == "USD") {
            if (i > static_cast<uint32_t>(std::numeric_limits<uint16_t>::max())) {
                fail("USD code exceeds uint16 range");
            }
            return static_cast<uint16_t>(i);
        }
    }
    fail("USD not found in dicts/uom.dict");
}

struct FilteredSubRow {
    uint32_t adsh;
    int32_t cik;
    uint32_t name_code;
};

struct PostingSlice {
    uint32_t filtered_idx;
    uint64_t start;
    uint32_t count;
};

struct OutputRow {
    uint32_t name_code;
    int32_t cik;
    int64_t total_cents;
};

int64_t find_entry_by_key(const uint8_t* entries, uint64_t entry_count, uint32_t key) {
    int64_t lo = 0;
    int64_t hi = static_cast<int64_t>(entry_count);
    while (lo < hi) {
        const int64_t mid = lo + ((hi - lo) >> 1);
        const uint8_t* ep = entries + static_cast<uint64_t>(mid) * 16ULL;
        const uint32_t k = read_u32_le(ep + 0);
        if (k < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if (lo >= static_cast<int64_t>(entry_count)) return -1;
    const uint8_t* ep = entries + static_cast<uint64_t>(lo) * 16ULL;
    return (read_u32_le(ep + 0) == key) ? lo : -1;
}

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

    MmapColumn<uint16_t> num_uom;
    MmapColumn<double> num_value;
    MmapColumn<uint8_t> num_adsh_fk_hash;

    std::vector<std::string> sub_name_dict;
    uint16_t usd_code = std::numeric_limits<uint16_t>::max();

    {
        GENDB_PHASE("data_loading");

        sub_adsh.open(gendb_dir + "/sub/adsh.bin");
        sub_fy.open(gendb_dir + "/sub/fy.bin");
        sub_cik.open(gendb_dir + "/sub/cik.bin");
        sub_name.open(gendb_dir + "/sub/name.bin");

        num_uom.open(gendb_dir + "/num/uom.bin");
        num_value.open(gendb_dir + "/num/value.bin");
        num_adsh_fk_hash.open(gendb_dir + "/num/indexes/num_adsh_fk_hash.bin");

        if (sub_adsh.count != sub_fy.count || sub_adsh.count != sub_cik.count || sub_adsh.count != sub_name.count) {
            fail("sub column size mismatch");
        }
        if (num_uom.count != num_value.count) {
            fail("num column size mismatch");
        }

        mmap_prefetch_all(sub_adsh, sub_fy, sub_cik, sub_name, num_uom, num_value, num_adsh_fk_hash);

        sub_name_dict = load_dict(gendb_dir + "/sub/name.dict");
        const std::vector<std::string> uom_dict = load_dict(gendb_dir + "/dicts/uom.dict");
        usd_code = resolve_usd_code(uom_dict);
    }

    std::vector<FilteredSubRow> filtered_sub_rows;

    {
        GENDB_PHASE("dim_filter");

        std::ifstream zm(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin", std::ios::binary);
        if (!zm) fail("cannot open sub_fy_zonemap.bin");

        uint64_t block_size = 0;
        uint64_t blocks = 0;
        zm.read(reinterpret_cast<char*>(&block_size), sizeof(block_size));
        zm.read(reinterpret_cast<char*>(&blocks), sizeof(blocks));
        if (!zm || block_size == 0) fail("invalid sub_fy_zonemap header");

        filtered_sub_rows.reserve(30000);
        const size_t sub_rows = sub_fy.count;
        for (uint64_t b = 0; b < blocks; ++b) {
            int32_t mn = 0;
            int32_t mx = 0;
            zm.read(reinterpret_cast<char*>(&mn), sizeof(mn));
            zm.read(reinterpret_cast<char*>(&mx), sizeof(mx));
            if (!zm) fail("invalid sub_fy_zonemap payload");
            if (mn > 2022 || mx < 2022) continue;

            const size_t start = static_cast<size_t>(b * block_size);
            size_t end = start + static_cast<size_t>(block_size);
            if (end > sub_rows) end = sub_rows;

            for (size_t row = start; row < end; ++row) {
                if (sub_fy[row] == 2022) {
                    filtered_sub_rows.push_back(FilteredSubRow{sub_adsh[row], sub_cik[row], sub_name[row]});
                }
            }
        }
    }

    const uint8_t* entries = nullptr;
    uint64_t entry_count = 0;
    const uint32_t* posting_rowids = nullptr;
    uint64_t rowid_count = 0;
    std::vector<PostingSlice> posting_slices;

    {
        GENDB_PHASE("build_joins");

        if (num_adsh_fk_hash.count < 16) fail("num_adsh_fk_hash.bin too small");

        const uint8_t* p = num_adsh_fk_hash.data;
        entry_count = read_u64_le(p + 0);
        rowid_count = read_u64_le(p + 8);
        const uint64_t need = 16ULL + entry_count * 16ULL + rowid_count * 4ULL;
        if (need != num_adsh_fk_hash.count) fail("num_adsh_fk_hash.bin size mismatch");

        entries = p + 16;
        posting_rowids = reinterpret_cast<const uint32_t*>(entries + entry_count * 16ULL);

        posting_slices.reserve(filtered_sub_rows.size());
        for (uint32_t i = 0; i < filtered_sub_rows.size(); ++i) {
            const int64_t pos = find_entry_by_key(entries, entry_count, filtered_sub_rows[i].adsh);
            if (pos < 0) continue;

            const uint8_t* ep = entries + static_cast<uint64_t>(pos) * 16ULL;
            const uint64_t start = read_u64_le(ep + 4);
            const uint32_t cnt = read_u32_le(ep + 12);
            if (cnt == 0) continue;
            if (start + static_cast<uint64_t>(cnt) > rowid_count) fail("posting slice out of bounds");

            posting_slices.push_back(PostingSlice{i, start, cnt});
        }
    }

    std::vector<std::vector<int64_t>> tl_sums;

    {
        GENDB_PHASE("main_scan");

        const size_t filtered_n = filtered_sub_rows.size();
        const int threads = omp_get_max_threads();
        tl_sums.resize(static_cast<size_t>(threads));
        for (int t = 0; t < threads; ++t) {
            tl_sums[static_cast<size_t>(t)].assign(filtered_n, 0);
        }

        const uint16_t* __restrict__ uom = num_uom.data;
        const double* __restrict__ val = num_value.data;

        std::atomic<uint64_t> next{0};
        constexpr uint64_t MORSEL = 256;

#pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            std::vector<int64_t>& local_sum = tl_sums[static_cast<size_t>(tid)];

            for (;;) {
                const uint64_t begin = next.fetch_add(MORSEL, std::memory_order_relaxed);
                if (begin >= posting_slices.size()) break;
                const uint64_t end = std::min<uint64_t>(begin + MORSEL, posting_slices.size());

                for (uint64_t i = begin; i < end; ++i) {
                    const PostingSlice s = posting_slices[static_cast<size_t>(i)];
                    int64_t acc = 0;
                    const uint64_t stop = s.start + static_cast<uint64_t>(s.count);
                    for (uint64_t pidx = s.start; pidx < stop; ++pidx) {
                        const uint32_t rowid = posting_rowids[pidx];
                        if (rowid >= num_uom.count) continue;
                        if (uom[rowid] != usd_code) continue;
                        const double v = val[rowid];
                        if (std::isnan(v)) continue;
                        acc += static_cast<int64_t>(std::llround(v * 100.0));
                    }
                    if (acc != 0) local_sum[s.filtered_idx] += acc;
                }
            }
        }
    }

    {
        GENDB_PHASE("output");

        const size_t filtered_n = filtered_sub_rows.size();
        std::vector<int64_t> merged_sum(filtered_n, 0);
        for (size_t t = 0; t < tl_sums.size(); ++t) {
            const std::vector<int64_t>& local = tl_sums[t];
            for (size_t i = 0; i < filtered_n; ++i) merged_sum[i] += local[i];
        }

        CompactHashMap<uint64_t, int64_t> name_cik_totals(32768);
        CompactHashMap<int32_t, int64_t> cik_totals(16384);

        for (size_t i = 0; i < filtered_n; ++i) {
            const int64_t subtotal = merged_sum[i];
            if (subtotal == 0) continue;

            const FilteredSubRow& s = filtered_sub_rows[i];
            const uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(s.cik)) << 32) |
                                 static_cast<uint64_t>(s.name_code);
            name_cik_totals[key] += subtotal;
            cik_totals[s.cik] += subtotal;
        }

        long double cik_sum = 0.0L;
        size_t cik_groups = 0;
        for (const auto& kv : cik_totals) {
            cik_sum += static_cast<long double>(kv.second);
            ++cik_groups;
        }

        const long double threshold =
            (cik_groups == 0) ? std::numeric_limits<long double>::infinity()
                              : (cik_sum / static_cast<long double>(cik_groups));

        std::vector<OutputRow> out_rows;
        out_rows.reserve(name_cik_totals.size());
        for (const auto& kv : name_cik_totals) {
            if (static_cast<long double>(kv.second) <= threshold) continue;
            const uint64_t packed = kv.first;
            out_rows.push_back(OutputRow{static_cast<uint32_t>(packed & 0xFFFFFFFFULL),
                                         static_cast<int32_t>(packed >> 32),
                                         kv.second});
        }

        std::sort(out_rows.begin(), out_rows.end(), [](const OutputRow& a, const OutputRow& b) {
            if (a.total_cents != b.total_cents) return a.total_cents > b.total_cents;
            if (a.cik != b.cik) return a.cik < b.cik;
            return a.name_code < b.name_code;
        });
        if (out_rows.size() > 100) out_rows.resize(100);

        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) fail("cannot open output: " + out_path);

        std::fprintf(out, "name,cik,total_value\n");
        for (const OutputRow& r : out_rows) {
            if (r.name_code >= sub_name_dict.size()) {
                std::fclose(out);
                fail("name code out of dictionary bounds");
            }
            const std::string esc_name = csv_escape(sub_name_dict[r.name_code]);
            const long double total_value = static_cast<long double>(r.total_cents) / 100.0L;
            std::fprintf(out, "%s,%d,%.2Lf\n", esc_name.c_str(), r.cik, total_value);
        }
        std::fclose(out);
    }

    return 0;
}
