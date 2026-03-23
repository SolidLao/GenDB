#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
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
    if (!in) {
        fail("cannot open dict: " + path);
    }

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    if (!in) {
        fail("cannot read dict header: " + path);
    }

    std::vector<std::string> values;
    values.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!in) {
            fail("cannot read dict len: " + path);
        }
        std::string s;
        s.resize(len);
        if (len > 0) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) {
                fail("cannot read dict payload: " + path);
            }
        }
        values.push_back(std::move(s));
    }
    return values;
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

    explicit PostingIndexView(const std::string& path) : file(path) {
        const uint8_t* p = file.data;
        const size_t nbytes = file.count;
        if (nbytes < 16) {
            fail("posting index too small: " + path);
        }

        const uint64_t entry_count = read_u64_le(p);
        const uint64_t rowid_count = read_u64_le(p + 8);

        const size_t entries_bytes = static_cast<size_t>(entry_count) * 16ULL;
        const size_t rowids_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
        const size_t expected = 16ULL + entries_bytes + rowids_bytes;
        if (expected != nbytes) {
            fail("posting index size mismatch: " + path);
        }

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
            const uint32_t mk = entries[mid].key;
            if (mk < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < entries.size() && entries[lo].key == key) {
            return &entries[lo];
        }
        return nullptr;
    }
};

std::string csv_escape(const std::string& s) {
    bool need_quote = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            need_quote = true;
            break;
        }
    }
    if (!need_quote) {
        return s;
    }

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

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        fail("usage: q3 <gendb_dir> <results_dir>");
    }

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

    PostingIndexView* num_uom_hash = nullptr;
    MmapColumn<uint8_t> sub_adsh_pk_file;

    std::vector<std::string> sub_name_dict;
    std::vector<std::string> uom_dict;

    uint32_t usd_code = std::numeric_limits<uint32_t>::max();

    {
        GENDB_PHASE("data_loading");

        sub_adsh.open(gendb_dir + "/sub/adsh.bin");
        sub_fy.open(gendb_dir + "/sub/fy.bin");
        sub_cik.open(gendb_dir + "/sub/cik.bin");
        sub_name.open(gendb_dir + "/sub/name.bin");

        num_adsh.open(gendb_dir + "/num/adsh.bin");
        num_uom.open(gendb_dir + "/num/uom.bin");
        num_value.open(gendb_dir + "/num/value.bin");

        mmap_prefetch_all(sub_adsh, sub_fy, sub_cik, sub_name, num_adsh, num_uom, num_value);

        sub_name_dict = load_dict(gendb_dir + "/sub/name.dict");
        uom_dict = load_dict(gendb_dir + "/dicts/uom.dict");

        for (uint32_t i = 0; i < static_cast<uint32_t>(uom_dict.size()); ++i) {
            if (uom_dict[i] == "USD") {
                usd_code = i;
                break;
            }
        }
        if (usd_code == std::numeric_limits<uint32_t>::max()) {
            fail("USD not found in uom.dict");
        }

        num_uom_hash = new PostingIndexView(gendb_dir + "/num/indexes/num_uom_hash.bin");
        sub_adsh_pk_file.open(gendb_dir + "/sub/indexes/sub_adsh_pk_hash.bin");
    }

    const size_t sub_rows = sub_fy.count;
    if (sub_adsh.count != sub_rows || sub_cik.count != sub_rows || sub_name.count != sub_rows) {
        fail("sub column size mismatch");
    }
    if (num_adsh.count != num_value.count || num_adsh.count != num_uom.count) {
        fail("num column size mismatch");
    }

    const uint8_t* pk_bytes = sub_adsh_pk_file.data;
    const size_t pk_size = sub_adsh_pk_file.count;
    if (pk_size < 8) {
        fail("sub_adsh_pk_hash too small");
    }
    const uint64_t lut_size_u64 = read_u64_le(pk_bytes);
    if (lut_size_u64 > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        fail("lut size overflow");
    }
    const size_t lut_size = static_cast<size_t>(lut_size_u64);
    if (8ULL + lut_size * sizeof(uint32_t) != pk_size) {
        fail("sub_adsh_pk_hash size mismatch");
    }
    const uint32_t* sub_lut = reinterpret_cast<const uint32_t*>(pk_bytes + 8);
    const uint32_t kMissing = std::numeric_limits<uint32_t>::max();

    std::vector<uint8_t> fy2022_by_adsh(lut_size, 0);

    {
        GENDB_PHASE("dim_filter");

        std::ifstream zm(gendb_dir + "/sub/indexes/sub_fy_zonemap.bin", std::ios::binary);
        if (!zm) {
            fail("cannot open sub_fy_zonemap.bin");
        }

        uint64_t block_size = 0;
        uint64_t blocks = 0;
        zm.read(reinterpret_cast<char*>(&block_size), sizeof(block_size));
        zm.read(reinterpret_cast<char*>(&blocks), sizeof(blocks));
        if (!zm || block_size == 0) {
            fail("bad sub_fy_zonemap header");
        }

        for (uint64_t b = 0; b < blocks; ++b) {
            int32_t mn = 0;
            int32_t mx = 0;
            zm.read(reinterpret_cast<char*>(&mn), sizeof(mn));
            zm.read(reinterpret_cast<char*>(&mx), sizeof(mx));
            if (!zm) {
                fail("bad sub_fy_zonemap payload");
            }

            if (mn > 2022 || mx < 2022) {
                continue;
            }

            const size_t start = static_cast<size_t>(b * block_size);
            size_t end = start + static_cast<size_t>(block_size);
            if (end > sub_rows) {
                end = sub_rows;
            }

            for (size_t row = start; row < end; ++row) {
                if (sub_fy[row] != 2022) {
                    continue;
                }
                const uint32_t adsh_code = sub_adsh[row];
                if (adsh_code < lut_size && sub_lut[adsh_code] != kMissing) {
                    fy2022_by_adsh[adsh_code] = 1;
                }
            }
        }
    }

    const PostingEntry* usd_entry = nullptr;

    {
        GENDB_PHASE("build_joins");
        usd_entry = num_uom_hash->find_key(usd_code);
        if (usd_entry == nullptr) {
            fail("USD key not found in num_uom_hash");
        }
    }

    std::vector<CompactHashMap<uint64_t, double>> tl_outer;
    std::vector<CompactHashMap<int32_t, double>> tl_inner;

    {
        GENDB_PHASE("main_scan");

        const uint32_t* __restrict__ num_adsh_data = num_adsh.data;
        const uint16_t* __restrict__ num_uom_data = num_uom.data;
        const double* __restrict__ num_value_data = num_value.data;
        const int32_t* __restrict__ sub_cik_data = sub_cik.data;
        const uint32_t* __restrict__ sub_name_data = sub_name.data;
        const uint8_t* __restrict__ fy_adsh_data = fy2022_by_adsh.data();
        const uint64_t num_rows = num_adsh.count;

        const int threads = omp_get_max_threads();
        tl_outer.reserve(static_cast<size_t>(threads));
        tl_inner.reserve(static_cast<size_t>(threads));
        for (int t = 0; t < threads; ++t) {
            tl_outer.emplace_back(4096);
            tl_inner.emplace_back(2048);
        }

        std::atomic<uint64_t> next{0};
        constexpr uint64_t MORSEL = 1ULL << 16;

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            CompactHashMap<uint64_t, double>& local_outer = tl_outer[static_cast<size_t>(tid)];
            CompactHashMap<int32_t, double>& local_inner = tl_inner[static_cast<size_t>(tid)];

            for (;;) {
                const uint64_t begin = next.fetch_add(MORSEL, std::memory_order_relaxed);
                if (begin >= num_rows) {
                    break;
                }
                const uint64_t end = std::min(begin + MORSEL, num_rows);

                for (uint64_t rowid = begin; rowid < end; ++rowid) {
                    if (num_uom_data[rowid] != static_cast<uint16_t>(usd_code)) {
                        continue;
                    }
                    const uint32_t adsh_code = num_adsh_data[rowid];
                    if (adsh_code >= lut_size || fy_adsh_data[adsh_code] == 0) {
                        continue;
                    }

                    const uint32_t sub_row = sub_lut[adsh_code];
                    if (sub_row == kMissing || sub_row >= sub_rows) {
                        continue;
                    }

                    const double value = num_value_data[rowid];
                    if (value != value) {
                        continue;
                    }

                    const int32_t cik = sub_cik_data[sub_row];
                    const uint32_t name_code = sub_name_data[sub_row];
                    const uint64_t key = (static_cast<uint64_t>(static_cast<uint32_t>(cik)) << 32) |
                                         static_cast<uint64_t>(name_code);

                    local_outer[key] += value;
                    local_inner[cik] += value;
                }
            }
        }
    }

    struct OutputRow {
        uint32_t name_code;
        int32_t cik;
        double total;
    };

    {
        GENDB_PHASE("output");

        CompactHashMap<uint64_t, double> outer_totals(32768);
        CompactHashMap<int32_t, double> cik_totals(16384);

        for (const auto& local : tl_outer) {
            for (const auto& kv : local) {
                outer_totals[kv.first] += kv.second;
            }
        }
        for (const auto& local : tl_inner) {
            for (const auto& kv : local) {
                cik_totals[kv.first] += kv.second;
            }
        }

        long double sum_subtotals = 0.0L;
        size_t cik_groups = 0;
        for (const auto& kv : cik_totals) {
            sum_subtotals += static_cast<long double>(kv.second);
            ++cik_groups;
        }
        const long double threshold =
            (cik_groups == 0) ? std::numeric_limits<long double>::infinity()
                              : (sum_subtotals / static_cast<long double>(cik_groups));

        std::vector<OutputRow> rows;
        rows.reserve(outer_totals.size());

        for (const auto& kv : outer_totals) {
            const double total = kv.second;
            if (static_cast<long double>(total) <= threshold) {
                continue;
            }
            const uint64_t packed = kv.first;
            const uint32_t name_code = static_cast<uint32_t>(packed & 0xFFFFFFFFULL);
            const int32_t cik = static_cast<int32_t>(packed >> 32);
            rows.push_back(OutputRow{name_code, cik, total});
        }

        std::sort(rows.begin(), rows.end(), [](const OutputRow& a, const OutputRow& b) {
            if (a.total != b.total) {
                return a.total > b.total;
            }
            if (a.cik != b.cik) {
                return a.cik < b.cik;
            }
            return a.name_code < b.name_code;
        });

        if (rows.size() > 100) {
            rows.resize(100);
        }

        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* out = std::fopen(out_path.c_str(), "w");
        if (!out) {
            fail("cannot open output csv: " + out_path);
        }

        std::fprintf(out, "name,cik,total_value\n");
        for (const OutputRow& row : rows) {
            if (row.name_code >= sub_name_dict.size()) {
                std::fclose(out);
                fail("name code out of dictionary range");
            }
            const std::string esc_name = csv_escape(sub_name_dict[row.name_code]);
            std::fprintf(out, "%s,%d,%.2f\n", esc_name.c_str(), row.cik, row.total);
        }

        std::fclose(out);
    }

    delete num_uom_hash;
    return 0;
}
