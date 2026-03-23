#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mmap_utils.h"

namespace {

struct PostingEntryU32 {
    uint32_t key;
    uint64_t start;
    uint32_t count;
};

struct ZoneMinMaxI32 {
    int32_t min_v;
    int32_t max_v;
};

struct ZoneMapI32 {
    uint64_t block_size = 0;
    std::vector<ZoneMinMaxI32> blocks;

    void load(const std::string& path) {
        gendb::MmapColumn<uint8_t> raw(path);
        const uint8_t* p = raw.data;
        const uint8_t* end = raw.data + raw.size();
        if (p + sizeof(uint64_t) * 2 > end) {
            throw std::runtime_error("zonemap too small: " + path);
        }
        uint64_t nblocks = 0;
        std::memcpy(&block_size, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        std::memcpy(&nblocks, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        blocks.resize(static_cast<size_t>(nblocks));
        for (uint64_t i = 0; i < nblocks; ++i) {
            if (p + sizeof(int32_t) * 2 > end) {
                throw std::runtime_error("zonemap truncated: " + path);
            }
            std::memcpy(&blocks[static_cast<size_t>(i)].min_v, p, sizeof(int32_t));
            p += sizeof(int32_t);
            std::memcpy(&blocks[static_cast<size_t>(i)].max_v, p, sizeof(int32_t));
            p += sizeof(int32_t);
        }
    }
};

struct PostingIndexU32 {
    gendb::MmapColumn<uint8_t> raw;
    std::vector<PostingEntryU32> entries;
    const uint32_t* rowids = nullptr;

    void open(const std::string& path) {
        raw.open(path);
        const uint8_t* p = raw.data;
        const uint8_t* end = raw.data + raw.size();
        if (p + sizeof(uint64_t) * 2 > end) {
            throw std::runtime_error("posting index too small: " + path);
        }

        uint64_t entry_count = 0;
        uint64_t rowid_count = 0;
        std::memcpy(&entry_count, p, sizeof(uint64_t));
        p += sizeof(uint64_t);
        std::memcpy(&rowid_count, p, sizeof(uint64_t));
        p += sizeof(uint64_t);

        entries.clear();
        entries.reserve(static_cast<size_t>(entry_count));
        for (uint64_t i = 0; i < entry_count; ++i) {
            if (p + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) > end) {
                throw std::runtime_error("posting entry truncated: " + path);
            }
            PostingEntryU32 e{};
            std::memcpy(&e.key, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
            std::memcpy(&e.start, p, sizeof(uint64_t));
            p += sizeof(uint64_t);
            std::memcpy(&e.count, p, sizeof(uint32_t));
            p += sizeof(uint32_t);
            entries.push_back(e);
        }

        const size_t expect_bytes = static_cast<size_t>(rowid_count) * sizeof(uint32_t);
        if (p + expect_bytes > end) {
            throw std::runtime_error("posting rowids truncated: " + path);
        }
        rowids = reinterpret_cast<const uint32_t*>(p);
    }

    std::pair<const uint32_t*, uint32_t> find(uint32_t key) const {
        size_t lo = 0;
        size_t hi = entries.size();
        while (lo < hi) {
            const size_t mid = lo + ((hi - lo) >> 1);
            if (entries[mid].key < key) lo = mid + 1;
            else hi = mid;
        }
        if (lo >= entries.size() || entries[lo].key != key) return {nullptr, 0};
        const PostingEntryU32& e = entries[lo];
        return {rowids + e.start, e.count};
    }
};

static std::vector<std::string> load_dict(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open dict: " + path);

    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(uint32_t));
    if (!in) throw std::runtime_error("cannot read dict count: " + path);

    std::vector<std::string> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        if (!in) throw std::runtime_error("cannot read dict len: " + path);
        std::string s(len, '\0');
        if (len) {
            in.read(&s[0], static_cast<std::streamsize>(len));
            if (!in) throw std::runtime_error("cannot read dict payload: " + path);
        }
        out.push_back(std::move(s));
    }
    return out;
}

static uint16_t find_pure_code(const std::vector<std::string>& dict) {
    for (uint32_t i = 0; i < dict.size(); ++i) {
        if (dict[i] == "pure") return static_cast<uint16_t>(i);
    }
    throw std::runtime_error("'pure' not found in uom dict");
}

inline uint64_t pack_key(uint32_t adsh, uint32_t tag) {
    return (static_cast<uint64_t>(adsh) << 32) | static_cast<uint64_t>(tag);
}

struct KeyHash {
    size_t operator()(uint64_t x) const noexcept {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return static_cast<size_t>(x);
    }
};

struct RunStats {
    double ms;
    uint64_t rows_touched;
    uint64_t filtered_rows;
    uint64_t groups;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>\n";
        return 1;
    }

    try {
        const std::string gendb = argv[1];

        const auto uom_dict = load_dict(gendb + "/dicts/uom.dict");
        const uint16_t pure_code = find_pure_code(uom_dict);

        gendb::MmapColumn<uint32_t> num_adsh(gendb + "/num/adsh.bin");
        gendb::MmapColumn<uint32_t> num_tag(gendb + "/num/tag.bin");
        gendb::MmapColumn<uint16_t> num_uom(gendb + "/num/uom.bin");
        gendb::MmapColumn<double> num_value(gendb + "/num/value.bin");

        gendb::MmapColumn<uint32_t> sub_adsh(gendb + "/sub/adsh.bin");
        gendb::MmapColumn<int32_t> sub_fy(gendb + "/sub/fy.bin");

        ZoneMapI32 sub_fy_zm;
        sub_fy_zm.load(gendb + "/sub/indexes/sub_fy_zonemap.bin");

        PostingIndexU32 num_adsh_post;
        num_adsh_post.open(gendb + "/num/indexes/num_adsh_fk_hash.bin");

        std::vector<uint32_t> filtered_sub_adsh;
        filtered_sub_adsh.reserve(sub_adsh.size() / 4);
        {
            const uint64_t bs = sub_fy_zm.block_size;
            for (size_t b = 0; b < sub_fy_zm.blocks.size(); ++b) {
                const auto& z = sub_fy_zm.blocks[b];
                if (z.min_v > 2022 || z.max_v < 2022) continue;
                const size_t start = b * static_cast<size_t>(bs);
                const size_t end = std::min(start + static_cast<size_t>(bs), sub_adsh.size());
                for (size_t i = start; i < end; ++i) {
                    if (sub_fy[i] == 2022) filtered_sub_adsh.push_back(sub_adsh[i]);
                }
            }
        }

        std::unordered_set<uint32_t> sub_set;
        sub_set.reserve(filtered_sub_adsh.size() * 2 + 1);
        for (uint32_t a : filtered_sub_adsh) sub_set.insert(a);

        auto run_order_a = [&]() -> RunStats {
            auto t0 = std::chrono::steady_clock::now();
            uint64_t touched = 0;
            uint64_t filtered = 0;
            std::unordered_map<uint64_t, double, KeyHash> max_map;
            max_map.reserve(200000);

            for (uint32_t adsh : filtered_sub_adsh) {
                const auto post = num_adsh_post.find(adsh);
                if (!post.first) continue;
                touched += post.second;
                for (uint32_t i = 0; i < post.second; ++i) {
                    const uint32_t rid = post.first[i];
                    if (num_uom[rid] != pure_code) continue;
                    const double v = num_value[rid];
                    if (std::isnan(v)) continue;
                    ++filtered;
                    const uint64_t k = pack_key(adsh, num_tag[rid]);
                    auto it = max_map.find(k);
                    if (it == max_map.end()) max_map.emplace(k, v);
                    else if (v > it->second) it->second = v;
                }
            }

            auto t1 = std::chrono::steady_clock::now();
            double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
            return {ms, touched, filtered, static_cast<uint64_t>(max_map.size())};
        };

        auto run_order_b = [&]() -> RunStats {
            auto t0 = std::chrono::steady_clock::now();
            uint64_t touched = 0;
            uint64_t filtered = 0;
            std::unordered_map<uint64_t, double, KeyHash> max_map;
            max_map.reserve(200000);

            for (size_t rid = 0; rid < num_adsh.size(); ++rid) {
                ++touched;
                if (num_uom[rid] != pure_code) continue;
                const double v = num_value[rid];
                if (std::isnan(v)) continue;
                const uint32_t adsh = num_adsh[rid];
                if (sub_set.find(adsh) == sub_set.end()) continue;
                ++filtered;
                const uint64_t k = pack_key(adsh, num_tag[rid]);
                auto it = max_map.find(k);
                if (it == max_map.end()) max_map.emplace(k, v);
                else if (v > it->second) it->second = v;
            }

            auto t1 = std::chrono::steady_clock::now();
            double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
            return {ms, touched, filtered, static_cast<uint64_t>(max_map.size())};
        };

        auto run_order_c = [&]() -> RunStats {
            auto t0 = std::chrono::steady_clock::now();
            uint64_t touched = 0;
            uint64_t filtered = 0;
            std::unordered_map<uint64_t, double, KeyHash> global_max;
            global_max.reserve(500000);

            for (size_t rid = 0; rid < num_adsh.size(); ++rid) {
                ++touched;
                if (num_uom[rid] != pure_code) continue;
                const double v = num_value[rid];
                if (std::isnan(v)) continue;
                const uint64_t k = pack_key(num_adsh[rid], num_tag[rid]);
                auto it = global_max.find(k);
                if (it == global_max.end()) global_max.emplace(k, v);
                else if (v > it->second) it->second = v;
            }

            std::unordered_map<uint64_t, double, KeyHash> joined_max;
            joined_max.reserve(200000);
            for (const auto& kv : global_max) {
                const uint32_t adsh = static_cast<uint32_t>(kv.first >> 32);
                if (sub_set.find(adsh) != sub_set.end()) {
                    ++filtered;
                    joined_max.emplace(kv.first, kv.second);
                }
            }

            auto t1 = std::chrono::steady_clock::now();
            double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
            return {ms, touched, filtered, static_cast<uint64_t>(joined_max.size())};
        };

        // warmup
        (void)run_order_a();
        (void)run_order_b();
        (void)run_order_c();

        constexpr int kRuns = 3;
        double a_sum = 0.0, b_sum = 0.0, c_sum = 0.0;
        RunStats a_last{}, b_last{}, c_last{};
        for (int i = 0; i < kRuns; ++i) {
            a_last = run_order_a();
            b_last = run_order_b();
            c_last = run_order_c();
            a_sum += a_last.ms;
            b_sum += b_last.ms;
            c_sum += c_last.ms;
        }

        std::cout << "filtered_sub_adsh=" << filtered_sub_adsh.size() << "\n";
        std::cout << "OrderA_sub_to_num_postings avg_ms=" << (a_sum / kRuns)
                  << " touched=" << a_last.rows_touched
                  << " filtered=" << a_last.filtered_rows
                  << " groups=" << a_last.groups << "\n";
        std::cout << "OrderB_num_scan_to_sub_hash avg_ms=" << (b_sum / kRuns)
                  << " touched=" << b_last.rows_touched
                  << " filtered=" << b_last.filtered_rows
                  << " groups=" << b_last.groups << "\n";
        std::cout << "OrderC_num_agg_then_sub_join avg_ms=" << (c_sum / kRuns)
                  << " touched=" << c_last.rows_touched
                  << " filtered=" << c_last.filtered_rows
                  << " groups=" << c_last.groups << "\n";

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }
}
