// Q22: Global Sales Opportunity — MQO tail (standalone, no shared deps)
#include "mqo_profile.hpp"
#include "../shared/mqo_io.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <omp.h>

namespace mqo::tails {

// Phone country codes to match — stored as uint16_t for fast comparison
// '13'=0x3331, '17'=0x3731, '18'=0x3831, '23'=0x3332, '29'=0x3932, '30'=0x3033, '31'=0x3133
static inline uint16_t encode_cc(char a, char b) {
    return static_cast<uint16_t>(static_cast<uint8_t>(a)) |
           (static_cast<uint16_t>(static_cast<uint8_t>(b)) << 8);
}

// Map a 2-char country code to bucket index 0..6 (sorted order), or -1 if not in set
// Sorted codes: 13, 17, 18, 23, 29, 30, 31
static inline int cc_to_bucket(uint16_t cc) {
    static const uint16_t codes[7] = {
        encode_cc('1','3'), encode_cc('1','7'), encode_cc('1','8'),
        encode_cc('2','3'), encode_cc('2','9'), encode_cc('3','0'), encode_cc('3','1')
    };
    // Linear search over 7 elements — fits in registers, branchless-friendly
    for (int i = 0; i < 7; ++i) {
        if (cc == codes[i]) return i;
    }
    return -1;
}

static const char* BUCKET_LABELS[7] = {"13", "17", "18", "23", "29", "30", "31"};

void run_Q22(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q22_tail");

    const std::string storage = ctx.gendb_dir;
    const std::string cust_dir = storage + "/customer";
    const std::string ord_dir  = storage + "/orders";

    // Read row counts
    const size_t n_cust = mqo::io::read_row_count(cust_dir + "/metadata.txt");
    const size_t n_ord  = mqo::io::read_row_count(ord_dir + "/metadata.txt");

    // mmap columns
    const int32_t* o_custkey;
    const int32_t* c_custkey;
    const double*  c_acctbal;
    mqo::io::VarlenAccessor c_phone;

    {
        MQO_TIME_PHASE("Q22_data_loading");
        o_custkey = mqo::io::mmap_column<int32_t>(ord_dir + "/o_custkey.bin", n_ord);
        c_custkey = mqo::io::mmap_column<int32_t>(cust_dir + "/c_custkey.bin", n_cust);
        c_acctbal = mqo::io::mmap_column<double>(cust_dir + "/c_acctbal.bin", n_cust);
        c_phone   = mqo::io::mmap_varlen(cust_dir, "c_phone");
    }

    // Step 1: Build bitset of custkeys that have orders
    // Max custkey = 1500000 for SF10
    constexpr size_t MAX_CUSTKEY = 1500001;
    constexpr size_t BITSET_WORDS = (MAX_CUSTKEY + 63) / 64;

    // Use atomic bitset for parallel build
    std::vector<std::atomic<uint64_t>> bitset_atomic(BITSET_WORDS);
    for (size_t i = 0; i < BITSET_WORDS; ++i) bitset_atomic[i].store(0, std::memory_order_relaxed);

    {
        MQO_TIME_PHASE("Q22_build_bitset");
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < n_ord; ++i) {
            uint32_t ck = static_cast<uint32_t>(o_custkey[i]);
            if (ck < MAX_CUSTKEY) {
                uint64_t bit = uint64_t(1) << (ck & 63);
                bitset_atomic[ck >> 6].fetch_or(bit, std::memory_order_relaxed);
            }
        }
    }

    // Copy to non-atomic for fast reads in subsequent steps
    std::vector<uint64_t> bitset(BITSET_WORDS);
    for (size_t i = 0; i < BITSET_WORDS; ++i) bitset[i] = bitset_atomic[i].load(std::memory_order_relaxed);
    // Free atomic memory
    { std::vector<std::atomic<uint64_t>>().swap(bitset_atomic); }

    auto has_order = [&](int32_t ck) -> bool {
        uint32_t k = static_cast<uint32_t>(ck);
        if (k >= MAX_CUSTKEY) return false;
        return (bitset[k >> 6] >> (k & 63)) & 1;
    };

    // Step 2: Compute AVG(c_acctbal) for customers with matching phone prefix and c_acctbal > 0
    double avg_acctbal;
    {
        MQO_TIME_PHASE("Q22_avg_subquery");
        int max_threads = omp_get_max_threads();
        std::vector<double> local_sum(max_threads, 0.0);
        std::vector<int64_t> local_cnt(max_threads, 0);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double my_sum = 0.0;
            int64_t my_cnt = 0;

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_cust; ++i) {
                double bal = c_acctbal[i];
                if (bal <= 0.0) continue;

                // Extract first 2 chars of phone
                auto phone = c_phone.get(i);
                if (phone.size() < 2) continue;
                uint16_t cc;
                std::memcpy(&cc, phone.data(), 2);
                if (cc_to_bucket(cc) < 0) continue;

                my_sum += bal;
                my_cnt++;
            }
            local_sum[tid] = my_sum;
            local_cnt[tid] = my_cnt;
        }

        double total_sum = 0.0;
        int64_t total_cnt = 0;
        for (int t = 0; t < max_threads; ++t) {
            total_sum += local_sum[t];
            total_cnt += local_cnt[t];
        }
        avg_acctbal = (total_cnt > 0) ? (total_sum / total_cnt) : 0.0;
    }

    // Step 3: Second pass — filter + anti-join + aggregate by cntrycode
    struct Bucket {
        int64_t count;
        double sum;
    };

    int max_threads = omp_get_max_threads();
    // Per-thread 7-bucket arrays
    std::vector<std::vector<Bucket>> thread_buckets(max_threads, std::vector<Bucket>(7, {0, 0.0}));

    {
        MQO_TIME_PHASE("Q22_main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& my_buckets = thread_buckets[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_cust; ++i) {
                // Filter 1: phone prefix IN set (eliminates ~72%)
                auto phone = c_phone.get(i);
                if (phone.size() < 2) continue;
                uint16_t cc;
                std::memcpy(&cc, phone.data(), 2);
                int bucket = cc_to_bucket(cc);
                if (bucket < 0) continue;

                // Filter 2: c_acctbal > avg (eliminates ~50%)
                double bal = c_acctbal[i];
                if (bal <= avg_acctbal) continue;

                // Filter 3: NOT EXISTS in orders (anti-join via bitset)
                int32_t ck = c_custkey[i];
                if (has_order(ck)) continue;

                // Aggregate
                my_buckets[bucket].count++;
                my_buckets[bucket].sum += bal;
            }
        }
    }

    // Step 4: Merge and output
    {
        MQO_TIME_PHASE("Q22_output");

        // Merge thread-local buckets
        Bucket final_buckets[7] = {};
        for (int t = 0; t < max_threads; ++t) {
            for (int b = 0; b < 7; ++b) {
                final_buckets[b].count += thread_buckets[t][b].count;
                final_buckets[b].sum   += thread_buckets[t][b].sum;
            }
        }

        // Write CSV (already sorted by bucket index = sorted by cntrycode)
        std::string out_path = ctx.output_dir + "/q22.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[Q22] Cannot open output: %s\n", out_path.c_str());
            return;
        }
        std::fprintf(fp, "cntrycode,numcust,totacctbal\n");
        for (int b = 0; b < 7; ++b) {
            if (final_buckets[b].count > 0) {
                std::fprintf(fp, "%s,%ld,%.2f\n",
                             BUCKET_LABELS[b],
                             static_cast<long>(final_buckets[b].count),
                             final_buckets[b].sum);
            }
        }
        std::fclose(fp);
    }
}

}  // namespace mqo::tails
