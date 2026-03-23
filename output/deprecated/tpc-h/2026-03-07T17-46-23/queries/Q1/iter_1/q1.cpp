// Q1: Pricing Summary Report — GenDB generated code
// Compact direct-array aggregation with morsel-driven parallelism
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <algorithm>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ── Helpers ──────────────────────────────────────────────────────────────────

template<typename T>
static inline const T* mmap_col(const std::string& path, size_t& count) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { std::perror(path.c_str()); std::exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    count = sz / sizeof(T);
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
    madvise(p, sz, MADV_SEQUENTIAL);
    ::close(fd);
    return static_cast<const T*>(p);
}

// ── Accumulator ──────────────────────────────────────────────────────────────

struct Acc {
    long double sum_qty;
    long double sum_base_price;
    long double sum_disc_price;
    long double sum_charge;
    long double sum_discount;
    int64_t count;
};

static constexpr int NUM_GROUPS = 6;

// ── Main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb(argv[1]);
    std::string results(argv[2]);

    GENDB_PHASE("total");

    // ── Data loading (mmap all columns) ──────────────────────────────────────
    size_t N;
    const int32_t*  l_shipdate;
    const uint8_t*  l_returnflag;
    const uint8_t*  l_linestatus;
    const double*   l_quantity;
    const double*   l_extendedprice;
    const double*   l_discount;
    const double*   l_tax;
    {
        GENDB_PHASE("data_loading");
        size_t dummy;
        l_shipdate      = mmap_col<int32_t>(gendb + "/lineitem/l_shipdate.bin", N);
        l_returnflag    = mmap_col<uint8_t>(gendb + "/lineitem/l_returnflag.bin", dummy);
        l_linestatus    = mmap_col<uint8_t>(gendb + "/lineitem/l_linestatus.bin", dummy);
        l_quantity      = mmap_col<double>(gendb + "/lineitem/l_quantity.bin", dummy);
        l_extendedprice = mmap_col<double>(gendb + "/lineitem/l_extendedprice.bin", dummy);
        l_discount      = mmap_col<double>(gendb + "/lineitem/l_discount.bin", dummy);
        l_tax           = mmap_col<double>(gendb + "/lineitem/l_tax.bin", dummy);
    }

    // ── Lookup tables for compact group key ──────────────────────────────────
    // returnflag: 'A'->0, 'N'->1, 'R'->2
    // linestatus: 'F'->0, 'O'->1
    // compact key = rf_idx * 2 + ls_idx  (range 0..5)
    alignas(64) uint8_t rf_map[256] = {};
    alignas(64) uint8_t ls_map[256] = {};
    rf_map['A'] = 0; rf_map['N'] = 1; rf_map['R'] = 2;
    ls_map['F'] = 0; ls_map['O'] = 1;

    // ── Zonemap ──────────────────────────────────────────────────────────────
    // Layout: uint32_t num_blocks, uint32_t block_size, then num_blocks × {int32_t min, int32_t max}
    struct ZoneEntry { int32_t min_val; int32_t max_val; };
    uint32_t zm_num_blocks = 0, zm_block_size = 0;
    const ZoneEntry* zm_entries = nullptr;
    void* zm_map_ptr = nullptr;
    {
        std::string zm_path = gendb + "/indexes/lineitem_shipdate_zonemap.bin";
        int fd = ::open(zm_path.c_str(), O_RDONLY);
        if (fd >= 0) {
            struct stat st; fstat(fd, &st);
            void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            ::close(fd);
            if (p != MAP_FAILED) {
                zm_map_ptr = p;
                const uint8_t* base = static_cast<const uint8_t*>(p);
                zm_num_blocks = *reinterpret_cast<const uint32_t*>(base);
                zm_block_size = *reinterpret_cast<const uint32_t*>(base + 4);
                zm_entries = reinterpret_cast<const ZoneEntry*>(base + 8);
            }
        }
    }

    constexpr int32_t THRESHOLD = 10471; // 1998-09-02

    // ── Parallel scan + aggregate ────────────────────────────────────────────
    int nthreads = omp_get_max_threads();
    // Per-thread accumulators, cache-line padded
    struct alignas(64) PaddedAcc { Acc groups[NUM_GROUPS]; };
    std::vector<PaddedAcc> thread_accs(nthreads);
    for (auto& ta : thread_accs) std::memset(&ta, 0, sizeof(PaddedAcc));

    {
        GENDB_PHASE("main_scan");

        if (zm_num_blocks > 0 && zm_block_size > 0) {
            // Zonemap-guided scan: skip blocks where min > threshold
            #pragma omp parallel
            {
                int tid = omp_get_thread_num();
                Acc* __restrict__ local = thread_accs[tid].groups;

                #pragma omp for schedule(dynamic, 4)
                for (uint32_t b = 0; b < zm_num_blocks; b++) {
                    if (zm_entries[b].min_val > THRESHOLD) continue; // skip block

                    size_t start = (size_t)b * zm_block_size;
                    size_t end = start + zm_block_size;
                    if (end > N) end = N;

                    // If entire block passes filter, skip per-row check
                    bool all_pass = (zm_entries[b].max_val <= THRESHOLD);

                    if (all_pass) {
                        for (size_t i = start; i < end; i++) {
                            uint8_t key = rf_map[l_returnflag[i]] * 2 + ls_map[l_linestatus[i]];
                            Acc& a = local[key];
                            double ep = l_extendedprice[i];
                            double disc = l_discount[i];
                            double disc_price = ep * (1.0 - disc);
                            a.sum_qty        += l_quantity[i];
                            a.sum_base_price += ep;
                            a.sum_disc_price += disc_price;
                            a.sum_charge     += disc_price * (1.0 + l_tax[i]);
                            a.sum_discount   += disc;
                            a.count++;
                        }
                    } else {
                        for (size_t i = start; i < end; i++) {
                            if (l_shipdate[i] > THRESHOLD) continue;
                            uint8_t key = rf_map[l_returnflag[i]] * 2 + ls_map[l_linestatus[i]];
                            Acc& a = local[key];
                            double ep = l_extendedprice[i];
                            double disc = l_discount[i];
                            double disc_price = ep * (1.0 - disc);
                            a.sum_qty        += l_quantity[i];
                            a.sum_base_price += ep;
                            a.sum_disc_price += disc_price;
                            a.sum_charge     += disc_price * (1.0 + l_tax[i]);
                            a.sum_discount   += disc;
                            a.count++;
                        }
                    }
                }
            }
        } else {
            // Fallback: full scan without zonemap
            #pragma omp parallel
            {
                int tid = omp_get_thread_num();
                Acc* __restrict__ local = thread_accs[tid].groups;

                #pragma omp for schedule(static)
                for (size_t i = 0; i < N; i++) {
                    if (l_shipdate[i] > THRESHOLD) continue;
                    uint8_t key = rf_map[l_returnflag[i]] * 2 + ls_map[l_linestatus[i]];
                    Acc& a = local[key];
                    double ep = l_extendedprice[i];
                    double disc = l_discount[i];
                    double disc_price = ep * (1.0 - disc);
                    a.sum_qty        += l_quantity[i];
                    a.sum_base_price += ep;
                    a.sum_disc_price += disc_price;
                    a.sum_charge     += disc_price * (1.0 + l_tax[i]);
                    a.sum_discount   += disc;
                    a.count++;
                }
            }
        }
    }

    // ── Merge thread-local accumulators ──────────────────────────────────────
    Acc final_groups[NUM_GROUPS];
    std::memset(final_groups, 0, sizeof(final_groups));
    for (int t = 0; t < nthreads; t++) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            const Acc& s = thread_accs[t].groups[g];
            Acc& d = final_groups[g];
            d.sum_qty        += s.sum_qty;
            d.sum_base_price += s.sum_base_price;
            d.sum_disc_price += s.sum_disc_price;
            d.sum_charge     += s.sum_charge;
            d.sum_discount   += s.sum_discount;
            d.count          += s.count;
        }
    }

    // ── Output ───────────────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        const char rf_chars[] = {'A', 'N', 'R'};
        const char ls_chars[] = {'F', 'O'};

        // Collect valid groups and sort by (returnflag, linestatus)
        struct Result {
            char rf, ls;
            const Acc* acc;
        };
        std::vector<Result> rows;
        for (int g = 0; g < NUM_GROUPS; g++) {
            if (final_groups[g].count == 0) continue;
            int rf_idx = g / 2;
            int ls_idx = g % 2;
            rows.push_back({rf_chars[rf_idx], ls_chars[ls_idx], &final_groups[g]});
        }
        std::sort(rows.begin(), rows.end(), [](const Result& a, const Result& b) {
            if (a.rf != b.rf) return a.rf < b.rf;
            return a.ls < b.ls;
        });

        std::string out_path = results + "/Q1.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) { std::perror(out_path.c_str()); return 1; }

        std::fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
        for (auto& r : rows) {
            const Acc& a = *r.acc;
            double avg_qty   = (double)(a.sum_qty / a.count);
            double avg_price = (double)(a.sum_base_price / a.count);
            double avg_disc  = (double)(a.sum_discount / a.count);
            std::fprintf(fp, "%c,%c,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%ld\n",
                         r.rf, r.ls,
                         (double)a.sum_qty, (double)a.sum_base_price,
                         (double)a.sum_disc_price, (double)a.sum_charge,
                         avg_qty, avg_price, avg_disc,
                         a.count);
        }
        std::fclose(fp);
    }

    return 0;
}
