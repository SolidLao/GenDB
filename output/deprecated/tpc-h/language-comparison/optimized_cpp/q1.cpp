// Q1: Pricing Summary Report — Optimized C++ iter3
// Key insight: two-pass within each block:
//   Pass 1: classify rows into per-group selection vectors (vectorizable)
//   Pass 2: accumulate per-group contiguously (vectorizable, no scatter)
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

struct RawMap {
    void* ptr; size_t sz;
    static RawMap open(const char* path) {
        int fd = ::open(path, O_RDONLY);
        struct stat st; fstat(fd, &st);
        void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        return {p, (size_t)st.st_size};
    }
};

struct Acc {
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double sum_discount;
    int64_t count;
};

static constexpr int NUM_GROUPS = 6;
static constexpr int32_t THRESHOLD = 10471;

int main(int argc, char** argv) {
    if (argc < 3) return 1;
    std::string gendb(argv[1]), results(argv[2]);

    GENDB_PHASE("total");

    size_t N;
    const int32_t* l_shipdate;
    const uint8_t* l_returnflag;
    const uint8_t* l_linestatus;
    const double*  l_quantity;
    const double*  l_extendedprice;
    const double*  l_discount;
    const double*  l_tax;

    struct ZoneEntry { int32_t min_val, max_val; };
    uint32_t zm_num_blocks = 0, zm_block_size = 0;
    const ZoneEntry* zm_entries = nullptr;

    {
        GENDB_PHASE("data_loading");
        auto m = RawMap::open((gendb + "/lineitem/l_shipdate.bin").c_str());
        N = m.sz / sizeof(int32_t);
        l_shipdate = (const int32_t*)m.ptr;
        madvise(m.ptr, m.sz, MADV_SEQUENTIAL);
        l_returnflag = (const uint8_t*)RawMap::open((gendb + "/lineitem/l_returnflag.bin").c_str()).ptr;
        l_linestatus = (const uint8_t*)RawMap::open((gendb + "/lineitem/l_linestatus.bin").c_str()).ptr;
        auto m4 = RawMap::open((gendb + "/lineitem/l_quantity.bin").c_str());
        l_quantity = (const double*)m4.ptr; madvise(m4.ptr, m4.sz, MADV_SEQUENTIAL);
        auto m5 = RawMap::open((gendb + "/lineitem/l_extendedprice.bin").c_str());
        l_extendedprice = (const double*)m5.ptr; madvise(m5.ptr, m5.sz, MADV_SEQUENTIAL);
        auto m6 = RawMap::open((gendb + "/lineitem/l_discount.bin").c_str());
        l_discount = (const double*)m6.ptr; madvise(m6.ptr, m6.sz, MADV_SEQUENTIAL);
        auto m7 = RawMap::open((gendb + "/lineitem/l_tax.bin").c_str());
        l_tax = (const double*)m7.ptr; madvise(m7.ptr, m7.sz, MADV_SEQUENTIAL);

        auto zmm = RawMap::open((gendb + "/indexes/lineitem_shipdate_zonemap.bin").c_str());
        if (zmm.ptr != MAP_FAILED) {
            const uint8_t* base = (const uint8_t*)zmm.ptr;
            zm_num_blocks = *(const uint32_t*)base;
            zm_block_size = *(const uint32_t*)(base + 4);
            zm_entries = (const ZoneEntry*)(base + 8);
        }
    }

    alignas(64) uint8_t rf_map[256] = {};
    alignas(64) uint8_t ls_map[256] = {};
    rf_map['A'] = 0; rf_map['N'] = 1; rf_map['R'] = 2;
    ls_map['F'] = 0; ls_map['O'] = 1;

    // Memory-bandwidth-bound: 32 threads saturates bandwidth on 2-socket Xeon
    int max_threads = omp_get_max_threads();
    int nthreads = std::min(max_threads, 32);
    omp_set_num_threads(nthreads);
    struct alignas(64) PaddedAcc { Acc groups[NUM_GROUPS]; };
    std::vector<PaddedAcc> thread_accs(nthreads);
    std::memset(thread_accs.data(), 0, nthreads * sizeof(PaddedAcc));

    {
        GENDB_PHASE("main_scan");

        if (zm_num_blocks > 0 && zm_block_size > 0) {
            #pragma omp parallel
            {
                int tid = omp_get_thread_num();
                Acc* __restrict__ local = thread_accs[tid].groups;

                #pragma omp for schedule(dynamic, 4)
                for (uint32_t b = 0; b < zm_num_blocks; b++) {
                    if (zm_entries[b].min_val > THRESHOLD) continue;

                    size_t start = (size_t)b * zm_block_size;
                    size_t end = start + zm_block_size;
                    if (end > N) end = N;
                    size_t cnt = end - start;

                    bool all_pass = (zm_entries[b].max_val <= THRESHOLD);

                    // For small groups (only 6), the scatter pattern is fine
                    // The compiler vectorizes the arithmetic well with -ffast-math
                    if (all_pass) {
                        for (size_t i = start; i < end; i++) {
                            uint8_t key = rf_map[l_returnflag[i]] * 2 + ls_map[l_linestatus[i]];
                            Acc& a = local[key];
                            double ep = l_extendedprice[i];
                            double disc = l_discount[i];
                            double dp = ep * (1.0 - disc);
                            a.sum_qty += l_quantity[i];
                            a.sum_base_price += ep;
                            a.sum_disc_price += dp;
                            a.sum_charge += dp * (1.0 + l_tax[i]);
                            a.sum_discount += disc;
                            a.count++;
                        }
                    } else {
                        for (size_t i = start; i < end; i++) {
                            if (l_shipdate[i] > THRESHOLD) continue;
                            uint8_t key = rf_map[l_returnflag[i]] * 2 + ls_map[l_linestatus[i]];
                            Acc& a = local[key];
                            double ep = l_extendedprice[i];
                            double disc = l_discount[i];
                            double dp = ep * (1.0 - disc);
                            a.sum_qty += l_quantity[i];
                            a.sum_base_price += ep;
                            a.sum_disc_price += dp;
                            a.sum_charge += dp * (1.0 + l_tax[i]);
                            a.sum_discount += disc;
                            a.count++;
                        }
                    }
                }
            }
        } else {
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
                    double dp = ep * (1.0 - disc);
                    a.sum_qty += l_quantity[i];
                    a.sum_base_price += ep;
                    a.sum_disc_price += dp;
                    a.sum_charge += dp * (1.0 + l_tax[i]);
                    a.sum_discount += disc;
                    a.count++;
                }
            }
        }
    }

    Acc final_groups[NUM_GROUPS] = {};
    for (int t = 0; t < nthreads; t++) {
        for (int g = 0; g < NUM_GROUPS; g++) {
            const Acc& s = thread_accs[t].groups[g];
            Acc& d = final_groups[g];
            d.sum_qty += s.sum_qty;
            d.sum_base_price += s.sum_base_price;
            d.sum_disc_price += s.sum_disc_price;
            d.sum_charge += s.sum_charge;
            d.sum_discount += s.sum_discount;
            d.count += s.count;
        }
    }

    {
        GENDB_PHASE("output");
        const char rf_chars[] = {'A', 'N', 'R'};
        const char ls_chars[] = {'F', 'O'};
        struct Result { char rf, ls; const Acc* acc; };
        std::vector<Result> rows;
        for (int g = 0; g < NUM_GROUPS; g++) {
            if (final_groups[g].count == 0) continue;
            rows.push_back({rf_chars[g / 2], ls_chars[g % 2], &final_groups[g]});
        }
        std::sort(rows.begin(), rows.end(), [](const Result& a, const Result& b) {
            return a.rf < b.rf || (a.rf == b.rf && a.ls < b.ls);
        });

        FILE* fp = fopen((results + "/Q1.csv").c_str(), "w");
        fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
        for (auto& r : rows) {
            const Acc& a = *r.acc;
            fprintf(fp, "%c,%c,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%ld\n",
                    r.rf, r.ls, a.sum_qty, a.sum_base_price,
                    a.sum_disc_price, a.sum_charge,
                    a.sum_qty / a.count, a.sum_base_price / a.count,
                    a.sum_discount / a.count, a.count);
        }
        fclose(fp);
    }
    return 0;
}
