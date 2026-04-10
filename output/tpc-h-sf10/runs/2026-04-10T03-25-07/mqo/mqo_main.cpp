// MQO Fused Execution Engine — TPC-H SF10 (22 queries)
// Single-binary, fully self-contained. No external utility headers.
//
// Usage:
//   ./mqo --gendb-dir <path> --output-dir <path> --all
//   ./mqo --gendb-dir <path> --output-dir <path> --query Q1 --query Q6
//   ./mqo --list
//
// The --gendb-dir points to the dataset root (e.g., output/tpc-h-sf10).
// The --output-dir is where Q1.csv..Q22.csv and profile.json are written.

#include "stages/common.hpp"
#include "stages/load_data.hpp"
#include "stages/fused_lineitem.hpp"
#include "stages/fused_orders.hpp"
#include "stages/index_queries.hpp"
#include "stages/finalize.hpp"

int main(int argc, char** argv) {
    std::string gendb_dir, output_dir;
    uint32_t active = 0;
    bool list_mode = false;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--all") {
            active = ALL_Q;
        } else if (a == "--list") {
            list_mode = true;
        } else if (a == "--gendb-dir" && i + 1 < argc) {
            gendb_dir = argv[++i];
        } else if (a == "--output-dir" && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (a == "--query" && i + 1 < argc) {
            std::string q = argv[++i];
            if (q.size() >= 2 && (q[0] == 'Q' || q[0] == 'q')) {
                int n = std::atoi(q.c_str() + 1);
                if (n >= 1 && n <= 22) active |= (1u << (n - 1));
                else fprintf(stderr, "Warning: unknown query '%s'\n", q.c_str());
            } else {
                fprintf(stderr, "Warning: unknown query '%s'\n", q.c_str());
            }
        }
    }

    if (list_mode) {
        printf("Supported queries: Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22\n");
        return 0;
    }

    if (!active) {
        fprintf(stderr, "No queries selected. Use --all or --query Q<n>\n");
        return 1;
    }
    if (gendb_dir.empty() || output_dir.empty()) {
        fprintf(stderr, "Usage: mqo --gendb-dir <dir> --output-dir <dir> [--all | --query Q<n>...]\n");
        return 1;
    }

    // Print active queries
    printf("[MQO] Active queries:");
    for (int i = 0; i < 22; i++)
        if (active & (1u << i)) printf(" Q%d", i + 1);
    printf("  (bitmask=0x%08x)\n", active);

    Ctx ctx;
    ctx.sd = gendb_dir + "/storage";
    ctx.id = gendb_dir + "/storage/indexes";
    ctx.od = output_dir;

    Res res;

    // Stage 1: Load all data (mmap columns + indexes + dicts)
    load_data(ctx, active);

    // Stage 2: Fused lineitem scan (10 consumer branches)
    fused_scan_lineitem(ctx, res, active);

    // Stage 3: Fused orders scan (Q4, Q13)
    fused_scan_orders(ctx, res, active);

    // Stage 4: Index-driven queries (Q2,Q8,Q9,Q11,Q16,Q17,Q19,Q20,Q21,Q22)
    run_index_queries(ctx, res, active);

    // Stage 5: Post-scan dependent (Q15_final, Q18_main)
    run_post_scan(ctx, res, active);

    // Stage 6: Finalize — sort, limit, CSV output
    finalize_output(ctx, res, active);

    MQO_PROFILE_FLUSH(output_dir + "/profile.json");
    printf("[MQO] All done.\n");
    return 0;
}
