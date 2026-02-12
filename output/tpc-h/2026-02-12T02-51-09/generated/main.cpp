#include "queries/queries.h"
#include <cstdio>
#include <string>

using namespace gendb;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <gendb_dir> [results_dir]\n", argv[0]);
        fprintf(stderr, "  gendb_dir: directory containing binary columnar storage\n");
        fprintf(stderr, "  results_dir: (optional) directory to write CSV results\n");
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    printf("Executing TPC-H queries on %s\n", gendb_dir.c_str());
    if (!results_dir.empty()) {
        printf("Results will be written to %s\n", results_dir.c_str());
    }
    printf("\n");

    // Execute Q1
    printf("Executing Q1...\n");
    execute_q1(gendb_dir, results_dir);
    printf("\n");

    // Execute Q3
    printf("Executing Q3...\n");
    execute_q3(gendb_dir, results_dir);
    printf("\n");

    // Execute Q6
    printf("Executing Q6...\n");
    execute_q6(gendb_dir, results_dir);
    printf("\n");

    printf("All queries completed.\n");

    return 0;
}
