#pragma once

#include <string>

namespace gendb {
namespace queries {

// Query functions accept gendb_dir and optional results_dir
// If results_dir is non-empty, write CSV output to <results_dir>/Q<N>.csv
// Always print row count and timing to stdout

void run_q1(const std::string& gendb_dir, const std::string& results_dir = "");
void run_q3(const std::string& gendb_dir, const std::string& results_dir = "");
void run_q6(const std::string& gendb_dir, const std::string& results_dir = "");

} // namespace queries
} // namespace gendb
