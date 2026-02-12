#pragma once

#include <string>

namespace gendb {

// Query execution functions
// Each query loads needed columns via mmap, executes, and prints results
// If results_dir is non-empty, also writes results to CSV

void execute_q1(const std::string& gendb_dir, const std::string& results_dir = "");
void execute_q3(const std::string& gendb_dir, const std::string& results_dir = "");
void execute_q6(const std::string& gendb_dir, const std::string& results_dir = "");

} // namespace gendb
