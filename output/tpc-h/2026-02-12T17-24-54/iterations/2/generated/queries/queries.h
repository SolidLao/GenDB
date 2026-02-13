#pragma once
#include <string>

namespace gendb {

// Query execution functions
// Each takes gendb_dir and optional results_dir for CSV output
// Prints row count + timing to terminal

void execute_q1(const std::string& gendb_dir, const std::string& results_dir = "");
void execute_q3(const std::string& gendb_dir, const std::string& results_dir = "");
void execute_q6(const std::string& gendb_dir, const std::string& results_dir = "");

} // namespace gendb
