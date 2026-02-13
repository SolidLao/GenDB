#ifndef QUERIES_H
#define QUERIES_H

#include <string>

// Query execution functions
// Each query loads only its needed columns via mmap
// Optional results_dir: if provided, write results to CSV in that directory
// All queries print row count and timing to stdout

void execute_q1(const std::string& gendb_dir, const std::string& results_dir = "");
void execute_q3(const std::string& gendb_dir, const std::string& results_dir = "");
void execute_q6(const std::string& gendb_dir, const std::string& results_dir = "");

#endif // QUERIES_H
