# GenDB TPC-H Makefile

CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -pthread
LDFLAGS = -pthread

# Source files
STORAGE_SRCS = storage/storage.cpp
QUERY_SRCS = queries/q1.cpp queries/q3.cpp queries/q6.cpp
INGEST_SRCS = ingest.cpp
MAIN_SRCS = main.cpp

# Object files
STORAGE_OBJS = $(STORAGE_SRCS:.cpp=.o)
QUERY_OBJS = $(QUERY_SRCS:.cpp=.o)
INGEST_OBJS = $(INGEST_SRCS:.cpp=.o)
MAIN_OBJS = $(MAIN_SRCS:.cpp=.o)

# Targets
.PHONY: all clean run-ingest run-main

all: ingest main

ingest: $(INGEST_OBJS) $(STORAGE_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

main: $(MAIN_OBJS) $(STORAGE_OBJS) $(QUERY_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f ingest main $(STORAGE_OBJS) $(QUERY_OBJS) $(INGEST_OBJS) $(MAIN_OBJS)

# Convenience targets for testing
run-ingest:
	./ingest ../../../benchmarks/tpc-h/data/sf10 ../../../benchmarks/tpc-h/gendb/tpch_sf10.gendb

run-main:
	./main ../../../benchmarks/tpc-h/gendb/tpch_sf10.gendb
