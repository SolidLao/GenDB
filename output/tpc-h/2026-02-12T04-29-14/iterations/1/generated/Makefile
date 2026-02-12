CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread
LDFLAGS = -lpthread

# Source files
STORAGE_SRC = storage/storage.cpp
QUERY_SRC = queries/q1.cpp queries/q3.cpp queries/q6.cpp
INGEST_SRC = ingest.cpp
MAIN_SRC = main.cpp

# Object files
STORAGE_OBJ = $(STORAGE_SRC:.cpp=.o)
QUERY_OBJ = $(QUERY_SRC:.cpp=.o)

# Targets
all: ingest main

ingest: $(INGEST_SRC) $(STORAGE_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

main: $(MAIN_SRC) $(STORAGE_OBJ) $(QUERY_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Pattern rule for object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f ingest main $(STORAGE_OBJ) $(QUERY_OBJ)

run-ingest: ingest
	./ingest ../../../benchmarks/tpc-h/data/sf10 ../../../benchmarks/tpc-h/gendb/tpch_sf10.gendb

run-main: main
	./main ../../../benchmarks/tpc-h/gendb/tpch_sf10.gendb

.PHONY: all clean run-ingest run-main
