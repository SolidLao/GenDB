CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -fPIC
LDFLAGS = -lpthread

# Compiler flags for high performance
PERF_FLAGS = -O3 -march=native -flto -fno-exceptions

TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) $(PERF_FLAGS) -o ingest ingest.cpp $(LDFLAGS)
	@echo "Built: ingest"

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) $(PERF_FLAGS) -o build_indexes build_indexes.cpp $(LDFLAGS)
	@echo "Built: build_indexes"

clean:
	rm -f $(TARGETS)
	@echo "Cleaned"

.PHONY: all clean
