CXX      = g++
CXXFLAGS = -O3 -march=native -std=c++17 -pthread
TARGETS  = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o build_indexes build_indexes.cpp

clean:
	rm -f $(TARGETS)

.PHONY: all clean
