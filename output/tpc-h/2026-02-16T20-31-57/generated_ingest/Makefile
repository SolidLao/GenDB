CXX = g++
CXXFLAGS_INGEST = -O2 -std=c++17 -Wall -lpthread -fopenmp
CXXFLAGS_INDEXES = -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS_INGEST) -o $@ $^

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS_INDEXES) -o $@ $^

clean:
	rm -f $(TARGETS)

.PHONY: all clean
