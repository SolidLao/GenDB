CXX = g++
CXXFLAGS_INGEST = -O2 -std=c++17 -Wall -lpthread
CXXFLAGS_INDEX = -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS_INGEST) -o ingest ingest.cpp -lstdc++fs

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS_INDEX) -o build_indexes build_indexes.cpp -lstdc++fs

clean:
	rm -f $(TARGETS)

.PHONY: all clean
