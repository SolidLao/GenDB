CXX = g++
CXXFLAGS_INGEST = -O2 -std=c++17 -Wall -lpthread
CXXFLAGS_INDEXES = -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

TARGETS = ingest build_indexes

.PHONY: all clean

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS_INGEST) -o ingest ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS_INDEXES) -o build_indexes build_indexes.cpp

clean:
	rm -f $(TARGETS)
