CXX = g++
CXXFLAGS_INGEST = -O2 -std=c++17 -Wall -lpthread
CXXFLAGS_INDEX  = -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS_INGEST) -o ingest ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS_INDEX) -o build_indexes build_indexes.cpp

clean:
	rm -f ingest build_indexes
