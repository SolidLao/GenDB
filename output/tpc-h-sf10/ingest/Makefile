CXX = g++
CXXFLAGS = -O3 -std=c++17 -pthread -march=native
LDFLAGS = -pthread

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f ingest build_indexes
