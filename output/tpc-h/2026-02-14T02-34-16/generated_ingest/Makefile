CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -march=native -pthread
LDFLAGS = -lpthread

TARGETS = ingest build_indexes

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS) -o build_indexes build_indexes.cpp $(LDFLAGS)

clean:
	rm -f $(TARGETS) *.o

.PHONY: all clean
