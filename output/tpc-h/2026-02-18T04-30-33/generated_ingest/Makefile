CXX = g++
CXXFLAGS = -std=c++17 -Wall -O2
LDFLAGS = -lpthread

CXXFLAGS_BUILD = -std=c++17 -Wall -O3 -march=native -fopenmp
LDFLAGS_BUILD = -lpthread -fopenmp

TARGETS = ingest build_indexes

.PHONY: all clean

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS) -o ingest ingest.cpp $(LDFLAGS)

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS_BUILD) -o build_indexes build_indexes.cpp $(LDFLAGS_BUILD)

clean:
	rm -f $(TARGETS) *.o

.PHONY: test
test: all
	@echo "Built all targets successfully"
