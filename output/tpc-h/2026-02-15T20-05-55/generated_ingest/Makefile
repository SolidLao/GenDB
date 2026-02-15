CXX := g++
CXXFLAGS_INGEST := -O2 -std=c++17 -Wall -lpthread
CXXFLAGS_INDEX := -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

TARGETS := ingest build_indexes

.PHONY: all clean

all: $(TARGETS)

ingest: ingest.cpp
	$(CXX) $(CXXFLAGS_INGEST) -o ingest ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(CXXFLAGS_INDEX) -o build_indexes build_indexes.cpp

clean:
	rm -f $(TARGETS)

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make all          - Build all targets (ingest and build_indexes)"
	@echo "  make ingest       - Build ingestion binary"
	@echo "  make build_indexes - Build index builder binary"
	@echo "  make clean        - Remove all built binaries"
