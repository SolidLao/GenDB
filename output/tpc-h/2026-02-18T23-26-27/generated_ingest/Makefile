CXX = g++

# Compiler flags for ingest.cpp (multi-threaded)
INGEST_FLAGS = -O2 -std=c++17 -Wall -lpthread

# Compiler flags for build_indexes.cpp (high optimization, SIMD, OpenMP)
INDEX_FLAGS = -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

# All targets
all: ingest build_indexes

# Build ingest program
ingest: ingest.cpp
	$(CXX) $(INGEST_FLAGS) -o ingest ingest.cpp

# Build index building program
build_indexes: build_indexes.cpp
	$(CXX) $(INDEX_FLAGS) -o build_indexes build_indexes.cpp

# Clean
clean:
	rm -f ingest build_indexes

.PHONY: all clean
