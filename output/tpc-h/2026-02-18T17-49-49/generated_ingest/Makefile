CXX = g++
INGEST_FLAGS = -O2 -std=c++17 -Wall -lpthread
BUILD_FLAGS = -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp

.PHONY: all clean

all: ingest build_indexes

ingest: ingest.cpp
	$(CXX) $(INGEST_FLAGS) -o ingest ingest.cpp

build_indexes: build_indexes.cpp
	$(CXX) $(BUILD_FLAGS) -o build_indexes build_indexes.cpp

clean:
	rm -f ingest build_indexes
