CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -I. -pthread
LDFLAGS = -pthread -ltbb

# Object files
STORAGE_OBJ = storage/storage.o

# Default target (only ingest for now; query targets added later)
all: ingest main

# Ingest program
ingest: ingest.o $(STORAGE_OBJ)
	$(CXX) $(CXXFLAGS) -o ingest ingest.o $(STORAGE_OBJ) $(LDFLAGS)

# Object file rules
ingest.o: ingest.cpp storage/storage.h utils/date_utils.h index/index.h
	$(CXX) $(CXXFLAGS) -c ingest.cpp -o ingest.o

storage/storage.o: storage/storage.cpp storage/storage.h
	$(CXX) $(CXXFLAGS) -c storage/storage.cpp -o storage/storage.o

# Clean
clean:
	rm -f ingest *.o storage/*.o queries/*.o

.PHONY: all clean

main: main.cpp storage/storage.cpp $(wildcard queries/*.cpp) $(wildcard operators/*.h)
	$(CXX) $(CXXFLAGS) -o main main.cpp storage/storage.cpp
