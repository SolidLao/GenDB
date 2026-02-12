CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread -I.
LDFLAGS = -pthread

# Source files
STORAGE_SRC = storage/storage.cpp
QUERY_SRC = queries/q1.cpp queries/q3.cpp queries/q6.cpp

# Object files
STORAGE_OBJ = $(STORAGE_SRC:.cpp=.o)
QUERY_OBJ = $(QUERY_SRC:.cpp=.o)

# Targets
all: ingest main

ingest: ingest.o $(STORAGE_OBJ)
	$(CXX) $(LDFLAGS) -o $@ $^

main: main.o $(STORAGE_OBJ) $(QUERY_OBJ)
	$(CXX) $(LDFLAGS) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f ingest main *.o storage/*.o queries/*.o

run-ingest: ingest
	./ingest $(DATA_DIR) $(GENDB_DIR)

run-main: main
	./main $(GENDB_DIR) $(RESULTS_DIR)

.PHONY: all clean run-ingest run-main
