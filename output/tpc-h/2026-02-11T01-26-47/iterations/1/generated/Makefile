# GenDB Makefile

CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -Wextra -pthread
LDFLAGS = -pthread

# Source files
STORAGE_SRC = storage/storage.cpp
QUERY_SRC = queries/q1.cpp queries/q3.cpp queries/q6.cpp
INGEST_SRC = ingest.cpp
MAIN_SRC = main.cpp

# Object files
STORAGE_OBJ = storage/storage.o
QUERY_OBJ = queries/q1.o queries/q3.o queries/q6.o
INGEST_OBJ = ingest.o
MAIN_OBJ = main.o

# Targets
all: ingest main

ingest: $(INGEST_OBJ) $(STORAGE_OBJ)
	$(CXX) $(LDFLAGS) -o $@ $^

main: $(MAIN_OBJ) $(STORAGE_OBJ) $(QUERY_OBJ)
	$(CXX) $(LDFLAGS) -o $@ $^

# Compilation rules
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Dependencies
ingest.o: ingest.cpp storage/storage.h utils/date_utils.h
main.o: main.cpp storage/storage.h queries/queries.h
storage/storage.o: storage/storage.cpp storage/storage.h utils/date_utils.h
queries/q1.o: queries/q1.cpp queries/queries.h storage/storage.h utils/date_utils.h index/index.h operators/scan.h
queries/q3.o: queries/q3.cpp queries/queries.h storage/storage.h utils/date_utils.h index/index.h operators/hash_join.h operators/sort.h
queries/q6.o: queries/q6.cpp queries/queries.h storage/storage.h utils/date_utils.h

# Convenience targets
run-ingest: ingest
	./ingest $(DATA_DIR) $(GENDB_DIR)

run-main: main
	./main $(GENDB_DIR)

clean:
	rm -f ingest main $(INGEST_OBJ) $(MAIN_OBJ) $(STORAGE_OBJ) $(QUERY_OBJ)

.PHONY: all clean run-ingest run-main
