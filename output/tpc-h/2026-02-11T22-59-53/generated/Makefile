CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -I.
LDFLAGS = -lpthread

STORAGE_SRC = storage/storage.cpp
QUERY_SRC = queries/q1.cpp queries/q3.cpp queries/q6.cpp

INGEST_SRC = ingest.cpp $(STORAGE_SRC)
MAIN_SRC = main.cpp $(STORAGE_SRC) $(QUERY_SRC)

INGEST_OBJ = $(INGEST_SRC:.cpp=.o)
MAIN_OBJ = $(MAIN_SRC:.cpp=.o)

.PHONY: all clean run-ingest run-main

all: ingest main

ingest: $(INGEST_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

main: $(MAIN_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f ingest main $(INGEST_OBJ) $(MAIN_OBJ)

run-ingest: ingest
	./ingest ../data/sf1 ../gendb/tpch_sf1.gendb

run-main: main
	./main ../gendb/tpch_sf1.gendb
