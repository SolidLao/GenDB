CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -I.
LDFLAGS = -lpthread

# Object files
STORAGE_OBJ = storage/storage.o
QUERY_OBJS = queries/q1.o queries/q3.o queries/q6.o

# Targets
all: ingest main

ingest: ingest.o $(STORAGE_OBJ)
	$(CXX) $(CXXFLAGS) -o ingest ingest.o $(STORAGE_OBJ) $(LDFLAGS)

main: main.o $(STORAGE_OBJ) $(QUERY_OBJS)
	$(CXX) $(CXXFLAGS) -o main main.o $(STORAGE_OBJ) $(QUERY_OBJS) $(LDFLAGS)

# Object file rules
ingest.o: ingest.cpp storage/storage.h
	$(CXX) $(CXXFLAGS) -c ingest.cpp

main.o: main.cpp queries/queries.h
	$(CXX) $(CXXFLAGS) -c main.cpp

storage/storage.o: storage/storage.cpp storage/storage.h utils/date_utils.h
	$(CXX) $(CXXFLAGS) -c storage/storage.cpp -o storage/storage.o

queries/q1.o: queries/q1.cpp queries/queries.h storage/storage.h index/index.h utils/date_utils.h
	$(CXX) $(CXXFLAGS) -c queries/q1.cpp -o queries/q1.o

queries/q3.o: queries/q3.cpp queries/queries.h storage/storage.h index/index.h utils/date_utils.h
	$(CXX) $(CXXFLAGS) -c queries/q3.cpp -o queries/q3.o

queries/q6.o: queries/q6.cpp queries/queries.h storage/storage.h utils/date_utils.h
	$(CXX) $(CXXFLAGS) -c queries/q6.cpp -o queries/q6.o

# Utility targets
clean:
	rm -f ingest main *.o storage/*.o queries/*.o

run-ingest:
	./ingest $(DATA_DIR) $(GENDB_DIR)

run-main:
	./main $(GENDB_DIR)

.PHONY: all clean run-ingest run-main
