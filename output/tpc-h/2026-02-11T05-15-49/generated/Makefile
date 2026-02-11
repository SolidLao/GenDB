CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread
INCLUDES = -I.

# Source files
STORAGE_SRC = storage/storage.cpp
QUERY_SRCS = queries/q1.cpp queries/q3.cpp queries/q6.cpp
INGEST_SRC = ingest.cpp
MAIN_SRC = main.cpp

# Object files
STORAGE_OBJ = storage/storage.o
QUERY_OBJS = queries/q1.o queries/q3.o queries/q6.o
INGEST_OBJ = ingest.o
MAIN_OBJ = main.o

# Targets
all: ingest main

ingest: $(INGEST_OBJ) $(STORAGE_OBJ)
	$(CXX) $(CXXFLAGS) -o ingest $(INGEST_OBJ) $(STORAGE_OBJ)

main: $(MAIN_OBJ) $(QUERY_OBJS) $(STORAGE_OBJ)
	$(CXX) $(CXXFLAGS) -o main $(MAIN_OBJ) $(QUERY_OBJS) $(STORAGE_OBJ)

# Object file rules
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Dependencies
$(INGEST_OBJ): $(INGEST_SRC) storage/storage.h utils/date_utils.h
$(MAIN_OBJ): $(MAIN_SRC) queries/queries.h
$(STORAGE_OBJ): $(STORAGE_SRC) storage/storage.h utils/date_utils.h
queries/q1.o: queries/q1.cpp queries/queries.h storage/storage.h index/index.h utils/date_utils.h
queries/q3.o: queries/q3.cpp queries/queries.h storage/storage.h index/index.h utils/date_utils.h
queries/q6.o: queries/q6.cpp queries/queries.h storage/storage.h utils/date_utils.h

clean:
	rm -f ingest main $(INGEST_OBJ) $(MAIN_OBJ) $(STORAGE_OBJ) $(QUERY_OBJS)

.PHONY: all clean
