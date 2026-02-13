CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread -I.
LDFLAGS = -pthread

# Source files
STORAGE_SRCS = storage/storage.cpp
QUERY_SRCS = queries/q1.cpp queries/q3.cpp queries/q6.cpp
INGEST_SRCS = ingest.cpp
MAIN_SRCS = main.cpp

# Object files
STORAGE_OBJS = $(STORAGE_SRCS:.cpp=.o)
QUERY_OBJS = $(QUERY_SRCS:.cpp=.o)
INGEST_OBJS = $(INGEST_SRCS:.cpp=.o)
MAIN_OBJS = $(MAIN_SRCS:.cpp=.o)

# Targets
all: ingest main

ingest: $(INGEST_OBJS) $(STORAGE_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

main: $(MAIN_OBJS) $(QUERY_OBJS) $(STORAGE_OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^

# Pattern rule for object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Dependencies
storage/storage.o: storage/storage.cpp storage/storage.h
queries/q1.o: queries/q1.cpp queries/queries.h storage/storage.h operators/hash_agg.h utils/date_utils.h
queries/q3.o: queries/q3.cpp queries/queries.h storage/storage.h operators/hash_join.h operators/hash_agg.h utils/date_utils.h
queries/q6.o: queries/q6.cpp queries/queries.h storage/storage.h utils/date_utils.h
ingest.o: ingest.cpp storage/storage.h utils/date_utils.h
main.o: main.cpp queries/queries.h

clean:
	rm -f $(STORAGE_OBJS) $(QUERY_OBJS) $(INGEST_OBJS) $(MAIN_OBJS) ingest main

.PHONY: all clean
