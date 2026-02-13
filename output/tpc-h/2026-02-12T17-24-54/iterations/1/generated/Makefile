CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -pthread -I.
LDFLAGS = -pthread

# Source files
STORAGE_SRC = storage/storage.cpp
QUERY_SRC = queries/q1.cpp queries/q3.cpp queries/q6.cpp
INGEST_SRC = ingest.cpp
MAIN_SRC = main.cpp

# Object files
STORAGE_OBJ = $(STORAGE_SRC:.cpp=.o)
QUERY_OBJ = $(QUERY_SRC:.cpp=.o)
INGEST_OBJ = $(INGEST_SRC:.cpp=.o)
MAIN_OBJ = $(MAIN_SRC:.cpp=.o)

# Targets
all: ingest main

ingest: $(INGEST_OBJ) $(STORAGE_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

main: $(MAIN_OBJ) $(QUERY_OBJ) $(STORAGE_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f ingest main $(STORAGE_OBJ) $(QUERY_OBJ) $(INGEST_OBJ) $(MAIN_OBJ)

.PHONY: all clean
