CXX = g++
CXXFLAGS = -O3 -march=native -std=c++17 -Wall -fopenmp -DGENDB_PROFILE -I.
TARGET = mqo
SRC = mqo_main.cpp

$(TARGET): $(SRC) mqo_profile.hpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f $(TARGET)

.PHONY: clean
