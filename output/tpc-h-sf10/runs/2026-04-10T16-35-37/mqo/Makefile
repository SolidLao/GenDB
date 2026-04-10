CXX      = g++
CXXFLAGS = -O3 -march=native -std=c++17 -Wall -fopenmp -DGENDB_PROFILE -I.
TARGET   = mqo
SRC      = mqo_main.cpp

all: $(TARGET)

$(TARGET): $(SRC) mqo_profile.hpp
	$(CXX) $(CXXFLAGS) -o $@ $(SRC)

clean:
	rm -f $(TARGET)

.PHONY: all clean
