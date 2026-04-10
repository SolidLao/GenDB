CXX      ?= g++
CXXFLAGS ?= -O3 -march=native -std=c++17 -Wall -Wextra -fopenmp -DGENDB_PROFILE -I.
LDFLAGS  ?= -lpthread -fopenmp

all: mqo

mqo: mqo_main.cpp stages/common.hpp stages/load_data.hpp stages/fused_lineitem.hpp stages/fused_orders.hpp stages/index_queries.hpp stages/finalize.hpp mqo_profile.hpp
	$(CXX) $(CXXFLAGS) -o $@ mqo_main.cpp $(LDFLAGS)

clean:
	rm -f mqo

.PHONY: all clean
