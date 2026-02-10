CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -Wextra
TARGET = main
OBJDIR = obj

# Source files
SOURCES = main.cpp \
          storage/storage.cpp \
          queries/q1.cpp \
          queries/q3.cpp \
          queries/q6.cpp

# Object files
OBJECTS = $(SOURCES:%.cpp=$(OBJDIR)/%.o)

# Create necessary directories
$(shell mkdir -p $(OBJDIR)/storage $(OBJDIR)/queries)

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(OBJDIR)/%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJDIR) $(TARGET)

run: $(TARGET)
	./$(TARGET) $(DATA_DIR)
