CXX := clang++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -pthread

SRC := src/main.cpp
HEADERS := $(wildcard src/*.hpp)
BIN := broker

.PHONY: all clean run

all: $(BIN)

$(BIN): $(SRC) $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $(BIN) $(SRC)

clean:
	rm -f $(BIN)

run: all
	./$(BIN)
