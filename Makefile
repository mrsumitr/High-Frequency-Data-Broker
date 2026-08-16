CXX := clang++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -pthread

SRC := src/main.cpp
BIN := broker

.PHONY: all clean run

all: $(BIN)

$(BIN): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(BIN) $(SRC)

clean:
	rm -f $(BIN)

run: all
	./$(BIN)
