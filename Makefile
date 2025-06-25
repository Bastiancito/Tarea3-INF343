BINARY=node

.PHONY: build run clean

build:
	go build -o bin/$(BINARY) ./cmd/node

run:
	go run ./cmd/node

clean:
	rm -rf bin