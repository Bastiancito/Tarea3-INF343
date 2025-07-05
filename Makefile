# Makefile

BINARY     := node
CMD        := ./cmd/node
BIN_DIR    := bin

SELF_ID    ?= 1
CONFIG     ?= conf$(SELF_ID).yaml
TRANSPORT  ?= rpc

.PHONY: all build run run1 run2 run3 clean

all: build

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY) $(CMD)


run: build
	$(BIN_DIR)/$(BINARY) \
	  --self_id $(SELF_ID) \
	  --config  $(CONFIG) \
	  --transport $(TRANSPORT)

run1:
	$(MAKE) run SELF_ID=1 CONFIG=conf1.yaml

run2:
	$(MAKE) run SELF_ID=2 CONFIG=conf2.yaml

run3:
	$(MAKE) run SELF_ID=3 CONFIG=conf3.yaml

clean:
	rm -rf $(BIN_DIR)
