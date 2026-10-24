.PHONY: all proto test-go test-python test-durability test-integration test-benchmarks build demo clean

all: build test-go test-python

proto:
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/raft/raft.proto proto/marketlog/marketlog.proto

build:
	go build -o bin/tickraft-node ./cmd/tickraft-node
	go build -o bin/tickraft-import ./cmd/tickraft-import
	go build -o bin/tickraft-export ./cmd/tickraft-export
	go build -o bin/tickraftctl ./cmd/tickraftctl

test-go:
	go test -v ./tests/raft/... ./tests/property/... ./tests/marketlog/... ./tests/durability/... ./tests/integration/...

test-python:
	python -m pytest -v python/tests

test-durability:
	go test -v ./tests/durability/...

test-integration:
	go test -v ./tests/integration/...

test-benchmarks:
	go test -v ./tests/benchmark/...
	python scripts/verify_benchmarks.py

demo: build
	bash scripts/run_full_demo.sh

clean:
	rm -rf bin/ data/demo_cluster/ data/export/ results/*.png results/*.json
