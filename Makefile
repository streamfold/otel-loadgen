.PHONY: build test deps tidy

build: deps
	mkdir -p dist && \
	  CGO_ENABLED=0 go build -o dist/otel-loadgen main.go

test: deps
	go test -count 1 ./...

deps:
	go mod download

tidy:
	go mod tidy