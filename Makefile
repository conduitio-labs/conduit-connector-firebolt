GOLINT := golangci-lint

.PHONY: build test

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-firebolt.version=${VERSION}'" -o conduit-connector-firebolt cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	$(GOLINT) run --timeout=5m -c .golangci.yml

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source source/iterator/snapshot.go -destination source/iterator/mock/snapshot.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
