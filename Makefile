GOLINT := golangci-lint

.PHONY: build test

build:
	go build -o conduit-connector-firebolt cmd/firebolt/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	$(GOLINT) run --timeout=5m -c .golangci.yml

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source source/iterator/snapshot.go -destination source/iterator/mock/snapshot.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
