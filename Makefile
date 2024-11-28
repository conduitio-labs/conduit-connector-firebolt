VERSION				:=	$(shell git describe --tags --dirty --always)

.PHONY:
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-firebolt.version=${VERSION}'" -o conduit-connector-firebolt cmd/connector/main.go

.PHONY:
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: lint
lint:
	golangci-lint run -v

.PHONY:
mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source source/iterator/snapshot.go -destination source/iterator/mock/snapshot.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy