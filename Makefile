GOLANG_CI_LINT_VER	:= 	v1.54.2
VERSION				:=	$(shell git describe --tags --dirty --always)

.PHONY:
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-firebolt.version=${VERSION}'" -o conduit-connector-firebolt cmd/connector/main.go

.PHONY:
test:
	go test $(GOTEST_FLAGS) -race ./...

.PHONY: golangci-lint-install
golangci-lint-install:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANG_CI_LINT_VER)

.PHONY: lint
lint: golangci-lint-install
	golangci-lint run -v

.PHONY:
mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source source/iterator/snapshot.go -destination source/iterator/mock/snapshot.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
