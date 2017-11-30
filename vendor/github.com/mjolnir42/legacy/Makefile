all: validate

validate:
	@go build ./...
	@go vet .
	@go vet ./cmd/metricsocket-client/
	@go tool vet -shadow .
	@go tool vet -shadow ./cmd/metricsocket-client/
	@golint .
	@golint ./cmd/metricsocket-client/
	@ineffassign .
	@ineffassign ./cmd/metricsocket-client/
