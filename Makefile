# vim: set ft=make ffs=unix fenc=utf8:
# vim: set noet ts=4 sw=4 tw=72 list:
#
all: freebsd linux

validate:
	@go build ./...
	@go vet ./cmd/...
	@go vet ./lib/...
	@go tool vet -shadow cmd/cyclone/
	@go tool vet -shadow lib/cyclone/
	@go tool vet -shadow lib/cyclone/cpu/
	@go tool vet -shadow lib/cyclone/disk/
	@go tool vet -shadow lib/cyclone/mem/
	@go tool vet -shadow lib/cyclone/metric/
	@golint ./cmd/...
	@golint ./lib/...
	@ineffassign cmd/cyclone/
	@ineffassign lib/cyclone/
	@ineffassign lib/cyclone/cpu/
	@ineffassign lib/cyclone/disk/
	@ineffassign lib/cyclone/mem/
	@ineffassign lib/cyclone/metric/

freebsd:
	@env GOOS=freebsd GOARCH=amd64 go install -ldflags "-X main.buildtime=`date -u +%Y-%m-%dT%H:%M:%S%z` -X main.githash=`git rev-parse HEAD` -X main.shorthash=`git rev-parse --short HEAD` -X main.builddate=`date -u +%Y%m%d`" ./...

linux:
	@env GOOS=linux GOARCH=amd64 go install -ldflags "-X main.buildtime=`date -u +%Y-%m-%dT%H:%M:%S%z` -X main.githash=`git rev-parse HEAD` -X main.shorthash=`git rev-parse --short HEAD` -X main.builddate=`date -u +%Y%m%d`" ./...
