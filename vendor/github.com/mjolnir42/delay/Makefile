# vim: set ft=make ffs=unix fenc=utf8:
# vim: set noet ts=4 sw=4 tw=72 list:
#
all: validate

validate:
	@go build ./...
	@go vet ./...
	@go tool vet -shadow .
	@golint ./...
	@ineffassign .
