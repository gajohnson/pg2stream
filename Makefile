.PHONY: build
build:
	go build ./cmd/pg2stream

.PHONY: fmt
fmt:
	go fmt cmd
