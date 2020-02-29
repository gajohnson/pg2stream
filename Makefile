DBNAME ?= test

.PHONY: build
build:
	go build .

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: test-sql
test-sql:
	psql -f test.sql $(DBNAME)
