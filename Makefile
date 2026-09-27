GO ?= go
GOFMT ?= gofmt
STATICCHECK ?= staticcheck
ERRCHECK ?= errcheck
.PHONY: fmt lint test-unit coverage-check build check check-docs
fmt:
	$(GO) fmt ./...
lint:
	test -z "$$($(GOFMT) -l internal)"
	$(GO) vet ./...
	$(STATICCHECK) ./...
	$(ERRCHECK) ./...
test-unit:
	mkdir -p .reports
	$(GO) test -race -covermode=atomic -coverpkg=./... -coverprofile=.reports/unit.cover ./...
coverage-check:
	python3 scripts/test_coverage.py
	python3 scripts/coverage.py .reports/unit.cover
build:
	$(GO) build ./...
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) build ./...
check-docs:
	python3 scripts/check_docs.py
check: lint test-unit coverage-check build check-docs check-contract

.PHONY: tools
tools:
	$(GO) install honnef.co/go/tools/cmd/staticcheck@v0.8.1
	$(GO) install github.com/kisielk/errcheck@v1.20.0

.PHONY: check-contract
check-contract:
	ruby scripts/check_contract.rb
