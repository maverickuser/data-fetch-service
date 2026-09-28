GO ?= go
GOFMT ?= gofmt
STATICCHECK ?= staticcheck
ERRCHECK ?= errcheck
.PHONY: fmt lint test-unit coverage-check build configcheck check check-docs check-contract
fmt:
	$(GO) fmt ./...
lint:
	test -z "$$($(GOFMT) -l internal cmd)"
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
configcheck:
	$(GO) run ./cmd/configcheck -config config/events.yaml
check-docs:
	python3 scripts/check_docs.py
check: lint test-unit coverage-check build configcheck check-docs check-contract

.PHONY: tools
tools:
	$(GO) install honnef.co/go/tools/cmd/staticcheck@v0.8.1
	$(GO) install github.com/kisielk/errcheck@v1.20.0

.PHONY: check-contract
check-contract:
	ruby scripts/check_contract.rb
