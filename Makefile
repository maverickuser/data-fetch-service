GO ?= go
GOFMT ?= gofmt
TERRAFORM ?= terraform
STATICCHECK ?= staticcheck
ERRCHECK ?= errcheck
.PHONY: fmt lint test-unit test-integration test-load coverage-check build configcheck check check-docs check-contract check-infra package
fmt:
	$(GO) fmt ./...
lint:
	test -z "$$($(GOFMT) -l internal cmd)"
	$(GO) vet -tags=integration ./...
	$(GO) vet -tags=aws ./internal/awsverify/...
	$(STATICCHECK) -tags=integration ./...
	$(ERRCHECK) -tags=integration ./...
test-unit:
	mkdir -p .reports
	$(GO) test -race -covermode=atomic -coverpkg=./... -coverprofile=.reports/unit.cover ./...
coverage-check:
	python3 scripts/test_coverage.py
	python3 scripts/coverage.py .reports/unit.cover
test-integration:
	$(GO) test -race -tags=integration ./internal/state -run '^(TestSDK|TestStateComposition)'
	$(GO) test -race -tags=integration ./internal/api -run '^TestHTTPComposition'
	$(GO) test -race -tags=integration ./internal/acquisition -run '^TestAcquisitionSDKComposition'
	$(GO) test -race -tags=integration ./internal/delivery -run '^TestDeliveryHTTPComposition'
test-load:
	$(GO) test -tags=load -run '^TestLoad' -count=1 -v -timeout=15m ./internal/acquisition
build:
	$(GO) build ./...
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) build ./...
configcheck:
	$(GO) run ./cmd/configcheck -config config/events.yaml
check-docs:
	python3 scripts/check_docs.py
package:
	GO="$(GO)" scripts/package.sh dist
check-infra:
	$(TERRAFORM) fmt -check -recursive infra
	$(TERRAFORM) -chdir=infra/bootstrap init -backend=false -input=false -lockfile=readonly
	$(TERRAFORM) -chdir=infra/bootstrap validate
	$(TERRAFORM) -chdir=infra/bootstrap test
	$(TERRAFORM) -chdir=infra/service init -backend=false -input=false -lockfile=readonly
	$(TERRAFORM) -chdir=infra/service validate
	$(TERRAFORM) -chdir=infra/service test
check: lint test-unit coverage-check test-integration build configcheck check-docs check-contract check-infra

.PHONY: tools
tools:
	$(GO) install honnef.co/go/tools/cmd/staticcheck@v0.8.1
	$(GO) install github.com/kisielk/errcheck@v1.20.0

.PHONY: check-contract
check-contract:
	ruby scripts/check_contract.rb
