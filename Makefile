GO ?= go
HELM ?= helm
PYTHON ?= python3
VERSION := $(shell cat VERSION)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: build fmt lint test test/race test/integration audit container-test chart-test chart-package release-check
build:
	mkdir -p dist
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/accessrelay ./cmd/accessrelay
fmt:
	gofmt -w cmd internal tests/integration
lint:
	@test -z "$$(gofmt -l cmd internal tests/integration)" || (gofmt -l cmd internal tests/integration; exit 1)
	golangci-lint run --build-tags=integration
test:
	$(GO) test -count=1 ./...
test/race:
	$(GO) test -race -count=1 ./...
test/integration: build
	ACCESSRELAY_BINARY=$(CURDIR)/dist/accessrelay $(GO) test -race -count=1 -tags=integration ./tests/integration
audit: lint test test/race test/integration
	$(GO) mod verify
container-test:
	./scripts/container_smoke.sh
chart-test: build
	$(HELM) lint --strict charts/accessrelay
	HELM=$(HELM) $(PYTHON) tests/helm/test_chart.py
chart-package: chart-test
	mkdir -p dist/charts
	$(HELM) package charts/accessrelay --destination dist/charts
release-check:
	./scripts/release_version.sh
	goreleaser check
